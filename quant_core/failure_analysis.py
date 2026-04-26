from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from .config import BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE
from .predictor import PROFIT_TARGET_PCT, apply_production_filters
from .daily_pick import list_daily_pick_results
from .storage import connect, init_db
from .strategy_lab import _daily_top, _stats_row, prepare_evaluated_candidates


def analyze_prediction_failures(months: int = 12, refresh: bool = False) -> dict[str, Any]:
    init_db()
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    evaluated = prepared["evaluated"]
    if evaluated.empty:
        return _empty_result(months, prepared["model_status"])

    production_pool = apply_production_filters(evaluated)
    picks = _daily_top(production_pool)
    picks = picks[np.isfinite(picks["open_premium"])].copy()
    if picks.empty:
        return _empty_result(months, "生产策略没有可评估样本")

    picks["success"] = picks["open_premium"] > PROFIT_TARGET_PCT
    failures = picks[~picks["success"]].copy()
    successes = picks[picks["success"]].copy()
    baseline = _stats_row("当前生产策略", "按当前生产规则每日选回归预期溢价最高标的。", picks)

    reason_rows = _failure_reason_rows(failures, successes)
    optimization_rows = _optimization_rows(production_pool, baseline)
    sample_failures = _sample_failure_rows(failures)
    strategy_groups = _strategy_group_rows(picks)
    summary = {
        "months": months,
        "start_date": prepared["start_date"],
        "end_date": prepared["end_date"],
        "trades": int(len(picks)),
        "success_count": int(len(successes)),
        "failure_count": int(len(failures)),
        "baseline": baseline,
        "model_status": prepared["model_status"],
        "repaired_pre_close_count": prepared["repaired_pre_close_count"],
        "repaired_volume_ratio_count": prepared["repaired_volume_ratio_count"],
        "can_optimize_model": True,
        "optimization_note": f"失败原因按开盘溢价>{PROFIT_TARGET_PCT:.2f}%定义成功，可用于两类优化：一是加入规则过滤器减少低质量出手，二是把失败标签/失败原因作为样本权重或辅助标签重训模型。",
    }
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "strategy_groups": strategy_groups,
        "reasons": reason_rows,
        "optimizations": optimization_rows,
        "sample_failures": sample_failures,
    }


def diagnose_dipbuy_failures(months: int = 12, refresh: bool = False) -> dict[str, Any]:
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    evaluated = prepared["evaluated"].copy()
    pick_rows = list_daily_pick_results(limit=10000).get("rows", [])
    picks = pd.DataFrame(pick_rows)
    if evaluated.empty or picks.empty:
        return {"count": 0, "feature_stats": [], "samples": [], "note": "没有可诊断的低吸失败样本"}

    picks["success"] = picks["success"].astype(object)
    dipbuy_failures = picks[(picks["strategy_type"] == "首阴低吸") & (picks["success"] == False)].copy()
    if dipbuy_failures.empty:
        return {"count": 0, "feature_stats": [], "samples": [], "note": "daily_picks 中没有首阴低吸失败记录"}

    evaluated["纯代码"] = evaluated["纯代码"].astype(str).str.zfill(6)
    dipbuy_failures["code"] = dipbuy_failures["code"].astype(str).str.zfill(6)
    joined = dipbuy_failures.merge(
        evaluated,
        left_on=["selection_date", "code"],
        right_on=["date", "纯代码"],
        how="left",
        suffixes=("_pick", ""),
    )
    feature_cols = [
        "近5日最高涨幅",
        "今日急跌度",
        "日内振幅",
        "10日均线乖离率",
        "3日累计涨幅",
        "昨日实体涨跌幅",
        "今日缩量比例",
        "均线趋势斜率",
        "光脚大阴线惩罚度",
        "open_premium",
    ]
    stats = []
    for col in feature_cols:
        values = pd.to_numeric(joined.get(col), errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if values.empty:
            continue
        stats.append(
            {
                "feature": col,
                "mean": round(float(values.mean()), 4),
                "median": round(float(values.median()), 4),
                "p25": round(float(values.quantile(0.25)), 4),
                "p75": round(float(values.quantile(0.75)), 4),
                "min": round(float(values.min()), 4),
                "max": round(float(values.max()), 4),
            }
        )
    samples = []
    sample_cols = ["selection_date", "code", "name", "open_premium", *feature_cols[:-1]]
    available = [col for col in sample_cols if col in joined.columns]
    for _, row in joined.sort_values("open_premium").head(20)[available].iterrows():
        item: dict[str, Any] = {}
        for col in available:
            value = row[col]
            if isinstance(value, (int, float, np.floating)) and pd.notna(value):
                item[col] = round(float(value), 4)
            else:
                item[col] = "" if pd.isna(value) else value
        samples.append(item)
    return {
        "count": int(len(dipbuy_failures)),
        "matched_count": int(joined["纯代码"].notna().sum()) if "纯代码" in joined.columns else 0,
        "feature_stats": stats,
        "samples": samples,
        "diagnosis": _dipbuy_failure_diagnosis(stats),
    }


def diagnose_reversal_quality(months: int = 12, refresh: bool = False) -> dict[str, Any]:
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    evaluated = prepared["evaluated"].copy()
    pick_rows = list_daily_pick_results(limit=10000).get("rows", [])
    picks = pd.DataFrame(pick_rows)
    if evaluated.empty or picks.empty:
        return {"count": 0, "quality_count": 0, "feature_stats": [], "samples": [], "diagnosis": ["没有可诊断的反转样本"]}

    picks["code"] = picks["code"].astype(str).str.zfill(6)
    reversal = picks[picks["strategy_type"].eq("中线超跌反转") & picks["t3_max_gain_pct"].notna()].copy()
    if reversal.empty:
        return {"count": 0, "quality_count": 0, "feature_stats": [], "samples": [], "diagnosis": ["daily_picks 中没有中线超跌反转记录"]}

    reversal["t3_max_gain_pct"] = pd.to_numeric(reversal["t3_max_gain_pct"], errors="coerce")
    low_quality = reversal[reversal["t3_max_gain_pct"] < 2.0].copy()
    failures = reversal[reversal["t3_max_gain_pct"] <= 0].copy()

    evaluated["纯代码"] = evaluated["纯代码"].astype(str).str.zfill(6)
    joined = low_quality.merge(
        evaluated,
        left_on=["selection_date", "code"],
        right_on=["date", "纯代码"],
        how="left",
        suffixes=("_pick", ""),
    )
    if not joined.empty:
        joined = _attach_reversal_pressure_features(joined)

    feature_cols = [
        "换手率",
        "volume_ratio_to_10d",
        "20日均线乖离率",
        "ma30_bias",
        "ma20_slope",
        "ma30_slope",
        "market_avg_change",
        "market_down_count",
        "market_amount",
        "t3_max_gain_pct_pick",
    ]
    stats = _feature_stats(joined, feature_cols)
    samples = _reversal_quality_samples(joined)
    return {
        "count": int(len(reversal)),
        "failure_count": int(len(failures)),
        "quality_count": int(len(low_quality)),
        "matched_count": int(joined["纯代码"].notna().sum()) if "纯代码" in joined.columns else 0,
        "feature_stats": stats,
        "samples": samples,
        "diagnosis": _reversal_quality_diagnosis(joined, stats),
    }


def _failure_reason_rows(failures: pd.DataFrame, successes: pd.DataFrame) -> list[dict[str, Any]]:
    definitions = [
        ("涨幅过高", "当日涨幅>=7%，次日容易低开消化获利盘。", lambda df: df["涨跌幅"] >= 7),
        ("长上影抛压", "上影线比例>=2%，尾盘前已有明显抛压。", lambda df: df["上影线比例"] >= 2),
        ("日内振幅过大", "日内振幅>=8%，博弈分歧大。", lambda df: df["日内振幅"] >= 8),
        ("换手过热", "换手率>=15%，短线资金分歧或兑现压力高。", lambda df: df["换手率"] >= 15),
        ("低价股波动", "股价<3元，容易受低价投机波动影响。", lambda df: df["最新价"] < 3),
        ("预期溢价偏低", "模型预期溢价<=0，综合分可能被其他项拉高。", lambda df: df["预期溢价"] <= 0),
        ("风险分偏低", "风险评分<55，形态或波动风险没有被充分过滤。", lambda df: df["风险评分"] < 55),
        ("流动性偏弱", "流动性评分<45，盘口承接质量偏弱。", lambda df: df["流动性评分"] < 45),
        ("综合分偏低", f"综合评分低于策略独立门槛（突破{BREAKOUT_MIN_SCORE:.1f}/低吸{DIPBUY_MIN_SCORE:.1f}），信号强度不足。", lambda df: df["综合评分"] < df["strategy_type"].map({"首阴低吸": DIPBUY_MIN_SCORE}).fillna(BREAKOUT_MIN_SCORE)),
        ("大盘弱势", "市场平均涨跌幅<=-0.5%或下跌家数>=3500，系统性风险高。", lambda df: (df["market_avg_change"] <= -0.5) | (df["market_down_count"] >= 3500)),
        ("趋势过度偏离", "20日均线乖离率绝对值>=18%，位置过高或过低。", lambda df: df["20日均线乖离率"].abs() >= 18),
        ("缺少主力异动", "10日量比<1.2且3日红盘比例<50%，资金参与不足。", lambda df: (df["10日量比"] < 1.2) & (df["3日红盘比例"] < 50)),
        ("虚拉诱多", "振幅换手比>3、缩量大涨或极端下影线，可能是尾盘轻量资金拉升。", lambda df: (df["振幅换手比"] > 3) | (df["缩量大涨标记"] >= 0.5) | (df["极端下影线标记"] >= 0.5)),
        ("近3日断头铡刀", "过去3个交易日出现过单日跌幅<=-7%，短线结构存在大面记忆。", lambda df: df["近3日断头铡刀标记"] >= 0.5),
    ]
    rows = []
    for name, description, predicate in definitions:
        fail_mask = predicate(failures) if not failures.empty else pd.Series(dtype=bool)
        success_mask = predicate(successes) if not successes.empty else pd.Series(dtype=bool)
        fail_count = int(fail_mask.sum()) if len(fail_mask) else 0
        success_count = int(success_mask.sum()) if len(success_mask) else 0
        fail_rate = fail_count / len(failures) * 100 if len(failures) else 0.0
        success_rate = success_count / len(successes) * 100 if len(successes) else 0.0
        rows.append(
            {
                "reason": name,
                "description": description,
                "failure_count": fail_count,
                "failure_rate": round(float(fail_rate), 4),
                "success_count": success_count,
                "success_rate": round(float(success_rate), 4),
                "lift_vs_success": round(float(fail_rate - success_rate), 4),
            }
        )
    rows.sort(key=lambda row: (row["lift_vs_success"], row["failure_count"]), reverse=True)
    return rows


def _attach_reversal_pressure_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    keys = [(str(row["code"]).zfill(6), str(row["selection_date"])) for _, row in out.iterrows()]
    feature_map = _load_ma_pressure_features(keys)
    for col in ["ma30_bias", "ma20_slope", "ma30_slope"]:
        out[col] = [feature_map.get((code, day), {}).get(col, np.nan) for code, day in keys]
    return out


def _load_ma_pressure_features(keys: list[tuple[str, str]]) -> dict[tuple[str, str], dict[str, float]]:
    if not keys:
        return {}
    codes = sorted({code for code, _ in keys})
    min_date = (pd.to_datetime(min(day for _, day in keys)) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
    max_date = max(day for _, day in keys)
    placeholders = ",".join("?" for _ in codes)
    try:
        with connect() as conn:
            raw = pd.read_sql_query(
                f"""
                SELECT code, date, close
                FROM stock_daily
                WHERE code IN ({placeholders}) AND date >= ? AND date <= ?
                ORDER BY code ASC, date ASC
                """,
                conn,
                params=[*codes, min_date, max_date],
            )
    except Exception:
        return {}
    if raw.empty:
        return {}
    raw["code"] = raw["code"].astype(str).str.zfill(6)
    raw["date"] = raw["date"].astype(str)
    raw["close"] = pd.to_numeric(raw["close"], errors="coerce")
    result: dict[tuple[str, str], dict[str, float]] = {}
    for code, group in raw.dropna(subset=["close"]).groupby("code", sort=False):
        g = group.sort_values("date").copy()
        ma20 = g["close"].rolling(20, min_periods=20).mean()
        ma30 = g["close"].rolling(30, min_periods=30).mean()
        g["ma30_bias"] = (g["close"] / ma30 - 1) * 100
        g["ma20_slope"] = (ma20 / ma20.shift(1) - 1) * 100
        g["ma30_slope"] = (ma30 / ma30.shift(1) - 1) * 100
        for _, row in g.iterrows():
            key = (str(code), str(row["date"]))
            if key in keys:
                result[key] = {
                    "ma30_bias": _safe_float(row.get("ma30_bias")),
                    "ma20_slope": _safe_float(row.get("ma20_slope")),
                    "ma30_slope": _safe_float(row.get("ma30_slope")),
                }
    return result


def _feature_stats(df: pd.DataFrame, feature_cols: list[str]) -> list[dict[str, Any]]:
    rows = []
    for col in feature_cols:
        values = pd.to_numeric(df.get(col), errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
        if values.empty:
            continue
        rows.append(
            {
                "feature": col,
                "mean": round(float(values.mean()), 4),
                "median": round(float(values.median()), 4),
                "p25": round(float(values.quantile(0.25)), 4),
                "p75": round(float(values.quantile(0.75)), 4),
                "min": round(float(values.min()), 4),
                "max": round(float(values.max()), 4),
            }
        )
    return rows


def _reversal_quality_samples(joined: pd.DataFrame) -> list[dict[str, Any]]:
    if joined.empty:
        return []
    sample_cols = [
        "selection_date",
        "code",
        "name",
        "t3_max_gain_pct_pick",
        "换手率",
        "volume_ratio_to_10d",
        "20日均线乖离率",
        "ma30_bias",
        "ma20_slope",
        "ma30_slope",
        "market_avg_change",
        "market_down_count",
    ]
    available = [col for col in sample_cols if col in joined.columns]
    rows = []
    for _, row in joined.sort_values("t3_max_gain_pct_pick")[available].iterrows():
        item: dict[str, Any] = {}
        for col in available:
            value = row[col]
            if isinstance(value, (int, float, np.floating)) and pd.notna(value):
                item[col] = round(float(value), 4)
            else:
                item[col] = "" if pd.isna(value) else value
        rows.append(item)
    return rows


def _reversal_quality_diagnosis(joined: pd.DataFrame, stats: list[dict[str, Any]]) -> list[str]:
    if joined.empty:
        return ["没有匹配到劣质反转特征快照。"]
    notes = []
    turnover = pd.to_numeric(joined.get("换手率"), errors="coerce")
    if turnover.notna().any():
        low2 = float((turnover < 2).mean() * 100)
        low3 = float((turnover < 3).mean() * 100)
        notes.append(f"劣质反转中换手率<2%的占 {low2:.2f}%，换手率<3%的占 {low3:.2f}%。")
    volume_ratio = pd.to_numeric(joined.get("volume_ratio_to_10d"), errors="coerce")
    if volume_ratio.notna().any():
        hot_volume = float((volume_ratio >= 3).mean() * 100)
        notes.append(f"劣质反转中10日量比>=3的占 {hot_volume:.2f}%，问题不是没量，而是放量后缺少持续溢价。")
    ma20_bias = pd.to_numeric(joined.get("20日均线乖离率"), errors="coerce")
    ma30_bias = pd.to_numeric(joined.get("ma30_bias"), errors="coerce")
    if ma20_bias.notna().any() or ma30_bias.notna().any():
        near_ma20 = float(ma20_bias.between(-1.0, 2.0).mean() * 100) if ma20_bias.notna().any() else 0.0
        near_ma30 = float(ma30_bias.between(-1.0, 2.0).mean() * 100) if ma30_bias.notna().any() else 0.0
        notes.append(f"收盘价贴近上方均线压力的比例：20日线[-1%,2%]占 {near_ma20:.2f}%，30日线[-1%,2%]占 {near_ma30:.2f}%。")
        high_ma20 = float((ma20_bias >= 8).mean() * 100) if ma20_bias.notna().any() else 0.0
        high_ma30 = float((ma30_bias >= 6).mean() * 100) if ma30_bias.notna().any() else 0.0
        notes.append(f"一阳穿线后位置偏离过高：20日乖离>=8%占 {high_ma20:.2f}%，30日乖离>=6%占 {high_ma30:.2f}%，更像短线急拉后的弱反抽。")
    ma20_slope = pd.to_numeric(joined.get("ma20_slope"), errors="coerce")
    ma30_slope = pd.to_numeric(joined.get("ma30_slope"), errors="coerce")
    if ma20_slope.notna().any() or ma30_slope.notna().any():
        down20 = float((ma20_slope < 0).mean() * 100) if ma20_slope.notna().any() else 0.0
        down30 = float((ma30_slope < 0).mean() * 100) if ma30_slope.notna().any() else 0.0
        notes.append(f"均线仍向下的比例：20日线斜率<0占 {down20:.2f}%，30日线斜率<0占 {down30:.2f}%。")
    market = pd.to_numeric(joined.get("market_avg_change"), errors="coerce")
    down_count = pd.to_numeric(joined.get("market_down_count"), errors="coerce")
    if market.notna().any():
        weak_market = ((market <= -0.5) | (down_count >= 3500)).mean() * 100
        notes.append(f"买入日处于大盘弱势或下跌家数过多的比例为 {weak_market:.2f}%。")
    if not notes:
        notes.append("劣质反转没有显著单因子，需要继续做组合过滤或重新训练反转模型。")
    return notes


def _safe_float(value: Any) -> float:
    try:
        if value is None or pd.isna(value):
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def format_reversal_quality_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# 雷达 3 号弱反抽尸检",
        "",
        f"- 反转出手数：{report.get('count', 0)}",
        f"- T+3 失败数（<=0）：{report.get('failure_count', 0)}",
        f"- 劣质反转数（<2%）：{report.get('quality_count', 0)}",
        f"- 匹配特征快照：{report.get('matched_count', 0)}",
        "",
        "## 关键特征统计",
        "",
        "| 特征 | 均值 | 中位数 | P25 | P75 | 最小 | 最大 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in report.get("feature_stats", []):
        lines.append(
            f"| {row['feature']} | {row['mean']:.2f} | {row['median']:.2f} | "
            f"{row['p25']:.2f} | {row['p75']:.2f} | {row['min']:.2f} | {row['max']:.2f} |"
        )
    lines.extend(["", "## 共性元凶", ""])
    lines.extend([f"- {note}" for note in report.get("diagnosis", [])])
    lines.extend(["", "## 劣质样本", "", "| 日期 | 代码 | 名称 | T+3涨幅 | 换手 | 10日量比 | 20日乖离 | 30日乖离 | 大盘均涨 | 下跌家数 |", "|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"])
    for row in report.get("samples", []):
        lines.append(
            f"| {row.get('selection_date', '')} | {row.get('code', '')} | {row.get('name', '')} | "
            f"{float(row.get('t3_max_gain_pct_pick') or 0):.2f}% | "
            f"{float(row.get('换手率') or 0):.2f}% | "
            f"{float(row.get('volume_ratio_to_10d') or 0):.2f} | "
            f"{float(row.get('20日均线乖离率') or 0):.2f}% | "
            f"{float(row.get('ma30_bias') or 0):.2f}% | "
            f"{float(row.get('market_avg_change') or 0):.2f}% | "
            f"{int(float(row.get('market_down_count') or 0))} |"
        )
    return "\n".join(lines)


def _strategy_group_rows(picks: pd.DataFrame) -> list[dict[str, Any]]:
    if picks.empty:
        return []
    rows = []
    for strategy_type, group in picks.groupby("strategy_type", dropna=False):
        premiums = pd.to_numeric(group["open_premium"], errors="coerce").dropna()
        success = pd.to_numeric(group["success"], errors="coerce").fillna(0).astype(bool)
        rows.append(
            {
                "strategy_type": str(strategy_type or "尾盘突破"),
                "trades": int(len(group)),
                "success_count": int(success.sum()),
                "failure_count": int((~success).sum()),
                "win_rate": round(float(success.mean() * 100), 4) if len(success) else 0.0,
                "avg_open_premium": round(float(premiums.mean()), 4) if len(premiums) else 0.0,
                "median_open_premium": round(float(premiums.median()), 4) if len(premiums) else 0.0,
            }
        )
    rows.sort(key=lambda row: row["trades"], reverse=True)
    return rows


def _dipbuy_failure_diagnosis(stats: list[dict[str, Any]]) -> list[str]:
    by_feature = {row["feature"]: row for row in stats}
    notes = []
    return_3d = by_feature.get("3日累计涨幅", {})
    if return_3d.get("median", 0) < 0:
        notes.append(f"失败低吸的3日累计涨幅中位数为{return_3d.get('median')}%，更像连续走弱后的伪首阴。")
    bias10 = by_feature.get("10日均线乖离率", {})
    if bias10.get("p25", 0) < -1.5:
        notes.append(f"失败样本中至少四分之一已经跌破10日线{abs(bias10.get('p25')):.2f}%以上，支撑偏弱。")
    flush = by_feature.get("今日急跌度", {})
    amplitude = by_feature.get("日内振幅", {})
    if abs(float(flush.get("median", 0))) < 5 and float(amplitude.get("median", 0)) < 6:
        notes.append("失败样本急跌和振幅中位数都不够极端，恐慌洗盘识别度不足。")
    if not notes:
        notes.append("失败样本没有单一极端特征，需要组合过滤或重训低吸模型。")
    return notes


def _optimization_rows(pool: pd.DataFrame, baseline: dict[str, Any]) -> list[dict[str, Any]]:
    rules = [
        ("过滤涨幅>=7%", lambda df: df[df["涨跌幅"] < 7].copy()),
        ("过滤上影线>=2%", lambda df: df[df["上影线比例"] < 2].copy()),
        ("过滤振幅>=8%", lambda df: df[df["日内振幅"] < 8].copy()),
        ("过滤换手>=15%", lambda df: df[df["换手率"] < 15].copy()),
        ("过滤低价<3元", lambda df: df[df["最新价"] >= 3].copy()),
        ("要求预期溢价>0", lambda df: df[df["预期溢价"] > 0].copy()),
        ("要求风险分>=55", lambda df: df[df["风险评分"] >= 55].copy()),
        (f"要求策略独立门槛：突破>={BREAKOUT_MIN_SCORE:.1f}/低吸>={DIPBUY_MIN_SCORE:.1f}", lambda df: df[df["综合评分"] >= df["strategy_type"].map({"首阴低吸": DIPBUY_MIN_SCORE}).fillna(BREAKOUT_MIN_SCORE)].copy()),
        ("大盘风控空仓", lambda df: df[(df["market_avg_change"] > -0.5) & (df["market_down_count"] < 3500)].copy()),
        (f"要求预期溢价>={PROFIT_TARGET_PCT:.1f}%", lambda df: df[df["预期溢价"] >= PROFIT_TARGET_PCT].copy()),
        ("要求10日量比>=1.2", lambda df: df[df["10日量比"] >= 1.2].copy()),
        ("过滤高位爆量", lambda df: df[df["高位爆量标记"] < 0.5].copy() if "高位爆量标记" in df.columns else df.copy()),
        ("过滤虚拉诱多", lambda df: df[(df["振幅换手比"] <= 3) & (df["缩量大涨标记"] < 0.5) & (df["极端下影线标记"] < 0.5)].copy()),
        ("过滤近3日断头铡刀", lambda df: df[df["近3日断头铡刀标记"] < 0.5].copy()),
        (
            "组合过滤：涨幅<7且上影<2且预期>0",
            lambda df: df[(df["涨跌幅"] < 7) & (df["上影线比例"] < 2) & (df["预期溢价"] > 0)].copy(),
        ),
        (
            "组合过滤：风险>=55且振幅<8且换手<15",
            lambda df: df[(df["风险评分"] >= 55) & (df["日内振幅"] < 8) & (df["换手率"] < 15)].copy(),
        ),
    ]
    rows = []
    for name, filter_fn in rules:
        filtered = filter_fn(pool)
        picks = _daily_top(filtered)
        stats = _stats_row(name, "失败归因候选过滤规则。", picks)
        stats["delta_win_rate"] = round(float(stats["win_rate"] - baseline["win_rate"]), 4)
        stats["delta_avg_open_premium"] = round(float(stats["avg_open_premium"] - baseline["avg_open_premium"]), 4)
        stats["coverage"] = round(float(stats["trades"] / baseline["trades"] * 100), 4) if baseline["trades"] else 0.0
        rows.append(stats)
    rows.sort(key=lambda row: (row["delta_avg_open_premium"], row["delta_win_rate"], row["trades"]), reverse=True)
    return rows


def _sample_failure_rows(failures: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    cols = ["date", "纯代码", "名称", "open_premium", "AI胜率", "预期溢价", "风险评分", "流动性评分", "综合评分", "涨跌幅", "换手率", "上影线比例", "日内振幅", "振幅换手比"]
    for _, row in failures.sort_values("open_premium").head(20)[cols].iterrows():
        rows.append(
            {
                "date": str(row["date"]),
                "code": str(row["纯代码"]),
                "name": str(row["名称"]),
                "open_premium": round(float(row["open_premium"]), 4),
                "win_rate": round(float(row["AI胜率"]), 4),
                "expected_premium": round(float(row["预期溢价"]), 4),
                "risk_score": round(float(row["风险评分"]), 4),
                "liquidity_score": round(float(row["流动性评分"]), 4),
                "composite_score": round(float(row["综合评分"]), 4),
                "change": round(float(row["涨跌幅"]), 4),
                "turnover": round(float(row["换手率"]), 4),
                "upper_shadow": round(float(row["上影线比例"]), 4),
                "amplitude": round(float(row["日内振幅"]), 4),
                "amplitude_turnover_ratio": round(float(row["振幅换手比"]), 4),
            }
        )
    return rows


def _empty_result(months: int, reason: str) -> dict[str, Any]:
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": {
            "months": months,
            "start_date": None,
            "end_date": None,
            "trades": 0,
            "success_count": 0,
            "failure_count": 0,
            "baseline": None,
            "model_status": reason,
            "can_optimize_model": False,
            "optimization_note": reason,
        },
        "reasons": [],
        "optimizations": [],
        "sample_failures": [],
    }


if __name__ == "__main__":
    import json

    report = diagnose_dipbuy_failures(months=12, refresh=False)
    print(json.dumps(report, ensure_ascii=False, indent=2))
