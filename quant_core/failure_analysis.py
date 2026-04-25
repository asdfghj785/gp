from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from .config import BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE
from .predictor import PROFIT_TARGET_PCT, apply_production_filters
from .daily_pick import list_daily_pick_results
from .storage import init_db
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
