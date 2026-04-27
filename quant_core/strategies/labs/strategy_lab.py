from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

import numpy as np
import pandas as pd

from quant_core.engine.backtest import (
    _fill_missing_names,
    _latest_trade_date,
    _load_daily_rows,
    _repair_missing_pre_close,
    _repair_missing_volume_ratio,
    _valid_trading_dates,
)
from quant_core.cache_utils import read_dataframe_cache, write_dataframe_cache
from quant_core.config import BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE, REVERSAL_MIN_SCORE
from quant_core.engine.predictor import PROFIT_TARGET_PCT, apply_production_filters, build_features, score_candidates
from quant_core.storage import init_db


def run_strategy_lab(months: int = 2, refresh: bool = False) -> dict[str, Any]:
    init_db()
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    if prepared["evaluated"].empty:
        return _empty_lab("有效交易日不足，无法实验")

    evaluated = prepared["evaluated"]

    variants = _strategy_variants()
    variant_rows = []
    for variant in variants:
        filtered = variant["filter"](evaluated)
        stats = _top_one_stats(filtered, variant["name"], variant["description"])
        variant_rows.append(stats)
    variant_rows.sort(key=lambda row: (row["avg_open_premium"], row["win_rate"], row["trades"]), reverse=True)

    threshold_rows = []
    for threshold in [50, 55, 60, 65, 70, 75, 80]:
        threshold_rows.append(_threshold_stats(evaluated, threshold))

    score_corr = {
        "composite_pearson": _safe_corr(evaluated["综合评分"], evaluated["open_premium"]),
        "composite_spearman": _safe_corr(evaluated["综合评分"], evaluated["open_premium"], method="spearman"),
        "signal_score_pearson": _safe_corr(evaluated["AI胜率"], evaluated["open_premium"]),
        "expected_premium_pearson": _safe_corr(evaluated["预期溢价"], evaluated["open_premium"]),
    }
    best = next((row for row in variant_rows if row["trades"] >= 8), variant_rows[0] if variant_rows else None)
    summary = {
        "months": months,
        "start_date": prepared["start_date"],
        "end_date": prepared["end_date"],
        "candidate_rows": int(len(evaluated)),
        "trading_days": int(evaluated["date"].nunique()),
        "model_status": prepared["model_status"],
        "repaired_pre_close_count": prepared["repaired_pre_close_count"],
        "repaired_volume_ratio_count": prepared["repaired_volume_ratio_count"],
        "best_strategy": best,
        "correlation": score_corr,
        "note": "实验室只用已发生历史数据评估规则，不代表未来收益；样本少于8次的高收益规则不能直接用于实盘。",
    }
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "variants": variant_rows,
        "thresholds": threshold_rows,
        "daily_picks": _daily_pick_rows(evaluated, limit=60),
    }


def prepare_evaluated_candidates(months: int, refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        cached = read_dataframe_cache("evaluated_candidates", months)
        if cached is not None:
            evaluated, extra = cached
            return {
                "evaluated": evaluated,
                "start_date": extra.get("start_date"),
                "end_date": extra.get("end_date"),
                "model_status": extra.get("model_status", "cache_ready"),
                "repaired_pre_close_count": int(extra.get("repaired_pre_close_count", 0)),
                "repaired_volume_ratio_count": int(extra.get("repaired_volume_ratio_count", 0)),
                "cache": {"hit": True, "namespace": "evaluated_candidates"},
            }

    latest_date = _latest_trade_date()
    if latest_date is None:
        return {
            "evaluated": pd.DataFrame(),
            "start_date": None,
            "end_date": None,
            "model_status": "数据库没有可实验的日线数据",
            "repaired_pre_close_count": 0,
            "repaired_volume_ratio_count": 0,
        }

    start_date = (pd.Timestamp(latest_date) - pd.DateOffset(months=months)).strftime("%Y-%m-%d")
    load_start_date = (pd.Timestamp(start_date) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
    raw = _fill_missing_names(_load_daily_rows(load_start_date))
    repaired_pre_close = _repair_missing_pre_close(raw)
    repaired_volume_ratio = _repair_missing_volume_ratio(raw)
    period_raw = raw[raw["date"] >= start_date].copy()
    trading_dates = _valid_trading_dates(period_raw)
    if len(trading_dates) < 2:
        return {
            "evaluated": pd.DataFrame(),
            "start_date": start_date,
            "end_date": None,
            "model_status": "有效交易日不足",
            "repaired_pre_close_count": repaired_pre_close,
            "repaired_volume_ratio_count": repaired_volume_ratio,
        }

    all_trading_dates = _valid_trading_dates(raw)
    raw = raw[raw["date"].isin(all_trading_dates)].copy()
    candidates = build_features(raw)
    candidates = candidates[candidates["date"].isin(trading_dates)].copy()
    candidates, model_status = score_candidates(candidates)
    candidates = _attach_next_open(candidates, raw, trading_dates)
    evaluated = candidates[np.isfinite(candidates["open_premium"])].copy()
    result = {
        "evaluated": evaluated,
        "start_date": start_date,
        "end_date": trading_dates[-1],
        "model_status": model_status,
        "repaired_pre_close_count": repaired_pre_close,
        "repaired_volume_ratio_count": repaired_volume_ratio,
        "cache": {"hit": False, "namespace": "evaluated_candidates"},
    }
    if not evaluated.empty:
        write_dataframe_cache(
            "evaluated_candidates",
            months,
            evaluated,
            {
                "start_date": result["start_date"],
                "end_date": result["end_date"],
                "model_status": result["model_status"],
                "repaired_pre_close_count": result["repaired_pre_close_count"],
                "repaired_volume_ratio_count": result["repaired_volume_ratio_count"],
            },
        )
    return result


def _attach_next_open(candidates: pd.DataFrame, raw: pd.DataFrame, trading_dates: list[str]) -> pd.DataFrame:
    next_trade_date = {trading_dates[index]: trading_dates[index + 1] for index in range(len(trading_dates) - 1)}
    opens = raw.set_index(["date", "code"])["open"]
    highs = raw.set_index(["date", "code"])["high"]
    out = candidates.copy()
    out["next_date"] = out["date"].map(next_trade_date)
    out["next_open"] = [opens.get((next_date, code), np.nan) for next_date, code in zip(out["next_date"], out["纯代码"])]
    out["open_premium"] = (pd.to_numeric(out["next_open"], errors="coerce") / out["最新价"] - 1) * 100
    future_dates = {
        day: trading_dates[index + 1 : index + 4]
        for index, day in enumerate(trading_dates)
        if index + 1 < len(trading_dates)
    }
    out["t3_exit_date"] = out["date"].map(lambda day: future_dates.get(str(day), [None])[-1] if len(future_dates.get(str(day), [])) == 3 else None)
    future_highs: list[float] = []
    for day, code in zip(out["date"], out["纯代码"]):
        candidate_dates = future_dates.get(str(day), [])
        if len(candidate_dates) < 3:
            future_highs.append(np.nan)
            continue
        values = [highs.get((future_day, code), np.nan) for future_day in candidate_dates]
        numeric = pd.to_numeric(pd.Series(values), errors="coerce").dropna()
        future_highs.append(float(numeric.max()) if len(numeric) == 3 else np.nan)
    out["t3_max_high"] = future_highs
    out["t3_max_gain_pct"] = (pd.to_numeric(out["t3_max_high"], errors="coerce") / out["最新价"] - 1) * 100
    return out


def _strategy_variants() -> list[dict[str, Any]]:
    return [
        {
            "name": "预期溢价 Top1",
            "description": "全候选池每日按回归模型预期溢价最高买入。",
            "filter": lambda df: df.copy(),
        },
        {
            "name": "高置信度 >=75",
            "description": "仅在综合评分不低于75时交易。",
            "filter": lambda df: df[df["综合评分"] >= 75].copy(),
        },
        {
            "name": "高置信度 >=80",
            "description": "仅在综合评分不低于80时交易，频率低。",
            "filter": lambda df: df[df["综合评分"] >= 80].copy(),
        },
        {
            "name": "剔除科创板",
            "description": "排除 68/689，降低高波动板块影响。",
            "filter": lambda df: df[~df["纯代码"].str.startswith(("68", "689"), na=False)].copy(),
        },
        {
            "name": "温和上涨",
            "description": "只看涨跌幅 0% 到 7% 的候选。",
            "filter": lambda df: df[df["涨跌幅"].between(0, 7)].copy(),
        },
        {
            "name": "流动性平衡",
            "description": "换手 2% 到 12%，价格 3 到 80 元。",
            "filter": lambda df: df[df["换手率"].between(2, 12) & df["最新价"].between(3, 80)].copy(),
        },
        {
            "name": "正预期溢价",
            "description": "只交易预期溢价大于0的候选。",
            "filter": lambda df: df[df["预期溢价"] > 0].copy(),
        },
        {
            "name": "生产风险过滤",
            "description": f"剔除科创板，叠加成交额豁免、策略独立门槛：突破>={BREAKOUT_MIN_SCORE:.1f}、低吸>={DIPBUY_MIN_SCORE:.1f}、反转>={REVERSAL_MIN_SCORE:.1f}%，并在阴天/震荡给低吸排序补偿。",
            "filter": lambda df: apply_production_filters(df),
        },
        {
            "name": "爆发力目标",
            "description": "预期溢价>=0.8%，只寻找能覆盖滑点费率的候选。",
            "filter": lambda df: df[(df["预期溢价"] >= PROFIT_TARGET_PCT) & (df["market_down_count"] < 3500)].copy(),
        },
        {
            "name": "缩量洗盘候选",
            "description": "缩量下跌或5日地量，且模型预期溢价为正。",
            "filter": lambda df: df[
                ((df["缩量下跌标记"] >= 0.5) | (df["5日地量标记"] >= 0.5))
                & (df["预期溢价"] > 0)
                & (df["market_avg_change"] > -0.5)
            ].copy(),
        },
        {
            "name": "低风险正预期",
            "description": "风险评分>=65 且预期溢价>0。",
            "filter": lambda df: df[(df["风险评分"] >= 65) & (df["预期溢价"] > 0)].copy(),
        },
    ]


def _top_one_stats(df: pd.DataFrame, name: str, description: str) -> dict[str, Any]:
    if df.empty:
        return _stats_row(name, description, pd.DataFrame())
    picks = _daily_top(df)
    return _stats_row(name, description, picks)


def _threshold_stats(df: pd.DataFrame, threshold: int) -> dict[str, Any]:
    picks = _daily_top(df)
    picks = picks[picks["综合评分"] >= threshold].copy()
    return _stats_row(f"综合评分 >= {threshold}", "每日最高分达到阈值才交易。", picks)


def _daily_top(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sort_cols = ["date"]
    if "策略优先级" in df.columns:
        sort_cols.append("策略优先级")
    sort_cols.extend(["排序评分", "预期溢价", "综合评分"] if "排序评分" in df.columns else ["预期溢价", "综合评分"])
    sorted_df = df.sort_values(sort_cols, ascending=[True] + [False] * (len(sort_cols) - 1))
    return sorted_df.drop_duplicates("date", keep="first").sort_values("date").copy()


def _stats_row(name: str, description: str, picks: pd.DataFrame) -> dict[str, Any]:
    if picks.empty:
        return {
            "name": name,
            "description": description,
            "trades": 0,
            "win_rate": 0.0,
            "avg_open_premium": 0.0,
            "median_open_premium": 0.0,
            "best_open_premium": 0.0,
            "worst_open_premium": 0.0,
            "avg_composite_score": 0.0,
        }
    premiums = pd.to_numeric(picks["open_premium"], errors="coerce").dropna()
    return {
        "name": name,
        "description": description,
        "trades": int(len(picks)),
        "win_rate": round(float((premiums > PROFIT_TARGET_PCT).mean() * 100), 4) if len(premiums) else 0.0,
        "avg_open_premium": round(float(premiums.mean()), 4) if len(premiums) else 0.0,
        "median_open_premium": round(float(premiums.median()), 4) if len(premiums) else 0.0,
        "best_open_premium": round(float(premiums.max()), 4) if len(premiums) else 0.0,
        "worst_open_premium": round(float(premiums.min()), 4) if len(premiums) else 0.0,
        "avg_composite_score": round(float(picks["综合评分"].mean()), 4),
    }


def _daily_pick_rows(df: pd.DataFrame, limit: int) -> list[dict[str, Any]]:
    picks = _daily_top(df).sort_values("date", ascending=False).head(limit)
    rows = []
    for _, row in picks.iterrows():
        strategy_type = str(row.get("strategy_type", "尾盘突破"))
        t3_gain = row.get("t3_max_gain_pct")
        open_premium = row.get("open_premium")
        success = bool(t3_gain > 0) if strategy_type in {"中线超跌反转", "右侧主升浪"} and pd.notna(t3_gain) else (bool(open_premium > PROFIT_TARGET_PCT) if pd.notna(open_premium) else None)
        rows.append(
            {
                "date": str(row["date"]),
                "code": str(row["纯代码"]),
                "name": str(row["名称"]),
                "strategy_type": strategy_type,
                "win_rate": round(float(row["AI胜率"]), 4),
                "expected_premium": round(float(row["预期溢价"]), 4),
                "risk_score": round(float(row["风险评分"]), 4),
                "liquidity_score": round(float(row["流动性评分"]), 4),
                "composite_score": round(float(row["综合评分"]), 4),
                "close": round(float(row["最新价"]), 4),
                "next_date": str(row["next_date"]) if pd.notna(row["next_date"]) else None,
                "t3_exit_date": str(row["t3_exit_date"]) if pd.notna(row.get("t3_exit_date")) else None,
                "next_open": round(float(row["next_open"]), 4) if pd.notna(row["next_open"]) else None,
                "open_premium": round(float(row["open_premium"]), 4) if pd.notna(row["open_premium"]) else None,
                "t3_max_gain_pct": round(float(row["t3_max_gain_pct"]), 4) if pd.notna(row.get("t3_max_gain_pct")) else None,
                "success": success,
            }
        )
    return rows


def _safe_corr(left: pd.Series, right: pd.Series, method: str = "pearson") -> float:
    try:
        value = left.corr(right, method=method)
    except Exception:
        return 0.0
    return round(float(value), 4) if pd.notna(value) else 0.0


def _empty_lab(reason: str) -> dict[str, Any]:
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": {
            "months": 2,
            "start_date": None,
            "end_date": None,
            "candidate_rows": 0,
            "trading_days": 0,
            "model_status": reason,
            "best_strategy": None,
            "correlation": {},
            "note": reason,
        },
        "variants": [],
        "thresholds": [],
        "daily_picks": [],
    }
