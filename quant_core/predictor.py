from __future__ import annotations

import json
import threading
from datetime import datetime
from functools import lru_cache
from typing import Any

import numpy as np
import pandas as pd

from .config import (
    BREAKOUT_MIN_SCORE,
    DATA_DIR,
    DIPBUY_MIN_SCORE,
    DIPBUY_PREMIUM_MODEL_PATH,
    LATEST_TOP50_PATH,
    MODEL_PATH,
    PREMIUM_MODEL_PATH,
    PROFIT_TARGET_PCT,
)
from .intraday_snapshot import attach_late_pull_trap
from .market import fetch_market_indices, fetch_sina_snapshot
from .storage import connect, save_prediction_snapshot, upsert_daily_rows


FEATURE_COLS = [
    "turn",
    "量比",
    "真实涨幅点数",
    "实体比例",
    "上影线比例",
    "下影线比例",
    "日内振幅",
    "5日累计涨幅",
    "3日累计涨幅",
    "5日均线乖离率",
    "20日均线乖离率",
    "3日平均换手率",
    "5日量能堆积",
    "10日量比",
    "3日红盘比例",
    "5日地量标记",
    "缩量下跌标记",
    "振幅换手比",
    "缩量大涨标记",
    "极端下影线标记",
    "近3日断头铡刀标记",
    "60日高位比例",
    "market_up_rate",
    "market_avg_change",
    "market_down_count",
]
DIPBUY_TEMPORAL_FEATURE_COLS = [
    "近5日最高涨幅",
    "今日急跌度",
    "10日均线乖离率",
    "今日缩量比例",
    "均线趋势斜率",
    "光脚大阴线惩罚度",
    "昨日实体涨跌幅",
]
DIPBUY_FEATURE_COLS = [
    "turn",
    "量比",
    "真实涨幅点数",
    "实体比例",
    "上影线比例",
    "下影线比例",
    "日内振幅",
    "近5日最高涨幅",
    "今日急跌度",
    "10日均线乖离率",
    "今日缩量比例",
    "均线趋势斜率",
    "光脚大阴线惩罚度",
]
DIPBUY_STRATEGY_TYPE = "首阴低吸"
BREAKOUT_STRATEGY_TYPE = "尾盘突破"
DIPBUY_FILTERS = {
    "min_5d_high_gain": 15.0,
    "min_intraday_flush": -9.5,
    "max_intraday_flush": -4.5,
    "bias10_low": -1.5,
    "bias10_high": 3.0,
    "max_amount_shrink_pct": 0.0,
    "min_3d_return": -3.0,
    "min_prev_body_change": -2.0,
}
LOW_LIQUIDITY_AMOUNT = 700_000_000_000
HIGH_LIQUIDITY_AMOUNT = 800_000_000_000
DIPBUY_SENTIMENT_BONUS = 10.0


@lru_cache(maxsize=1)
def _load_model():
    if not MODEL_PATH.exists():
        return None, f"找不到模型文件: {MODEL_PATH}"
    try:
        import xgboost as xgb

        model = xgb.XGBClassifier()
        model.load_model(str(MODEL_PATH))
        return model, None
    except Exception as exc:
        return None, f"模型加载失败: {exc}"


@lru_cache(maxsize=1)
def _load_premium_model():
    if not PREMIUM_MODEL_PATH.exists():
        return None, f"找不到溢价模型文件: {PREMIUM_MODEL_PATH}"
    try:
        import xgboost as xgb

        model = xgb.XGBRegressor()
        model.load_model(str(PREMIUM_MODEL_PATH))
        return model, None
    except Exception as exc:
        return None, f"溢价模型加载失败: {exc}"


@lru_cache(maxsize=1)
def _load_dipbuy_premium_model():
    if not DIPBUY_PREMIUM_MODEL_PATH.exists():
        return None, f"找不到低吸溢价模型文件: {DIPBUY_PREMIUM_MODEL_PATH}"
    try:
        import xgboost as xgb

        model = xgb.XGBRegressor()
        model.load_model(str(DIPBUY_PREMIUM_MODEL_PATH))
        return model, None
    except Exception as exc:
        return None, f"低吸溢价模型加载失败: {exc}"


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "code" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["纯代码"] = out["code"].astype(str).str.extract(r"(\d{6})")[0]
    out["名称"] = out["name"].fillna("")
    out["最新价"] = pd.to_numeric(out["close"], errors="coerce").fillna(0)
    out["涨跌幅"] = pd.to_numeric(out["change_pct"], errors="coerce").fillna(0)
    out["换手率"] = pd.to_numeric(out["turnover"], errors="coerce").fillna(0)
    if "volume_ratio" not in out.columns:
        out["volume_ratio"] = 0.0
    out["量比"] = pd.to_numeric(out["volume_ratio"], errors="coerce").fillna(0)
    out["昨收"] = pd.to_numeric(out["pre_close"], errors="coerce").fillna(0)
    out["今开"] = pd.to_numeric(out["open"], errors="coerce").fillna(0)
    out["最高"] = pd.to_numeric(out["high"], errors="coerce").fillna(0)
    out["最低"] = pd.to_numeric(out["low"], errors="coerce").fillna(0)
    out["volume"] = pd.to_numeric(out.get("volume", 0), errors="coerce").fillna(0)
    out["amount"] = pd.to_numeric(out.get("amount", 0), errors="coerce").fillna(0)
    out["date"] = out["date"].astype(str)

    out = _add_market_context(out)
    out = out[~out["纯代码"].str.startswith(("30", "68", "4", "8", "92"), na=False)].copy()
    out = out[~out["名称"].str.contains("ST|退", case=False, na=False)].copy()
    out = out[(out["最新价"] > 0) & (out["昨收"] > 0)].copy()
    high_limit_board = out["纯代码"].str.startswith(("30", "68"), na=False)
    out = out[((high_limit_board) & (out["涨跌幅"] < 19.5)) | ((~high_limit_board) & (out["涨跌幅"] < 9.5))].copy()
    if out.empty:
        return out
    out = _add_temporal_features(out)

    out["实体比例"] = ((out["最新价"] - out["今开"]) / out["昨收"] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    out["上影线比例"] = ((out["最高"] - out[["今开", "最新价"]].max(axis=1)) / out["昨收"] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    out["下影线比例"] = ((out[["今开", "最新价"]].min(axis=1) - out["最低"]) / out["昨收"] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    out["日内振幅"] = ((out["最高"] - out["最低"]) / out["昨收"] * 100).replace([np.inf, -np.inf], 0).fillna(0)
    out["真实涨幅点数"] = out["涨跌幅"]
    out["turn"] = out["换手率"]
    out["振幅换手比"] = (out["日内振幅"] / out["换手率"].replace(0, np.nan)).replace([np.inf, -np.inf], 0).fillna(0)
    out["缩量大涨标记"] = ((out["涨跌幅"] > 3) & (_num(out, "5日量能堆积") < 1)).astype(float)
    out["极端下影线标记"] = ((out["下影线比例"] > out["实体比例"].abs() * 2) & (out["涨跌幅"] > 3)).astype(float)
    return out


def score_candidates(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    scored = df.copy()
    for col in dict.fromkeys([*FEATURE_COLS, *DIPBUY_FEATURE_COLS]):
        if col not in scored.columns:
            scored[col] = 0.0
        scored[col] = pd.to_numeric(scored[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)
    if scored.empty:
        return scored, "ready"

    scored["strategy_type"] = BREAKOUT_STRATEGY_TYPE
    scored["预期溢价"] = _fallback_expected_premium(scored)
    _log_dipbuy_diagnostics(scored)
    dipbuy_mask = _dipbuy_physical_mask(scored)
    premium_model, premium_error = _load_premium_model()
    dipbuy_model, dipbuy_error = _load_dipbuy_premium_model()
    status_parts: list[str] = []

    if premium_model is not None:
        try:
            breakout_index = scored.index[~dipbuy_mask]
            if len(breakout_index) > 0:
                scored.loc[breakout_index, "预期溢价"] = premium_model.predict(scored.loc[breakout_index, FEATURE_COLS].values)
            status_parts.append("breakout_regressor_ready")
        except Exception as exc:
            status_parts.append(f"尾盘突破回归模型失败，已降级规则估算: {exc}")
    else:
        status_parts.append(premium_error or "breakout_model_unavailable")

    if dipbuy_mask.any():
        scored.loc[dipbuy_mask, "strategy_type"] = DIPBUY_STRATEGY_TYPE
        if dipbuy_model is not None:
            try:
                scored.loc[dipbuy_mask, "预期溢价"] = dipbuy_model.predict(scored.loc[dipbuy_mask, DIPBUY_FEATURE_COLS].values)
                status_parts.append(f"dipbuy_regressor_ready:{int(dipbuy_mask.sum())}")
            except Exception as exc:
                status_parts.append(f"首阴低吸回归模型失败，已降级规则估算: {exc}")
        else:
            status_parts.append(dipbuy_error or "dipbuy_model_unavailable")
    else:
        status_parts.append("dipbuy_no_physical_match")

    scored["预期溢价"] = pd.to_numeric(scored["预期溢价"], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)

    scored["风险评分"] = _risk_score(scored)
    scored["流动性评分"] = _liquidity_score(scored)
    scored["AI胜率"] = _regression_signal_score(scored)
    premium_score = (50 + scored["预期溢价"].clip(-5, 5) * 10).clip(0, 100)
    dipbuy_premium_score = (55 + scored["预期溢价"].clip(-5, 5) * 16).clip(0, 100)
    scored["综合评分"] = (
        premium_score * 0.60
        + scored["风险评分"] * 0.20
        + scored["流动性评分"] * 0.10
        + scored["AI胜率"] * 0.10
    ).clip(0, 100)
    is_dipbuy = scored["strategy_type"].eq(DIPBUY_STRATEGY_TYPE)
    scored.loc[is_dipbuy, "综合评分"] = (
        dipbuy_premium_score.loc[is_dipbuy] * 0.85
        + scored.loc[is_dipbuy, "AI胜率"] * 0.10
        + scored.loc[is_dipbuy, "流动性评分"] * 0.05
    ).clip(0, 100)
    model_status = "; ".join(status_parts) if status_parts else "ready"
    return scored, model_status


def _dipbuy_physical_mask(df: pd.DataFrame) -> pd.Series:
    return (
        (_num(df, "近5日最高涨幅") > DIPBUY_FILTERS["min_5d_high_gain"])
        & (_num(df, "今日急跌度") > DIPBUY_FILTERS["min_intraday_flush"])
        & (_num(df, "今日急跌度") < DIPBUY_FILTERS["max_intraday_flush"])
        & (_num(df, "10日均线乖离率").between(DIPBUY_FILTERS["bias10_low"], DIPBUY_FILTERS["bias10_high"]))
        & (_num(df, "今日缩量比例") < DIPBUY_FILTERS["max_amount_shrink_pct"])
        & (_num(df, "3日累计涨幅") > DIPBUY_FILTERS["min_3d_return"])
        & (_num(df, "昨日实体涨跌幅") > DIPBUY_FILTERS["min_prev_body_change"])
    ).fillna(False)


def _log_dipbuy_diagnostics(df: pd.DataFrame) -> None:
    if df.empty:
        print("📊 [双轨诊断] 候选池为空，无法计算首阴低吸分流。")
        return
    probe_cols = ["纯代码", "名称", "3日累计涨幅", *DIPBUY_TEMPORAL_FEATURE_COLS]
    available_cols = [col for col in probe_cols if col in df.columns]
    sample = df[available_cols].sample(n=min(5, len(df)), random_state=42)
    print("🔎 [双轨诊断] 低吸特征探针:")
    print(sample.to_string(index=False))
    dipbuy_pool = df[
        (_num(df, "近5日最高涨幅") > DIPBUY_FILTERS["min_5d_high_gain"])
        & (_num(df, "今日急跌度") > DIPBUY_FILTERS["min_intraday_flush"])
        & (_num(df, "今日急跌度") < DIPBUY_FILTERS["max_intraday_flush"])
        & (_num(df, "10日均线乖离率").between(DIPBUY_FILTERS["bias10_low"], DIPBUY_FILTERS["bias10_high"]))
        & (_num(df, "今日缩量比例") < DIPBUY_FILTERS["max_amount_shrink_pct"])
        & (_num(df, "3日累计涨幅") > DIPBUY_FILTERS["min_3d_return"])
        & (_num(df, "昨日实体涨跌幅") > DIPBUY_FILTERS["min_prev_body_change"])
    ]
    print(f"📊 [双轨诊断] 全市场满足低吸物理条件的股票数量: {len(dipbuy_pool)} 只")
    if not dipbuy_pool.empty:
        show_cols = [col for col in probe_cols if col in dipbuy_pool.columns]
        print("📊 [双轨诊断] 低吸候选样例:")
        print(dipbuy_pool[show_cols].head(10).to_string(index=False))


def apply_production_filters(df: pd.DataFrame, gate: dict[str, Any] | None = None) -> pd.DataFrame:
    """Default live strategy filters selected from the current strategy lab."""
    if df.empty:
        return df
    if gate and gate.get("blocked"):
        return df.iloc[0:0].copy()
    filtered = df[~df["纯代码"].str.startswith(("68", "689"), na=False)].copy()
    if gate is None:
        filtered = _attach_historical_market_modes(filtered)
        filtered = filtered[~filtered["market_gate_mode"].isin(["雷暴", "缩量下跌"])].copy()
    else:
        filtered["market_gate_mode"] = str(gate.get("mode") or "晴天")
    if "涨跌幅" in filtered.columns:
        filtered = filtered[filtered["涨跌幅"] < 7].copy()
    if "上影线比例" in filtered.columns:
        is_dipbuy = filtered.get("strategy_type", "").eq(DIPBUY_STRATEGY_TYPE) if "strategy_type" in filtered.columns else pd.Series(False, index=filtered.index)
        filtered = filtered[(is_dipbuy) | (filtered["上影线比例"] < 2)].copy()
    if "预期溢价" in filtered.columns:
        filtered = filtered[filtered["预期溢价"] > 0].copy()
    if {"60日高位比例", "量比", "5日量能堆积"}.issubset(filtered.columns):
        high_volume_trap = (
            (pd.to_numeric(filtered["60日高位比例"], errors="coerce").fillna(0) >= 97)
            & (
                (pd.to_numeric(filtered["量比"], errors="coerce").fillna(0) > 3)
                | (pd.to_numeric(filtered["5日量能堆积"], errors="coerce").fillna(0) > 3)
            )
        )
        filtered = filtered[~high_volume_trap].copy()
    if "尾盘诱多标记" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["尾盘诱多标记"], errors="coerce").fillna(0) < 0.5].copy()
    if "近3日断头铡刀标记" in filtered.columns:
        is_dipbuy = filtered.get("strategy_type", "").eq(DIPBUY_STRATEGY_TYPE) if "strategy_type" in filtered.columns else pd.Series(False, index=filtered.index)
        filtered = filtered[
            (is_dipbuy)
            | (pd.to_numeric(filtered["近3日断头铡刀标记"], errors="coerce").fillna(0) < 0.5)
        ].copy()
    return apply_strategy_score_gate(filtered, gate)


def apply_strategy_score_gate(df: pd.DataFrame, gate: dict[str, Any] | None = None) -> pd.DataFrame:
    if df.empty or "综合评分" not in df.columns:
        return df
    filtered = df.copy()
    filtered["strategy_type"] = filtered.get("strategy_type", BREAKOUT_STRATEGY_TYPE)
    filtered["strategy_type"] = filtered["strategy_type"].fillna(BREAKOUT_STRATEGY_TYPE)
    score = _num(filtered, "综合评分").replace([np.inf, -np.inf], 0).fillna(0)
    is_dipbuy = filtered["strategy_type"].eq(DIPBUY_STRATEGY_TYPE)
    is_breakout = filtered["strategy_type"].eq(BREAKOUT_STRATEGY_TYPE) | ~is_dipbuy
    threshold = pd.Series(BREAKOUT_MIN_SCORE, index=filtered.index, dtype="float64")
    threshold.loc[is_dipbuy] = DIPBUY_MIN_SCORE
    qualified = ((is_breakout) & (score >= BREAKOUT_MIN_SCORE)) | ((is_dipbuy) & (score >= DIPBUY_MIN_SCORE))
    filtered = filtered[qualified].copy()
    filtered["生产门槛"] = threshold.loc[filtered.index]
    return apply_strategy_sort_score(filtered, gate)


def apply_strategy_sort_score(df: pd.DataFrame, gate: dict[str, Any] | None = None) -> pd.DataFrame:
    if df.empty:
        return df
    scored = df.copy()
    scored["strategy_type"] = scored.get("strategy_type", BREAKOUT_STRATEGY_TYPE)
    scored["strategy_type"] = scored["strategy_type"].fillna(BREAKOUT_STRATEGY_TYPE)
    base_score = _num(scored, "综合评分").replace([np.inf, -np.inf], 0).fillna(0)
    if gate is not None:
        mode = str(gate.get("mode") or "晴天")
        modes = pd.Series(mode, index=scored.index)
    elif "market_gate_mode" in scored.columns:
        modes = scored["market_gate_mode"].fillna("晴天").astype(str)
    else:
        scored = _attach_historical_market_modes(scored)
        modes = scored["market_gate_mode"].fillna("晴天").astype(str)
    is_dipbuy = scored["strategy_type"].eq(DIPBUY_STRATEGY_TYPE)
    bonus = pd.Series(0.0, index=scored.index)
    bonus.loc[is_dipbuy & modes.isin(["阴天", "震荡"])] = DIPBUY_SENTIMENT_BONUS
    scored["情绪补偿分"] = bonus
    scored["排序评分"] = (base_score + bonus).clip(0, 110)
    scored["market_gate_mode"] = modes
    return scored


def _attach_historical_market_modes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    amount = _num(out, "market_amount")
    down_count = _num(out, "market_down_count")
    avg_change = _num(out, "market_avg_change")
    up_rate = _num(out, "market_up_rate")
    low_liquidity = (amount > 0) & (amount < LOW_LIQUIDITY_AMOUNT)
    high_liquidity = amount >= HIGH_LIQUIDITY_AMOUNT
    thunder = (avg_change <= -1.2) | (down_count > 4200)
    shrink_down = ((avg_change <= -0.5) | (down_count >= 3000)) & low_liquidity
    cloudy = ((avg_change <= -0.5) | (down_count >= 3000)) & ~high_liquidity
    choppy = (avg_change <= 0) | (down_count >= 2500) | (up_rate < 50)
    mode = pd.Series("晴天", index=out.index)
    mode.loc[choppy] = "震荡"
    mode.loc[cloudy] = "阴天"
    mode.loc[shrink_down] = "缩量下跌"
    mode.loc[thunder] = "雷暴"
    out["market_gate_mode"] = mode
    return out


def scan_market(
    limit: int = 50,
    persist_snapshot: bool = True,
    cache_prediction: bool = True,
    async_persist: bool = False,
) -> dict[str, Any]:
    snapshot = fetch_sina_snapshot()
    if snapshot.empty:
        raise RuntimeError("实时行情源返回空数据")
    _repair_snapshot_volume_ratio(snapshot)
    market_indices = fetch_market_indices()

    if persist_snapshot and async_persist:
        threading.Thread(target=_persist_snapshot, args=(snapshot,), daemon=True).start()
    elif persist_snapshot:
        upsert_daily_rows(snapshot, source="sina_snapshot")

    df = build_features(snapshot)
    if df.empty:
        return {"created_at": datetime.now().isoformat(timespec="seconds"), "model_status": "ready", "rows": []}
    df, intraday_snapshot = attach_late_pull_trap(df)

    df, model_status = score_candidates(df)
    gate = market_risk_gate(df, market_indices)
    if gate["blocked"]:
        payload = {
            "id": None,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "model_status": f"{model_status}; 大盘风控触发，强制空仓",
            "strategy": "大盘风控：雷暴模式强制空仓；大盘下跌且市场缩量时空仓；高成交额或指数站上20日均线时允许AI按动态阈值选股。",
            "market_gate": gate,
            "intraday_snapshot": intraday_snapshot,
            "rows": [],
        }
        if cache_prediction:
            LATEST_TOP50_PATH.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")
        return payload

    df = apply_production_filters(df, gate)

    final_limit = min(max(int(limit), 1), 1)
    df = df.sort_values(["排序评分", "预期溢价", "综合评分"], ascending=[False, False, False]).head(final_limit)
    rows = [_row_to_api(row) for _, row in df.iterrows()]
    snapshot_id = save_prediction_snapshot("dual_xgboost_regressor" if "regressor_ready" in model_status else "rule_fallback", rows) if cache_prediction else None
    payload = {
        "id": snapshot_id,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "model_status": model_status,
        "strategy": f"生产策略：尾盘突破与首阴低吸双轨回归器预测次日开盘预期溢价；突破门槛>={BREAKOUT_MIN_SCORE:.1f}，低吸门槛>={DIPBUY_MIN_SCORE:.1f}；阴天/震荡时首阴低吸仅排序加{DIPBUY_SENTIMENT_BONUS:.0f}分，不改变原始综合评分；雷暴或大盘下跌且缩量时空仓；高位爆量、尾盘诱多直接剔除；近3日断头铡刀和上影线强过滤仅约束尾盘突破，首阴低吸豁免。",
        "market_gate": gate,
        "intraday_snapshot": intraday_snapshot,
        "rows": rows,
    }
    if cache_prediction:
        LATEST_TOP50_PATH.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def market_risk_gate(df: pd.DataFrame, indices: dict[str, dict[str, float | str]] | None = None) -> dict[str, Any]:
    if df.empty:
        return {
            "blocked": True,
            "reasons": ["候选池为空"],
            "market_up_rate": 0.0,
            "market_down_count": 0,
            "market_avg_change": 0.0,
            "market_amount": 0.0,
            "mode": "雷暴",
            "min_ai_win_rate": None,
            "indices": indices or {},
        }
    market_up_rate = float(pd.to_numeric(df.get("market_up_rate", 0), errors="coerce").dropna().iloc[0]) if "market_up_rate" in df.columns else 0.0
    market_down_count = int(pd.to_numeric(df.get("market_down_count", 0), errors="coerce").dropna().iloc[0]) if "market_down_count" in df.columns else 0
    market_avg_change = float(pd.to_numeric(df.get("market_avg_change", 0), errors="coerce").dropna().iloc[0]) if "market_avg_change" in df.columns else 0.0
    market_amount = float(pd.to_numeric(df.get("market_amount", 0), errors="coerce").dropna().iloc[0]) if "market_amount" in df.columns else 0.0
    reasons: list[str] = []
    safe_indices = indices or {}
    index_changes: list[float] = []
    index_above_ma20 = False
    for code, label in (("sh000001", "上证指数"), ("sh000852", "中证1000")):
        change = safe_indices.get(code, {}).get("change_pct")
        if isinstance(change, (int, float)):
            index_changes.append(float(change))
        if safe_indices.get(code, {}).get("above_ma20") is True:
            index_above_ma20 = True

    worst_index_change = min(index_changes) if index_changes else market_avg_change
    high_liquidity = market_amount >= HIGH_LIQUIDITY_AMOUNT
    low_liquidity = 0 < market_amount < LOW_LIQUIDITY_AMOUNT
    index_down = worst_index_change <= -0.5
    broad_down = market_down_count >= 3000
    thunder = worst_index_change <= -1.2 or market_down_count > 4200
    shrink_down = (index_down or market_avg_change <= -0.5) and low_liquidity
    trend_exempt = index_above_ma20 and not thunder

    if thunder:
        mode = "雷暴"
        blocked = True
        min_ai_win_rate: float | None = None
        if worst_index_change <= -1.2:
            reasons.append(f"指数最大跌幅 {worst_index_change:.2f}% <= -1.20%")
        if market_down_count > 4200:
            reasons.append(f"全市场下跌家数 {market_down_count} > 4200")
    elif shrink_down and not trend_exempt:
        mode = "缩量下跌"
        blocked = True
        min_ai_win_rate = None
        reasons.append(f"大盘下跌且总成交额 {market_amount / 100000000:.0f} 亿 < 7000 亿")
    elif (index_down or broad_down or market_avg_change <= -0.5) and not high_liquidity and not trend_exempt:
        mode = "阴天"
        blocked = False
        min_ai_win_rate = 75.0
        reasons.append("市场偏弱，仅保留高质量回归溢价信号")
    elif market_avg_change <= 0 or market_down_count >= 2500 or market_up_rate < 50:
        mode = "震荡"
        blocked = False
        min_ai_win_rate = 65.0
        reasons.append("市场震荡，首阴低吸排序获得情绪补偿")
    else:
        mode = "晴天"
        blocked = False
        min_ai_win_rate = 60.0
        if high_liquidity and (index_down or market_avg_change <= -0.5):
            reasons.append(f"总成交额 {market_amount / 100000000:.0f} 亿 >= 8000 亿，流动性豁免")
        if trend_exempt and (index_down or market_avg_change <= -0.5):
            reasons.append("指数仍在20日均线之上，趋势豁免")

    return {
        "blocked": blocked,
        "reasons": reasons,
        "mode": mode,
        "min_ai_win_rate": min_ai_win_rate,
        "market_up_rate": round(market_up_rate, 4),
        "market_down_count": market_down_count,
        "market_avg_change": round(market_avg_change, 4),
        "market_amount": round(market_amount, 2),
        "market_amount_yi": round(market_amount / 100000000, 2),
        "worst_index_change": round(float(worst_index_change), 4),
        "high_liquidity": high_liquidity,
        "low_liquidity": low_liquidity,
        "index_above_ma20": index_above_ma20,
        "indices": safe_indices,
    }


def _dynamic_win_rate_thresholds(df: pd.DataFrame) -> pd.Series:
    amount = _num(df, "market_amount")
    down_count = _num(df, "market_down_count")
    avg_change = _num(df, "market_avg_change")
    thunder = (avg_change <= -1.2) | (down_count > 4200)
    shrink_down = ((avg_change <= -0.5) | (down_count >= 3000)) & (amount > 0) & (amount < LOW_LIQUIDITY_AMOUNT)
    cloudy = ((avg_change <= -0.5) | (down_count >= 3000)) & ~(amount >= HIGH_LIQUIDITY_AMOUNT)
    thresholds = pd.Series(60.0, index=df.index)
    thresholds.loc[cloudy] = 75.0
    thresholds.loc[thunder | shrink_down] = np.inf
    return thresholds


def _add_market_context(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    daily = (
        out.groupby("date", dropna=False)
        .agg(
            market_up_rate=("涨跌幅", lambda values: float((pd.to_numeric(values, errors="coerce") > 0).mean() * 100)),
            market_down_count=("涨跌幅", lambda values: int((pd.to_numeric(values, errors="coerce") < 0).sum())),
            market_avg_change=("涨跌幅", "mean"),
            market_amount=("amount", "sum"),
        )
        .reset_index()
    )
    return out.merge(daily, on="date", how="left")


def _add_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    current = df.copy()
    current["_current_row"] = True
    combined = current
    if current["date"].nunique() == 1:
        history = _load_recent_history_for_codes(current["纯代码"].dropna().astype(str).unique().tolist(), str(current["date"].iloc[0]))
        if not history.empty:
            history["_current_row"] = False
            combined = pd.concat([history, current], ignore_index=True, sort=False)

    for col in ["最新价", "今开", "最高", "最低", "volume", "amount", "换手率"]:
        if col not in combined.columns:
            combined[col] = 0.0
        combined[col] = pd.to_numeric(combined[col], errors="coerce").fillna(0.0)
    combined["date_sort"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.sort_values(["纯代码", "date_sort", "_current_row"]).copy()
    group = combined.groupby("纯代码", sort=False)
    close = combined["最新价"]
    open_price = combined["今开"]
    high = combined["最高"]
    low = combined["最低"]
    volume = combined["volume"]
    amount = combined["amount"]
    turnover = combined["换手率"]
    pre_close = combined["昨收"]

    prev3_close = group["最新价"].shift(3)
    prev5_close = group["最新价"].shift(5)
    ma5 = group["最新价"].transform(lambda values: values.rolling(5, min_periods=3).mean())
    ma10 = group["最新价"].transform(lambda values: values.rolling(10, min_periods=5).mean())
    ma20 = group["最新价"].transform(lambda values: values.rolling(20, min_periods=10).mean())
    ma10_prev = ma10.groupby(combined["纯代码"], sort=False).shift(1)
    high5 = group["最高"].transform(lambda values: values.rolling(5, min_periods=3).max())
    high60 = group["最新价"].transform(lambda values: values.rolling(60, min_periods=20).max())
    avg_turn3 = group["换手率"].transform(lambda values: values.rolling(3, min_periods=2).mean())
    avg_vol5 = group["volume"].transform(lambda values: values.shift(1).rolling(5, min_periods=3).mean())
    avg_vol10 = group["volume"].transform(lambda values: values.shift(1).rolling(10, min_periods=5).mean())
    red3 = (close > open_price).astype(float).groupby(combined["纯代码"], sort=False).transform(lambda values: values.rolling(3, min_periods=2).mean() * 100)
    min_vol5 = group["volume"].transform(lambda values: values.rolling(5, min_periods=3).min())
    prev_close = group["最新价"].shift(1)
    prev_amount = group["amount"].shift(1)
    recent_3d_min_change = group["涨跌幅"].transform(lambda values: values.shift(1).rolling(3, min_periods=1).min())

    combined["5日累计涨幅"] = ((close / prev5_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["3日累计涨幅"] = ((close / prev3_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["5日均线乖离率"] = ((close / ma5 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["10日均线乖离率"] = ((close / ma10 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["20日均线乖离率"] = ((close / ma20 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["近5日最高涨幅"] = ((high5 / prev5_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["今日急跌度"] = ((low / prev_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["今日缩量比例"] = ((amount / prev_amount - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["均线趋势斜率"] = ((ma10 / ma10_prev - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["光脚大阴线惩罚度"] = ((close - low) / (high - low + 1e-5)).replace([np.inf, -np.inf], 0).fillna(0)
    daily_body_change = ((close - open_price) / pre_close.replace(0, np.nan) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["昨日实体涨跌幅"] = daily_body_change.groupby(combined["纯代码"], sort=False).shift(1).replace([np.inf, -np.inf], 0).fillna(0)
    combined["3日平均换手率"] = avg_turn3.replace([np.inf, -np.inf], 0).fillna(turnover)
    combined["5日量能堆积"] = (volume / avg_vol5).replace([np.inf, -np.inf], 0).fillna(0)
    combined["10日量比"] = (volume / avg_vol10).replace([np.inf, -np.inf], 0).fillna(0)
    combined["3日红盘比例"] = red3.replace([np.inf, -np.inf], 0).fillna(0)
    combined["5日地量标记"] = ((volume > 0) & (volume <= min_vol5)).astype(float)
    combined["缩量下跌标记"] = ((close < prev_close) & (volume < avg_vol5)).astype(float)
    combined["近3日断头铡刀标记"] = (recent_3d_min_change <= -7).astype(float)
    combined["60日高位比例"] = ((close / high60) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["高位爆量标记"] = ((combined["60日高位比例"] >= 97) & ((combined["量比"] > 3) | (combined["5日量能堆积"] > 3))).astype(float)
    for col in ["振幅换手比", "缩量大涨标记", "极端下影线标记"]:
        if col not in combined.columns:
            combined[col] = 0.0
    if "尾盘诱多标记" not in combined.columns:
        combined["尾盘诱多标记"] = 0.0
    combined[DIPBUY_TEMPORAL_FEATURE_COLS] = combined[DIPBUY_TEMPORAL_FEATURE_COLS].replace([np.inf, -np.inf], 0).fillna(0)

    result = combined[combined["_current_row"]].copy()
    return result.drop(columns=["_current_row", "date_sort"], errors="ignore")


def _load_recent_history_for_codes(codes: list[str], current_date: str) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    unique_codes = tuple(sorted({str(code).zfill(6) for code in codes if str(code).strip()}))
    if not unique_codes:
        return pd.DataFrame()
    history = _load_recent_history_from_parquet(unique_codes, current_date)
    if not history.empty:
        return history
    return _load_recent_history_from_db(list(unique_codes), current_date)


@lru_cache(maxsize=4)
def _load_recent_history_from_parquet(codes: tuple[str, ...], current_date: str) -> pd.DataFrame:
    if not codes or not DATA_DIR.exists():
        return pd.DataFrame()
    start_date = (pd.Timestamp(current_date) - pd.DateOffset(days=140)).strftime("%Y-%m-%d")
    columns = [
        "symbol", "code", "name", "date", "open", "high", "low", "close", "pre_close",
        "change_pct", "真实涨幅点数", "pctChg", "volume", "amount", "turnover", "turn", "volume_ratio", "量比"
    ]
    try:
        raw = pd.read_parquet(DATA_DIR, columns=columns)
    except Exception:
        return pd.DataFrame()
    if raw.empty or "date" not in raw.columns:
        return pd.DataFrame()
    raw["纯代码"] = raw.get("code", "").astype(str).str.extract(r"(\d{6})")[0]
    symbol_code = raw.get("symbol", "").astype(str).str.extract(r"(\d{6})")[0]
    raw["纯代码"] = raw["纯代码"].where(raw["纯代码"].notna(), symbol_code)
    raw = raw[raw["纯代码"].isin(codes)].copy()
    if raw.empty:
        return pd.DataFrame()
    raw["_date_sort"] = pd.to_datetime(raw["date"].astype(str), errors="coerce")
    start_ts = pd.Timestamp(start_date)
    current_ts = pd.Timestamp(current_date)
    raw = raw[(raw["_date_sort"] >= start_ts) & (raw["_date_sort"] < current_ts)].copy()
    if raw.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=raw.index)
    out["code"] = raw["纯代码"]
    out["name"] = raw["name"] if "name" in raw.columns else ""
    out["date"] = raw["_date_sort"].dt.strftime("%Y-%m-%d")
    out["open"] = _source_num(raw, "open")
    out["high"] = _source_num(raw, "high")
    out["low"] = _source_num(raw, "low")
    out["close"] = _source_num(raw, "close")
    out["pre_close"] = _source_num(raw, "pre_close")
    out["change_pct"] = _first_available_num(raw, ["change_pct", "真实涨幅点数", "pctChg"])
    out["volume"] = _source_num(raw, "volume")
    out["amount"] = _source_num(raw, "amount")
    out["turnover"] = _first_available_num(raw, ["turnover", "turn"])
    out["volume_ratio"] = _first_available_num(raw, ["volume_ratio", "量比"])
    return _format_history_frame(out)


def _read_kline_history_file(code: str, start_date: str, current_date: str) -> pd.DataFrame:
    path = DATA_DIR / f"{code}_daily.parquet"
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()
    if df.empty or "date" not in df.columns:
        return pd.DataFrame()
    df = df[(df["date"].astype(str) >= start_date) & (df["date"].astype(str) < current_date)].copy()
    if df.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=df.index)
    out["code"] = str(code)
    out["name"] = df["name"] if "name" in df.columns else ""
    out["date"] = df["date"].astype(str)
    out["open"] = _source_num(df, "open")
    out["high"] = _source_num(df, "high")
    out["low"] = _source_num(df, "low")
    out["close"] = _source_num(df, "close")
    out["pre_close"] = _source_num(df, "pre_close")
    out["change_pct"] = _first_available_num(df, ["change_pct", "真实涨幅点数", "pctChg"])
    out["volume"] = _source_num(df, "volume")
    out["amount"] = _source_num(df, "amount")
    out["turnover"] = _first_available_num(df, ["turnover", "turn"])
    out["volume_ratio"] = _first_available_num(df, ["volume_ratio", "量比"])
    return _format_history_frame(out)


def _source_num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)


def _first_available_num(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    for col in cols:
        if col in df.columns:
            return _source_num(df, col)
    return pd.Series(0.0, index=df.index)


def _load_recent_history_from_db(codes: list[str], current_date: str) -> pd.DataFrame:
    start_date = (pd.Timestamp(current_date) - pd.DateOffset(days=140)).strftime("%Y-%m-%d")
    placeholders = ",".join("?" for _ in codes)
    query = f"""
        SELECT code, name, date, open, high, low, close, pre_close, change_pct,
               volume, amount, turnover, volume_ratio
        FROM stock_daily
        WHERE code IN ({placeholders}) AND date >= ? AND date < ?
        ORDER BY code ASC, date ASC
    """
    try:
        with connect() as conn:
            history = pd.read_sql_query(query, conn, params=[*codes, start_date, current_date])
    except Exception:
        return pd.DataFrame()
    if history.empty:
        return history
    return _format_history_frame(history)


def _format_history_frame(history: pd.DataFrame) -> pd.DataFrame:
    history["纯代码"] = history["code"].astype(str).str.extract(r"(\d{6})")[0]
    history["名称"] = history["name"].fillna("")
    history["最新价"] = pd.to_numeric(history["close"], errors="coerce").fillna(0)
    history["涨跌幅"] = pd.to_numeric(history["change_pct"], errors="coerce").fillna(0)
    history["换手率"] = pd.to_numeric(history["turnover"], errors="coerce").fillna(0)
    history["量比"] = pd.to_numeric(history["volume_ratio"], errors="coerce").fillna(0)
    history["昨收"] = pd.to_numeric(history["pre_close"], errors="coerce").fillna(0)
    history["今开"] = pd.to_numeric(history["open"], errors="coerce").fillna(0)
    history["最高"] = pd.to_numeric(history["high"], errors="coerce").fillna(0)
    history["最低"] = pd.to_numeric(history["low"], errors="coerce").fillna(0)
    history["volume"] = pd.to_numeric(history["volume"], errors="coerce").fillna(0)
    history["amount"] = pd.to_numeric(history["amount"], errors="coerce").fillna(0)
    numeric_cols = history.select_dtypes(include=[np.number]).columns
    history[numeric_cols] = history[numeric_cols].replace([np.inf, -np.inf], 0).fillna(0)
    history["name"] = history["name"].fillna("")
    history["名称"] = history["名称"].fillna("")
    return history


def _persist_snapshot(snapshot: pd.DataFrame) -> None:
    try:
        upsert_daily_rows(snapshot, source="sina_snapshot")
    except Exception as exc:
        print(f"[radar] 快照后台入库失败: {exc}")


def _fallback_score(df: pd.DataFrame) -> pd.Series:
    momentum = df["涨跌幅"].clip(-5, 9.5) * 4
    liquidity = np.log1p(df["换手率"].clip(0, 30)) * 12
    shape = (df["实体比例"] - df["上影线比例"] * 0.7 + df["下影线比例"] * 0.35).clip(-12, 12)
    trend = (
        _num(df, "5日累计涨幅").clip(-12, 18) * 0.55
        + _num(df, "3日累计涨幅").clip(-8, 12) * 0.45
        - _num(df, "5日均线乖离率").clip(-15, 25).abs() * 0.22
        - _num(df, "20日均线乖离率").clip(-20, 30).abs() * 0.18
    )
    volume_signal = (_num(df, "5日量能堆积").clip(0, 4) - 1).clip(-1, 3) * 4 + _num(df, "3日红盘比例").clip(0, 100) * 0.06
    fake_pull_penalty = (
        (_num(df, "振幅换手比") - 3).clip(lower=0) * 1.8
        + _num(df, "缩量大涨标记") * 6
        + _num(df, "极端下影线标记") * 4
    )
    score = 45 + momentum + liquidity + shape + trend + volume_signal - fake_pull_penalty
    return pd.Series(score.clip(0, 99), index=df.index)


def _fallback_expected_premium(df: pd.DataFrame) -> pd.Series:
    momentum = df["涨跌幅"].clip(-5, 9.5) * 0.06
    liquidity = np.log1p(df["换手率"].clip(0, 30)) * 0.08
    trend = _num(df, "3日累计涨幅").clip(-8, 12) * 0.018 + _num(df, "5日累计涨幅").clip(-12, 18) * 0.01
    volume_signal = (_num(df, "5日量能堆积").clip(0, 4) - 1).clip(-1, 3) * 0.08
    shadow_penalty = df["上影线比例"].clip(0, 12) * 0.05
    amplitude_penalty = (df["日内振幅"].clip(0, 20) - 6).clip(lower=0) * 0.03
    fake_pull_penalty = (_num(df, "振幅换手比") - 3).clip(lower=0) * 0.05 + _num(df, "缩量大涨标记") * 0.18 + _num(df, "极端下影线标记") * 0.12
    market_penalty = (-0.5 - _num(df, "market_avg_change")).clip(lower=0) * 0.5
    expected = -0.25 + momentum + liquidity + trend + volume_signal - shadow_penalty - amplitude_penalty - fake_pull_penalty - market_penalty
    return pd.Series(expected.clip(-8, 8), index=df.index)


def _regression_signal_score(df: pd.DataFrame) -> pd.Series:
    premium_signal = (50 + _num(df, "预期溢价").clip(-5, 5) * 12).clip(0, 100)
    risk_adjust = (_num(df, "风险评分") - 50).clip(-50, 50) * 0.18
    liquidity_adjust = (_num(df, "流动性评分") - 50).clip(-50, 50) * 0.08
    return (premium_signal + risk_adjust + liquidity_adjust).clip(0, 99)


def _risk_score(df: pd.DataFrame) -> pd.Series:
    score = pd.Series(88.0, index=df.index)
    score -= (df["日内振幅"].clip(0, 25) * 1.3)
    score -= (df["上影线比例"].clip(0, 15) * 1.5)
    score -= ((df["涨跌幅"] - 7).clip(lower=0) * 3.0)
    score -= ((df["换手率"] - 18).clip(lower=0) * 0.9)
    score -= (3 - df["最新价"]).clip(lower=0) * 8.0
    score -= (-0.5 - _num(df, "market_avg_change")).clip(lower=0) * 20.0
    score -= (_num(df, "5日均线乖离率").abs() - 10).clip(lower=0) * 1.4
    score -= (_num(df, "20日均线乖离率").abs() - 18).clip(lower=0) * 1.2
    score -= ((_num(df, "60日高位比例") - 97).clip(lower=0) * (_num(df, "5日量能堆积") > 3).astype(float) * 12)
    score -= (_num(df, "振幅换手比") - 3).clip(lower=0) * 4.0
    score -= _num(df, "缩量大涨标记") * 10.0
    score -= _num(df, "极端下影线标记") * 8.0
    score -= _num(df, "近3日断头铡刀标记") * 18.0
    return score.clip(0, 100)


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _liquidity_score(df: pd.DataFrame) -> pd.Series:
    amount = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)
    amount_score = (np.log10(amount.clip(lower=1)) - 6).clip(0, 3) / 3 * 70
    turn = df["换手率"].clip(0, 20)
    turn_score = (100 - (turn - 6).abs() * 8).clip(0, 100) * 0.30
    return pd.Series((amount_score + turn_score).clip(0, 100), index=df.index)


def _repair_snapshot_volume_ratio(snapshot: pd.DataFrame) -> int:
    if snapshot.empty or "volume" not in snapshot.columns:
        return 0
    if "volume_ratio" not in snapshot.columns:
        snapshot["volume_ratio"] = 0.0
    snapshot["volume_ratio"] = pd.to_numeric(snapshot["volume_ratio"], errors="coerce").fillna(0.0)
    missing = snapshot["volume_ratio"] <= 0
    if not missing.any():
        return 0
    codes = snapshot.loc[missing, "code"].dropna().astype(str).tolist()
    if not codes:
        return 0
    placeholders = ",".join("?" for _ in codes)
    query = f"""
        SELECT code, date, volume
        FROM stock_daily
        WHERE code IN ({placeholders})
        ORDER BY code, date DESC
    """
    try:
        with connect() as conn:
            history = pd.read_sql_query(query, conn, params=codes)
    except Exception:
        return 0
    if history.empty:
        return 0
    avg_volume = (
        history.assign(volume=pd.to_numeric(history["volume"], errors="coerce"))
        .dropna(subset=["volume"])
        .groupby("code")["volume"]
        .apply(lambda values: values.head(5).mean())
    )
    current_volume = pd.to_numeric(snapshot.loc[missing, "volume"], errors="coerce")
    repaired = snapshot.loc[missing, "code"].map(avg_volume)
    valid = repaired.notna() & (repaired > 0) & current_volume.notna() & (current_volume > 0)
    if not valid.any():
        return 0
    target_index = repaired[valid].index
    snapshot.loc[target_index, "volume_ratio"] = current_volume.loc[target_index] / repaired.loc[target_index]
    return int(valid.sum())


def _row_to_api(row: pd.Series) -> dict[str, Any]:
    return {
        "code": str(row["纯代码"]),
        "name": str(row["名称"]),
        "strategy_type": str(row.get("strategy_type", BREAKOUT_STRATEGY_TYPE)),
        "price": round(float(row["最新价"]), 4),
        "change": round(float(row["涨跌幅"]), 4),
        "volume_ratio": round(float(row["量比"]), 4),
        "turnover": round(float(row["换手率"]), 4),
        "win_rate": round(float(row["AI胜率"]), 4),
        "expected_premium": round(float(row.get("预期溢价", 0)), 4),
        "risk_score": round(float(row.get("风险评分", 0)), 4),
        "liquidity_score": round(float(row.get("流动性评分", 0)), 4),
        "composite_score": round(float(row.get("综合评分", row["AI胜率"])), 4),
        "sort_score": round(float(row.get("排序评分", row.get("综合评分", row["AI胜率"]))), 4),
        "score_threshold": round(float(row.get("生产门槛", BREAKOUT_MIN_SCORE)), 4),
        "sentiment_bonus": round(float(row.get("情绪补偿分", 0)), 4),
        "market_gate_mode": str(row.get("market_gate_mode", "")),
        "tech_features": {
            "body_ratio": round(float(row["实体比例"]), 4),
            "upper_shadow": round(float(row["上影线比例"]), 4),
            "lower_shadow": round(float(row["下影线比例"]), 4),
            "amplitude": round(float(row["日内振幅"]), 4),
        },
        "trend_features": {
            "return_5d": round(float(row.get("5日累计涨幅", 0)), 4),
            "return_3d": round(float(row.get("3日累计涨幅", 0)), 4),
            "bias_5d": round(float(row.get("5日均线乖离率", 0)), 4),
            "bias_10d": round(float(row.get("10日均线乖离率", 0)), 4),
            "bias_20d": round(float(row.get("20日均线乖离率", 0)), 4),
            "max_gain_5d": round(float(row.get("近5日最高涨幅", 0)), 4),
            "intraday_flush": round(float(row.get("今日急跌度", 0)), 4),
            "amount_shrink_pct": round(float(row.get("今日缩量比例", 0)), 4),
            "ma_trend_slope": round(float(row.get("均线趋势斜率", 0)), 4),
            "bald_bear_ratio": round(float(row.get("光脚大阴线惩罚度", 0)), 4),
            "prev_body_change": round(float(row.get("昨日实体涨跌幅", 0)), 4),
            "avg_turnover_3d": round(float(row.get("3日平均换手率", 0)), 4),
            "volume_stack_5d": round(float(row.get("5日量能堆积", 0)), 4),
            "volume_ratio_10d": round(float(row.get("10日量比", 0)), 4),
            "red_ratio_3d": round(float(row.get("3日红盘比例", 0)), 4),
            "high_position_60d": round(float(row.get("60日高位比例", 0)), 4),
            "amplitude_turnover_ratio": round(float(row.get("振幅换手比", 0)), 4),
            "late_pull_pct": round(float(row.get("尾盘拉升幅度", 0)), 4),
            "is_5d_low_volume": bool(float(row.get("5日地量标记", 0)) >= 0.5),
            "is_shrink_down": bool(float(row.get("缩量下跌标记", 0)) >= 0.5),
            "is_low_volume_rally": bool(float(row.get("缩量大涨标记", 0)) >= 0.5),
            "is_extreme_lower_shadow": bool(float(row.get("极端下影线标记", 0)) >= 0.5),
            "is_recent_guillotine": bool(float(row.get("近3日断头铡刀标记", 0)) >= 0.5),
            "is_high_volume_trap": bool(float(row.get("高位爆量标记", 0)) >= 0.5),
            "is_late_pull_trap": bool(float(row.get("尾盘诱多标记", 0)) >= 0.5),
        },
        "market_context": {
            "up_rate": round(float(row.get("market_up_rate", 0)), 4),
            "down_count": int(float(row.get("market_down_count", 0) or 0)),
            "avg_change": round(float(row.get("market_avg_change", 0)), 4),
            "amount_yi": round(float(row.get("market_amount", 0) or 0) / 100000000, 4),
        },
    }
