from __future__ import annotations

import json
import threading
from datetime import datetime
from functools import lru_cache
from typing import Any

import numpy as np
import pandas as pd

from quant_core.config import (
    BREAKOUT_MIN_SCORE,
    DATA_DIR,
    DIPBUY_MIN_SCORE,
    DIPBUY_PREMIUM_MODEL_PATH,
    GLOBAL_DAILY_META_PATH,
    GLOBAL_DAILY_MODEL_PATH,
    GLOBAL_MIN_SCORE,
    LATEST_TOP50_PATH,
    MAIN_WAVE_MIN_SCORE,
    MAIN_WAVE_MODEL_PATH,
    MODEL_PATH,
    PREMIUM_MODEL_PATH,
    PROFIT_TARGET_PCT,
    REVERSAL_MIN_SCORE,
    REVERSAL_MODEL_PATH,
)
from quant_core.data_pipeline.intraday_snapshot import attach_late_pull_trap
from quant_core.data_pipeline.market import fetch_market_indices, fetch_sina_snapshot
from quant_core.engine.daily_factor_factory import THEME_FACTOR_COLUMNS, generate_daily_factors
from quant_core.storage import connect, save_prediction_snapshot, upsert_daily_rows


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
REVERSAL_STRATEGY_TYPE = "中线超跌反转"
MAIN_WAVE_STRATEGY_TYPE = "右侧主升浪"
GLOBAL_MOMENTUM_STRATEGY_TYPE = "全局动量狙击"
SWING_STRATEGY_TYPES = {REVERSAL_STRATEGY_TYPE, MAIN_WAVE_STRATEGY_TYPE, GLOBAL_MOMENTUM_STRATEGY_TYPE}
STRATEGY_PRIORITY = {
    GLOBAL_MOMENTUM_STRATEGY_TYPE: 4,
    MAIN_WAVE_STRATEGY_TYPE: 3,
    REVERSAL_STRATEGY_TYPE: 2,
    BREAKOUT_STRATEGY_TYPE: 1,
    DIPBUY_STRATEGY_TYPE: 0,
}
PRODUCTION_OUTPUT_STRATEGIES = [GLOBAL_MOMENTUM_STRATEGY_TYPE, MAIN_WAVE_STRATEGY_TYPE, REVERSAL_STRATEGY_TYPE, BREAKOUT_STRATEGY_TYPE]
REVERSAL_FEATURE_COLS = [
    "body_pct",
    "upper_shadow_pct",
    "lower_shadow_pct",
    "amplitude_pct",
    "change_pct",
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "ma5_bias",
    "ma10_bias",
    "ma20_bias",
    "ma60_bias_prev",
    "drawdown_60d",
    "low_position_60d",
    "min_volume_5d_ratio_to_60d",
    "volume_ratio_to_10d",
    "volume_ratio_to_60d",
    "ma_convergence_pct",
    "amount_ratio_to_10d",
    "turnover",
]
MAIN_WAVE_FEATURE_COLS = [
    "body_pct",
    "upper_shadow_pct",
    "lower_shadow_pct",
    "amplitude_pct",
    "change_pct",
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "ma5_bias",
    "ma10_bias",
    "ma20_bias",
    "ma60_bias",
    "ma20_ma60_spread",
    "pullback_from_60d_high",
    "contraction_amplitude_5d",
    "prev_volume_ratio_to_5d",
    "breakout_strength",
    "volume_burst_ratio",
    "volume_ratio_to_20d",
    "amount_ratio_to_20d",
    "turnover",
]
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
LIVE_VOLUME_EXTRAPOLATION_FACTOR = 1.05
LIVE_NEAR_LIMIT_CHANGE_PCT = 8.5
GLOBAL_MOMENTUM_MAX_LIVE_CHANGE_PCT = 9.0
ABSOLUTE_BOTTOM_PROBA = 0.55
PRODUCTION_MAX_PICKS_PER_STRATEGY = 3
RISK_WARNING_DYNAMIC_FLOOR = "⚠️ 动态下探: 逆势相对龙头，注意控制仓位"
KELLY_WIN_LOSS_RATIO = 1.5
HALF_KELLY_FACTOR = 0.5
BASE_POSITION_MIN = 0.10
BASE_POSITION_MAX = 0.30
DYNAMIC_FLOOR_POSITION = 0.05


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


@lru_cache(maxsize=1)
def _load_reversal_model():
    if not REVERSAL_MODEL_PATH.exists():
        return None, f"找不到中线反转模型文件: {REVERSAL_MODEL_PATH}"
    try:
        import xgboost as xgb

        model = xgb.XGBRegressor()
        model.load_model(str(REVERSAL_MODEL_PATH))
        return model, None
    except Exception as exc:
        return None, f"中线反转模型加载失败: {exc}"


@lru_cache(maxsize=1)
def _load_main_wave_model():
    if not MAIN_WAVE_MODEL_PATH.exists():
        return None, f"找不到右侧主升浪模型文件: {MAIN_WAVE_MODEL_PATH}"
    try:
        import xgboost as xgb

        model = xgb.XGBRegressor()
        model.load_model(str(MAIN_WAVE_MODEL_PATH))
        return model, None
    except Exception as exc:
        return None, f"右侧主升浪模型加载失败: {exc}"


@lru_cache(maxsize=1)
def _load_global_daily_model():
    if not GLOBAL_DAILY_MODEL_PATH.exists():
        return None, f"找不到全局日线模型文件: {GLOBAL_DAILY_MODEL_PATH}", []
    if not GLOBAL_DAILY_META_PATH.exists():
        return None, f"找不到全局日线模型元数据: {GLOBAL_DAILY_META_PATH}", []
    try:
        import xgboost as xgb

        model = xgb.XGBClassifier()
        model.load_model(str(GLOBAL_DAILY_MODEL_PATH))
        meta = json.loads(GLOBAL_DAILY_META_PATH.read_text(encoding="utf-8"))
        feature_cols = list(meta.get("feature_columns") or [])
        if not feature_cols:
            return None, f"全局日线模型元数据缺少 feature_columns: {GLOBAL_DAILY_META_PATH}", []
        return model, None, feature_cols
    except Exception as exc:
        return None, f"全局日线模型加载失败: {exc}", []


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
    if "准涨停未封板标记" not in out.columns:
        out["准涨停未封板标记"] = 0.0
    return out


def score_candidates(df: pd.DataFrame, production_global_hard_filter: bool = False) -> tuple[pd.DataFrame, str]:
    scored = df.copy()
    for col in dict.fromkeys([*FEATURE_COLS, *DIPBUY_FEATURE_COLS, *REVERSAL_FEATURE_COLS, *MAIN_WAVE_FEATURE_COLS]):
        if col not in scored.columns:
            scored[col] = 0.0
        scored[col] = pd.to_numeric(scored[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)
    if scored.empty:
        return scored, "ready"

    scored["strategy_type"] = BREAKOUT_STRATEGY_TYPE
    scored["预期溢价"] = _fallback_expected_premium(scored)
    reversal_mask = _reversal_physical_mask(scored)
    main_wave_mask = _main_wave_physical_mask(scored)
    _log_dipbuy_diagnostics(scored)
    dipbuy_mask = _dipbuy_physical_mask(scored) & ~reversal_mask & ~main_wave_mask
    premium_model, premium_error = _load_premium_model()
    dipbuy_model, dipbuy_error = _load_dipbuy_premium_model()
    reversal_model, reversal_error = _load_reversal_model()
    main_wave_model, main_wave_error = _load_main_wave_model()
    status_parts: list[str] = []

    if premium_model is not None:
        try:
            breakout_index = scored.index[~dipbuy_mask & ~reversal_mask & ~main_wave_mask]
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

    if reversal_mask.any():
        scored.loc[reversal_mask, "strategy_type"] = REVERSAL_STRATEGY_TYPE
        if reversal_model is not None:
            try:
                scored.loc[reversal_mask, "预期溢价"] = reversal_model.predict(scored.loc[reversal_mask, REVERSAL_FEATURE_COLS].values)
                status_parts.append(f"reversal_t3_regressor_ready:{int(reversal_mask.sum())}")
            except Exception as exc:
                status_parts.append(f"中线超跌反转模型失败，已降级规则估算: {exc}")
        else:
            status_parts.append(reversal_error or "reversal_model_unavailable")
    else:
        status_parts.append("reversal_no_physical_match")

    if main_wave_mask.any():
        main_wave_index = scored.index[main_wave_mask]
        if main_wave_model is not None:
            try:
                main_wave_pred = pd.Series(
                    main_wave_model.predict(scored.loc[main_wave_index, MAIN_WAVE_FEATURE_COLS].values),
                    index=main_wave_index,
                )
                current_strategy = scored.loc[main_wave_index, "strategy_type"].fillna(BREAKOUT_STRATEGY_TYPE)
                current_pred = pd.to_numeric(scored.loc[main_wave_index, "预期溢价"], errors="coerce").fillna(-999)
                update_index = main_wave_index[(~current_strategy.eq(REVERSAL_STRATEGY_TYPE)) | (main_wave_pred >= current_pred)]
                scored.loc[update_index, "strategy_type"] = MAIN_WAVE_STRATEGY_TYPE
                scored.loc[update_index, "预期溢价"] = main_wave_pred.loc[update_index]
                status_parts.append(f"main_wave_t3_regressor_ready:{int(main_wave_mask.sum())}; selected:{len(update_index)}")
            except Exception as exc:
                status_parts.append(f"右侧主升浪模型失败，已降级规则估算: {exc}")
        else:
            non_reversal_index = main_wave_index[~scored.loc[main_wave_index, "strategy_type"].eq(REVERSAL_STRATEGY_TYPE)]
            scored.loc[non_reversal_index, "strategy_type"] = MAIN_WAVE_STRATEGY_TYPE
            status_parts.append(main_wave_error or "main_wave_model_unavailable")
    else:
        status_parts.append("main_wave_no_physical_match")

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
    is_swing = scored["strategy_type"].isin(SWING_STRATEGY_TYPES)
    scored.loc[is_dipbuy, "综合评分"] = (
        dipbuy_premium_score.loc[is_dipbuy] * 0.85
        + scored.loc[is_dipbuy, "AI胜率"] * 0.10
        + scored.loc[is_dipbuy, "流动性评分"] * 0.05
    ).clip(0, 100)
    scored.loc[is_swing, "综合评分"] = _num(scored.loc[is_swing], "预期溢价").clip(-20, 30)
    scored, global_status = _append_global_momentum_candidates(
        scored,
        production_hard_filter=production_global_hard_filter,
    )
    status_parts.append(global_status)
    model_status = "; ".join(status_parts) if status_parts else "ready"
    return scored, model_status


def _append_global_momentum_candidates(scored: pd.DataFrame, production_hard_filter: bool = False) -> tuple[pd.DataFrame, str]:
    """Add the global daily XGBoost model as the fourth independent legion."""
    if scored.empty:
        return scored, "global_momentum_empty_pool"
    model, error, feature_cols = _load_global_daily_model()
    if model is None:
        return scored, error or "global_momentum_model_unavailable"

    probabilities: dict[Any, float] = {}
    errors = 0
    for idx, row in scored.iterrows():
        try:
            factors = generate_daily_factors(_stitch_global_daily_frame_from_live_row(row))
            if factors.empty:
                errors += 1
                continue
            latest = factors.tail(1)
            aligned = _align_global_daily_features(latest, feature_cols)
            probabilities[idx] = float(model.predict_proba(aligned)[:, 1][0])
        except Exception:
            errors += 1

    if not probabilities:
        return scored, f"global_momentum_no_valid_factor; errors:{errors}"

    prob = pd.Series(probabilities, dtype="float64")
    hard_filtered = 0
    eligible_prob = prob
    if production_hard_filter:
        hard_mask = _global_momentum_production_hard_filter_mask(scored.loc[prob.index])
        hard_filtered = int((~hard_mask).sum())
        eligible_prob = prob.loc[hard_mask[hard_mask].index]
    filter_status = f"; hard_filtered:{hard_filtered}" if production_hard_filter else ""
    if len(eligible_prob) == 0:
        base = scored.copy()
        base["global_probability"] = eligible_prob.reindex(base.index)
        return base, f"global_momentum_ready:0/{len(prob)}{filter_status}; errors:{errors}"

    global_rows = scored.loc[eligible_prob.index].copy()
    global_rows["strategy_type"] = GLOBAL_MOMENTUM_STRATEGY_TYPE
    global_rows["global_probability"] = eligible_prob
    global_rows["global_probability_pct"] = global_rows["global_probability"] * 100
    global_rows["预期溢价"] = _global_expected_t3_pct(global_rows["global_probability"])
    global_rows["AI胜率"] = global_rows["global_probability_pct"]
    global_rows["综合评分"] = global_rows["global_probability_pct"]
    global_rows["排序评分"] = global_rows["global_probability_pct"]
    global_rows["生产门槛"] = GLOBAL_MIN_SCORE
    global_rows["策略优先级"] = STRATEGY_PRIORITY[GLOBAL_MOMENTUM_STRATEGY_TYPE]
    min_count = int((eligible_prob >= GLOBAL_MIN_SCORE).sum())
    out = pd.concat([scored, global_rows], ignore_index=True, sort=False)
    return out, f"global_momentum_ready:{min_count}/{len(prob)}; eligible:{len(global_rows)}{filter_status}; errors:{errors}"


def _global_momentum_production_hard_filter_mask(df: pd.DataFrame) -> pd.Series:
    """Production-only buyability wall for global momentum picks."""
    if df.empty:
        return pd.Series(False, index=df.index)
    code_source = df["纯代码"] if "纯代码" in df.columns else df.get("code", pd.Series("", index=df.index))
    code_text = code_source.fillna("").astype(str)
    code = code_text.str.extract(r"(\d{6})", expand=False).fillna("")
    code = code.where(code.ne(""), code_text.str.zfill(6).str[-6:])
    name_source = df["名称"] if "名称" in df.columns else df.get("name", pd.Series("", index=df.index))
    name = name_source.fillna("").astype(str).str.upper()
    live_change_pct = _global_momentum_live_change_pct(df)

    mainboard_mask = code.str.startswith(("00", "60"), na=False)
    non_st_mask = ~name.str.contains("ST", regex=False, na=False)
    tradable_price_mask = live_change_pct.fillna(np.inf) < GLOBAL_MOMENTUM_MAX_LIVE_CHANGE_PCT
    return (mainboard_mask & non_st_mask & tradable_price_mask).fillna(False)


def _global_momentum_live_change_pct(df: pd.DataFrame) -> pd.Series:
    for col in ("涨跌幅", "pctChg", "change_pct", "change"):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")
    if {"最新价", "昨收"}.issubset(df.columns):
        latest = pd.to_numeric(df["最新价"], errors="coerce")
        pre_close = pd.to_numeric(df["昨收"], errors="coerce")
        return ((latest - pre_close) / pre_close * 100).where(pre_close > 0)
    if {"close", "pre_close"}.issubset(df.columns):
        latest = pd.to_numeric(df["close"], errors="coerce")
        pre_close = pd.to_numeric(df["pre_close"], errors="coerce")
        return ((latest - pre_close) / pre_close * 100).where(pre_close > 0)
    return pd.Series(np.nan, index=df.index)


def _stitch_global_daily_frame_from_live_row(row: pd.Series, history_days: int = 90) -> pd.DataFrame:
    code = str(row.get("纯代码") or row.get("code") or "").zfill(6)[-6:]
    path = DATA_DIR / f"{code}_daily.parquet"
    if not path.exists():
        raise FileNotFoundError(f"缺少本地日线: {path}")
    history = pd.read_parquet(path).tail(history_days).copy()
    if history.empty:
        raise ValueError(f"{code} 本地日线为空")
    if "datetime" in history.columns:
        history_dt = pd.to_datetime(history["datetime"], errors="coerce")
    else:
        history_dt = pd.to_datetime(history["date"].astype(str), errors="coerce")
    live_date = pd.to_datetime(str(row.get("date") or datetime.now().date().isoformat()), errors="coerce")
    if pd.isna(live_date):
        live_date = pd.Timestamp(datetime.now().date())
    history = history[history_dt.dt.normalize() != live_date.normalize()].copy()

    last = history.iloc[-1].copy() if not history.empty else pd.Series(dtype="object")
    live = last.copy()
    live["date"] = live_date.strftime("%Y%m%d")
    live["datetime"] = live_date
    live["symbol"] = code
    live["code"] = code
    live["name"] = str(row.get("名称") or row.get("name") or code)
    live["open"] = float(row.get("今开") or row.get("open") or row.get("最新价") or 0)
    live["high"] = float(row.get("最高") or row.get("high") or row.get("最新价") or 0)
    live["low"] = float(row.get("最低") or row.get("low") or row.get("最新价") or 0)
    live["close"] = float(row.get("最新价") or row.get("close") or 0)
    live["volume"] = float(row.get("volume") or 0)
    live["amount"] = float(row.get("amount") or 0)
    live["pre_close"] = float(row.get("昨收") or row.get("pre_close") or 0)
    live["pctChg"] = float(row.get("涨跌幅") or row.get("change_pct") or 0)
    live["change_pct"] = float(row.get("涨跌幅") or row.get("change_pct") or 0)
    live["turn"] = float(row.get("换手率") or row.get("turnover") or 0)
    live["turnover"] = float(row.get("换手率") or row.get("turnover") or 0)
    live["volume_ratio"] = float(row.get("量比") or row.get("volume_ratio") or 0)
    return pd.concat([history, pd.DataFrame([live])], ignore_index=True, sort=False)


def _align_global_daily_features(frame: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in feature_cols:
        if col not in out.columns:
            out[col] = np.nan if col in THEME_FACTOR_COLUMNS else 0.0
    aligned = out[feature_cols].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    fillable_cols = [col for col in aligned.columns if col not in THEME_FACTOR_COLUMNS]
    if fillable_cols:
        aligned[fillable_cols] = aligned[fillable_cols].ffill().fillna(0.0)
    return aligned.astype("float32", copy=False)


def _global_expected_t3_pct(probability: pd.Series) -> pd.Series:
    prob = pd.to_numeric(probability, errors="coerce").fillna(0.0)
    return (4.0 + (prob - GLOBAL_MIN_SCORE).clip(lower=0) * 20.0).clip(4.0, 9.0)


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


def _reversal_physical_mask(df: pd.DataFrame) -> pd.Series:
    return (
        (_num(df, "ma60_bias_prev") < 0)
        & (_num(df, "drawdown_60d") <= -20.0)
        & (_num(df, "min_volume_5d_ratio_to_60d") < 0.4)
        & (_num(df, "body_pct") >= 5.0)
        & (_num(df, "ma5_bias") > 0)
        & (_num(df, "ma10_bias") > 0)
        & (_num(df, "ma20_bias") <= 5.0)
        & (_num(df, "ma30_bias") <= 5.0)
        & (_num(df, "ma30_slope") >= -0.005)
        & (_num(df, "volume_ratio_to_10d") >= 2.0)
    ).fillna(False)


def _main_wave_physical_mask(df: pd.DataFrame) -> pd.Series:
    return (
        (_num(df, "ma20_ma60_spread_prev") > 0)
        & (_num(df, "pullback_from_60d_high") >= -15.0)
        & (_num(df, "contraction_amplitude_5d") <= 15.0)
        & (_num(df, "prev_volume_ratio_to_5d") < 1.0)
        & (_num(df, "breakout_strength") > 0)
        & (_num(df, "body_pct") >= 3.5)
        & (_num(df, "volume_burst_ratio") >= 1.15)
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
        is_swing = filtered.get("strategy_type", "").isin(SWING_STRATEGY_TYPES) if "strategy_type" in filtered.columns else pd.Series(False, index=filtered.index)
        filtered = filtered[(is_swing) | (filtered["涨跌幅"] < 7)].copy()
    if "准涨停未封板标记" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["准涨停未封板标记"], errors="coerce").fillna(0) < 0.5].copy()
    if "上影线比例" in filtered.columns:
        is_dipbuy = filtered.get("strategy_type", "").eq(DIPBUY_STRATEGY_TYPE) if "strategy_type" in filtered.columns else pd.Series(False, index=filtered.index)
        is_swing = filtered.get("strategy_type", "").isin(SWING_STRATEGY_TYPES) if "strategy_type" in filtered.columns else pd.Series(False, index=filtered.index)
        filtered = filtered[(is_dipbuy) | (is_swing) | (filtered["上影线比例"] < 2)].copy()
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
        is_swing = filtered.get("strategy_type", "").isin(SWING_STRATEGY_TYPES) if "strategy_type" in filtered.columns else pd.Series(False, index=filtered.index)
        filtered = filtered[
            (is_dipbuy)
            | (is_swing)
            | (pd.to_numeric(filtered["近3日断头铡刀标记"], errors="coerce").fillna(0) < 0.5)
        ].copy()
    return apply_strategy_score_gate(filtered, gate)


def _strategy_min_score(strategy_type: str) -> float:
    if strategy_type == DIPBUY_STRATEGY_TYPE:
        return DIPBUY_MIN_SCORE
    if strategy_type == REVERSAL_STRATEGY_TYPE:
        return REVERSAL_MIN_SCORE
    if strategy_type == MAIN_WAVE_STRATEGY_TYPE:
        return MAIN_WAVE_MIN_SCORE
    if strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE:
        return GLOBAL_MIN_SCORE
    return BREAKOUT_MIN_SCORE


def _strategy_selection_score(df: pd.DataFrame, strategy_type: str) -> pd.Series:
    if strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE and "global_probability" in df.columns:
        return _num(df, "global_probability")
    return _num(df, "综合评分")


def apply_strategy_score_gate(df: pd.DataFrame, gate: dict[str, Any] | None = None) -> pd.DataFrame:
    if df.empty or "综合评分" not in df.columns:
        return df
    filtered = df.copy()
    filtered["strategy_type"] = filtered.get("strategy_type", BREAKOUT_STRATEGY_TYPE)
    filtered["strategy_type"] = filtered["strategy_type"].fillna(BREAKOUT_STRATEGY_TYPE)
    threshold = filtered["strategy_type"].map(lambda item: _strategy_min_score(str(item))).astype(float)
    filtered["生产门槛"] = threshold
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
    is_swing = scored["strategy_type"].isin(SWING_STRATEGY_TYPES)
    is_global = scored["strategy_type"].eq(GLOBAL_MOMENTUM_STRATEGY_TYPE)
    bonus = pd.Series(0.0, index=scored.index)
    bonus.loc[is_dipbuy & modes.isin(["阴天", "震荡"])] = DIPBUY_SENTIMENT_BONUS
    scored["情绪补偿分"] = bonus
    scored["排序评分"] = (base_score + bonus).clip(0, 110)
    scored.loc[is_swing, "排序评分"] = (50 + _num(scored.loc[is_swing], "综合评分").clip(-5, 15) * 5).clip(0, 110)
    scored.loc[is_global, "排序评分"] = _num(scored.loc[is_global], "global_probability_pct").clip(0, 100)
    scored["策略优先级"] = scored["strategy_type"].map(STRATEGY_PRIORITY).fillna(1).astype(float)
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
    raw_snapshot = fetch_sina_snapshot()
    if raw_snapshot.empty:
        raise RuntimeError("实时行情源返回空数据")
    snapshot = _prepare_live_inference_snapshot(raw_snapshot)
    _repair_snapshot_volume_ratio(snapshot)
    market_indices = fetch_market_indices()

    if persist_snapshot and async_persist:
        threading.Thread(target=_persist_snapshot, args=(raw_snapshot,), daemon=True).start()
    elif persist_snapshot:
        upsert_daily_rows(raw_snapshot, source="sina_snapshot")

    df = build_features(snapshot)
    if df.empty:
        return {"created_at": datetime.now().isoformat(timespec="seconds"), "model_status": "ready", "rows": []}
    df, intraday_snapshot = attach_late_pull_trap(df)

    df, model_status = score_candidates(df, production_global_hard_filter=True)
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

    if df.empty:
        payload = {
            "id": None,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "model_status": f"{model_status}; 生产过滤后无合格标的",
        "strategy": f"生产策略：实时 14:50 快照以最新价平替收盘价，成交量/成交额按 {LIVE_VOLUME_EXTRAPOLATION_FACTOR:.2f} 外推；准涨停未封板、高位爆量、尾盘诱多直接剔除；当前没有股票满足四轨动态底线。",
            "market_gate": gate,
            "intraday_snapshot": intraday_snapshot,
            "rows": [],
        }
        if cache_prediction:
            LATEST_TOP50_PATH.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
    df = select_strategy_top_picks(df, limit_per_strategy=PRODUCTION_MAX_PICKS_PER_STRATEGY)
    if limit > 0:
        df = df.head(min(len(df), max(int(limit), len(PRODUCTION_OUTPUT_STRATEGIES))))
    rows = [_row_to_api(row) for _, row in df.iterrows()]
    snapshot_id = save_prediction_snapshot("quad_xgboost_regressor" if "regressor_ready" in model_status else "rule_fallback", rows) if cache_prediction else None
    payload = {
        "id": snapshot_id,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "model_status": model_status,
        "strategy": f"生产策略：四大核心军团分档出票，每个策略基准线最多 Top{PRODUCTION_MAX_PICKS_PER_STRATEGY}，无达标票时按合规池 99 分位动态下探 1 只并提示风偏；尾盘突破预测次日开盘预期溢价，中线超跌反转/右侧主升浪/全局动量狙击复用 T+3 波段收益口径；实时 14:50 快照以最新价平替收盘价，成交量/成交额按 {LIVE_VOLUME_EXTRAPOLATION_FACTOR:.2f} 外推；突破门槛>={BREAKOUT_MIN_SCORE:.1f}，反转门槛>={REVERSAL_MIN_SCORE:.1f}%，主升浪门槛>={MAIN_WAVE_MIN_SCORE:.1f}%，全局狙击概率>={GLOBAL_MIN_SCORE:.2f}，绝对安全底线>={ABSOLUTE_BOTTOM_PROBA:.2f}；雷暴或大盘下跌且缩量时空仓；高位爆量、尾盘诱多直接剔除；近3日断头铡刀和上影线强过滤仅约束尾盘突破，波段策略豁免。",
        "market_gate": gate,
        "intraday_snapshot": intraday_snapshot,
        "rows": rows,
    }
    if cache_prediction:
        LATEST_TOP50_PATH.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def select_strategy_top_picks(df: pd.DataFrame, limit_per_strategy: int = PRODUCTION_MAX_PICKS_PER_STRATEGY) -> pd.DataFrame:
    """Return tiered production picks per strategy, avoiding duplicate stocks per day."""
    if df.empty:
        return df
    selected: list[pd.DataFrame] = []
    used_codes: set[str] = set()
    selection_limit = max(1, int(limit_per_strategy))
    for strategy_type in PRODUCTION_OUTPUT_STRATEGIES:
        strategy_series = df["strategy_type"] if "strategy_type" in df.columns else pd.Series(BREAKOUT_STRATEGY_TYPE, index=df.index)
        pool = df[strategy_series.eq(strategy_type)].copy()
        if pool.empty:
            continue
        pool = pool.sort_values(["排序评分", "预期溢价", "综合评分"], ascending=[False, False, False])
        score = _strategy_selection_score(pool, strategy_type).replace([np.inf, -np.inf], 0).fillna(0)
        legal_pool = pool.assign(score=score)
        min_score = _strategy_min_score(strategy_type)
        dynamic_floor = max(ABSOLUTE_BOTTOM_PROBA, float(legal_pool["score"].quantile(0.99)))
        print(
            f"[AdaptiveFloor] strategy={strategy_type} legal_pool={len(legal_pool)} "
            f"min_score={min_score:.4f} dynamic_floor={dynamic_floor:.4f} "
            f"absolute_bottom={ABSOLUTE_BOTTOM_PROBA:.4f}"
        )
        qualified_pool = legal_pool[legal_pool["score"] >= min_score].copy()
        if not qualified_pool.empty:
            kept = _take_unique_pick_indices(qualified_pool, used_codes, selection_limit)
            if kept:
                picked = legal_pool.loc[kept].copy()
                picked["risk_warning"] = ""
                picked["selection_tier"] = "base"
                picked["dynamic_floor"] = dynamic_floor
                picked["下探底线"] = dynamic_floor
                selected.append(picked)
            continue

        fallback_pool = legal_pool[~legal_pool.apply(lambda row: _pick_code(row) in used_codes, axis=1)].copy()
        if fallback_pool.empty:
            continue
        top_idx = fallback_pool.index[0]
        top_score = float(score.loc[top_idx])
        if top_score >= dynamic_floor:
            picked = legal_pool.loc[[top_idx]].copy()
            picked["risk_warning"] = RISK_WARNING_DYNAMIC_FLOOR
            picked["selection_tier"] = "dynamic_floor"
            picked["dynamic_floor"] = dynamic_floor
            picked["下探底线"] = dynamic_floor
            code = _pick_code(picked.iloc[0])
            if code:
                used_codes.add(code)
            selected.append(picked)
    if not selected:
        return df.iloc[0:0].copy()
    out = pd.concat(selected, ignore_index=False)
    out["策略优先级"] = out.get("策略优先级", out["strategy_type"].map(STRATEGY_PRIORITY).fillna(1)).astype(float)
    return out.sort_values(["策略优先级", "排序评分", "预期溢价", "综合评分"], ascending=[False, False, False, False])


def _pick_code(row: pd.Series) -> str:
    return str(row.get("纯代码") or row.get("code") or "")


def _take_unique_pick_indices(pool: pd.DataFrame, used_codes: set[str], limit: int) -> list[Any]:
    kept: list[Any] = []
    for idx, row in pool.iterrows():
        code = _pick_code(row)
        if code and code in used_codes:
            continue
        kept.append(idx)
        if code:
            used_codes.add(code)
        if len(kept) >= limit:
            break
    return kept


def calculate_suggested_position(probability: float, tier: str) -> float:
    if str(tier or "") == "dynamic_floor":
        return DYNAMIC_FLOOR_POSITION
    probability = float(pd.to_numeric(pd.Series([probability]), errors="coerce").fillna(0.0).iloc[0])
    probability = float(np.clip(probability, 0.0, 1.0))
    kelly_fraction = probability - (1.0 - probability) / KELLY_WIN_LOSS_RATIO
    half_kelly = max(0.0, kelly_fraction * HALF_KELLY_FACTOR)
    return float(np.clip(half_kelly, BASE_POSITION_MIN, BASE_POSITION_MAX))


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


def _prepare_live_inference_snapshot(snapshot: pd.DataFrame) -> pd.DataFrame:
    """Normalize the 14:50 snapshot into a 15:00-compatible inference proxy."""
    out = snapshot.copy()
    if out.empty:
        return out

    current_price = _first_existing_num(out, ["current_price", "now", "price", "trade", "close"])
    if "close" not in out.columns:
        out["close"] = 0.0
    valid_price = current_price.notna() & (current_price > 0)
    out.loc[valid_price, "close"] = current_price.loc[valid_price]
    for col in ["open", "high", "low", "pre_close", "close"]:
        if col not in out.columns:
            out[col] = 0.0
        out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0.0)
    out["high"] = out[["high", "open", "close"]].max(axis=1)
    positive_low = out["low"] > 0
    out.loc[~positive_low, "low"] = out.loc[~positive_low, ["open", "close"]].min(axis=1)
    out["low"] = out[["low", "open", "close"]].min(axis=1)
    original_change = _num(out, "change_pct")
    out["change_pct"] = ((out["close"] / out["pre_close"].replace(0, np.nan) - 1) * 100).replace([np.inf, -np.inf], 0).fillna(original_change)

    for col in ["volume", "amount", "turnover", "volume_ratio"]:
        if col not in out.columns:
            continue
        out[col] = (
            pd.to_numeric(out[col], errors="coerce")
            .replace([np.inf, -np.inf], 0)
            .fillna(0.0)
            * LIVE_VOLUME_EXTRAPOLATION_FACTOR
        )
    change = pd.to_numeric(out["change_pct"], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0.0).round(4)
    main_board_near_limit = change >= LIVE_NEAR_LIMIT_CHANGE_PCT
    main_board_sealed = change >= 9.5
    out["准涨停未封板标记"] = (main_board_near_limit & ~main_board_sealed).astype(float)
    out["live_proxy_factor"] = LIVE_VOLUME_EXTRAPOLATION_FACTOR
    return out


def _first_existing_num(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    result = pd.Series(np.nan, index=df.index, dtype="float64")
    for col in cols:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        result = result.where(result.notna(), values)
    return result


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
    prev10_close = group["最新价"].shift(10)
    prev20_close = group["最新价"].shift(20)
    prev60_close = group["最新价"].shift(60)
    ma5 = group["最新价"].transform(lambda values: values.rolling(5, min_periods=3).mean())
    ma10 = group["最新价"].transform(lambda values: values.rolling(10, min_periods=5).mean())
    ma20 = group["最新价"].transform(lambda values: values.rolling(20, min_periods=10).mean())
    ma30 = group["最新价"].transform(lambda values: values.rolling(30, min_periods=30).mean())
    ma60 = group["最新价"].transform(lambda values: values.rolling(60, min_periods=60).mean())
    ma10_prev = ma10.groupby(combined["纯代码"], sort=False).shift(1)
    ma20_prev = ma20.groupby(combined["纯代码"], sort=False).shift(1)
    ma30_prev = ma30.groupby(combined["纯代码"], sort=False).shift(1)
    ma60_prev = ma60.groupby(combined["纯代码"], sort=False).shift(1)
    high5 = group["最高"].transform(lambda values: values.rolling(5, min_periods=3).max())
    high60 = group["最新价"].transform(lambda values: values.rolling(60, min_periods=20).max())
    high_60_prev = group["最高"].transform(lambda values: values.shift(1).rolling(60, min_periods=60).max())
    low_60_prev = group["最低"].transform(lambda values: values.shift(1).rolling(60, min_periods=60).min())
    platform_high = group["最高"].transform(lambda values: values.shift(1).rolling(5, min_periods=5).max())
    platform_low = group["最低"].transform(lambda values: values.shift(1).rolling(5, min_periods=5).min())
    platform_max_close = group["最新价"].transform(lambda values: values.shift(1).rolling(5, min_periods=5).max())
    avg_turn3 = group["换手率"].transform(lambda values: values.rolling(3, min_periods=2).mean())
    avg_vol5 = group["volume"].transform(lambda values: values.shift(1).rolling(5, min_periods=3).mean())
    avg_vol10 = group["volume"].transform(lambda values: values.shift(1).rolling(10, min_periods=5).mean())
    avg_vol20 = group["volume"].transform(lambda values: values.shift(1).rolling(20, min_periods=10).mean())
    avg_vol60 = group["volume"].transform(lambda values: values.shift(1).rolling(60, min_periods=60).mean())
    avg_amount10 = group["amount"].transform(lambda values: values.shift(1).rolling(10, min_periods=5).mean())
    avg_amount20 = group["amount"].transform(lambda values: values.shift(1).rolling(20, min_periods=10).mean())
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
    combined["body_pct"] = ((close / open_price.replace(0, np.nan) - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["upper_shadow_pct"] = ((high - combined[["今开", "最新价"]].max(axis=1)) / prev_close.replace(0, np.nan) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["lower_shadow_pct"] = ((combined[["今开", "最新价"]].min(axis=1) - low) / prev_close.replace(0, np.nan) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["amplitude_pct"] = ((high - low) / prev_close.replace(0, np.nan) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["change_pct"] = combined["涨跌幅"]
    combined["return_5d"] = ((close / prev5_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["return_10d"] = ((close / prev10_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["return_20d"] = ((close / prev20_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["return_60d"] = ((close / prev60_close - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma5_bias"] = ((close / ma5 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma10_bias"] = ((close / ma10 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma20_bias"] = ((close / ma20 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma30_bias"] = ((close / ma30 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma60_bias"] = ((close / ma60 - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma30_slope"] = ((ma30 - ma30_prev) / ma30_prev).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma20_ma60_spread"] = ((ma20 - ma60) / ma60).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma20_ma60_spread_prev"] = ((ma20_prev - ma60_prev) / ma60_prev).replace([np.inf, -np.inf], 0).fillna(0)
    combined["ma60_bias_prev"] = ((prev_close / ma60_prev - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["drawdown_60d"] = ((low_60_prev / high_60_prev - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["pullback_from_60d_high"] = ((prev_close / high_60_prev - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["low_position_60d"] = ((close / low_60_prev - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["min_volume_5d_ratio_to_60d"] = (group["volume"].transform(lambda values: values.shift(1).rolling(5, min_periods=5).min()) / avg_vol60).replace([np.inf, -np.inf], 0).fillna(0)
    combined["volume_ratio_to_10d"] = (volume / avg_vol10).replace([np.inf, -np.inf], 0).fillna(0)
    combined["volume_ratio_to_60d"] = (volume / avg_vol60).replace([np.inf, -np.inf], 0).fillna(0)
    combined["contraction_amplitude_5d"] = ((platform_high / platform_low - 1) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["prev_volume_ratio_to_5d"] = (group["volume"].shift(1) / avg_vol5).replace([np.inf, -np.inf], 0).fillna(0)
    combined["breakout_strength"] = (close / platform_max_close - 1).replace([np.inf, -np.inf], 0).fillna(0)
    combined["volume_burst_ratio"] = (volume / avg_vol5).replace([np.inf, -np.inf], 0).fillna(0)
    combined["volume_ratio_to_20d"] = (volume / avg_vol20).replace([np.inf, -np.inf], 0).fillna(0)
    ma_stack = pd.concat([ma5, ma10, ma20], axis=1)
    combined["ma_convergence_pct"] = ((ma_stack.max(axis=1) - ma_stack.min(axis=1)) / close.replace(0, np.nan) * 100).replace([np.inf, -np.inf], 0).fillna(0)
    combined["amount_ratio_to_10d"] = (amount / avg_amount10).replace([np.inf, -np.inf], 0).fillna(0)
    combined["amount_ratio_to_20d"] = (amount / avg_amount20).replace([np.inf, -np.inf], 0).fillna(0)
    for col in ["振幅换手比", "缩量大涨标记", "极端下影线标记"]:
        if col not in combined.columns:
            combined[col] = 0.0
    if "尾盘诱多标记" not in combined.columns:
        combined["尾盘诱多标记"] = 0.0
    combined[DIPBUY_TEMPORAL_FEATURE_COLS] = combined[DIPBUY_TEMPORAL_FEATURE_COLS].replace([np.inf, -np.inf], 0).fillna(0)
    combined[REVERSAL_FEATURE_COLS] = combined[REVERSAL_FEATURE_COLS].replace([np.inf, -np.inf], 0).fillna(0)
    combined[MAIN_WAVE_FEATURE_COLS] = combined[MAIN_WAVE_FEATURE_COLS].replace([np.inf, -np.inf], 0).fillna(0)

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


def _position_probability(row: pd.Series) -> float:
    if str(row.get("strategy_type", "")) == GLOBAL_MOMENTUM_STRATEGY_TYPE:
        return float(pd.to_numeric(pd.Series([row.get("global_probability")]), errors="coerce").fillna(0.0).iloc[0])
    win_rate = float(pd.to_numeric(pd.Series([row.get("AI胜率")]), errors="coerce").fillna(0.0).iloc[0])
    return float(np.clip(win_rate / 100.0, 0.0, 1.0))


def _row_to_api(row: pd.Series) -> dict[str, Any]:
    tier = str(row.get("selection_tier", "base"))
    position_probability = _position_probability(row)
    suggested_position = calculate_suggested_position(position_probability, tier)
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
        "expected_t3_max_gain_pct": round(float(row.get("预期溢价", 0)), 4) if str(row.get("strategy_type", "")) in SWING_STRATEGY_TYPES else None,
        "global_probability": round(float(row.get("global_probability", 0)), 6) if str(row.get("strategy_type", "")) == GLOBAL_MOMENTUM_STRATEGY_TYPE else None,
        "global_probability_pct": round(float(row.get("global_probability_pct", 0)), 4) if str(row.get("strategy_type", "")) == GLOBAL_MOMENTUM_STRATEGY_TYPE else None,
        "risk_score": round(float(row.get("风险评分", 0)), 4),
        "liquidity_score": round(float(row.get("流动性评分", 0)), 4),
        "composite_score": round(float(row.get("综合评分", row["AI胜率"])), 4),
        "sort_score": round(float(row.get("排序评分", row.get("综合评分", row["AI胜率"]))), 4),
        "score_threshold": round(float(row.get("生产门槛", BREAKOUT_MIN_SCORE)), 4),
        "selection_score": round(float(row.get("score", _strategy_selection_score(pd.DataFrame([row]), str(row.get("strategy_type", BREAKOUT_STRATEGY_TYPE))).iloc[0])), 6),
        "dynamic_floor": round(float(row.get("dynamic_floor", row.get("下探底线", ABSOLUTE_BOTTOM_PROBA))), 6),
        "score_floor": round(float(row.get("下探底线", row.get("dynamic_floor", ABSOLUTE_BOTTOM_PROBA))), 6),
        "selection_tier": tier,
        "risk_warning": str(row.get("risk_warning", "") or ""),
        "position_probability": round(position_probability, 6),
        "suggested_position": round(suggested_position, 4),
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
            "is_near_limit_unsealed": bool(float(row.get("准涨停未封板标记", 0)) >= 0.5),
            "live_proxy_factor": round(float(row.get("live_proxy_factor", 1.0)), 4),
            "reversal_drawdown_60d": round(float(row.get("drawdown_60d", 0)), 4),
            "reversal_min_volume_ratio": round(float(row.get("min_volume_5d_ratio_to_60d", 0)), 4),
            "reversal_volume_ratio_10d": round(float(row.get("volume_ratio_to_10d", 0)), 4),
            "reversal_ma_convergence": round(float(row.get("ma_convergence_pct", 0)), 4),
            "reversal_ma30_bias": round(float(row.get("ma30_bias", 0)), 4),
            "reversal_ma30_slope": round(float(row.get("ma30_slope", 0)), 6),
        },
        "market_context": {
            "up_rate": round(float(row.get("market_up_rate", 0)), 4),
            "down_count": int(float(row.get("market_down_count", 0) or 0)),
            "avg_change": round(float(row.get("market_avg_change", 0)), 4),
            "amount_yi": round(float(row.get("market_amount", 0) or 0) / 100000000, 4),
        },
    }
