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
from quant_core.data_pipeline.concept_engine import CONCEPT_CATALOG_PATH, CONCEPT_INDEX_PATH, get_stock_concept_map
from quant_core.data_pipeline.intraday_snapshot import attach_late_pull_trap
from quant_core.data_pipeline.market import fetch_market_indices, fetch_sina_snapshot
from quant_core.data_pipeline.sector_engine import get_stock_sector_map
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
GLOBAL_MOMENTUM_DYNAMIC_ABSOLUTE_FLOOR = GLOBAL_MIN_SCORE
PRODUCTION_MAX_PICKS_PER_STRATEGY = 3
RISK_WARNING_DYNAMIC_FLOOR = "⚠️ 动态下探: 逆势相对龙头，注意控制仓位"
KELLY_WIN_LOSS_RATIO = 1.5
HALF_KELLY_FACTOR = 0.5
BASE_POSITION_MIN = 0.10
BASE_POSITION_MAX = 0.30
DYNAMIC_FLOOR_POSITION = 0.05
REGULAR_ARMY_STRATEGIES = {BREAKOUT_STRATEGY_TYPE, GLOBAL_MOMENTUM_STRATEGY_TYPE, MAIN_WAVE_STRATEGY_TYPE}
LIMIT_UP_MAIN_BOARD_BLOCK_PCT = 9.8
LIMIT_UP_GROWTH_BOARD_BLOCK_PCT = 19.8
THEME_EMOTION_WEIGHT = 0.18
THEME_MAIN_WAVE_WEIGHT = 0.05
THEME_REVERSAL_PENALTY_WEIGHT = 0.30
THEME_HOT_SCORE = 70.0
THEME_EXTREME_HOT_SCORE = 82.0
THEME_LAGGARD_RS_FLOOR = 0.0
THEME_LAGGARD_MAX_PENALTY = 12.0
THEME_EXTREME_REVERSAL_PENALTY = 10.0


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
    tradable_mask = _not_limit_up_tradable_mask(out)
    out["涨停不可交易标记"] = (~tradable_mask).astype(float)
    out = out[tradable_mask].copy()
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

    historical_mask = _historical_playback_mask(scored)
    if historical_mask.any():
        if "historical_global_probability" in scored.columns:
            precomputed = pd.to_numeric(scored.loc[historical_mask, "historical_global_probability"], errors="coerce")
            valid = precomputed.dropna()
            probabilities.update({idx: float(value) for idx, value in valid.items()})
            errors += int(historical_mask.sum() - len(valid))
        else:
            historical = scored.loc[historical_mask].copy()
            historical_codes = _normalized_code_series(historical)
            historical_dates = historical["date"].map(_normalize_trade_date) if "date" in historical.columns else pd.Series("", index=historical.index)
            for code, index_values in historical_codes.groupby(historical_codes).groups.items():
                if not code:
                    errors += len(index_values)
                    continue
                try:
                    proba_by_date = _global_probability_history_for_code(str(code), tuple(feature_cols))
                except Exception:
                    proba_by_date = {}
                if not proba_by_date:
                    errors += len(index_values)
                    continue
                matched = historical_dates.loc[index_values].map(proba_by_date)
                valid = pd.to_numeric(matched, errors="coerce").dropna()
                probabilities.update({idx: float(value) for idx, value in valid.items()})
                errors += int(len(index_values) - len(valid))

    live_rows = scored.loc[~historical_mask]
    for idx, row in live_rows.iterrows():
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


def _historical_playback_mask(df: pd.DataFrame) -> pd.Series:
    if "historical_playback" not in df.columns:
        return pd.Series(False, index=df.index)
    return df["historical_playback"].fillna(False).astype(bool)


def _normalized_code_series(df: pd.DataFrame) -> pd.Series:
    if "纯代码" in df.columns:
        source = df["纯代码"]
    elif "code" in df.columns:
        source = df["code"]
    elif "symbol" in df.columns:
        source = df["symbol"]
    else:
        source = pd.Series("", index=df.index)
    extracted = source.fillna("").astype(str).str.extract(r"(\d{6})", expand=False)
    return extracted.fillna(source.fillna("").astype(str).str.zfill(6).str[-6:])


def _normalize_trade_date(value: Any) -> str:
    parsed = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


@lru_cache(maxsize=8192)
def _global_probability_history_for_code(code: str, feature_cols: tuple[str, ...]) -> dict[str, float]:
    """Precompute one stock's historical global probabilities from complete 15:00 daily bars."""
    model, error, loaded_feature_cols = _load_global_daily_model()
    if model is None:
        raise RuntimeError(error or "global_momentum_model_unavailable")
    columns = list(feature_cols or tuple(loaded_feature_cols))
    path = DATA_DIR / f"{str(code).zfill(6)[-6:]}_daily.parquet"
    if not path.exists():
        return {}
    frame = pd.read_parquet(path).copy()
    if frame.empty:
        return {}
    factors = generate_daily_factors(frame)
    if factors.empty:
        return {}
    aligned = _align_global_daily_features(factors, columns)
    probabilities = model.predict_proba(aligned)[:, 1]
    dates = _factor_date_series(factors)
    out: dict[str, float] = {}
    for trade_date, probability in zip(dates, probabilities):
        if trade_date and np.isfinite(probability):
            out[str(trade_date)] = float(probability)
    return out


def _factor_date_series(factors: pd.DataFrame) -> pd.Series:
    if "datetime" in factors.columns:
        dates = pd.to_datetime(factors["datetime"], errors="coerce")
    else:
        dates = pd.to_datetime(factors["date"].astype(str), errors="coerce")
    return dates.dt.strftime("%Y-%m-%d").fillna("")


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
    raw_history = pd.read_parquet(path).copy()
    if raw_history.empty:
        raise ValueError(f"{code} 本地日线为空")
    if "datetime" in raw_history.columns:
        raw_dt = pd.to_datetime(raw_history["datetime"], errors="coerce")
    else:
        raw_dt = pd.to_datetime(raw_history["date"].astype(str), errors="coerce")
    live_date = pd.to_datetime(str(row.get("date") or datetime.now().date().isoformat()), errors="coerce")
    if pd.isna(live_date):
        live_date = pd.Timestamp(datetime.now().date())
    history = raw_history[raw_dt.dt.normalize() < live_date.normalize()].tail(history_days).copy()

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
        & (_num(df, "contraction_amplitude_5d") <= 12.0)
        & (_num(df, "prev_volume_ratio_to_5d") < 1.0)
        & (_num(df, "breakout_strength") > 0)
        & (_num(df, "body_pct") >= 3.5)
        & (_num(df, "volume_burst_ratio") >= 1.30)
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
    if "涨停不可交易标记" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["涨停不可交易标记"], errors="coerce").fillna(0) < 0.5].copy()
    else:
        filtered = filtered[_not_limit_up_tradable_mask(filtered)].copy()
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


def _strategy_requires_absolute_floor(strategy_type: str) -> bool:
    return strategy_type in REGULAR_ARMY_STRATEGIES


def _strategy_dynamic_absolute_floor(strategy_type: str) -> float:
    if strategy_type == BREAKOUT_STRATEGY_TYPE:
        return BREAKOUT_MIN_SCORE
    if strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE:
        return max(GLOBAL_MIN_SCORE, GLOBAL_MOMENTUM_DYNAMIC_ABSOLUTE_FLOOR)
    if strategy_type == MAIN_WAVE_STRATEGY_TYPE:
        return MAIN_WAVE_MIN_SCORE
    return float("-inf")


def _optional_float_value(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if np.isfinite(parsed) else default


def apply_strategy_score_gate(df: pd.DataFrame, gate: dict[str, Any] | None = None) -> pd.DataFrame:
    if df.empty or "综合评分" not in df.columns:
        return df
    filtered = df.copy()
    filtered["strategy_type"] = filtered.get("strategy_type", BREAKOUT_STRATEGY_TYPE)
    filtered["strategy_type"] = filtered["strategy_type"].fillna(BREAKOUT_STRATEGY_TYPE)
    threshold = filtered["strategy_type"].map(lambda item: _strategy_min_score(str(item))).astype(float)
    filtered["生产门槛"] = threshold
    return apply_strategy_sort_score(filtered, gate)


def _normalised_score_column(df: pd.DataFrame, columns: tuple[str, ...]) -> pd.Series | None:
    for col in columns:
        if col not in df.columns:
            continue
        values = _num(df, col).replace([np.inf, -np.inf], np.nan)
        if values.dropna().empty:
            continue
        max_abs = float(values.dropna().abs().max())
        if max_abs <= 1.5:
            values = values * 100.0
        return values.fillna(50.0).clip(0, 100)
    return None


def _theme_heat_score(df: pd.DataFrame) -> pd.Series:
    existing = _normalised_score_column(
        df,
        ("theme_heat_score", "theme_score", "concept_score", "题材评分", "主题评分"),
    )
    if existing is not None:
        return existing
    if not any(col in df.columns for col in THEME_FACTOR_COLUMNS):
        return pd.Series(50.0, index=df.index)

    theme_1 = _num(df, "theme_pct_chg_1") * 100.0
    theme_3 = _num(df, "theme_pct_chg_3") * 100.0
    theme_trend = _num(df, "rs_theme_ema_5") * 100.0
    volatility = _num(df, "theme_volatility_5") * 100.0
    heat = (
        50.0
        + theme_1.clip(-8, 10) * 4.0
        + theme_3.clip(-12, 16) * 2.0
        + theme_trend.clip(-8, 10) * 2.5
        - volatility.clip(0, 12) * 0.5
    )
    return heat.replace([np.inf, -np.inf], 50.0).fillna(50.0).clip(0, 100)


def _strategy_theme_score(df: pd.DataFrame) -> pd.Series:
    existing = _normalised_score_column(df, ("theme_score", "concept_score", "题材评分", "主题评分"))
    if existing is not None:
        return existing
    heat = _theme_heat_score(df)
    relative_strength = _num(df, "rs_stock_vs_theme") * 100.0
    score = heat + relative_strength.clip(-10, 12) * 1.5
    return score.replace([np.inf, -np.inf], 50.0).fillna(50.0).clip(0, 100)


def _theme_laggard_penalty(df: pd.DataFrame, heat_score: pd.Series) -> pd.Series:
    relative_strength = _num(df, "rs_stock_vs_theme") * 100.0
    hotness = (heat_score - THEME_HOT_SCORE).clip(lower=0)
    laggard_depth = (THEME_LAGGARD_RS_FLOOR - relative_strength).clip(lower=0)
    penalty = (hotness / 30.0 * THEME_LAGGARD_MAX_PENALTY) + (laggard_depth / 10.0 * THEME_LAGGARD_MAX_PENALTY)
    penalty = penalty.where((heat_score >= THEME_HOT_SCORE) & (relative_strength < THEME_LAGGARD_RS_FLOOR), 0.0)
    return penalty.replace([np.inf, -np.inf], 0.0).fillna(0.0).clip(0, THEME_LAGGARD_MAX_PENALTY)


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
    is_breakout = scored["strategy_type"].eq(BREAKOUT_STRATEGY_TYPE)
    is_reversal = scored["strategy_type"].eq(REVERSAL_STRATEGY_TYPE)
    is_main_wave = scored["strategy_type"].eq(MAIN_WAVE_STRATEGY_TYPE)
    is_global = scored["strategy_type"].eq(GLOBAL_MOMENTUM_STRATEGY_TYPE)
    bonus = pd.Series(0.0, index=scored.index)
    bonus.loc[is_dipbuy & modes.isin(["阴天", "震荡"])] = DIPBUY_SENTIMENT_BONUS
    scored["情绪补偿分"] = bonus
    scored["排序评分"] = (base_score + bonus).clip(0, 110)
    scored.loc[is_swing, "排序评分"] = (50 + _num(scored.loc[is_swing], "综合评分").clip(-5, 15) * 5).clip(0, 110)
    scored.loc[is_global, "排序评分"] = _num(scored.loc[is_global], "global_probability_pct").clip(0, 100)

    theme_score = _strategy_theme_score(scored)
    theme_heat = _theme_heat_score(scored)
    theme_alpha = theme_score - 50.0
    laggard_penalty = _theme_laggard_penalty(scored, theme_heat)
    reversal_penalty = (theme_heat - 50.0).clip(lower=0) * THEME_REVERSAL_PENALTY_WEIGHT
    reversal_penalty += (theme_heat >= THEME_EXTREME_HOT_SCORE).astype(float) * THEME_EXTREME_REVERSAL_PENALTY

    scored["theme_score"] = theme_score
    scored["theme_heat_score"] = theme_heat
    scored["theme_weight"] = 0.0
    scored["theme_adjustment"] = 0.0
    scored["theme_laggard_penalty"] = 0.0
    scored["theme_reversal_penalty"] = 0.0

    emotion_flow = is_breakout | is_global
    scored.loc[emotion_flow, "theme_weight"] = THEME_EMOTION_WEIGHT
    scored.loc[emotion_flow, "theme_adjustment"] = theme_alpha.loc[emotion_flow] * THEME_EMOTION_WEIGHT
    scored.loc[emotion_flow, "排序评分"] += scored.loc[emotion_flow, "theme_adjustment"]

    main_wave_adjustment = theme_alpha * THEME_MAIN_WAVE_WEIGHT - laggard_penalty
    scored.loc[is_main_wave, "theme_weight"] = THEME_MAIN_WAVE_WEIGHT
    scored.loc[is_main_wave, "theme_adjustment"] = main_wave_adjustment.loc[is_main_wave]
    scored.loc[is_main_wave, "theme_laggard_penalty"] = laggard_penalty.loc[is_main_wave]
    scored.loc[is_main_wave, "排序评分"] += scored.loc[is_main_wave, "theme_adjustment"]

    scored.loc[is_reversal, "theme_weight"] = -THEME_REVERSAL_PENALTY_WEIGHT
    scored.loc[is_reversal, "theme_reversal_penalty"] = reversal_penalty.loc[is_reversal]
    scored.loc[is_reversal, "theme_adjustment"] = -reversal_penalty.loc[is_reversal]
    scored.loc[is_reversal, "排序评分"] -= reversal_penalty.loc[is_reversal]

    scored["排序评分"] = scored["排序评分"].replace([np.inf, -np.inf], 0).fillna(0).clip(0, 110)
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


def prepare_historical_playback_candidates(
    start_date: str = "2024-03-01",
    end_date: str | None = None,
) -> dict[str, Any]:
    """Build scored historical candidates with the same feature and model path used by production."""
    latest_date = end_date or _latest_historical_trade_date()
    if latest_date is None:
        return {
            "candidates": pd.DataFrame(),
            "trading_dates": [],
            "start_date": start_date,
            "end_date": end_date,
            "model_status": "stock_daily_empty",
            "repaired_pre_close_count": 0,
            "repaired_volume_ratio_count": 0,
        }

    load_start = (pd.Timestamp(start_date) - pd.DateOffset(days=160)).strftime("%Y-%m-%d")
    raw = _load_historical_daily_rows(load_start, latest_date)
    repaired_pre_close = _repair_historical_pre_close(raw)
    repaired_volume_ratio = 0
    valid_dates = _valid_historical_trading_dates(raw)
    trading_dates = [day for day in valid_dates if str(start_date) <= day <= str(latest_date)]
    if not trading_dates:
        return {
            "candidates": pd.DataFrame(),
            "trading_dates": [],
            "start_date": start_date,
            "end_date": latest_date,
            "model_status": "no_valid_trading_dates",
            "repaired_pre_close_count": repaired_pre_close,
            "repaired_volume_ratio_count": repaired_volume_ratio,
        }

    raw = raw[raw["date"].isin(valid_dates)].copy()
    repaired_volume_ratio = _repair_historical_volume_ratio(raw)
    features = build_features(raw)
    features = features[features["date"].isin(trading_dates)].copy()
    features, global_batch_status = _attach_historical_global_probabilities(features, raw)
    if features.empty:
        return {
            "candidates": pd.DataFrame(),
            "trading_dates": trading_dates,
            "start_date": trading_dates[0],
            "end_date": trading_dates[-1],
            "model_status": "historical_features_empty; data_source=stock_daily_1500",
            "repaired_pre_close_count": repaired_pre_close,
            "repaired_volume_ratio_count": repaired_volume_ratio,
        }
    features["historical_playback"] = True
    scored, model_status = score_candidates(features, production_global_hard_filter=True)
    scored = _attach_historical_outcomes(scored, raw, valid_dates)
    model_status = f"{model_status}; {global_batch_status}; data_source=stock_daily_1500"
    return {
        "candidates": scored,
        "trading_dates": trading_dates,
        "start_date": trading_dates[0],
        "end_date": trading_dates[-1],
        "model_status": model_status,
        "repaired_pre_close_count": repaired_pre_close,
        "repaired_volume_ratio_count": repaired_volume_ratio,
    }


def _latest_historical_trade_date() -> str | None:
    with connect() as conn:
        row = conn.execute("SELECT MAX(date) AS latest_date FROM stock_daily").fetchone()
    return str(row["latest_date"]) if row and row["latest_date"] else None


def _load_historical_daily_rows(start_date: str, end_date: str) -> pd.DataFrame:
    with connect() as conn:
        raw = pd.read_sql_query(
            """
            SELECT code, name, date, open, high, low, close, pre_close, change_pct,
                   volume, amount, turnover, volume_ratio
            FROM stock_daily
            WHERE date >= ? AND date <= ?
            ORDER BY date ASC, code ASC
            """,
            conn,
            params=(start_date, end_date),
        )
    if raw.empty:
        return raw
    raw["code"] = raw["code"].astype(str).str.extract(r"(\d{6})")[0].fillna("")
    raw["name"] = raw["name"].fillna("")
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    numeric_cols = ["open", "high", "low", "close", "pre_close", "change_pct", "volume", "amount", "turnover", "volume_ratio"]
    for col in numeric_cols:
        raw[col] = pd.to_numeric(raw[col], errors="coerce")
    return raw.dropna(subset=["code", "date"]).copy()


def _attach_historical_global_probabilities(features: pd.DataFrame, raw: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    if features.empty or raw.empty:
        return features, "historical_global_batch_empty"
    model, error, feature_cols = _load_global_daily_model()
    if model is None:
        return features, error or "historical_global_batch_model_unavailable"
    try:
        factors = generate_daily_factors(raw)
        if factors.empty:
            return features, "historical_global_batch_no_factors"
        aligned = _align_global_daily_features(factors, list(feature_cols))
        probabilities = model.predict_proba(aligned)[:, 1]
    except Exception as exc:
        return features, f"historical_global_batch_failed:{exc}"

    factor_dates = _factor_date_series(factors)
    factor_codes = _normalized_code_series(factors)
    probability_frame = pd.DataFrame(
        {
            "_global_date": factor_dates.astype(str),
            "_global_code": factor_codes.astype(str).str.zfill(6).str[-6:],
            "historical_global_probability": probabilities,
        }
    )
    probability_frame = probability_frame.dropna(subset=["_global_date", "_global_code"])
    probability_frame = probability_frame.drop_duplicates(["_global_date", "_global_code"], keep="last")

    out = features.copy()
    out["_global_date"] = out["date"].astype(str)
    out["_global_code"] = out["纯代码"].astype(str).str.zfill(6).str[-6:]
    out = out.merge(probability_frame, on=["_global_date", "_global_code"], how="left")
    matched = int(pd.to_numeric(out["historical_global_probability"], errors="coerce").notna().sum())
    out = out.drop(columns=["_global_date", "_global_code"], errors="ignore")
    return out, f"historical_global_batch_ready:{matched}/{len(out)}"


def _repair_historical_pre_close(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df.sort_values(["code", "date"], inplace=True)
    previous_close = pd.to_numeric(df.groupby("code", sort=False)["close"].shift(1), errors="coerce")
    pre_close = pd.to_numeric(df["pre_close"], errors="coerce")
    missing = (pre_close.isna() | (pre_close <= 0)) & previous_close.notna() & (previous_close > 0)
    repaired = int(missing.sum())
    if repaired:
        df.loc[missing, "pre_close"] = previous_close.loc[missing]
    return repaired


def _repair_historical_volume_ratio(df: pd.DataFrame, window: int = 5) -> int:
    if df.empty:
        return 0
    df.sort_values(["code", "date"], inplace=True)
    volume = pd.to_numeric(df["volume"], errors="coerce")
    volume_ratio = pd.to_numeric(df["volume_ratio"], errors="coerce")
    avg_volume = df.groupby("code", sort=False)["volume"].transform(lambda values: values.shift(1).rolling(window, min_periods=3).mean())
    missing = (volume_ratio.isna() | (volume_ratio <= 0)) & avg_volume.notna() & (avg_volume > 0) & volume.notna() & (volume > 0)
    repaired = int(missing.sum())
    if repaired:
        df.loc[missing, "volume_ratio"] = volume.loc[missing] / avg_volume.loc[missing]
    return repaired


def _valid_historical_trading_dates(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    daily = (
        df.groupby("date", as_index=False)
        .agg(row_count=("code", "nunique"), amount_sum=("amount", "sum"))
        .sort_values("date")
    )
    daily["weekday"] = pd.to_datetime(daily["date"], errors="coerce").dt.weekday
    valid = daily[(daily["weekday"] < 5) & (daily["row_count"] >= 1000) & (daily["amount_sum"].fillna(0) > 0)].copy()
    return valid["date"].astype(str).tolist()


def _attach_historical_outcomes(candidates: pd.DataFrame, raw: pd.DataFrame, trading_dates: list[str]) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    next_trade_date = {trading_dates[index]: trading_dates[index + 1] for index in range(len(trading_dates) - 1)}
    future_dates = {
        day: trading_dates[index + 1 : index + 4]
        for index, day in enumerate(trading_dates)
        if index + 1 < len(trading_dates)
    }
    raw_index = raw.set_index(["date", "code"])
    opens = raw_index["open"]
    highs = raw_index["high"]
    closes = raw_index["close"]

    out = candidates.copy()
    out["next_date"] = out["date"].map(next_trade_date)
    out["next_open"] = [opens.get((next_date, code), np.nan) for next_date, code in zip(out["next_date"], out["纯代码"])]
    out["open_premium"] = (pd.to_numeric(out["next_open"], errors="coerce") / out["最新价"] - 1) * 100
    out["t3_exit_date"] = out["date"].map(lambda day: future_dates.get(str(day), [None])[-1] if len(future_dates.get(str(day), [])) == 3 else None)

    future_highs: list[float] = []
    t3_closes: list[float] = []
    for day, code, exit_date in zip(out["date"], out["纯代码"], out["t3_exit_date"]):
        candidate_dates = future_dates.get(str(day), [])
        if len(candidate_dates) < 3:
            future_highs.append(np.nan)
            t3_closes.append(np.nan)
            continue
        high_values = [highs.get((future_day, code), np.nan) for future_day in candidate_dates]
        numeric_highs = pd.to_numeric(pd.Series(high_values), errors="coerce").dropna()
        future_highs.append(float(numeric_highs.max()) if len(numeric_highs) == 3 else np.nan)
        t3_closes.append(float(closes.get((exit_date, code), np.nan)) if exit_date else np.nan)

    out["t3_max_high"] = future_highs
    out["t3_close"] = t3_closes
    out["t3_max_gain_pct"] = (pd.to_numeric(out["t3_max_high"], errors="coerce") / out["最新价"] - 1) * 100
    out["t3_close_return_pct"] = (pd.to_numeric(out["t3_close"], errors="coerce") / out["最新价"] - 1) * 100
    return out


def scan_market(
    limit: int = 50,
    persist_snapshot: bool = True,
    cache_prediction: bool = True,
    async_persist: bool = False,
    target_date: str | None = None,
    historical_candidates: pd.DataFrame | None = None,
) -> dict[str, Any]:
    if target_date is not None:
        return _scan_historical_market(
            target_date=target_date,
            limit=limit,
            cache_prediction=cache_prediction,
            historical_candidates=historical_candidates,
        )

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


def _scan_historical_market(
    *,
    target_date: str,
    limit: int,
    cache_prediction: bool,
    historical_candidates: pd.DataFrame | None = None,
) -> dict[str, Any]:
    trade_date = _normalize_trade_date(target_date)
    if not trade_date:
        raise ValueError(f"invalid target_date: {target_date}")

    if historical_candidates is None:
        prepared = prepare_historical_playback_candidates(start_date=trade_date, end_date=trade_date)
        candidates = prepared.get("candidates", pd.DataFrame())
        model_status = str(prepared.get("model_status") or "historical_ready")
    else:
        candidates = historical_candidates
        model_status = "historical_candidates_ready"

    df = candidates[candidates["date"].astype(str).eq(trade_date)].copy() if not candidates.empty and "date" in candidates.columns else pd.DataFrame()
    if df.empty:
        return {
            "id": None,
            "created_at": f"{trade_date}T14:50:00",
            "prediction_date": trade_date,
            "model_status": f"{model_status}; 历史目标日无候选池",
            "strategy": "V4.4 Historical Playback: production scan_market(target_date) returned empty pool.",
            "market_gate": {"blocked": True, "mode": "空池", "reasons": ["目标日无有效候选"]},
            "rows": [],
        }

    gate = market_risk_gate(df, indices={})
    if gate["blocked"]:
        return {
            "id": None,
            "created_at": f"{trade_date}T14:50:00",
            "prediction_date": trade_date,
            "model_status": f"{model_status}; 历史大盘风控触发，强制空仓",
            "strategy": "V4.4 Historical Playback: production market_risk_gate blocked the day.",
            "market_gate": gate,
            "rows": [],
        }

    df = apply_production_filters(df, gate)
    if df.empty:
        return {
            "id": None,
            "created_at": f"{trade_date}T14:50:00",
            "prediction_date": trade_date,
            "model_status": f"{model_status}; 历史生产过滤后无合格标的",
            "strategy": "V4.4 Historical Playback: production filters returned no legal pool.",
            "market_gate": gate,
            "rows": [],
        }

    df = select_strategy_top_picks(df, limit_per_strategy=PRODUCTION_MAX_PICKS_PER_STRATEGY)
    if limit > 0:
        df = df.head(min(len(df), max(int(limit), len(PRODUCTION_OUTPUT_STRATEGIES))))
    rows = [_row_to_api(row) for _, row in df.iterrows()]
    snapshot_id = save_prediction_snapshot("historical_playback_v44", rows) if cache_prediction else None
    return {
        "id": snapshot_id,
        "created_at": f"{trade_date}T14:50:00",
        "prediction_date": trade_date,
        "model_status": model_status,
        "strategy": f"V4.4 Historical Playback: scan_market(target_date={trade_date}) 复用生产过滤、动态底线和 Half-Kelly 仓位。",
        "market_gate": gate,
        "rows": rows,
    }


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
        absolute_floor = _strategy_dynamic_absolute_floor(strategy_type)
        print(
            f"[AdaptiveFloor] strategy={strategy_type} legal_pool={len(legal_pool)} "
            f"min_score={min_score:.4f} dynamic_floor={dynamic_floor:.4f} "
            f"absolute_bottom={ABSOLUTE_BOTTOM_PROBA:.4f} absolute_floor={absolute_floor:.4f}"
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
        if top_score >= dynamic_floor and top_score >= absolute_floor:
            picked = legal_pool.loc[[top_idx]].copy()
            picked["risk_warning"] = RISK_WARNING_DYNAMIC_FLOOR
            picked["selection_tier"] = "dynamic_floor"
            picked["dynamic_floor"] = dynamic_floor
            picked["下探底线"] = dynamic_floor
            picked["absolute_floor"] = absolute_floor
            code = _pick_code(picked.iloc[0])
            if code:
                used_codes.add(code)
            selected.append(picked)
        elif _strategy_requires_absolute_floor(strategy_type):
            print(
                f"[AdaptiveFloor][BLOCKED] strategy={strategy_type} top_score={top_score:.4f} "
                f"dynamic_floor={dynamic_floor:.4f} absolute_floor={absolute_floor:.4f}"
            )
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
    out["涨停不可交易标记"] = (~_not_limit_up_tradable_mask(out)).astype(float)
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


def _not_limit_up_tradable_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(False, index=df.index)
    code_source = df["纯代码"] if "纯代码" in df.columns else df.get("code", pd.Series("", index=df.index))
    code = code_source.fillna("").astype(str).str.extract(r"(\d{6})", expand=False).fillna("")
    code = code.where(code.ne(""), code_source.fillna("").astype(str).str.zfill(6).str[-6:])
    current_price = _first_existing_num(df, ["最新价", "current_price", "now", "price", "trade", "close"])
    pre_close = _first_existing_num(df, ["昨收", "pre_close"])
    change_pct = _first_existing_num(df, ["涨跌幅", "change_pct", "pctChg", "change"])
    computed_change = ((current_price / pre_close.replace(0, np.nan) - 1) * 100).replace([np.inf, -np.inf], np.nan)
    change_pct = change_pct.where(change_pct.notna(), computed_change)
    growth_board = code.str.startswith(("30", "68"), na=False)
    limit_pct = pd.Series(LIMIT_UP_MAIN_BOARD_BLOCK_PCT, index=df.index, dtype="float64")
    limit_pct.loc[growth_board] = LIMIT_UP_GROWTH_BOARD_BLOCK_PCT
    exact_limit_ratio = pd.Series(1.10, index=df.index, dtype="float64")
    exact_limit_ratio.loc[growth_board] = 1.20
    synthetic_limit_price = (pre_close * exact_limit_ratio).round(2)
    explicit_limit_price = _first_existing_num(df, ["limit_up_price", "up_limit", "涨停价"])
    limit_price = explicit_limit_price.where(explicit_limit_price.notna() & (explicit_limit_price > 0), synthetic_limit_price)
    blocked = (current_price >= limit_price) | (change_pct > limit_pct)
    valid = current_price.notna() & (current_price > 0) & pre_close.notna() & (pre_close > 0)
    return (valid & ~blocked.fillna(True)).fillna(False)


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


def attach_pick_theme_fields(item: dict[str, Any], source: Any | None = None) -> dict[str, Any]:
    out = dict(item)
    source_obj = source if source is not None else out
    raw = out.get("raw") if isinstance(out.get("raw"), dict) else {}
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    code = _clean_stock_code(
        _source_get(source_obj, "纯代码")
        or _source_get(source_obj, "code")
        or out.get("code")
        or winner.get("code")
    )
    trade_date = str(
        _source_get(source_obj, "date")
        or out.get("selection_date")
        or out.get("date")
        or winner.get("date")
        or ""
    )
    theme_name = (
        _safe_text(_source_get(source_obj, "theme_name"))
        or _safe_text(out.get("theme_name"))
        or _safe_text(winner.get("theme_name"))
    )
    theme_source = (
        _safe_text(_source_get(source_obj, "theme_source"))
        or _safe_text(out.get("theme_source"))
        or _safe_text(winner.get("theme_source"))
    )
    if not theme_name and code:
        theme_name, theme_source = _theme_name_for_code(code)
    theme_pct_values = [
        winner.get("theme_pct_chg_3"),
        winner.get("theme_momentum_3d"),
        winner.get("theme_momentum"),
        raw.get("theme_momentum_3d") if isinstance(raw, dict) else None,
        raw.get("theme_pct_chg_3") if isinstance(raw, dict) else None,
    ]
    if source is not None:
        theme_pct_values = [
            _source_get(source_obj, "theme_pct_chg_3"),
            _source_get(source_obj, "theme_momentum_3d"),
            _source_get(source_obj, "theme_momentum"),
            *theme_pct_values,
        ]
    if _safe_text(out.get("core_theme")) or _safe_text(out.get("theme_name")):
        theme_pct_values.extend([out.get("theme_pct_chg_3"), out.get("theme_momentum_3d"), out.get("theme_momentum")])
    theme_pct = _optional_theme_float(*theme_pct_values)
    if theme_pct is None and code:
        theme_pct = _latest_theme_pct_chg_3_for_code(code, trade_date)
    theme_pct = round(float(theme_pct), 6) if theme_pct is not None else 0.0
    theme_name = theme_name or "-"
    theme_source = theme_source or ""

    out["theme_name"] = theme_name
    out["theme_source"] = theme_source
    out["theme_pct_chg_3"] = theme_pct
    out["core_theme"] = "" if theme_name == "-" else theme_name
    out["theme_momentum"] = theme_pct
    out["theme_momentum_3d"] = theme_pct
    if isinstance(raw, dict) and isinstance(winner, dict):
        if not _safe_text(winner.get("theme_name")):
            winner["theme_name"] = theme_name
        if not _safe_text(winner.get("theme_source")):
            winner["theme_source"] = theme_source
        if _optional_theme_float(winner.get("theme_pct_chg_3")) is None:
            winner["theme_pct_chg_3"] = theme_pct
        if not _safe_text(winner.get("core_theme")):
            winner["core_theme"] = out["core_theme"]
        if _optional_theme_float(winner.get("theme_momentum")) is None:
            winner["theme_momentum"] = theme_pct
        if _optional_theme_float(winner.get("theme_momentum_3d")) is None:
            winner["theme_momentum_3d"] = theme_pct
    return out


def _source_get(source: Any, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, pd.Series):
        return source.get(key)
    if isinstance(source, dict):
        return source.get(key)
    return None


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and not np.isfinite(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "-"} else text


def _optional_theme_float(*values: Any) -> float | None:
    for value in values:
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if np.isfinite(parsed):
            return parsed
    return None


def _clean_stock_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else ""


@lru_cache(maxsize=1)
def _stock_concept_map_cached() -> dict[str, str]:
    return get_stock_concept_map(refresh=False)


@lru_cache(maxsize=1)
def _stock_sector_map_cached() -> dict[str, str]:
    return get_stock_sector_map(refresh=False)


@lru_cache(maxsize=1)
def _concept_name_map_cached() -> dict[str, str]:
    mapping: dict[str, str] = {}
    try:
        if CONCEPT_INDEX_PATH.exists():
            index = pd.read_parquet(CONCEPT_INDEX_PATH, columns=["concept_code", "concept_name"])
            mapping.update(
                {
                    str(row.get("concept_code") or ""): str(row.get("concept_name") or "")
                    for row in index.to_dict("records")
                    if row.get("concept_code") and row.get("concept_name")
                }
            )
        if CONCEPT_CATALOG_PATH.exists():
            catalog = json.loads(CONCEPT_CATALOG_PATH.read_text(encoding="utf-8"))
            for item in catalog if isinstance(catalog, list) else []:
                code = str(item.get("concept_code") or "")
                name = str(item.get("concept_name") or "")
                if code and name:
                    mapping.setdefault(code, name)
    except Exception:
        return {}
    return mapping


def _theme_name_for_code(code: str) -> tuple[str, str]:
    clean = _clean_stock_code(code)
    concept_code = _stock_concept_map_cached().get(clean, "")
    if concept_code:
        return _concept_name_map_cached().get(concept_code, concept_code), "concept"
    sector_name = _stock_sector_map_cached().get(clean, "")
    if sector_name:
        return sector_name, "sector"
    return "-", ""


@lru_cache(maxsize=8192)
def _latest_theme_pct_chg_3_for_code(code: str, trade_date: str = "") -> float | None:
    clean = _clean_stock_code(code)
    if not clean:
        return None
    path = DATA_DIR / f"{clean}_daily.parquet"
    if not path.exists():
        return None
    try:
        raw = pd.read_parquet(path)
        if trade_date:
            dates = pd.to_datetime(raw.get("datetime", raw.get("date")).astype(str), errors="coerce")
            cutoff = pd.to_datetime(trade_date, errors="coerce")
            if pd.notna(cutoff):
                raw = raw[dates.dt.normalize() <= cutoff.normalize()].copy()
        factors = generate_daily_factors(raw.tail(120))
        if factors.empty:
            return None
        value = pd.to_numeric(pd.Series([factors.iloc[-1].get("theme_pct_chg_3")]), errors="coerce").iloc[0]
        return float(value) if pd.notna(value) and np.isfinite(float(value)) else None
    except Exception:
        return None


def _row_to_api(row: pd.Series) -> dict[str, Any]:
    tier = str(row.get("selection_tier", "base"))
    strategy_type = str(row.get("strategy_type", BREAKOUT_STRATEGY_TYPE))
    position_probability = _position_probability(row)
    suggested_position = calculate_suggested_position(position_probability, tier)
    absolute_floor = _optional_float_value(row.get("absolute_floor"), _strategy_dynamic_absolute_floor(strategy_type))
    return attach_pick_theme_fields({
        "code": str(row["纯代码"]),
        "name": str(row["名称"]),
        "strategy_type": strategy_type,
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
        "absolute_floor": (
            round(float(absolute_floor), 6)
            if np.isfinite(float(absolute_floor))
            else None
        ),
        "selection_tier": tier,
        "risk_warning": str(row.get("risk_warning", "") or ""),
        "position_probability": round(position_probability, 6),
        "suggested_position": round(suggested_position, 4),
        "sentiment_bonus": round(float(row.get("情绪补偿分", 0)), 4),
        "theme_score": round(_optional_float_value(row.get("theme_score"), 50.0), 4),
        "theme_heat_score": round(_optional_float_value(row.get("theme_heat_score"), 50.0), 4),
        "theme_weight": round(_optional_float_value(row.get("theme_weight"), 0.0), 4),
        "theme_adjustment": round(_optional_float_value(row.get("theme_adjustment"), 0.0), 4),
        "theme_laggard_penalty": round(_optional_float_value(row.get("theme_laggard_penalty"), 0.0), 4),
        "theme_reversal_penalty": round(_optional_float_value(row.get("theme_reversal_penalty"), 0.0), 4),
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
        "date": str(row.get("date", "")),
        "next_date": str(row.get("next_date")) if pd.notna(row.get("next_date")) else None,
        "t3_exit_date": str(row.get("t3_exit_date")) if pd.notna(row.get("t3_exit_date")) else None,
        "next_open": round(float(row.get("next_open")), 4) if pd.notna(row.get("next_open")) else None,
        "open_premium": round(float(row.get("open_premium")), 4) if pd.notna(row.get("open_premium")) else None,
        "t3_max_gain_pct": round(float(row.get("t3_max_gain_pct")), 4) if pd.notna(row.get("t3_max_gain_pct")) else None,
        "t3_close": round(float(row.get("t3_close")), 4) if pd.notna(row.get("t3_close")) else None,
        "t3_close_return_pct": round(float(row.get("t3_close_return_pct")), 4) if pd.notna(row.get("t3_close_return_pct")) else None,
    }, row)
