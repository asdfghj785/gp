from __future__ import annotations

import argparse
import json
import math
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover - local environment guard
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

from quant_core.config import GLOBAL_MIN_SCORE, SQLITE_PATH
from quant_core.engine.intraday_exit import (
    FEATURE_COLUMNS as BASE_SELL_FEATURE_COLUMNS,
    LABEL_HOLD,
    LABEL_NAMES,
    LABEL_STOP_LOSS,
    LABEL_TAKE_PROFIT,
    _align_features,
    _attach_exit_labels,
    _build_trade_feature_rows,
    _complete_window_day_count,
    _consecutive_count,
    _load_code_minute_frame,
    _minute_coverage_index,
    _next3_trading_dates,
    _safe_div_series,
    _trading_dates_from_db,
)
from quant_core.engine.predictor import (
    GLOBAL_MOMENTUM_STRATEGY_TYPE,
    _latest_historical_playback_trade_date,
    prepare_historical_playback_candidates,
)


EXPERIMENT_DIR = BASE_DIR / "data" / "strategy_cache" / "experiments"
MODEL_DIR = BASE_DIR / "models" / "experiments"
DEFAULT_START_DATE = "2025-01-02"
BUY_LOCK_TIME = "14:50:00"
MIN_FULL_DAY_BARS = 45

BUY_5M_FEATURE_COLUMNS = [
    "buy5m_bar_count",
    "buy5m_entry_price",
    "buy5m_ret_from_open_pct",
    "buy5m_ret_15m_pct",
    "buy5m_ret_30m_pct",
    "buy5m_ret_60m_pct",
    "buy5m_ret_120m_pct",
    "buy5m_high_gain_pct",
    "buy5m_low_gain_pct",
    "buy5m_pullback_from_high_pct",
    "buy5m_close_location",
    "buy5m_vwap_dev_pct",
    "buy5m_last_bar_ret_pct",
    "buy5m_last_bar_range_pct",
    "buy5m_last_bar_body_pct",
    "buy5m_last_bar_upper_pct",
    "buy5m_last_bar_lower_pct",
    "buy5m_last_bar_close_location",
    "buy5m_volume_ratio_3",
    "buy5m_volume_ratio_6",
    "buy5m_volume_ratio_12",
    "buy5m_amount_ratio_6",
    "buy5m_amount_ratio_12",
    "buy5m_rolling_volatility_12",
    "buy5m_ma_bias_6_pct",
    "buy5m_ma_bias_12_pct",
    "buy5m_consecutive_up",
    "buy5m_consecutive_down",
]

STATIC_DAILY_FEATURE_COLUMNS = [
    "global_probability",
    "global_probability_pct",
    "综合评分",
    "排序评分",
    "预期溢价",
    "涨跌幅",
    "换手率",
    "量比",
    "market_up_rate",
    "market_down_count",
    "market_avg_change",
    "market_amount",
    "theme_momentum_3d",
    "theme_pct_chg_1",
    "theme_pct_chg_3",
    "theme_volatility_5",
    "rs_stock_vs_theme",
    "rs_theme_ema_5",
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "ma5_bias",
    "ma10_bias",
    "ma20_bias",
    "ma30_bias",
    "ma60_bias",
    "ma30_slope",
    "ma20_ma60_spread",
    "drawdown_60d",
    "pullback_from_60d_high",
    "low_position_60d",
    "volume_ratio_to_10d",
    "volume_ratio_to_60d",
    "volume_burst_ratio",
    "amount_ratio_to_10d",
    "amount_ratio_to_20d",
    "body_pct",
    "upper_shadow_pct",
    "lower_shadow_pct",
    "amplitude_pct",
    "60日高位比例",
    "高位爆量标记",
    "近3日断头铡刀标记",
]

BUY_FEATURE_COLUMNS = list(dict.fromkeys([*STATIC_DAILY_FEATURE_COLUMNS, *BUY_5M_FEATURE_COLUMNS]))
SELL_FEATURE_COLUMNS = list(dict.fromkeys([*BASE_SELL_FEATURE_COLUMNS, *STATIC_DAILY_FEATURE_COLUMNS, *BUY_5M_FEATURE_COLUMNS]))


@dataclass(frozen=True)
class RuleExit:
    action: str
    checked_at: str
    price: float
    return_pct: float
    highest_gain_pct: float


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _optional_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _pct(numerator: int, denominator: int) -> float:
    return round(float(numerator) / denominator * 100.0, 4) if denominator else 0.0


def _return_summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
    values = pd.to_numeric(frame.get(column, pd.Series(dtype="float64")), errors="coerce").dropna()
    if values.empty:
        return {"trades": 0, "win_rate": 0.0, "avg_return_pct": 0.0, "median_return_pct": 0.0, "worst_return_pct": None}
    return {
        "trades": int(len(values)),
        "win_rate": round(float((values > 0).mean() * 100.0), 4),
        "avg_return_pct": round(float(values.mean()), 4),
        "median_return_pct": round(float(values.median()), 4),
        "worst_return_pct": round(float(values.min()), 4),
    }


def _latest_stock_daily_date() -> str:
    latest = _latest_historical_playback_trade_date()
    if latest:
        return latest
    import sqlite3

    with sqlite3.connect(str(SQLITE_PATH)) as conn:
        row = conn.execute("SELECT MAX(date) FROM stock_daily").fetchone()
    return str(row[0]) if row and row[0] else datetime.now().date().isoformat()


def _candidate_cache_path(start_date: str, end_date: str, top_n: int) -> Path:
    safe_start = start_date.replace("-", "")
    safe_end = end_date.replace("-", "")
    return EXPERIMENT_DIR / f"global_sniper_5m_pool_{safe_start}_{safe_end}_top{int(top_n)}.parquet"


def load_or_build_global_pool(start_date: str, end_date: str, top_n: int, refresh: bool) -> tuple[pd.DataFrame, dict[str, Any]]:
    EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = _candidate_cache_path(start_date, end_date, top_n)
    if cache_path.exists() and not refresh:
        frame = pd.read_parquet(cache_path)
        return frame, {"cache": "hit", "path": str(cache_path)}

    prepared = prepare_historical_playback_candidates(start_date=start_date, end_date=end_date)
    candidates = prepared.get("candidates", pd.DataFrame())
    if candidates.empty:
        return candidates, {"cache": "miss", "model_status": prepared.get("model_status"), "reason": "empty_candidates"}

    global_rows = candidates[candidates["strategy_type"].astype(str).eq(GLOBAL_MOMENTUM_STRATEGY_TYPE)].copy()
    global_rows["date"] = pd.to_datetime(global_rows["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    global_rows["code"] = global_rows.get("纯代码", global_rows.get("code", "")).fillna("").astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
    global_rows["global_probability"] = pd.to_numeric(global_rows["global_probability"], errors="coerce")
    global_rows["entry_daily_price"] = _first_numeric(global_rows, ["最新价", "close"])
    global_rows = global_rows[
        (global_rows["code"].str.len() == 6)
        & (global_rows["global_probability"] >= float(GLOBAL_MIN_SCORE))
        & (pd.to_numeric(global_rows["entry_daily_price"], errors="coerce") > 0)
        & (global_rows["t3_exit_date"].notna())
    ].copy()
    sort_cols = ["date", "global_probability", "排序评分", "预期溢价", "综合评分"]
    global_rows = global_rows.sort_values(sort_cols, ascending=[True, False, False, False, False])
    pool = global_rows.groupby("date", sort=False).head(max(1, int(top_n))).reset_index(drop=True)
    pool["pool_rank"] = pool.groupby("date").cumcount() + 1
    pool.to_parquet(cache_path, index=False)
    meta = {
        "cache": "miss",
        "path": str(cache_path),
        "prepared_rows": int(len(candidates)),
        "global_rows_above_threshold": int(len(global_rows)),
        "pool_rows": int(len(pool)),
        "model_status": prepared.get("model_status"),
    }
    return pool, meta


def _first_numeric(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    result = pd.Series(np.nan, index=df.index, dtype="float64")
    for col in columns:
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        result = result.where(result.notna(), values)
    return result


def _future_days_for_row(row: pd.Series, next3: dict[str, list[str]]) -> list[str]:
    day = str(row.get("date") or "")[:10]
    days = next3.get(day, [])
    if len(days) == 3:
        return days
    exit_date = str(row.get("t3_exit_date") or "")[:10]
    if exit_date:
        trading_dates = sorted({*next3.keys(), *[item for values in next3.values() for item in values]})
        after = [item for item in trading_dates if item > day]
        if len(after) >= 3 and after[2] == exit_date:
            return after[:3]
    return []


def _buy_day_5m_features(code_frame: pd.DataFrame, trade_date: str) -> Optional[dict[str, float]]:
    if code_frame.empty:
        return None
    day = code_frame[
        (code_frame["trade_date"].astype(str) == trade_date)
        & (code_frame["trade_time"].astype(str) <= BUY_LOCK_TIME)
    ].copy()
    if day.empty or len(day) < 12:
        return None
    day = day.sort_values("datetime").reset_index(drop=True)
    close = pd.to_numeric(day["close"], errors="coerce").ffill().fillna(0.0)
    open_ = pd.to_numeric(day["open"], errors="coerce").ffill().fillna(close)
    high = pd.to_numeric(day["high"], errors="coerce").ffill().fillna(close)
    low = pd.to_numeric(day["low"], errors="coerce").ffill().fillna(close)
    volume = pd.to_numeric(day["volume"], errors="coerce").fillna(0.0)
    amount = pd.to_numeric(day["amount"], errors="coerce").fillna(0.0)
    entry = float(close.iloc[-1])
    if entry <= 0:
        return None

    first_open = float(open_.iloc[0]) if float(open_.iloc[0]) > 0 else entry
    day_high = float(high.max())
    day_low = float(low.min())
    vwap = float(amount.sum() / volume.sum()) if float(volume.sum()) > 0 and float(amount.sum()) > 0 else entry
    prev_close = close.shift(1).replace(0, np.nan)
    returns = close.pct_change().fillna(0.0) * 100.0
    last_high = float(high.iloc[-1])
    last_low = float(low.iloc[-1])
    last_open = float(open_.iloc[-1])
    last_close = float(close.iloc[-1])
    last_range_base = float(prev_close.iloc[-1]) if pd.notna(prev_close.iloc[-1]) and float(prev_close.iloc[-1]) > 0 else last_close

    def ret_n(n: int) -> float:
        if len(close) <= n:
            return 0.0
        base = float(close.iloc[-n - 1])
        return (entry / base - 1.0) * 100.0 if base > 0 else 0.0

    def ratio_last(series: pd.Series, n: int) -> float:
        prev = series.rolling(n, min_periods=1).mean().shift(1)
        denom = float(prev.iloc[-1]) if pd.notna(prev.iloc[-1]) else 0.0
        return float(series.iloc[-1]) / denom if denom > 0 else 1.0

    up = (close.diff() > 0).astype(int)
    down = (close.diff() < 0).astype(int)
    ma6 = close.rolling(6, min_periods=1).mean()
    ma12 = close.rolling(12, min_periods=1).mean()
    return {
        "buy5m_bar_count": float(len(day)),
        "buy5m_entry_price": entry,
        "buy5m_ret_from_open_pct": (entry / first_open - 1.0) * 100.0 if first_open > 0 else 0.0,
        "buy5m_ret_15m_pct": ret_n(3),
        "buy5m_ret_30m_pct": ret_n(6),
        "buy5m_ret_60m_pct": ret_n(12),
        "buy5m_ret_120m_pct": ret_n(24),
        "buy5m_high_gain_pct": (day_high / first_open - 1.0) * 100.0 if first_open > 0 else 0.0,
        "buy5m_low_gain_pct": (day_low / first_open - 1.0) * 100.0 if first_open > 0 else 0.0,
        "buy5m_pullback_from_high_pct": (entry / day_high - 1.0) * 100.0 if day_high > 0 else 0.0,
        "buy5m_close_location": (entry - day_low) / (day_high - day_low) if day_high > day_low else 0.5,
        "buy5m_vwap_dev_pct": (entry / vwap - 1.0) * 100.0 if vwap > 0 else 0.0,
        "buy5m_last_bar_ret_pct": returns.iloc[-1],
        "buy5m_last_bar_range_pct": (last_high - last_low) / last_range_base * 100.0 if last_range_base > 0 else 0.0,
        "buy5m_last_bar_body_pct": (last_close / last_open - 1.0) * 100.0 if last_open > 0 else 0.0,
        "buy5m_last_bar_upper_pct": (last_high - max(last_open, last_close)) / last_range_base * 100.0 if last_range_base > 0 else 0.0,
        "buy5m_last_bar_lower_pct": (min(last_open, last_close) - last_low) / last_range_base * 100.0 if last_range_base > 0 else 0.0,
        "buy5m_last_bar_close_location": (last_close - last_low) / (last_high - last_low) if last_high > last_low else 0.5,
        "buy5m_volume_ratio_3": ratio_last(volume, 3),
        "buy5m_volume_ratio_6": ratio_last(volume, 6),
        "buy5m_volume_ratio_12": ratio_last(volume, 12),
        "buy5m_amount_ratio_6": ratio_last(amount, 6),
        "buy5m_amount_ratio_12": ratio_last(amount, 12),
        "buy5m_rolling_volatility_12": float(returns.rolling(12, min_periods=2).std(ddof=0).iloc[-1] or 0.0),
        "buy5m_ma_bias_6_pct": (entry / float(ma6.iloc[-1]) - 1.0) * 100.0 if float(ma6.iloc[-1]) > 0 else 0.0,
        "buy5m_ma_bias_12_pct": (entry / float(ma12.iloc[-1]) - 1.0) * 100.0 if float(ma12.iloc[-1]) > 0 else 0.0,
        "buy5m_consecutive_up": float(_consecutive_count(up).iloc[-1]),
        "buy5m_consecutive_down": float(_consecutive_count(down).iloc[-1]),
    }


def _global_context(row: pd.Series) -> dict[str, float]:
    return {
        "strategy_code": 3.0,
        "is_global_momentum": 1.0,
        "is_main_wave": 0.0,
        "is_reversal": 0.0,
        "selection_tier_code": 0.0,
        "selection_score": _safe_float(row.get("综合评分")),
        "sort_score": _safe_float(row.get("排序评分")),
        "expected_t3_max_gain_pct": _safe_float(row.get("预期溢价")),
        "theme_momentum_3d": _safe_float(row.get("theme_momentum_3d") or row.get("theme_pct_chg_3")),
        "market_gate_code": 0.0,
    }


def _static_feature_values(row: pd.Series, buy_features: dict[str, float]) -> dict[str, float]:
    values = {col: _safe_float(row.get(col)) for col in STATIC_DAILY_FEATURE_COLUMNS}
    values.update({col: _safe_float(buy_features.get(col)) for col in BUY_5M_FEATURE_COLUMNS})
    return values


def _add_static_features(frame: pd.DataFrame, static_values: dict[str, float]) -> pd.DataFrame:
    out = frame.copy()
    for key, value in static_values.items():
        out[key] = float(value)
    return out


def simulate_rule_exit(window: pd.DataFrame, entry_price: float) -> RuleExit:
    bars = window.sort_values("datetime").reset_index(drop=True)
    entry = max(float(entry_price), 0.0001)
    highest = entry
    trailing_active = False
    disaster_stop = entry * 0.96
    eod_stop = entry * 0.985

    for idx, bar in bars.iterrows():
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        ts = pd.Timestamp(bar["datetime"])
        checked_at = ts.strftime("%Y-%m-%d %H:%M:%S")
        highest = max(highest, high)
        if low <= disaster_stop:
            return _rule_exit("rule_stop_4pct", checked_at, disaster_stop, entry, highest)
        if highest >= entry * 1.04:
            trailing_active = True
        trailing_price = highest * 0.98
        if trailing_active and low <= trailing_price:
            return _rule_exit("rule_trailing_4_2", checked_at, min(trailing_price, close), entry, highest)
        if ts.strftime("%H:%M:%S") in {"14:50:00", "14:55:00"} and close <= eod_stop:
            return _rule_exit("rule_eod_stop_1_5pct", checked_at, close, entry, highest)

    last = bars.iloc[-1]
    return _rule_exit(
        "rule_hold_to_t3_close",
        pd.Timestamp(last["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
        float(last["close"]),
        entry,
        highest,
    )


def _rule_exit(action: str, checked_at: str, price: float, entry: float, highest: float) -> RuleExit:
    return RuleExit(
        action=action,
        checked_at=checked_at,
        price=round(float(price), 4),
        return_pct=round((float(price) / entry - 1.0) * 100.0, 4),
        highest_gain_pct=round((float(highest) / entry - 1.0) * 100.0, 4),
    )


def build_joint_dataset(pool: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    trading_dates = _trading_dates_from_db()
    next3 = _next3_trading_dates(trading_dates)
    required_dates = {day for date_value in pool["date"].astype(str).unique() for day in next3.get(date_value, [])}
    coverage_index = _minute_coverage_index(required_dates)

    candidate_rows: list[dict[str, Any]] = []
    sell_frames: list[pd.DataFrame] = []
    minute_cache: dict[str, pd.DataFrame] = {}
    skipped: dict[str, int] = {
        "missing_future_days": 0,
        "missing_code_minute": 0,
        "missing_buyday_5m": 0,
        "incomplete_future_5m": 0,
    }

    for _, row in pool.iterrows():
        code = str(row.get("code") or "").zfill(6)[-6:]
        trade_date = str(row.get("date") or "")[:10]
        future_days = _future_days_for_row(row, next3)
        if len(future_days) != 3:
            skipped["missing_future_days"] += 1
            continue
        if _covered_future_day_count(code, future_days, coverage_index) < 3:
            skipped["incomplete_future_5m"] += 1
            continue
        if code not in minute_cache:
            minute_cache[code] = _load_code_minute_frame(code)
        code_frame = minute_cache[code]
        if code_frame.empty:
            skipped["missing_code_minute"] += 1
            continue
        buy_features = _buy_day_5m_features(code_frame, trade_date)
        if not buy_features:
            skipped["missing_buyday_5m"] += 1
            continue
        window = code_frame[code_frame["trade_date"].isin(set(future_days))].copy()
        if _complete_window_day_count(window) < 3:
            skipped["incomplete_future_5m"] += 1
            continue

        entry_price = float(buy_features["buy5m_entry_price"])
        rule = simulate_rule_exit(window, entry_price)
        oracle_max_gain_pct = (float(window["high"].max()) / entry_price - 1.0) * 100.0
        t3_close = float(window.iloc[-1]["close"])
        t3_close_return = (t3_close / entry_price - 1.0) * 100.0
        static_values = _static_feature_values(row, buy_features)

        item = row.to_dict()
        item.update(static_values)
        item.update(
            {
                "code": code,
                "date": trade_date,
                "future_days": ",".join(future_days),
                "entry_price": round(entry_price, 4),
                "rule_exit_action": rule.action,
                "rule_exit_checked_at": rule.checked_at,
                "rule_exit_price": rule.price,
                "rule_return_pct": rule.return_pct,
                "rule_highest_gain_pct": rule.highest_gain_pct,
                "t3_close_price_5m": round(t3_close, 4),
                "t3_close_return_pct_5m": round(t3_close_return, 4),
                "oracle_max_gain_pct_5m": round(float(oracle_max_gain_pct), 4),
                "buy_label_rule_win": 1 if rule.return_pct > 0 else 0,
                "buy_label_t3_win": 1 if t3_close_return > 0 else 0,
            }
        )
        candidate_rows.append(item)

        sell_features = _build_trade_feature_rows(window, entry_price, _global_context(row))
        sell_features = _add_static_features(sell_features, static_values)
        sell_features = _attach_exit_labels(sell_features, entry_price)
        sell_features["selection_date"] = trade_date
        sell_features["code"] = code
        sell_features["pool_rank"] = int(row.get("pool_rank") or 0)
        sampled = _sample_sell_training_bars(sell_features)
        if not sampled.empty:
            sell_frames.append(sampled)

    candidates = pd.DataFrame(candidate_rows)
    sell_dataset = pd.concat(sell_frames, ignore_index=True, sort=False) if sell_frames else pd.DataFrame()
    meta = {
        "pool_rows": int(len(pool)),
        "candidate_rows_with_complete_5m": int(len(candidates)),
        "sell_training_bar_rows": int(len(sell_dataset)),
        "minute_codes_loaded": int(len(minute_cache)),
        "skipped": skipped,
    }
    return candidates, sell_dataset, meta


def _covered_future_day_count(code: str, future_days: list[str], coverage_index: dict[str, set[str]]) -> int:
    days = coverage_index.get(str(code).zfill(6), set())
    return int(sum(1 for day in future_days if day in days))


def _sample_sell_training_bars(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return features
    non_hold = features["label"].astype(int) != LABEL_HOLD
    hold_sample = (features["bar_index"].astype(int) % 12 == 0) | (features["bar_index_day"].astype(int) == 0)
    sampled = features[non_hold | hold_sample].copy()
    keep_cols = ["selection_date", "code", "datetime", "label", *SELL_FEATURE_COLUMNS]
    for col in keep_cols:
        if col not in sampled.columns:
            sampled[col] = 0.0
    return sampled[keep_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)


def _split_date(frame: pd.DataFrame, train_ratio: float) -> str:
    dates = sorted(frame["date"].astype(str).unique().tolist())
    if len(dates) < 5:
        raise RuntimeError("可用交易日太少，无法做时间切分")
    index = max(1, min(len(dates) - 2, int(len(dates) * train_ratio)))
    return dates[index - 1]


def train_buy_model(train: pd.DataFrame, feature_columns: list[str], model_path: Path) -> tuple[Any, dict[str, Any]]:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 不可导入：{XGBOOST_IMPORT_ERROR}")
    label = train["buy_label_rule_win"].astype(int)
    if label.nunique() < 2:
        raise RuntimeError("买入模型训练集只有单一标签，无法训练")
    model = XGBClassifier(
        objective="binary:logistic",
        max_depth=3,
        learning_rate=0.045,
        n_estimators=260,
        min_child_weight=4,
        subsample=0.82,
        colsample_bytree=0.82,
        reg_lambda=4.0,
        eval_metric="logloss",
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    X_train = _align_features(train, feature_columns)
    model.fit(X_train, label, verbose=False)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))
    return model, {
        "train_rows": int(len(train)),
        "label_counts": {str(k): int(v) for k, v in label.value_counts().sort_index().to_dict().items()},
        "feature_importance_top20": _feature_importance(model, feature_columns),
        "model_path": str(model_path),
    }


def train_sell_model(train_bars: pd.DataFrame, feature_columns: list[str], model_path: Path) -> tuple[Any, dict[str, Any]]:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 不可导入：{XGBOOST_IMPORT_ERROR}")
    label = train_bars["label"].astype(int)
    missing = [name for value, name in LABEL_NAMES.items() if int((label == value).sum()) <= 0]
    if missing:
        raise RuntimeError(f"卖出模型训练集缺少标签：{missing}")
    model = XGBClassifier(
        objective="multi:softprob",
        num_class=3,
        max_depth=4,
        learning_rate=0.055,
        n_estimators=240,
        min_child_weight=4,
        subsample=0.86,
        colsample_bytree=0.86,
        reg_lambda=2.2,
        eval_metric="mlogloss",
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    weights = label.map({LABEL_HOLD: 1.0, LABEL_TAKE_PROFIT: 2.4, LABEL_STOP_LOSS: 2.8}).astype(float)
    model.fit(_align_features(train_bars, feature_columns), label, sample_weight=weights, verbose=False)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))
    return model, {
        "train_bar_rows": int(len(train_bars)),
        "label_counts": {LABEL_NAMES[int(k)]: int(v) for k, v in label.value_counts().sort_index().to_dict().items()},
        "feature_importance_top20": _feature_importance(model, feature_columns),
        "model_path": str(model_path),
    }


def replay_exit_model_for_candidates(candidates: pd.DataFrame, sell_model: Any) -> pd.DataFrame:
    minute_cache: dict[str, pd.DataFrame] = {}
    out_rows: list[dict[str, Any]] = []
    for _, row in candidates.iterrows():
        code = str(row.get("code") or "").zfill(6)[-6:]
        future_days = str(row.get("future_days") or "").split(",")
        if code not in minute_cache:
            minute_cache[code] = _load_code_minute_frame(code)
        code_frame = minute_cache[code]
        window = code_frame[code_frame["trade_date"].isin(set(future_days))].copy()
        if _complete_window_day_count(window) < 3:
            continue
        entry = _safe_float(row.get("entry_price"))
        static_values = {col: _safe_float(row.get(col)) for col in [*STATIC_DAILY_FEATURE_COLUMNS, *BUY_5M_FEATURE_COLUMNS]}
        features = _build_trade_feature_rows(window, entry, _global_context(row))
        features = _add_static_features(features, static_values)
        probabilities = sell_model.predict_proba(_align_features(features, SELL_FEATURE_COLUMNS))
        action, prob, exit_row, exit_price = _hybrid_model_exit(features, probabilities, entry)
        item = row.to_dict()
        item.update(
            {
                "model_exit_action": action,
                "model_exit_probability": round(float(prob), 6) if prob is not None else None,
                "model_exit_checked_at": pd.Timestamp(exit_row["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                "model_exit_price": round(exit_price, 4),
                "model_exit_return_pct": round((exit_price / entry - 1.0) * 100.0, 4),
            }
        )
        out_rows.append(item)
    return pd.DataFrame(out_rows)


def _hybrid_model_exit(features: pd.DataFrame, probabilities: np.ndarray, entry: float) -> tuple[str, Optional[float], pd.Series, float]:
    """Model exit signal with non-negotiable 5m hard risk guards."""
    safe_entry = max(float(entry), 0.0001)
    highest = safe_entry
    trailing_active = False
    disaster_stop = safe_entry * 0.96
    eod_stop = safe_entry * 0.985

    for idx in range(len(features)):
        row = features.iloc[idx]
        high = float(row["high"])
        low = float(row["low"])
        close = float(row["close"])
        ts = pd.Timestamp(row["datetime"])
        highest = max(highest, high)

        if low <= disaster_stop:
            return "hard_stop_4pct", None, row, float(disaster_stop)
        if highest >= safe_entry * 1.04:
            trailing_active = True
        trailing_price = highest * 0.98
        if trailing_active and low <= trailing_price:
            return "hard_trailing_4_2", None, row, float(min(trailing_price, close))
        if ts.strftime("%H:%M:%S") in {"14:50:00", "14:55:00"} and close <= eod_stop:
            return "hard_eod_stop_1_5pct", None, row, close

        current_gain = float(row["current_gain_pct"])
        take_prob = float(probabilities[idx, LABEL_TAKE_PROFIT])
        stop_prob = float(probabilities[idx, LABEL_STOP_LOSS])
        if stop_prob >= 0.52 and current_gain <= -2.50:
            execution_idx = min(idx + 1, len(features) - 1)
            execution_row = features.iloc[execution_idx]
            price = float(execution_row["open"] if execution_idx > idx else execution_row["close"])
            return "model_stop_loss", stop_prob, execution_row, price
        if take_prob >= 0.52 and current_gain >= 0.80:
            execution_idx = min(idx + 1, len(features) - 1)
            execution_row = features.iloc[execution_idx]
            price = float(execution_row["open"] if execution_idx > idx else execution_row["close"])
            return "model_take_profit", take_prob, execution_row, price

    last = features.iloc[-1]
    return "model_hold_to_t3_close", None, last, float(last["close"])


def _first_sell_model_signal(features: pd.DataFrame, probabilities: np.ndarray) -> tuple[Optional[int], Optional[str], Optional[float]]:
    for idx in range(len(features)):
        current_gain = float(features.iloc[idx]["current_gain_pct"])
        take_prob = float(probabilities[idx, LABEL_TAKE_PROFIT])
        stop_prob = float(probabilities[idx, LABEL_STOP_LOSS])
        if stop_prob >= 0.52 and current_gain <= -2.50:
            return idx, "model_stop_loss", stop_prob
        if take_prob >= 0.52 and current_gain >= 0.80:
            return idx, "model_take_profit", take_prob
    return None, None, None


def _feature_importance(model: Any, feature_columns: list[str], topn: int = 20) -> dict[str, float]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return {}
    pairs = sorted(zip(feature_columns, [float(v) for v in values]), key=lambda item: item[1], reverse=True)
    return {name: round(value, 8) for name, value in pairs[:topn]}


def _choose_daily_top(frame: pd.DataFrame, score_col: str, min_score: Optional[float] = None) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    pool = frame.copy()
    pool[score_col] = pd.to_numeric(pool[score_col], errors="coerce")
    if min_score is not None:
        pool = pool[pool[score_col] >= float(min_score)].copy()
    if pool.empty:
        return pool
    pool = pool.sort_values(["date", score_col, "global_probability"], ascending=[True, False, False])
    return pool.groupby("date", sort=False).head(1).reset_index(drop=True)


def evaluate(test: pd.DataFrame, thresholds: list[float]) -> dict[str, Any]:
    baseline = _choose_daily_top(test, "global_probability")
    rows: list[dict[str, Any]] = []
    top_by_buy = _choose_daily_top(test, "buy_model_probability")
    rows.append(
        {
            "name": "buy_model_top1_no_threshold",
            "threshold": None,
            "dates_with_trade": int(top_by_buy["date"].nunique()) if not top_by_buy.empty else 0,
            "rule_exit": _return_summary(top_by_buy, "rule_return_pct"),
            "model_exit": _return_summary(top_by_buy, "model_exit_return_pct"),
            "t3_close": _return_summary(top_by_buy, "t3_close_return_pct_5m"),
        }
    )
    for threshold in thresholds:
        picked = _choose_daily_top(test, "buy_model_probability", min_score=threshold)
        rows.append(
            {
                "name": f"buy_model_threshold_{threshold:.2f}",
                "threshold": round(float(threshold), 4),
                "dates_with_trade": int(picked["date"].nunique()) if not picked.empty else 0,
                "rule_exit": _return_summary(picked, "rule_return_pct"),
                "model_exit": _return_summary(picked, "model_exit_return_pct"),
                "t3_close": _return_summary(picked, "t3_close_return_pct_5m"),
            }
        )
    return {
        "test_candidate_rows": int(len(test)),
        "test_trading_dates": int(test["date"].nunique()),
        "baseline_global_probability_top1": {
            "dates_with_trade": int(baseline["date"].nunique()) if not baseline.empty else 0,
            "rule_exit": _return_summary(baseline, "rule_return_pct"),
            "model_exit": _return_summary(baseline, "model_exit_return_pct"),
            "t3_close": _return_summary(baseline, "t3_close_return_pct_5m"),
        },
        "buy_model_sweep": rows,
    }


def run_experiment(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    end_date = args.end_date or _latest_stock_daily_date()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pool, pool_meta = load_or_build_global_pool(args.start_date, end_date, args.top_n, args.refresh_candidates)
    if pool.empty:
        raise RuntimeError(f"全局候选池为空：{pool_meta}")

    candidates, sell_dataset, dataset_meta = build_joint_dataset(pool)
    if candidates.empty:
        raise RuntimeError(f"完整 5m 候选样本为空：{dataset_meta}")
    split_date = _split_date(candidates, args.train_ratio)
    train = candidates[candidates["date"].astype(str) <= split_date].copy()
    test = candidates[candidates["date"].astype(str) > split_date].copy()
    train_bars = sell_dataset[sell_dataset["selection_date"].astype(str) <= split_date].copy()
    if train.empty or test.empty or train_bars.empty:
        raise RuntimeError("训练集或测试集为空，无法完成联合实验")

    buy_model_path = MODEL_DIR / f"global_sniper_5m_buy_xgb_{timestamp}.json"
    sell_model_path = MODEL_DIR / f"global_sniper_5m_sell_xgb_{timestamp}.json"
    buy_model, buy_meta = train_buy_model(train, BUY_FEATURE_COLUMNS, buy_model_path)
    sell_model, sell_meta = train_sell_model(train_bars, SELL_FEATURE_COLUMNS, sell_model_path)

    scored_candidates = candidates.copy()
    scored_candidates["buy_model_probability"] = buy_model.predict_proba(_align_features(scored_candidates, BUY_FEATURE_COLUMNS))[:, 1]
    replayed = replay_exit_model_for_candidates(scored_candidates, sell_model)
    test_replayed = replayed[replayed["date"].astype(str) > split_date].copy()
    threshold_values = [float(item) for item in args.buy_thresholds.split(",") if str(item).strip()]
    evaluation = evaluate(test_replayed, threshold_values)

    candidate_path = EXPERIMENT_DIR / f"global_sniper_5m_joint_candidates_{timestamp}.parquet"
    latest_candidate_path = EXPERIMENT_DIR / "global_sniper_5m_joint_candidates_latest.parquet"
    replayed.to_parquet(candidate_path, index=False)
    replayed.to_parquet(latest_candidate_path, index=False)

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ready",
        "scope": {
            "strategy": GLOBAL_MOMENTUM_STRATEGY_TYPE,
            "start_date": args.start_date,
            "end_date": end_date,
            "global_min_score": float(GLOBAL_MIN_SCORE),
            "top_n_per_day": int(args.top_n),
            "train_ratio": float(args.train_ratio),
            "split_date": split_date,
            "note": "实验模型不覆盖生产模型；买入特征只使用日K候选字段和买入日14:50以前5m字段；卖出模型只使用T+1到T+3的当前及历史5m字段。",
        },
        "pool": pool_meta,
        "dataset": {
            **dataset_meta,
            "train_candidate_rows": int(len(train)),
            "test_candidate_rows": int(len(test)),
            "train_sell_bar_rows": int(len(train_bars)),
            "candidate_path": str(candidate_path),
            "latest_candidate_path": str(latest_candidate_path),
        },
        "buy_model": buy_meta,
        "sell_model": sell_meta,
        "evaluation": evaluation,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    report_path = EXPERIMENT_DIR / f"global_sniper_5m_joint_report_{timestamp}.json"
    latest_report_path = EXPERIMENT_DIR / "global_sniper_5m_joint_report_latest.json"
    report_path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    latest_report_path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Global sniper 5m + daily joint buy/sell experiment.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--train-ratio", type=float, default=0.72)
    parser.add_argument("--buy-thresholds", default="0.55,0.60,0.65,0.70,0.75,0.80")
    parser.add_argument("--refresh-candidates", action="store_true")
    args = parser.parse_args()
    report = run_experiment(args)
    print(
        json.dumps(
            _json_safe(
                {
                    "scope": report.get("scope"),
                    "dataset": report.get("dataset"),
                    "baseline": report.get("evaluation", {}).get("baseline_global_probability_top1"),
                    "buy_model_sweep": report.get("evaluation", {}).get("buy_model_sweep"),
                    "buy_top_features": report.get("buy_model", {}).get("feature_importance_top20"),
                    "sell_top_features": report.get("sell_model", {}).get("feature_importance_top20"),
                    "elapsed_seconds": report.get("elapsed_seconds"),
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
