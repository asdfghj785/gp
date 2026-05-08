from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    from xgboost import XGBClassifier
except Exception as exc:  # pragma: no cover
    XGBClassifier = None  # type: ignore[assignment]
    XGBOOST_IMPORT_ERROR = exc
else:
    XGBOOST_IMPORT_ERROR = None

from quant_core.config import SQLITE_PATH
from quant_core.engine.intraday_exit import (
    _complete_window_day_count,
    _load_code_minute_frame,
    _next3_trading_dates,
    _trading_dates_from_db,
)
from quant_core.engine.predictor import _latest_historical_playback_trade_date
from scripts.experiments.train_global_sniper_5m_joint import (
    BUY_5M_FEATURE_COLUMNS,
    simulate_rule_exit,
)


EXPERIMENT_DIR = BASE_DIR / "data" / "strategy_cache" / "experiments"
MODEL_DIR = BASE_DIR / "models" / "experiments"
DEFAULT_START_DATE = "2025-01-02"

DAILY_FEATURE_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change_pct",
    "turnover",
    "volume_ratio",
    "amount",
    "entry_change_pct_1450",
    "limit_up_blocked",
    "market_up_rate",
    "market_down_count",
    "market_avg_change",
    "market_amount",
    "is_st",
    "is_mainboard",
    "is_chinext",
    "is_star",
    "body_pct",
    "upper_shadow_pct",
    "lower_shadow_pct",
    "amplitude_pct",
    "gap_pct",
    "return_3d",
    "return_5d",
    "return_10d",
    "return_20d",
    "return_60d",
    "ma5_bias",
    "ma10_bias",
    "ma20_bias",
    "ma30_bias",
    "ma60_bias",
    "ma20_ma60_spread",
    "high_position_60d",
    "drawdown_60d",
    "pullback_from_60d_high",
    "low_position_60d",
    "volume_ratio_to_5d",
    "volume_ratio_to_10d",
    "volume_ratio_to_20d",
    "amount_ratio_to_5d",
    "amount_ratio_to_10d",
    "amount_ratio_to_20d",
    "turnover_mean_5d",
    "turnover_mean_20d",
    "volatility_5d",
    "volatility_10d",
    "volatility_20d",
]

FEATURE_COLUMNS = list(dict.fromkeys([*DAILY_FEATURE_COLUMNS, *BUY_5M_FEATURE_COLUMNS]))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


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


def _latest_completed_date() -> str:
    return _latest_historical_playback_trade_date() or datetime.now().date().isoformat()


def _cache_path(start_date: str, end_date: str, universe: str, max_codes: int) -> Path:
    suffix = f"max{int(max_codes)}" if max_codes else "allcodes"
    return EXPERIMENT_DIR / (
        f"global_5m_full_panel_{start_date.replace('-', '')}_{end_date.replace('-', '')}_{universe}_{suffix}.parquet"
    )


def augment_buyability_columns(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    pre_close = pd.to_numeric(out.get("pre_close"), errors="coerce").replace(0, np.nan)
    entry = pd.to_numeric(out.get("entry_price"), errors="coerce")
    out["entry_change_pct_1450"] = (entry / pre_close - 1.0) * 100.0
    code = out.get("code", pd.Series("", index=out.index)).fillna("").astype(str)
    is_st = pd.to_numeric(out.get("is_st", 0.0), errors="coerce").fillna(0.0) >= 0.5
    is_20cm = code.str.startswith(("30", "68", "92"), na=False)
    limit_threshold = pd.Series(9.0, index=out.index, dtype="float64")
    limit_threshold.loc[is_st] = 4.8
    limit_threshold.loc[~is_st & is_20cm] = 19.0
    out["limit_up_blocked"] = (
        pd.to_numeric(out["entry_change_pct_1450"], errors="coerce").fillna(999.0) >= limit_threshold
    ).astype(float)
    return out


def load_stock_daily(start_date: str, end_date: str) -> pd.DataFrame:
    warmup = (pd.Timestamp(start_date) - pd.DateOffset(days=110)).strftime("%Y-%m-%d")
    with sqlite3.connect(str(SQLITE_PATH)) as conn:
        df = pd.read_sql_query(
            """
            SELECT code, name, date, open, high, low, close, pre_close,
                   change_pct, volume, amount, turnover, volume_ratio
            FROM stock_daily
            WHERE date >= ? AND date <= ?
            ORDER BY code ASC, date ASC
            """,
            conn,
            params=(warmup, end_date),
        )
    if df.empty:
        return df
    df["code"] = df["code"].fillna("").astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["name"] = df["name"].fillna("").astype(str)
    for col in ["open", "high", "low", "close", "pre_close", "change_pct", "volume", "amount", "turnover", "volume_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["code", "date", "close"]).copy()


def build_daily_features(raw: pd.DataFrame, start_date: str) -> pd.DataFrame:
    if raw.empty:
        return raw
    out = raw.sort_values(["code", "date"]).copy()
    prev_close = out.groupby("code", sort=False)["close"].shift(1)
    out["pre_close"] = out["pre_close"].where(out["pre_close"] > 0, prev_close)
    out["change_pct"] = out["change_pct"].where(
        out["change_pct"].notna(),
        (out["close"] / out["pre_close"] - 1.0) * 100.0,
    )
    code = out["code"].astype(str)
    name = out["name"].astype(str).str.upper()
    out["is_st"] = name.str.contains("ST", regex=False, na=False).astype(float)
    out["is_mainboard"] = code.str.startswith(("00", "60"), na=False).astype(float)
    out["is_chinext"] = code.str.startswith("30", na=False).astype(float)
    out["is_star"] = code.str.startswith("68", na=False).astype(float)

    pre = out["pre_close"].replace(0, np.nan)
    close = out["close"].replace(0, np.nan)
    open_ = out["open"].replace(0, np.nan)
    high = out["high"]
    low = out["low"]
    out["body_pct"] = (out["close"] / open_ - 1.0) * 100.0
    out["upper_shadow_pct"] = (high - np.maximum(out["open"], out["close"])) / pre * 100.0
    out["lower_shadow_pct"] = (np.minimum(out["open"], out["close"]) - low) / pre * 100.0
    out["amplitude_pct"] = (high - low) / pre * 100.0
    out["gap_pct"] = (out["open"] / pre - 1.0) * 100.0

    grouped = out.groupby("code", sort=False)
    for window in [3, 5, 10, 20, 60]:
        shifted = grouped["close"].shift(window)
        out[f"return_{window}d"] = (out["close"] / shifted - 1.0) * 100.0
    for window in [5, 10, 20, 30, 60]:
        ma = grouped["close"].transform(lambda s, w=window: s.rolling(w, min_periods=1).mean())
        out[f"ma{window}_bias"] = (out["close"] / ma.replace(0, np.nan) - 1.0) * 100.0
    out["ma20_ma60_spread"] = out["ma20_bias"] - out["ma60_bias"]

    high60 = grouped["high"].transform(lambda s: s.rolling(60, min_periods=1).max())
    low60 = grouped["low"].transform(lambda s: s.rolling(60, min_periods=1).min())
    out["high_position_60d"] = (out["close"] - low60) / (high60 - low60).replace(0, np.nan)
    out["low_position_60d"] = (out["low"] - low60) / (high60 - low60).replace(0, np.nan)
    out["drawdown_60d"] = (out["close"] / high60.replace(0, np.nan) - 1.0) * 100.0
    out["pullback_from_60d_high"] = out["drawdown_60d"]

    for window in [5, 10, 20]:
        vol_ma = grouped["volume"].transform(lambda s, w=window: s.rolling(w, min_periods=1).mean())
        amount_ma = grouped["amount"].transform(lambda s, w=window: s.rolling(w, min_periods=1).mean())
        out[f"volume_ratio_to_{window}d"] = out["volume"] / vol_ma.replace(0, np.nan)
        out[f"amount_ratio_to_{window}d"] = out["amount"] / amount_ma.replace(0, np.nan)
    for window in [5, 20]:
        out[f"turnover_mean_{window}d"] = grouped["turnover"].transform(lambda s, w=window: s.rolling(w, min_periods=1).mean())
    ret = grouped["change_pct"]
    for window in [5, 10, 20]:
        out[f"volatility_{window}d"] = ret.transform(lambda s, w=window: s.rolling(w, min_periods=2).std(ddof=0))

    market = out.groupby("date", sort=False).agg(
        market_up_rate=("change_pct", lambda s: float((pd.to_numeric(s, errors="coerce") > 0).mean() * 100.0)),
        market_down_count=("change_pct", lambda s: int((pd.to_numeric(s, errors="coerce") < 0).sum())),
        market_avg_change=("change_pct", "mean"),
        market_amount=("amount", "sum"),
    )
    out = out.merge(market, on="date", how="left")
    out = out[out["date"].astype(str) >= start_date].copy()
    out = out.replace([np.inf, -np.inf], np.nan)
    numeric = out.select_dtypes(include=[np.number]).columns
    out[numeric] = out.groupby("code", sort=False)[numeric].ffill().fillna(0.0)
    return out


def _universe_filter(df: pd.DataFrame, universe: str) -> pd.DataFrame:
    if universe == "all":
        return df.copy()
    if universe == "mainboard":
        return df[df["is_mainboard"] >= 0.5].copy()
    if universe == "mainboard_non_st":
        return df[(df["is_mainboard"] >= 0.5) & (df["is_st"] < 0.5)].copy()
    raise ValueError(f"unknown universe: {universe}")


def build_minute_outcomes(
    codes: list[str],
    start_date: str,
    end_date: str,
    max_codes: int = 0,
    workers: int = 1,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    trading_dates = _trading_dates_from_db()
    next3 = _next3_trading_dates(trading_dates)
    code_list = sorted({str(code).zfill(6)[-6:] for code in codes if str(code).strip()})
    if max_codes > 0:
        code_list = code_list[: int(max_codes)]

    rows: list[dict[str, Any]] = []
    skipped = {
        "empty_minute": 0,
        "missing_buyday_5m": 0,
        "missing_future_days": 0,
        "incomplete_future_5m": 0,
    }
    started = time.perf_counter()
    worker_count = max(1, min(int(workers or 1), cpu_count(), len(code_list) or 1))
    tasks = [(code, start_date, end_date, next3) for code in code_list]
    if worker_count <= 1:
        iterator = map(_minute_outcomes_for_code, tasks)
        for idx, (code_rows, code_skipped) in enumerate(iterator, start=1):
            rows.extend(code_rows)
            for key, value in code_skipped.items():
                skipped[key] = skipped.get(key, 0) + int(value)
            if idx % 250 == 0:
                print(
                    f"[FullPanel5m] processed_codes={idx}/{len(code_list)} rows={len(rows)} "
                    f"elapsed={time.perf_counter() - started:.1f}s",
                    flush=True,
                )
    else:
        with Pool(processes=worker_count) as pool:
            for idx, (code_rows, code_skipped) in enumerate(pool.imap_unordered(_minute_outcomes_for_code, tasks, chunksize=8), start=1):
                rows.extend(code_rows)
                for key, value in code_skipped.items():
                    skipped[key] = skipped.get(key, 0) + int(value)
                if idx % 250 == 0:
                    print(
                        f"[FullPanel5m] processed_codes={idx}/{len(code_list)} rows={len(rows)} "
                        f"workers={worker_count} elapsed={time.perf_counter() - started:.1f}s",
                        flush=True,
                    )
    meta = {
        "codes_requested": int(len(code_list)),
        "workers": int(worker_count),
        "minute_outcome_rows": int(len(rows)),
        "skipped": skipped,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    return pd.DataFrame(rows), meta


def _minute_outcomes_for_code(task: tuple[str, str, str, dict[str, list[str]]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    code, start_date, end_date, next3 = task
    rows: list[dict[str, Any]] = []
    skipped = {
        "empty_minute": 0,
        "missing_buyday_5m": 0,
        "missing_future_days": 0,
        "incomplete_future_5m": 0,
    }
    frame = _load_code_minute_frame(code)
    if frame.empty:
        skipped["empty_minute"] += 1
        return rows, skipped
    frame = frame[(frame["trade_date"].astype(str) >= start_date) & (frame["trade_date"].astype(str) <= end_date)].copy()
    if frame.empty:
        skipped["empty_minute"] += 1
        return rows, skipped
    available_dates = sorted(frame["trade_date"].astype(str).unique().tolist())
    grouped = {str(day): part.sort_values("datetime").reset_index(drop=True) for day, part in frame.groupby("trade_date", sort=False)}
    for trade_date in available_dates:
        if trade_date < start_date or trade_date > end_date:
            continue
        future_days = next3.get(trade_date, [])
        if len(future_days) != 3:
            skipped["missing_future_days"] += 1
            continue
        future_parts = [grouped.get(day) for day in future_days]
        if any(part is None for part in future_parts):
            skipped["incomplete_future_5m"] += 1
            continue
        buy_features = _buy_day_5m_features_from_group(grouped.get(trade_date))
        if not buy_features:
            skipped["missing_buyday_5m"] += 1
            continue
        window = pd.concat([part for part in future_parts if part is not None], ignore_index=True, sort=False)
        if _complete_window_day_count(window) < 3:
            skipped["incomplete_future_5m"] += 1
            continue
        entry = float(buy_features["buy5m_entry_price"])
        rule = simulate_rule_exit(window, entry)
        t3_close = float(window.iloc[-1]["close"])
        row = {
            "code": code,
            "date": trade_date,
            "future_days": ",".join(future_days),
            "entry_price": round(entry, 4),
            "rule_exit_action": rule.action,
            "rule_exit_checked_at": rule.checked_at,
            "rule_exit_price": rule.price,
            "rule_return_pct": rule.return_pct,
            "rule_highest_gain_pct": rule.highest_gain_pct,
            "t3_close_price_5m": round(t3_close, 4),
            "t3_close_return_pct_5m": round((t3_close / entry - 1.0) * 100.0, 4),
            "oracle_max_gain_pct_5m": round((float(window["high"].max()) / entry - 1.0) * 100.0, 4),
            "label_rule_win": 1 if rule.return_pct > 0 else 0,
            "label_t3_win": 1 if t3_close / entry > 1.0 else 0,
        }
        row.update({col: _safe_float(buy_features.get(col)) for col in BUY_5M_FEATURE_COLUMNS})
        rows.append(row)
    return rows, skipped


def _buy_day_5m_features_from_group(day_frame: Optional[pd.DataFrame]) -> Optional[dict[str, float]]:
    if day_frame is None or day_frame.empty:
        return None
    day = day_frame[day_frame["trade_time"].astype(str) <= "14:50:00"].copy()
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
        "buy5m_last_bar_ret_pct": float(returns.iloc[-1]),
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
        "buy5m_consecutive_up": _consecutive_count(up),
        "buy5m_consecutive_down": _consecutive_count(down),
    }


def _consecutive_count(flags: pd.Series) -> float:
    if flags.empty:
        return 0.0
    groups = flags.ne(flags.shift()).cumsum()
    counts = flags.groupby(groups).cumcount() + 1
    return float(counts.where(flags.astype(bool), 0).iloc[-1])


def build_or_load_panel(
    start_date: str,
    end_date: str,
    universe: str,
    max_codes: int,
    workers: int,
    buyable_only: bool,
    refresh: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(start_date, end_date, universe, max_codes)
    if path.exists() and not refresh:
        frame = pd.read_parquet(path)
        frame = augment_buyability_columns(frame)
        raw_rows = int(len(frame))
        if buyable_only:
            frame = frame[pd.to_numeric(frame["limit_up_blocked"], errors="coerce").fillna(1.0) < 0.5].copy()
        return frame, {"cache": "hit", "path": str(path), "rows": int(len(frame))}

    raw_daily = load_stock_daily(start_date, end_date)
    daily = build_daily_features(raw_daily, start_date)
    daily = _universe_filter(daily, universe)
    if max_codes > 0:
        codes = sorted(daily["code"].dropna().astype(str).unique().tolist())[: int(max_codes)]
        daily = daily[daily["code"].isin(codes)].copy()
    minute, minute_meta = build_minute_outcomes(
        daily["code"].unique().tolist(),
        start_date,
        end_date,
        max_codes=max_codes,
        workers=workers,
    )
    if minute.empty:
        return minute, {"cache": "miss", "reason": "empty_minute_panel", "minute": minute_meta}
    panel = daily.merge(minute, on=["code", "date"], how="inner")
    panel = augment_buyability_columns(panel)
    panel = panel.dropna(subset=["label_rule_win", "rule_return_pct", "entry_price"]).copy()
    raw_rows = int(len(panel))
    if buyable_only:
        panel = panel[pd.to_numeric(panel["limit_up_blocked"], errors="coerce").fillna(1.0) < 0.5].copy()
    for col in FEATURE_COLUMNS:
        if col not in panel.columns:
            panel[col] = 0.0
        panel[col] = pd.to_numeric(panel[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    panel.to_parquet(path, index=False)
    meta = {
        "cache": "miss",
        "path": str(path),
        "daily_rows_after_universe": int(len(daily)),
        "raw_rows_before_buyability": raw_rows,
        "buyable_only": bool(buyable_only),
        "filtered_unbuyable_rows": int(raw_rows - len(panel)),
        "rows": int(len(panel)),
        "minute": minute_meta,
    }
    return panel, meta


def align_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    for col in FEATURE_COLUMNS:
        if col not in out.columns:
            out[col] = 0.0
    aligned = out[FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
    return aligned.replace([np.inf, -np.inf], np.nan).fillna(0.0).astype("float32")


def train_model(train: pd.DataFrame, model_path: Path) -> tuple[Any, dict[str, Any]]:
    if XGBClassifier is None:
        raise RuntimeError(f"xgboost 不可导入：{XGBOOST_IMPORT_ERROR}")
    y = train["label_rule_win"].astype(int)
    if y.nunique() < 2:
        raise RuntimeError("训练集 label 只有单一类别")
    neg = int((y == 0).sum())
    pos = int((y == 1).sum())
    model = XGBClassifier(
        objective="binary:logistic",
        max_depth=5,
        learning_rate=0.045,
        n_estimators=260,
        min_child_weight=8,
        subsample=0.85,
        colsample_bytree=0.85,
        reg_lambda=4.0,
        eval_metric="logloss",
        scale_pos_weight=neg / max(1, pos),
        tree_method="hist",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(align_features(train), y, verbose=False)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(model_path))
    return model, {
        "model_path": str(model_path),
        "train_rows": int(len(train)),
        "label_counts": {str(k): int(v) for k, v in y.value_counts().sort_index().to_dict().items()},
        "feature_importance_top25": feature_importance(model, FEATURE_COLUMNS, 25),
    }


def feature_importance(model: Any, columns: list[str], topn: int) -> dict[str, float]:
    values = getattr(model, "feature_importances_", None)
    if values is None:
        return {}
    pairs = sorted(zip(columns, [float(value) for value in values]), key=lambda item: item[1], reverse=True)
    return {key: round(value, 8) for key, value in pairs[:topn]}


def split_train_test(panel: pd.DataFrame, train_ratio: float) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    dates = sorted(panel["date"].astype(str).unique().tolist())
    if len(dates) < 30:
        raise RuntimeError("有效交易日不足，无法时序切分")
    split_idx = max(10, min(len(dates) - 2, int(len(dates) * train_ratio)))
    split_date = dates[split_idx]
    train_end_idx = max(0, split_idx - 3)
    train_end_date = dates[train_end_idx]
    train = panel[panel["date"].astype(str) <= train_end_date].copy()
    test = panel[panel["date"].astype(str) >= split_date].copy()
    return train, test, split_date


def score_and_evaluate(model: Any, test: pd.DataFrame, thresholds: list[float]) -> dict[str, Any]:
    scored = test.copy()
    scored["model_probability"] = model.predict_proba(align_features(scored))[:, 1]
    scored = scored.sort_values(["date", "model_probability", "oracle_max_gain_pct_5m"], ascending=[True, False, False])
    top1 = scored.groupby("date", sort=False).head(1).reset_index(drop=True)
    sweeps = []
    for threshold in thresholds:
        picked = top1[top1["model_probability"] >= threshold].copy()
        sweeps.append(
            {
                "threshold": round(float(threshold), 4),
                "model_probability_top1": return_summary(picked, "rule_return_pct"),
                "t3_close": return_summary(picked, "t3_close_return_pct_5m"),
                "avg_probability": round(float(picked["model_probability"].mean()), 6) if not picked.empty else 0.0,
            }
        )
    return {
        "test_rows": int(len(test)),
        "test_dates": int(test["date"].nunique()),
        "top1_no_threshold": {
            "rule_exit": return_summary(top1, "rule_return_pct"),
            "t3_close": return_summary(top1, "t3_close_return_pct_5m"),
            "avg_probability": round(float(top1["model_probability"].mean()), 6) if not top1.empty else 0.0,
        },
        "threshold_sweep": sweeps,
    }


def return_summary(frame: pd.DataFrame, column: str) -> dict[str, Any]:
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


def run(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    end_date = args.end_date or _latest_completed_date()
    panel, panel_meta = build_or_load_panel(
        args.start_date,
        end_date,
        args.universe,
        args.max_codes,
        args.workers,
        not args.include_unbuyable,
        args.refresh_panel,
    )
    if panel.empty:
        raise RuntimeError(f"全市场 5m 面板为空：{panel_meta}")
    train, test, split_date = split_train_test(panel, args.train_ratio)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = MODEL_DIR / f"global_5m_full_panel_buy_xgb_{timestamp}.json"
    model, model_meta = train_model(train, model_path)
    thresholds = [float(item) for item in args.thresholds.split(",") if item.strip()]
    evaluation = score_and_evaluate(model, test, thresholds)
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "status": "ready",
        "scope": {
            "start_date": args.start_date,
            "end_date": end_date,
            "universe": args.universe,
            "max_codes": int(args.max_codes),
            "workers": int(args.workers),
            "buyable_only": not bool(args.include_unbuyable),
            "train_ratio": float(args.train_ratio),
            "split_date": split_date,
            "note": "全市场底层面板：不使用旧全局狙击候选池，不按旧模型 TopN 截断；买入特征使用日K与买入日14:50前5m，标签使用完整T+1到T+3 5m Sentinel硬风控卖出结果。",
        },
        "panel": {
            **panel_meta,
            "train_rows": int(len(train)),
            "test_rows": int(len(test)),
            "train_dates": int(train["date"].nunique()),
            "test_dates": int(test["date"].nunique()),
        },
        "model": model_meta,
        "evaluation": evaluation,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
    }
    EXPERIMENT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = EXPERIMENT_DIR / f"global_5m_full_panel_report_{timestamp}.json"
    latest_report_path = EXPERIMENT_DIR / "global_5m_full_panel_report_latest.json"
    report_path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    latest_report_path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Train full-market daily+5m global model without old top-N candidate gate.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--universe", choices=["all", "mainboard", "mainboard_non_st"], default="all")
    parser.add_argument("--max-codes", type=int, default=0, help="debug only; 0 means all codes")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--include-unbuyable", action="store_true", help="include 14:50 limit-up/unbuyable rows in training")
    parser.add_argument("--train-ratio", type=float, default=0.72)
    parser.add_argument("--thresholds", default="0.45,0.50,0.55,0.60,0.65,0.70,0.75")
    parser.add_argument("--refresh-panel", action="store_true")
    args = parser.parse_args()
    report = run(args)
    print(
        json.dumps(
            _json_safe(
                {
                    "scope": report["scope"],
                    "panel": report["panel"],
                    "evaluation": report["evaluation"],
                    "top_features": report["model"]["feature_importance_top25"],
                    "elapsed_seconds": report["elapsed_seconds"],
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
