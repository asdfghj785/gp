from __future__ import annotations

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

try:
    import pandas_ta as pandas_ta  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    pandas_ta = None


PRICE_COLS = ["open", "high", "low", "close"]
BASE_COLS = ["datetime", "open", "high", "low", "close", "volume", "amount", "money", "code", "symbol"]
ROLLING_WINDOWS = [3, 5, 8, 10, 13, 15, 20, 30, 48, 60]
MICRO_WINDOWS = [3, 5, 10, 20, 48]


def generate_standard_ta(df: pd.DataFrame) -> pd.DataFrame:
    """Generate 80+ standard technical factors.

    If pandas_ta is installed, a compact curated pandas_ta block is added first.
    A pure pandas/numpy vectorized fallback always runs and guarantees a rich,
    deterministic factor matrix even when optional packages are unavailable.
    """
    frame = _normalize_ohlcv(df)
    factors: dict[str, pd.Series] = {}

    if pandas_ta is not None:
        factors.update(_pandas_ta_block(frame))

    close = frame["close"]
    high = frame["high"]
    low = frame["low"]
    open_ = frame["open"]
    volume = frame["volume"]
    amount = frame["amount"]
    returns = close.pct_change()
    typical = (high + low + close) / 3

    for window in ROLLING_WINDOWS:
        roll = close.rolling(window, min_periods=1)
        high_roll = high.rolling(window, min_periods=1)
        low_roll = low.rolling(window, min_periods=1)
        vol_roll = volume.rolling(window, min_periods=1)
        ret_roll = returns.rolling(window, min_periods=2)

        sma = roll.mean()
        ema = close.ewm(span=window, adjust=False, min_periods=1).mean()
        std = roll.std(ddof=0)
        high_max = high_roll.max()
        low_min = low_roll.min()

        factors[f"sma_{window}"] = sma
        factors[f"ema_{window}"] = ema
        factors[f"close_sma_bias_{window}"] = _safe_div(close, sma) - 1
        factors[f"close_ema_bias_{window}"] = _safe_div(close, ema) - 1
        factors[f"roc_{window}"] = close.pct_change(window)
        factors[f"ret_mean_{window}"] = ret_roll.mean()
        factors[f"ret_std_{window}"] = ret_roll.std(ddof=0)
        factors[f"zscore_{window}"] = _safe_div(close - sma, std)
        factors[f"bb_upper_{window}"] = sma + 2 * std
        factors[f"bb_lower_{window}"] = sma - 2 * std
        factors[f"bb_width_{window}"] = _safe_div(4 * std, sma)
        factors[f"atr_{window}"] = _atr(high, low, close, window)
        factors[f"rsi_{window}"] = _rsi(close, window)
        factors[f"stoch_k_{window}"] = _safe_div(close - low_min, high_max - low_min) * 100
        factors[f"willr_{window}"] = _safe_div(high_max - close, high_max - low_min) * -100
        factors[f"volume_ratio_{window}"] = _safe_div(volume, vol_roll.mean())
        factors[f"amount_ratio_{window}"] = _safe_div(amount, amount.rolling(window, min_periods=1).mean())
        factors[f"obv_slope_{window}"] = _obv(close, volume).diff(window)
        factors[f"mfi_{window}"] = _mfi(high, low, close, volume, window)
        factors[f"donchian_pos_{window}"] = _safe_div(close - low_min, high_max - low_min)
        factors[f"range_pct_{window}"] = _safe_div(high_max - low_min, close)
        factors[f"typical_bias_{window}"] = _safe_div(typical, typical.rolling(window, min_periods=1).mean()) - 1

    for fast, slow, signal in [(5, 20, 9), (8, 21, 9), (12, 26, 9), (20, 60, 9)]:
        macd = close.ewm(span=fast, adjust=False).mean() - close.ewm(span=slow, adjust=False).mean()
        macd_signal = macd.ewm(span=signal, adjust=False).mean()
        factors[f"macd_{fast}_{slow}"] = macd
        factors[f"macd_signal_{fast}_{slow}"] = macd_signal
        factors[f"macd_hist_{fast}_{slow}"] = macd - macd_signal

    factors["body_pct"] = _safe_div(close - open_, open_)
    factors["upper_shadow_pct"] = _safe_div(high - np.maximum(open_, close), close)
    factors["lower_shadow_pct"] = _safe_div(np.minimum(open_, close) - low, close)
    factors["bar_range_pct"] = _safe_div(high - low, close)
    factors["close_location_value"] = _safe_div((close - low) - (high - close), high - low)
    factors["obv"] = _obv(close, volume)
    factors["ad_line"] = (_safe_div((close - low) - (high - close), high - low) * volume).cumsum()

    factor_df = pd.DataFrame(factors, index=frame.index)
    out = pd.concat([frame, factor_df], axis=1)
    return _clean_numeric_frame(out)


def generate_custom_micro_factors(df: pd.DataFrame) -> pd.DataFrame:
    """Generate A-share intraday micro-structure alpha factors."""
    frame = _normalize_ohlcv(df)
    out = frame.copy()
    dt = pd.to_datetime(out["datetime"], errors="coerce")
    trade_date = dt.dt.date
    trade_time = dt.dt.time

    cum_amount = out.groupby(trade_date)["amount"].cumsum()
    cum_volume = out.groupby(trade_date)["volume"].cumsum()
    out["intraday_vwap"] = _safe_div(cum_amount, cum_volume)
    out["vwap_dev"] = (_safe_div(out["close"], out["intraday_vwap"]) - 1) * 100

    ret_5m = out["close"].pct_change()
    prev_avg_volume = out["volume"].rolling(5, min_periods=1).mean().shift(1)
    out["vp_divergence"] = ((ret_5m > 0) & (out["volume"] < prev_avg_volume)).astype(float)
    out["vp_divergence_strength"] = np.where(
        ret_5m > 0,
        ret_5m * _safe_div(prev_avg_volume - out["volume"], prev_avg_volume),
        0.0,
    )

    is_tail = (trade_time >= pd.Timestamp("14:30").time()) & (trade_time <= pd.Timestamp("15:00").time())
    day_total_volume = out.groupby(trade_date)["volume"].transform("sum")
    tail_volume = out["volume"].where(is_tail, 0).groupby(trade_date).transform("sum")
    out["tail_pump_volume_ratio"] = _safe_div(tail_volume, day_total_volume)
    tail_open = out["open"].where(is_tail).groupby(trade_date).transform("first")
    tail_close = out["close"].where(is_tail).groupby(trade_date).transform("last")
    out["tail_pump_return"] = (_safe_div(tail_close, tail_open) - 1) * 100
    out["is_tail_window"] = is_tail.astype(float)

    bar_pressure = _safe_div(out["close"] - out["low"], out["high"] - out["low"])
    out["buying_pressure"] = bar_pressure
    for window in MICRO_WINDOWS:
        out[f"buying_pressure_mean_{window}"] = bar_pressure.rolling(window, min_periods=1).mean()
        out[f"buying_pressure_std_{window}"] = bar_pressure.rolling(window, min_periods=2).std(ddof=0)
        out[f"volume_imbalance_{window}"] = _safe_div(
            out["volume"] - out["volume"].rolling(window, min_periods=1).mean(),
            out["volume"].rolling(window, min_periods=1).mean(),
        )
        out[f"range_efficiency_{window}"] = _safe_div(
            out["close"].diff(window).abs(),
            (out["high"] - out["low"]).rolling(window, min_periods=1).sum(),
        )
        out[f"vwap_dev_mean_{window}"] = out["vwap_dev"].rolling(window, min_periods=1).mean()

    out["gap_from_prev_bar"] = (_safe_div(out["open"], out["close"].shift(1)) - 1) * 100
    out["amount_per_volume"] = _safe_div(out["amount"], out["volume"])
    out["close_to_high"] = _safe_div(out["close"], out["high"]) - 1
    out["close_to_low"] = _safe_div(out["close"], out["low"]) - 1
    return _clean_numeric_frame(out)


def prepare_xgboost_dataset(df: pd.DataFrame, target_horizon: int = 1) -> pd.DataFrame:
    """Create future-return label and clean the factor matrix for XGBoost."""
    if target_horizon < 1:
        raise ValueError("target_horizon must be >= 1")
    frame = _normalize_ohlcv(df)
    out = frame.copy()
    out["future_return"] = out["close"].shift(-target_horizon) / out["close"] - 1
    out["label"] = (out["future_return"] > 0.005).astype(int)
    out = out.dropna(subset=["future_return"]).copy()

    numeric_cols = out.select_dtypes(include=[np.number]).columns.tolist()
    protected = {"label", "future_return"}
    feature_cols = [col for col in numeric_cols if col not in protected]
    if feature_cols:
        out[feature_cols] = out[feature_cols].replace([np.inf, -np.inf], np.nan)
        out[feature_cols] = out[feature_cols].ffill().fillna(0.0)
        out[feature_cols] = _winsorize(out[feature_cols])
    out["future_return"] = out["future_return"].replace([np.inf, -np.inf], np.nan)
    out = out.dropna(subset=["future_return"]).reset_index(drop=True)
    return out


def build_features_for_ticker(parquet_path: str | Path, target_horizon: int = 1) -> pd.DataFrame:
    start = time.perf_counter()
    raw = pd.read_parquet(parquet_path)
    base = _normalize_ohlcv(raw)
    standard = generate_standard_ta(base)
    micro_cols = generate_custom_micro_factors(base)
    micro_only = micro_cols[[col for col in micro_cols.columns if col not in standard.columns or col in ("datetime",)]]
    merged = pd.concat([standard, micro_only.drop(columns=["datetime"], errors="ignore")], axis=1)
    dataset = prepare_xgboost_dataset(merged, target_horizon=target_horizon)
    elapsed = time.perf_counter() - start
    dataset.attrs["factor_elapsed_seconds"] = elapsed
    dataset.attrs["factor_count"] = len(_feature_columns(dataset))
    return dataset


def build_features_for_universe(
    parquet_paths: Sequence[str | Path],
    target_horizon: int = 1,
    max_workers: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Build factor matrices concurrently for many tickers."""
    results: dict[str, pd.DataFrame] = {}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(build_features_for_ticker, str(path), target_horizon): str(path)
            for path in parquet_paths
        }
        for future in as_completed(futures):
            path = futures[future]
            results[path] = future.result()
    return results


def _pandas_ta_block(df: pd.DataFrame) -> dict[str, pd.Series]:
    factors: dict[str, pd.Series] = {}
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]
    for length in [5, 15, 60]:
        for name, func in [
            (f"pta_rsi_{length}", lambda: pandas_ta.rsi(close, length=length)),
            (f"pta_atr_{length}", lambda: pandas_ta.atr(high, low, close, length=length)),
            (f"pta_obv_{length}", lambda: pandas_ta.obv(close, volume).diff(length)),
        ]:
            try:
                value = func()
                if value is not None:
                    factors[name] = value
            except Exception:
                continue
    try:
        macd = pandas_ta.macd(close)
        if macd is not None:
            for col in macd.columns:
                factors[f"pta_{col.lower()}"] = macd[col]
    except Exception:
        pass
    try:
        bbands = pandas_ta.bbands(close, length=20)
        if bbands is not None:
            for col in bbands.columns:
                factors[f"pta_{col.lower()}"] = bbands[col]
    except Exception:
        pass
    return factors


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "datetime" not in out.columns:
        raise ValueError("DataFrame must contain datetime")
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    for col in PRICE_COLS + ["volume"]:
        if col not in out.columns:
            raise ValueError(f"DataFrame must contain {col}")
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "amount" not in out.columns:
        out["amount"] = pd.to_numeric(out.get("money", 0), errors="coerce")
    else:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    if "money" not in out.columns:
        out["money"] = out["amount"]
    out = out.dropna(subset=["datetime", "open", "high", "low", "close"]).sort_values("datetime")
    out = out.reset_index(drop=True)
    return out


def _safe_div(left, right) -> pd.Series:
    left_series = pd.Series(left) if not isinstance(left, pd.Series) else left
    right_series = pd.Series(right, index=left_series.index) if not isinstance(right, pd.Series) else right
    denom = right_series.replace(0, np.nan)
    return left_series / denom


def _rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, adjust=False, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / window, adjust=False, min_periods=1).mean()
    rs = _safe_div(gain, loss)
    return 100 - 100 / (1 + rs)


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False, min_periods=1).mean()


def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def _mfi(high: pd.Series, low: pd.Series, close: pd.Series, volume: pd.Series, window: int) -> pd.Series:
    typical = (high + low + close) / 3
    flow = typical * volume
    positive = flow.where(typical.diff() > 0, 0.0).rolling(window, min_periods=1).sum()
    negative = flow.where(typical.diff() < 0, 0.0).abs().rolling(window, min_periods=1).sum()
    ratio = _safe_div(positive, negative)
    return 100 - 100 / (1 + ratio)


def _clean_numeric_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    numeric_cols = out.select_dtypes(include=[np.number]).columns
    out[numeric_cols] = out[numeric_cols].replace([np.inf, -np.inf], np.nan)
    return out


def _winsorize(features: pd.DataFrame, lower: float = 0.01, upper: float = 0.99) -> pd.DataFrame:
    if features.empty:
        return features
    low = features.quantile(lower)
    high = features.quantile(upper)
    return features.clip(lower=low, upper=high, axis=1)


def _feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = set(BASE_COLS + ["future_return", "label"])
    return [col for col in df.columns if col not in excluded and pd.api.types.is_numeric_dtype(df[col])]


def _default_test_path() -> Path:
    candidates = [
        Path("/Users/eudis/ths/data/min_kline/5m/sh600000.parquet"),
        Path("/Users/eudis/ths/data/min_kline/5m/600000.parquet"),
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError("未找到 sh600000 或 600000 的 5m Parquet 测试文件")


if __name__ == "__main__":
    test_path = _default_test_path()
    start_time = time.perf_counter()
    features = build_features_for_ticker(test_path, target_horizon=1)
    elapsed = time.perf_counter() - start_time
    feature_cols = _feature_columns(features)
    print(f"Input parquet: {test_path}")
    print(f"pandas_ta available: {pandas_ta is not None}")
    print(f"Elapsed seconds: {elapsed:.4f}")
    print(f"DataFrame shape: {features.shape}")
    print(f"Feature count: {len(feature_cols)}")
    print(f"First 10 feature columns: {feature_cols[:10]}")
    print(features[["datetime", "close", "future_return", "label"] + feature_cols[:5]].tail(5).to_string(index=False))
