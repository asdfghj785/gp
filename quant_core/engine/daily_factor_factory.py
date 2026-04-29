from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


PRICE_COLS = ["open", "high", "low", "close"]
BASE_COLS = ["datetime", "date", "symbol", "code", "name", "open", "high", "low", "close", "volume", "amount"]
FACTOR_WINDOWS = [3, 5, 10, 20, 30, 60]
MOMENTUM_WINDOWS = [3, 5, 10]


def build_daily_factors(
    source: str | Path | pd.DataFrame,
    *,
    symbol: str | None = None,
    target_horizon: int = 3,
) -> pd.DataFrame:
    """Build daily cross-sectional factors and labels for one stock.

    Label definition:
    label = 1 when max(high[t+1:t+target_horizon]) / close[t] - 1 > 4%.
    """
    raw = pd.read_parquet(source) if not isinstance(source, pd.DataFrame) else source.copy()
    frame = normalize_daily_frame(raw, symbol=symbol)
    factors = generate_daily_factors(frame)
    dataset = add_daily_labels(factors, target_horizon=target_horizon)
    return clean_daily_dataset(dataset)


def normalize_daily_frame(df: pd.DataFrame, *, symbol: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if "datetime" not in out.columns:
        if "date" not in out.columns:
            raise ValueError("daily frame must contain date or datetime")
        out["datetime"] = pd.to_datetime(out["date"].astype(str), errors="coerce")
    else:
        out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")

    for col in PRICE_COLS + ["volume"]:
        if col not in out.columns:
            raise ValueError(f"daily frame must contain {col}")
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "amount" not in out.columns:
        out["amount"] = pd.to_numeric(out.get("money", 0.0), errors="coerce")
    else:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    if "turn" in out.columns:
        out["turn"] = pd.to_numeric(out["turn"], errors="coerce")
    elif "turnover" in out.columns:
        out["turn"] = pd.to_numeric(out["turnover"], errors="coerce")
    else:
        out["turn"] = 0.0

    if "symbol" not in out.columns:
        out["symbol"] = symbol or ""
    out["symbol"] = out["symbol"].astype(str).replace({"": symbol or ""})
    if "code" not in out.columns:
        out["code"] = out["symbol"].str.extract(r"(\d{6})", expand=False).fillna(symbol or "")

    out = out.dropna(subset=["datetime", "open", "high", "low", "close"])
    out = out.sort_values("datetime").reset_index(drop=True)
    return out


def generate_daily_factors(df: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_daily_frame(df)
    factors: dict[str, pd.Series] = {}
    close = frame["close"]
    open_ = frame["open"]
    high = frame["high"]
    low = frame["low"]
    volume = frame["volume"]
    amount = frame["amount"]
    ret = close.pct_change()
    typical = (high + low + close) / 3

    for window in FACTOR_WINDOWS:
        ma = close.rolling(window, min_periods=1).mean()
        ema = close.ewm(span=window, adjust=False, min_periods=1).mean()
        std = close.rolling(window, min_periods=2).std(ddof=0)
        high_max = high.rolling(window, min_periods=1).max()
        low_min = low.rolling(window, min_periods=1).min()
        vol_ma = volume.rolling(window, min_periods=1).mean()
        amount_ma = amount.rolling(window, min_periods=1).mean()

        atr_value = atr(high, low, close, window)
        factors[f"ma_bias_{window}"] = safe_div(close, ma) - 1
        factors[f"ema_bias_{window}"] = safe_div(close, ema) - 1
        factors[f"rsi_{window}"] = rsi(close, window)
        factors[f"boll_width_{window}"] = safe_div(4 * std, ma)
        factors[f"boll_pos_{window}"] = safe_div(close - (ma - 2 * std), 4 * std)
        factors[f"atr_{window}"] = atr_value
        factors[f"atr_pct_{window}"] = safe_div(atr_value, close)
        factors[f"donchian_pos_{window}"] = safe_div(close - low_min, high_max - low_min)
        factors[f"volume_ratio_{window}"] = safe_div(volume, vol_ma)
        factors[f"amount_ratio_{window}"] = safe_div(amount, amount_ma)
        factors[f"turn_mean_{window}"] = frame["turn"].rolling(window, min_periods=1).mean()
        factors[f"ret_mean_{window}"] = ret.rolling(window, min_periods=2).mean()
        factors[f"ret_std_{window}"] = ret.rolling(window, min_periods=2).std(ddof=0)
        factors[f"close_zscore_{window}"] = safe_div(close - ma, std)
        factors[f"range_pct_{window}"] = safe_div(high_max - low_min, close)
        factors[f"typical_bias_{window}"] = safe_div(typical, typical.rolling(window, min_periods=1).mean()) - 1

    for window in MOMENTUM_WINDOWS:
        factors[f"momentum_{window}d"] = close / close.shift(window) - 1
        factors[f"cumret_{window}d"] = (1 + ret).rolling(window, min_periods=1).apply(np.prod, raw=True) - 1
        factors[f"up_days_{window}d"] = (ret > 0).rolling(window, min_periods=1).sum()
        factors[f"down_days_{window}d"] = (ret < 0).rolling(window, min_periods=1).sum()

    macd = close.ewm(span=12, adjust=False).mean() - close.ewm(span=26, adjust=False).mean()
    macd_signal = macd.ewm(span=9, adjust=False).mean()
    obv_value = obv(close, volume)
    factors["macd"] = macd
    factors["macd_signal"] = macd_signal
    factors["macd_hist"] = macd - macd_signal
    factors["macd_hist_delta"] = factors["macd_hist"].diff()
    factors["body_pct"] = safe_div(close - open_, open_)
    factors["upper_shadow_pct"] = safe_div(high - np.maximum(open_, close), close)
    factors["lower_shadow_pct"] = safe_div(np.minimum(open_, close) - low, close)
    factors["bar_range_pct"] = safe_div(high - low, close)
    factors["close_location_value"] = safe_div((close - low) - (high - close), high - low)
    factors["gap_pct"] = safe_div(open_, close.shift(1)) - 1
    factors["amount_per_volume"] = safe_div(amount, volume)
    factors["obv"] = obv_value
    factors["obv_delta_5"] = obv_value.diff(5)
    return pd.concat([frame, pd.DataFrame(factors, index=frame.index)], axis=1).copy()


def add_daily_labels(df: pd.DataFrame, *, target_horizon: int = 3) -> pd.DataFrame:
    if target_horizon < 1:
        raise ValueError("target_horizon must be >= 1")
    out = df.copy()
    future_high = pd.concat(
        [out["high"].shift(-offset) for offset in range(1, target_horizon + 1)],
        axis=1,
    ).max(axis=1)
    out["future_max_return"] = future_high / out["close"] - 1
    out["label"] = (out["future_max_return"] > 0.04).astype(int)
    out.loc[future_high.isna(), ["future_max_return", "label"]] = np.nan
    return out.dropna(subset=["future_max_return"]).reset_index(drop=True)


def clean_daily_dataset(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    numeric_cols = out.select_dtypes(include=[np.number]).columns.tolist()
    out[numeric_cols] = out[numeric_cols].replace([np.inf, -np.inf], np.nan)
    protected = {"label", "future_max_return"}
    feature_cols = [col for col in numeric_cols if col not in protected]
    out[feature_cols] = out[feature_cols].ffill().fillna(0.0)
    out[feature_cols] = winsorize(out[feature_cols])
    out = out.dropna(subset=["label", "future_max_return"])
    out["label"] = out["label"].astype(int)
    return out.reset_index(drop=True)


def safe_div(left, right) -> pd.Series:
    left_series = left if isinstance(left, pd.Series) else pd.Series(left)
    right_series = right if isinstance(right, pd.Series) else pd.Series(right, index=left_series.index)
    return left_series / right_series.replace(0, np.nan)


def rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, adjust=False, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / window, adjust=False, min_periods=1).mean()
    rs = safe_div(gain, loss)
    return 100 - 100 / (1 + rs)


def atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / window, adjust=False, min_periods=1).mean()


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff()).fillna(0)
    return (direction * volume).cumsum()


def winsorize(features: pd.DataFrame, lower: float = 0.01, upper: float = 0.99) -> pd.DataFrame:
    if features.empty:
        return features
    low = features.quantile(lower)
    high = features.quantile(upper)
    return features.clip(lower=low, upper=high, axis=1)


def feature_columns(df: pd.DataFrame) -> list[str]:
    excluded = set(BASE_COLS + ["future_max_return", "label"])
    return [col for col in df.columns if col not in excluded and pd.api.types.is_numeric_dtype(df[col])]


if __name__ == "__main__":
    sample = Path("/Users/eudis/ths/data/all_kline/000001_daily.parquet")
    dataset = build_daily_factors(sample)
    cols = feature_columns(dataset)
    print(f"Input: {sample}")
    print(f"Shape: {dataset.shape}")
    print(f"Feature count: {len(cols)}")
    print(f"First 15 features: {cols[:15]}")
    print(dataset[["datetime", "symbol", "close", "future_max_return", "label"] + cols[:5]].tail().to_string(index=False))
