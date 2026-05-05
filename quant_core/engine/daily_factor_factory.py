from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from quant_core.data_pipeline.concept_engine import (
    get_stock_concept_map,
    load_concept_daily,
)
from quant_core.data_pipeline.sector_engine import (
    get_stock_sector_map,
    load_sector_daily,
)


PRICE_COLS = ["open", "high", "low", "close"]
BASE_COLS = ["datetime", "date", "symbol", "code", "name", "open", "high", "low", "close", "volume", "amount"]
FACTOR_WINDOWS = [3, 5, 10, 20, 30, 60]
MOMENTUM_WINDOWS = [3, 5, 10]
THEME_FACTOR_COLUMNS = [
    "theme_pct_chg_1",
    "theme_pct_chg_3",
    "theme_volatility_5",
    "rs_stock_vs_theme",
    "rs_theme_ema_5",
]
_STOCK_CONCEPT_MAP_CACHE: Optional[dict[str, str]] = None
_STOCK_SECTOR_MAP_CACHE: Optional[dict[str, str]] = None


def build_daily_factors(
    source: str | Path | pd.DataFrame,
    *,
    symbol: Optional[str] = None,
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


def normalize_daily_frame(df: pd.DataFrame, *, symbol: Optional[str] = None) -> pd.DataFrame:
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

    theme_factors = theme_relative_factor_frame(
        frame,
        code=_frame_code(frame),
        concept_map=_stock_concept_map(),
        sector_map=_stock_sector_map(),
    )
    for col in THEME_FACTOR_COLUMNS:
        if col not in theme_factors.columns:
            theme_factors[col] = np.nan
    return pd.concat([frame, pd.DataFrame(factors, index=frame.index), theme_factors[THEME_FACTOR_COLUMNS]], axis=1).copy()


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
    fillable_cols = [col for col in feature_cols if col not in THEME_FACTOR_COLUMNS]
    if fillable_cols:
        out[fillable_cols] = out[fillable_cols].ffill().fillna(0.0)
    for col in THEME_FACTOR_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
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


def _stock_concept_map() -> dict[str, str]:
    global _STOCK_CONCEPT_MAP_CACHE
    if _STOCK_CONCEPT_MAP_CACHE is None:
        _STOCK_CONCEPT_MAP_CACHE = get_stock_concept_map(refresh=False)
    return _STOCK_CONCEPT_MAP_CACHE


def _stock_sector_map() -> dict[str, str]:
    global _STOCK_SECTOR_MAP_CACHE
    if _STOCK_SECTOR_MAP_CACHE is None:
        _STOCK_SECTOR_MAP_CACHE = get_stock_sector_map(refresh=False)
    return _STOCK_SECTOR_MAP_CACHE


def theme_relative_factor_frame(
    stock_frame: pd.DataFrame,
    *,
    code: Optional[str] = None,
    concept_map: Optional[dict[str, str]] = None,
    sector_map: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """Build cascaded theme factors: concept first, sector as fallback.

    Missing concept+sector mapping or missing theme K-line intentionally
    returns NaN, not 0, so XGBoost can learn missing-value branches.
    """
    missing = pd.DataFrame(np.nan, index=stock_frame.index, columns=THEME_FACTOR_COLUMNS)
    if stock_frame.empty or "datetime" not in stock_frame.columns or "close" not in stock_frame.columns:
        return missing

    stock_code = _normalize_stock_code(code or _first_frame_value(stock_frame, "code") or _first_frame_value(stock_frame, "symbol"))
    if not stock_code:
        return missing

    concepts = concept_map if concept_map is not None else _stock_concept_map()
    sectors = sector_map if sector_map is not None else _stock_sector_map()
    concept_code = concepts.get(stock_code, "")
    if concept_code:
        theme = load_concept_daily(concept_code)
    else:
        sector_name = sectors.get(stock_code, "")
        theme = load_sector_daily(sector_name) if sector_name else pd.DataFrame()
    if theme.empty:
        return missing

    stock_dates = pd.to_datetime(stock_frame["datetime"], errors="coerce").dt.normalize()
    stock_close = pd.to_numeric(stock_frame["close"], errors="coerce")
    stock_ret = stock_close.pct_change(fill_method=None)

    theme = theme.copy()
    theme["datetime"] = pd.to_datetime(theme["datetime"], errors="coerce").dt.normalize()
    theme = theme.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")
    theme_close = pd.to_numeric(theme["close"], errors="coerce")
    if "pct_chg" in theme.columns:
        theme_ret = _normalize_pct_series(theme["pct_chg"])
    else:
        theme_ret = theme_close.pct_change(fill_method=None)
    theme["theme_pct_chg_1"] = theme_ret
    theme["theme_pct_chg_3"] = (1 + theme_ret).rolling(3, min_periods=1).apply(np.prod, raw=True) - 1
    theme["theme_volatility_5"] = _theme_true_range_pct(theme).rolling(5, min_periods=1).mean()

    aligned = pd.DataFrame({"datetime": stock_dates}, index=stock_frame.index).merge(
        theme[["datetime", "theme_pct_chg_1", "theme_pct_chg_3", "theme_volatility_5"]],
        on="datetime",
        how="left",
    )
    out = pd.DataFrame(index=stock_frame.index)
    out["theme_pct_chg_1"] = pd.to_numeric(aligned["theme_pct_chg_1"], errors="coerce")
    out["theme_pct_chg_3"] = pd.to_numeric(aligned["theme_pct_chg_3"], errors="coerce")
    out["theme_volatility_5"] = pd.to_numeric(aligned["theme_volatility_5"], errors="coerce")
    out["rs_stock_vs_theme"] = stock_ret.reset_index(drop=True) - out["theme_pct_chg_1"].reset_index(drop=True)
    out["rs_theme_ema_5"] = out["rs_stock_vs_theme"].ewm(span=5, adjust=False, min_periods=1).mean()
    out.index = stock_frame.index
    return out.replace([np.inf, -np.inf], np.nan)[THEME_FACTOR_COLUMNS]


def _normalize_pct_series(value) -> pd.Series:
    series = pd.to_numeric(value, errors="coerce") if not isinstance(value, pd.Series) else pd.to_numeric(value, errors="coerce")
    finite = series.replace([np.inf, -np.inf], np.nan).dropna()
    if not finite.empty and finite.abs().quantile(0.95) > 1.5:
        series = series / 100.0
    return series


def _theme_true_range_pct(theme: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(theme["high"], errors="coerce")
    low = pd.to_numeric(theme["low"], errors="coerce")
    close = pd.to_numeric(theme["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr / close.replace(0, np.nan)


def _normalize_stock_code(value) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else ""


def _first_frame_value(frame: pd.DataFrame, col: str) -> str:
    if col not in frame.columns:
        return ""
    for value in frame[col].dropna().astype(str):
        if value:
            return value
    return ""


def _frame_code(frame: pd.DataFrame) -> str:
    for col in ("code", "symbol"):
        if col not in frame.columns:
            continue
        for value in frame[col].dropna().astype(str):
            digits = "".join(ch for ch in value if ch.isdigit())
            if len(digits) >= 6:
                return digits[-6:]
    return ""


if __name__ == "__main__":
    sample = Path("/Users/eudis/ths/data/all_kline/000001_daily.parquet")
    dataset = build_daily_factors(sample)
    cols = feature_columns(dataset)
    print(f"Input: {sample}")
    print(f"Shape: {dataset.shape}")
    print(f"Feature count: {len(cols)}")
    print(f"First 15 features: {cols[:15]}")
    print(dataset[["datetime", "symbol", "close", "future_max_return", "label"] + cols[:5]].tail().to_string(index=False))
