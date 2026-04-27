from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_core.config import BASE_DIR, DATA_DIR


OUTPUT_PATH = BASE_DIR / "data" / "ml_dataset" / "reversal_train_data.parquet"
SCAN_COLUMNS = [
    "date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "turn",
    "turnover",
    "pre_close",
    "change_pct",
    "pctChg",
    "code",
    "symbol",
    "name",
]
EXCLUDED_PREFIXES = ("68", "4", "8", "92")
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


def build_reversal_dataset(data_dir: Path = DATA_DIR, output_path: Path = OUTPUT_PATH, months: int | None = None) -> dict[str, Any]:
    files = sorted(data_dir.glob("*_daily.parquet"))
    if not files:
        raise RuntimeError(f"没有找到 Parquet 日线文件: {data_dir}")

    latest_date = _latest_valid_date(files)
    if latest_date is None:
        raise RuntimeError("无法识别有效交易日期")
    start_date = latest_date - pd.DateOffset(months=months) if months else None
    load_start_date = start_date - pd.DateOffset(days=140) if start_date is not None else None

    frames: list[pd.DataFrame] = []
    scanned_stocks = 0
    skipped_stocks = 0

    for path in files:
        code = path.name.split("_", 1)[0]
        if code.startswith(EXCLUDED_PREFIXES):
            skipped_stocks += 1
            continue
        df = _load_one_stock(path, load_start_date)
        if df.empty:
            skipped_stocks += 1
            continue
        latest_name = _latest_name(df)
        if _is_excluded_name(latest_name):
            skipped_stocks += 1
            continue
        samples = _build_stock_samples(df, start_date=start_date, latest_date=latest_date, default_code=code, default_name=latest_name)
        scanned_stocks += 1
        if not samples.empty:
            frames.append(samples)

    if not frames:
        raise RuntimeError("没有提取到符合中线超跌反转条件的训练样本")

    dataset = pd.concat(frames, ignore_index=True)
    dataset = dataset.replace([np.inf, -np.inf], np.nan)
    dataset = dataset.dropna(subset=["t3_max_gain_pct", "t3_max_drawdown_pct", *REVERSAL_FEATURE_COLS]).copy()
    for col in REVERSAL_FEATURE_COLS + ["t3_max_gain_pct", "t3_max_drawdown_pct"]:
        dataset[col] = pd.to_numeric(dataset[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)
    dataset = dataset.sort_values(["date", "code"]).reset_index(drop=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(output_path, engine="pyarrow")
    result = {
        "output_path": str(output_path),
        "rows": int(len(dataset)),
        "scanned_stocks": scanned_stocks,
        "skipped_stocks": skipped_stocks,
        "start_date": str(dataset["date"].min()),
        "end_date": str(dataset["date"].max()),
        "months": months or "all",
        "avg_t3_max_gain_pct": round(float(dataset["t3_max_gain_pct"].mean()), 4),
        "positive_t3_rate_pct": round(float((dataset["t3_max_gain_pct"] > 0).mean() * 100), 4),
        "avg_t3_max_drawdown_pct": round(float(dataset["t3_max_drawdown_pct"].mean()), 4),
        "features": REVERSAL_FEATURE_COLS,
    }
    return result


def _build_stock_samples(
    df: pd.DataFrame,
    start_date: pd.Timestamp | None,
    latest_date: pd.Timestamp,
    default_code: str,
    default_name: str,
) -> pd.DataFrame:
    if len(df) < 70:
        return pd.DataFrame()

    out = df.copy()
    prev_close = out["close"].shift(1)
    ma5 = out["close"].rolling(5, min_periods=5).mean()
    ma10 = out["close"].rolling(10, min_periods=10).mean()
    ma20 = out["close"].rolling(20, min_periods=20).mean()
    ma30 = out["close"].rolling(30, min_periods=30).mean()
    ma60 = out["close"].rolling(60, min_periods=60).mean()
    ma30_slope = (ma30 - ma30.shift(1)) / ma30.shift(1)
    vol_ma10_prev = out["volume"].shift(1).rolling(10, min_periods=10).mean()
    vol_ma60_prev = out["volume"].shift(1).rolling(60, min_periods=60).mean()
    amount_ma10_prev = out["amount"].shift(1).rolling(10, min_periods=10).mean()
    high_60_prev = out["high"].shift(1).rolling(60, min_periods=60).max()
    low_60_prev = out["low"].shift(1).rolling(60, min_periods=60).min()

    out["body_pct"] = (out["close"] / out["open"] - 1) * 100
    out["upper_shadow_pct"] = (out["high"] - out[["open", "close"]].max(axis=1)) / prev_close * 100
    out["lower_shadow_pct"] = (out[["open", "close"]].min(axis=1) - out["low"]) / prev_close * 100
    out["amplitude_pct"] = (out["high"] - out["low"]) / prev_close * 100
    out["change_pct"] = out["change_pct"].fillna(out["pctChg"]).fillna((out["close"] / prev_close - 1) * 100)
    out["return_5d"] = (out["close"] / out["close"].shift(5) - 1) * 100
    out["return_10d"] = (out["close"] / out["close"].shift(10) - 1) * 100
    out["return_20d"] = (out["close"] / out["close"].shift(20) - 1) * 100
    out["return_60d"] = (out["close"] / out["close"].shift(60) - 1) * 100
    out["ma5_bias"] = (out["close"] / ma5 - 1) * 100
    out["ma10_bias"] = (out["close"] / ma10 - 1) * 100
    out["ma20_bias"] = (out["close"] / ma20 - 1) * 100
    out["ma30_bias"] = (out["close"] / ma30 - 1) * 100
    out["ma30_slope"] = ma30_slope
    out["ma60_bias_prev"] = (out["close"].shift(1) / ma60.shift(1) - 1) * 100
    out["drawdown_60d"] = (low_60_prev / high_60_prev - 1) * 100
    out["low_position_60d"] = (out["close"] / low_60_prev - 1) * 100
    out["min_volume_5d_ratio_to_60d"] = out["volume"].shift(1).rolling(5, min_periods=5).min() / vol_ma60_prev
    out["volume_ratio_to_10d"] = out["volume"] / vol_ma10_prev
    out["volume_ratio_to_60d"] = out["volume"] / vol_ma60_prev
    out["amount_ratio_to_10d"] = out["amount"] / amount_ma10_prev
    ma_stack = pd.concat([ma5, ma10, ma20], axis=1)
    out["ma_convergence_pct"] = (ma_stack.max(axis=1) - ma_stack.min(axis=1)) / out["close"] * 100
    out["turnover"] = out["turnover"].fillna(out["turn"]).fillna(0)

    future_1_3_high = pd.concat([out["high"].shift(-1), out["high"].shift(-2), out["high"].shift(-3)], axis=1).max(axis=1)
    future_1_3_low = pd.concat([out["low"].shift(-1), out["low"].shift(-2), out["low"].shift(-3)], axis=1).min(axis=1)
    out["t3_max_gain_pct"] = (future_1_3_high / out["close"] - 1) * 100
    out["t3_max_drawdown_pct"] = (future_1_3_low / out["close"] - 1) * 100
    out["next_open_premium"] = (out["open"].shift(-1) / out["close"] - 1) * 100
    out["next_date"] = out["date"].shift(-1)

    physical_mask = (
        (out["close"].shift(1) < ma60.shift(1))
        & (out["drawdown_60d"] <= -20.0)
        & (out["min_volume_5d_ratio_to_60d"] < 0.4)
        & (out["body_pct"] >= 5.0)
        & (out["close"] > ma5)
        & (out["close"] > ma10)
        & (out["ma20_bias"] <= 5.0)
        & (out["ma30_bias"] <= 5.0)
        & (out["ma30_slope"] >= -0.005)
        & (out["volume_ratio_to_10d"] >= 2.0)
        & out["t3_max_gain_pct"].notna()
        & out["t3_max_drawdown_pct"].notna()
    )
    if start_date is not None:
        physical_mask &= out["date"] >= start_date
    physical_mask &= out["date"] <= latest_date

    samples = out[physical_mask].copy()
    if samples.empty:
        return samples
    samples["code"] = samples["code"].fillna(default_code).astype(str).str.extract(r"(\d{6})")[0].fillna(default_code).str.zfill(6)
    names = samples["name"].astype(str).str.strip()
    samples["name"] = names.mask(names.eq("") | names.str.lower().eq("nan"), default_name)
    keep_cols = [
        "code",
        "name",
        "date",
        "next_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "next_open_premium",
        "t3_max_gain_pct",
        "t3_max_drawdown_pct",
        "ma30_bias",
        "ma30_slope",
        *REVERSAL_FEATURE_COLS,
    ]
    return samples[keep_cols]


def _latest_valid_date(files: list[Path]) -> pd.Timestamp | None:
    latest: pd.Timestamp | None = None
    for path in files:
        try:
            dates = pd.read_parquet(path, columns=["date"])["date"]
        except Exception:
            continue
        parsed = _parse_dates(dates)
        parsed = parsed[parsed.dt.weekday < 5].dropna()
        if parsed.empty:
            continue
        current = parsed.max()
        if latest is None or current > latest:
            latest = current
    return latest


def _load_one_stock(path: Path, load_start_date: pd.Timestamp | None) -> pd.DataFrame:
    try:
        columns = pd.read_parquet(path).columns
        selected = [col for col in SCAN_COLUMNS if col in columns]
        df = pd.read_parquet(path, columns=selected)
    except Exception:
        return pd.DataFrame()
    if df.empty or not {"date", "open", "high", "low", "close", "volume"}.issubset(df.columns):
        return pd.DataFrame()

    out = df.copy()
    out["date"] = _parse_dates(out["date"])
    out = out[out["date"].notna() & (out["date"].dt.weekday < 5)].copy()
    if load_start_date is not None:
        out = out[out["date"] >= load_start_date].copy()
    if out.empty:
        return out
    for col in ["open", "high", "low", "close", "volume", "amount", "turn", "turnover", "pre_close", "change_pct", "pctChg"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "amount" not in out.columns:
        out["amount"] = 0.0
    if "code" not in out.columns:
        out["code"] = path.name.split("_", 1)[0]
    out["code"] = out["code"].fillna(path.name.split("_", 1)[0]).astype(str).str.extract(r"(\d{6})")[0].fillna(path.name.split("_", 1)[0])
    if "name" not in out.columns:
        out["name"] = ""
    out["name"] = out["name"].fillna("")
    out = out.dropna(subset=["open", "high", "low", "close", "volume"]).copy()
    out = out[(out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0) & (out["close"] > 0) & (out["volume"] > 0)].copy()
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return out


def _parse_dates(values: pd.Series) -> pd.Series:
    text = values.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    yyyymmdd = text.str.fullmatch(r"\d{8}", na=False)
    parsed.loc[yyyymmdd] = pd.to_datetime(text.loc[yyyymmdd], format="%Y%m%d", errors="coerce")
    parsed.loc[~yyyymmdd] = pd.to_datetime(text.loc[~yyyymmdd], errors="coerce")
    return parsed


def _latest_name(df: pd.DataFrame) -> str:
    names = df["name"].astype(str).str.strip()
    names = names[names != ""]
    return str(names.iloc[-1]) if not names.empty else ""


def _is_excluded_name(name: str) -> bool:
    upper = str(name).upper()
    return "ST" in upper or "退" in str(name)


def main() -> None:
    parser = argparse.ArgumentParser(description="构建中线超跌反转训练集")
    parser.add_argument("--months", type=int, default=6, help="限制最近 N 个月；0 表示使用本地全部历史")
    parser.add_argument("--min-samples", type=int, default=1500, help="样本不足时自动扩展窗口，0 表示不扩展")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()
    result = _build_with_auto_expansion(
        data_dir=args.data_dir,
        output_path=args.output,
        months=args.months or None,
        min_samples=max(0, int(args.min_samples)),
    )
    for key, value in result.items():
        print(f"{key}: {value}")


def _build_with_auto_expansion(data_dir: Path, output_path: Path, months: int | None, min_samples: int) -> dict[str, Any]:
    if not min_samples or months is None:
        return build_reversal_dataset(data_dir=data_dir, output_path=output_path, months=months)

    windows: list[int | None] = []
    for candidate in [months, 12, 24, None]:
        if candidate not in windows:
            windows.append(candidate)

    last_result: dict[str, Any] | None = None
    for window in windows:
        try:
            result = build_reversal_dataset(data_dir=data_dir, output_path=output_path, months=window)
        except RuntimeError:
            if window is None:
                raise
            continue
        result["auto_expanded"] = window != months
        result["min_samples"] = min_samples
        last_result = result
        if int(result["rows"]) >= min_samples:
            return result
    assert last_result is not None
    last_result["warning"] = f"本地可用历史全量仅生成 {last_result['rows']} 条样本，低于目标 {min_samples} 条。"
    return last_result


if __name__ == "__main__":
    main()
