from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import re
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
MINUTE_ROOT = Path("/Users/eudis/5min/organized_5min_pre_adj")
DEFAULT_START_DATE = "2025-01-02"
DEFAULT_END_DATE = "2026-01-28"
OUTPUT_DATASET = BASE_DIR / "data" / "dataset" / "global_sniper_v5_7_dataset.csv"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "extract_5m_features_latest.json"
MICRO_CACHE_PATH = BASE_DIR / "scripts" / "research" / "5m_micro_features_20250102_20260128.parquet"

FEATURE_COLUMNS = ["smart_money_ratio", "tail_accel", "intra_volatility"]
ZIP_DATE_RE = re.compile(r"(\d{8})_5min\.zip$")
CODE_RE = re.compile(r"(\d{6})")


@dataclass(frozen=True)
class ExtractionTask:
    zip_path: str
    trade_date: str
    codes: frozenset[str]


def normalize_code(value: Any) -> str:
    text = str(value or "")
    match = CODE_RE.search(text)
    return match.group(1) if match else text.zfill(6)[-6:]


def discover_source_dataset(explicit_path: str = "") -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"指定日线宽表不存在：{path}")
        return path

    candidates = [
        BASE_DIR / "data" / "dataset" / "global_sniper_dataset.csv",
        BASE_DIR / "data" / "dataset" / "global_sniper_dataset.parquet",
        BASE_DIR / "data" / "strategy_cache" / "experiments" / "global_5m_full_panel_20250102_20260506_all_allcodes.parquet",
        BASE_DIR / "data" / "strategy_cache" / "experiments" / "global_sniper_5m_pool_20250102_20260506_top20.parquet",
        BASE_DIR / "data" / "strategy_cache" / "experiments" / "global_sniper_5m_joint_candidates_latest.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path

    globbed = sorted(
        (BASE_DIR / "data" / "strategy_cache" / "experiments").glob("global_5m_full_panel_*_all_allcodes.parquet"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if globbed:
        return globbed[0]
    raise FileNotFoundError("未找到可用的全局狙击历史日线宽表。")


def read_wide_dataset(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        frame = pd.read_parquet(path)
    else:
        frame = pd.read_csv(path, dtype={"code": "string", "symbol": "string", "stock_code": "string"})
    code_col = infer_code_column(frame)
    date_col = infer_date_column(frame)
    out = frame.copy()
    out["_merge_code"] = out[code_col].map(normalize_code)
    out["_merge_date"] = pd.to_datetime(out[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    out = out.dropna(subset=["_merge_code", "_merge_date"]).copy()
    return out


def infer_code_column(frame: pd.DataFrame) -> str:
    for col in ("stock_code", "code", "symbol", "纯代码"):
        if col in frame.columns:
            return col
    raise ValueError("日线宽表缺少 stock_code/code/symbol/纯代码 字段。")


def infer_date_column(frame: pd.DataFrame) -> str:
    for col in ("date", "datetime", "trade_date", "selection_date"):
        if col in frame.columns:
            return col
    raise ValueError("日线宽表缺少 date/datetime/trade_date/selection_date 字段。")


def filter_dataset_range(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    mask = (frame["_merge_date"] >= start_date) & (frame["_merge_date"] <= end_date)
    return frame.loc[mask].copy()


def discover_zip_tasks(
    minute_root: Path,
    start_date: str,
    end_date: str,
    codes_by_date: dict[str, frozenset[str]],
    max_days: int = 0,
) -> list[ExtractionTask]:
    root = minute_root / "sh_sz"
    paths: list[tuple[str, Path]] = []
    for path in sorted(root.glob("*/*_5min.zip")):
        match = ZIP_DATE_RE.search(path.name)
        if not match:
            continue
        raw = match.group(1)
        trade_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
        if trade_date < start_date or trade_date > end_date:
            continue
        if trade_date not in codes_by_date:
            continue
        paths.append((trade_date, path))
    if max_days > 0:
        paths = paths[: int(max_days)]
    return [ExtractionTask(str(path), trade_date, codes_by_date[trade_date]) for trade_date, path in paths]


def build_codes_by_date(frame: pd.DataFrame) -> dict[str, frozenset[str]]:
    grouped = frame.groupby("_merge_date", sort=True)["_merge_code"].agg(lambda values: frozenset(values.dropna().astype(str)))
    return {str(day): codes for day, codes in grouped.items() if codes}


def extract_features_from_zips(tasks: list[ExtractionTask], workers: int) -> pd.DataFrame:
    if not tasks:
        return pd.DataFrame(columns=["stock_code", "date", *FEATURE_COLUMNS])
    workers = max(1, int(workers))
    if workers == 1:
        chunks = [process_zip_task(task) for task in tasks]
    else:
        with mp.Pool(processes=workers) as pool:
            chunks = list(pool.imap_unordered(process_zip_task, tasks, chunksize=1))
    chunks = [chunk for chunk in chunks if not chunk.empty]
    if not chunks:
        return pd.DataFrame(columns=["stock_code", "date", *FEATURE_COLUMNS])
    out = pd.concat(chunks, ignore_index=True)
    out = out.drop_duplicates(["stock_code", "date"], keep="last").reset_index(drop=True)
    return out


def process_zip_task(task: ExtractionTask) -> pd.DataFrame:
    rows: list[tuple[str, str, float, float, float]] = []
    with zipfile.ZipFile(task.zip_path) as zf:
        for member in zf.namelist():
            if not member.endswith(".csv"):
                continue
            match = CODE_RE.search(member)
            if not match:
                continue
            code = match.group(1)
            if code not in task.codes:
                continue
            try:
                raw = zf.read(member)
                values = compute_member_features(raw)
            except Exception:
                continue
            if values is None:
                continue
            rows.append((code, task.trade_date, values[0], values[1], values[2]))
    return pd.DataFrame(rows, columns=["stock_code", "date", *FEATURE_COLUMNS])


def compute_member_features(raw: bytes) -> Optional[tuple[float, float, float]]:
    text = raw.decode("utf-8-sig", errors="ignore")
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []
    clocks: list[str] = []

    for line in text.splitlines()[1:]:
        if not line:
            continue
        parts = line.split(",")
        if len(parts) < 8:
            continue
        try:
            open_price = float(parts[3])
            close_price = float(parts[4])
            high_price = float(parts[5])
            low_price = float(parts[6])
            volume = float(parts[7])
        except ValueError:
            continue
        if not all(math.isfinite(item) for item in (open_price, close_price, high_price, low_price, volume)):
            continue
        clock = str(parts[0]).strip()[-5:]
        opens.append(open_price)
        highs.append(high_price)
        lows.append(low_price)
        closes.append(close_price)
        volumes.append(max(0.0, volume))
        clocks.append(clock)

    if not closes:
        return None

    open_arr = np.asarray(opens, dtype="float64")
    high_arr = np.asarray(highs, dtype="float64")
    low_arr = np.asarray(lows, dtype="float64")
    close_arr = np.asarray(closes, dtype="float64")
    volume_arr = np.asarray(volumes, dtype="float64")
    clock_arr = np.asarray(clocks, dtype="U5")

    smart_money_ratio = calc_smart_money_ratio(open_arr, close_arr, volume_arr)
    avg_price = (open_arr + high_arr + low_arr + close_arr) / 4.0
    tail_accel = calc_tail_accel(avg_price, clock_arr)
    intra_volatility = calc_intra_volatility(close_arr)
    return smart_money_ratio, tail_accel, intra_volatility


def calc_smart_money_ratio(open_arr: np.ndarray, close_arr: np.ndarray, volume_arr: np.ndarray) -> float:
    valid = np.isfinite(volume_arr) & (volume_arr >= 0)
    if int(valid.sum()) <= 0:
        return float("nan")
    volume = volume_arr[valid]
    opens = open_arr[valid]
    closes = close_arr[valid]
    top_n = max(1, int(math.ceil(len(volume) * 0.2)))
    if top_n >= len(volume):
        idx = np.arange(len(volume))
    else:
        idx = np.argpartition(-volume, top_n - 1)[:top_n]
    top_volume = volume[idx]
    buy_volume = float(top_volume[closes[idx] > opens[idx]].sum())
    sell_volume = float(top_volume[closes[idx] <= opens[idx]].sum())
    total = buy_volume + sell_volume
    return buy_volume / total if total > 0 else float("nan")


def calc_tail_accel(avg_price: np.ndarray, clock_arr: np.ndarray) -> float:
    valid = np.isfinite(avg_price) & (avg_price > 0)
    early_mask = valid & (clock_arr >= "09:30") & (clock_arr < "14:00")
    tail_mask = valid & (clock_arr >= "14:00") & (clock_arr <= "15:00")
    if not early_mask.any() or not tail_mask.any():
        return float("nan")
    early_avg = float(avg_price[early_mask].mean())
    tail_avg = float(avg_price[tail_mask].mean())
    return tail_avg / early_avg if early_avg > 0 else float("nan")


def calc_intra_volatility(close_arr: np.ndarray) -> float:
    valid = close_arr[np.isfinite(close_arr) & (close_arr > 0)]
    if len(valid) <= 1:
        return float("nan")
    mean_price = float(valid.mean())
    return float(valid.std(ddof=0) / mean_price) if mean_price > 0 else float("nan")


def merge_features(source: pd.DataFrame, micro: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float], dict[str, float]]:
    micro_keyed = micro.rename(columns={"stock_code": "_merge_code", "date": "_merge_date"})
    merged = source.merge(
        micro_keyed[["_merge_code", "_merge_date", *FEATURE_COLUMNS]],
        on=["_merge_code", "_merge_date"],
        how="left",
    )
    raw_coverage = {
        col: round(float(pd.to_numeric(merged[col], errors="coerce").notna().mean() * 100.0), 4)
        for col in FEATURE_COLUMNS
    }
    fill_values: dict[str, float] = {}
    for col in FEATURE_COLUMNS:
        numeric = pd.to_numeric(merged[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
        median = float(numeric.median()) if numeric.notna().any() else 0.0
        fill_values[col] = median
        merged[col] = numeric.fillna(median)
    merged = merged.drop(columns=["_merge_code", "_merge_date"], errors="ignore")
    after_fill_coverage = {
        col: round(float(pd.to_numeric(merged[col], errors="coerce").notna().mean() * 100.0), 4)
        for col in FEATURE_COLUMNS
    }
    return merged, raw_coverage, after_fill_coverage


def write_outputs(
    dataset: pd.DataFrame,
    output_path: Path,
    report_path: Path,
    report: dict[str, Any],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(output_path, index=False)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build isolated 5m microstructure factors and merge into global sniper dataset.")
    parser.add_argument("--source-dataset", default="", help="Existing daily/global wide table. Auto-detected when omitted.")
    parser.add_argument("--minute-root", default=str(MINUTE_ROOT))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--output", default=str(OUTPUT_DATASET))
    parser.add_argument("--micro-cache", default=str(MICRO_CACHE_PATH))
    parser.add_argument("--report", default=str(REPORT_PATH))
    parser.add_argument("--workers", type=int, default=max(1, min(8, mp.cpu_count() - 1)))
    parser.add_argument("--refresh-5m", action="store_true", help="Ignore existing micro feature cache.")
    parser.add_argument("--max-days", type=int, default=0, help="Research/debug limiter; 0 means all days.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    source_path = discover_source_dataset(args.source_dataset)
    source = read_wide_dataset(source_path)
    source = filter_dataset_range(source, args.start_date, args.end_date)
    if source.empty:
        raise SystemExit(f"源宽表在 {args.start_date} -> {args.end_date} 区间内没有行。")

    micro_cache = Path(args.micro_cache)
    if micro_cache.exists() and not args.refresh_5m and int(args.max_days or 0) == 0:
        micro = pd.read_parquet(micro_cache)
        micro_source = str(micro_cache)
    else:
        codes_by_date = build_codes_by_date(source)
        tasks = discover_zip_tasks(Path(args.minute_root), args.start_date, args.end_date, codes_by_date, int(args.max_days or 0))
        print(
            f"[5m] zip_tasks={len(tasks)} workers={int(args.workers)} source_rows={len(source)} "
            f"unique_codes={source['_merge_code'].nunique()}",
            flush=True,
        )
        micro = extract_features_from_zips(tasks, workers=int(args.workers))
        if int(args.max_days or 0) == 0:
            micro_cache.parent.mkdir(parents=True, exist_ok=True)
            micro.to_parquet(micro_cache, index=False)
        micro_source = str(Path(args.minute_root))

    merged, raw_coverage, after_fill_coverage = merge_features(source, micro)
    elapsed = time.perf_counter() - started
    report = {
        "source": "scripts/research/extract_5m_features.py",
        "source_dataset": str(source_path),
        "minute_source": micro_source,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "source_rows_after_date_filter": int(len(source)),
        "micro_feature_rows": int(len(micro)),
        "output_rows": int(len(merged)),
        "output_columns": int(len(merged.columns)),
        "feature_columns": FEATURE_COLUMNS,
        "raw_non_null_coverage_pct": raw_coverage,
        "after_fill_non_null_coverage_pct": after_fill_coverage,
        "output_path": str(Path(args.output)),
        "elapsed_seconds": round(float(elapsed), 3),
    }
    write_outputs(merged, Path(args.output), Path(args.report), report)

    print("\n========== 5m Micro Feature Extraction Report ==========")
    print(f"Source Dataset : {source_path}")
    print(f"Minute Source  : {micro_source}")
    print(f"Date Range     : {args.start_date} -> {args.end_date}")
    print(f"Source Rows    : {len(source)}")
    print(f"Micro Rows     : {len(micro)}")
    print(f"Output         : {Path(args.output)}")
    print("Raw Coverage   :")
    for col in FEATURE_COLUMNS:
        print(f"  {col}: {raw_coverage[col]:.4f}%")
    print("After Fill     :")
    for col in FEATURE_COLUMNS:
        print(f"  {col}: {after_fill_coverage[col]:.4f}%")
    print(f"Elapsed Seconds: {elapsed:.3f}")
    print("=======================================================\n")


if __name__ == "__main__":
    main()
