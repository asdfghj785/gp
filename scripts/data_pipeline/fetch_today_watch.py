from __future__ import annotations

import os
import sys
import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path

import pandas as pd

# AkShare/东方财富近期分时必须直连国内数据源，避免系统代理造成 ProxyError。
for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy"):
    os.environ[key] = ""
os.environ["NO_PROXY"] = "*"

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import MIN_KLINE_DIR
from quant_core.data_pipeline.fetch_minute_data import (
    fetch_from_akshare,
    minute_parquet_path,
    normalize_period,
    normalize_stock_code,
    write_minute_parquet,
)


WATCH_CODES = ("002709", "600865")
START_DATE = "2026-04-01 09:30:00"
END_DATE = "2026-04-28 15:00:00"


def fetch_today_watch(period: str = "5") -> list[dict[str, object]]:
    safe_period = normalize_period(period)
    results: list[dict[str, object]] = []
    for code in WATCH_CODES:
        safe_code = normalize_stock_code(code)
        frames: list[pd.DataFrame] = []
        errors: list[str] = []
        for segment_start, segment_end in split_day_segments(START_DATE, END_DATE):
            try:
                df = fetch_akshare_with_retry(safe_code, safe_period, segment_start, segment_end)
                if not df.empty:
                    frames.append(df)
            except Exception as exc:
                errors.append(f"{segment_start}->{segment_end}: {exc}")
                print(f"[watch-min][WARN] {safe_code} {segment_start}->{segment_end} failed: {exc}")
        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        path = minute_parquet_path(safe_code, period=safe_period, output_root=MIN_KLINE_DIR)
        written_rows = write_minute_parquet(df, path, code=safe_code, period=safe_period, merge_existing=False) if not df.empty else 0
        item = {
            "code": safe_code,
            "period": f"{safe_period}m",
            "path": str(path),
            "fetched_rows": len(df),
            "written_rows": written_rows,
            "errors": errors[:10],
            "status": "saved" if written_rows else "empty",
        }
        print(f"[watch-min] {safe_code} fetched={len(df)} written={written_rows} path={path}")
        results.append(item)
    return results


def fetch_akshare_with_retry(code: str, period: str, start_date: str, end_date: str) -> pd.DataFrame:
    waits = (0, 3, 8)
    last_error: Exception | None = None
    for attempt, wait_seconds in enumerate(waits, start=1):
        if wait_seconds:
            time.sleep(wait_seconds)
        try:
            return fetch_from_akshare(code, period=period, start_date=start_date, end_date=end_date)
        except Exception as exc:
            last_error = exc
            print(f"[watch-min][RETRY] {code} attempt={attempt} wait_next={wait_seconds}s error={exc}")
    raise RuntimeError(f"{code} AkShare 分片抓取失败：{last_error}") from last_error


def split_day_segments(start_date: str, end_date: str) -> list[tuple[str, str]]:
    start_dt = parse_dt(start_date, is_end=False)
    end_dt = parse_dt(end_date, is_end=True)
    segments: list[tuple[str, str]] = []
    cursor = start_dt
    while cursor.date() <= end_dt.date():
        day_start = max(cursor, datetime.combine(cursor.date(), dt_time(9, 30)))
        day_end = min(end_dt, datetime.combine(cursor.date(), dt_time(15, 0)))
        if day_start <= day_end:
            segments.append((day_start.strftime("%Y-%m-%d %H:%M:%S"), day_end.strftime("%Y-%m-%d %H:%M:%S")))
        cursor = datetime.combine(cursor.date() + timedelta(days=1), dt_time(9, 30))
    return segments


def parse_dt(value: str, is_end: bool) -> datetime:
    text = str(value).strip()
    if len(text) == 10:
        parsed = datetime.fromisoformat(text)
        return datetime.combine(parsed.date(), dt_time(15, 0) if is_end else dt_time(9, 30))
    return datetime.fromisoformat(text)


if __name__ == "__main__":
    print(fetch_today_watch())
