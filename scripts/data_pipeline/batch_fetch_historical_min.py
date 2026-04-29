from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Sequence

from tqdm import tqdm

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import DATA_DIR, MIN_KLINE_DIR
from quant_core.data_pipeline.fetch_minute_data import (
    get_stock_min_data,
    init_jq,
    minute_parquet_path,
    normalize_period,
    normalize_stock_code,
    prefixed_symbol,
    write_minute_parquet,
)


DEFAULT_MIN_FILE_SIZE_BYTES = 10 * 1024
QUOTA_STOP_BUFFER_ROWS = 3000
DEFAULT_COLD_START_DATE = "2025-01-19 09:30:00"
DEFAULT_COLD_END_DATE = "2026-01-23 15:00:00"
PROGRESS_FILENAME = "jq_cold_5m_progress.json"
MIN_EXISTING_JQ_ROWS_PER_SEGMENT = 20


def load_universe(codes: Sequence[str] | None = None, code_file: str | Path | None = None) -> list[str]:
    if codes:
        return sorted({normalize_stock_code(code) for code in codes})
    if code_file:
        text = Path(code_file).read_text(encoding="utf-8")
        items = [item.strip() for line in text.splitlines() for item in line.replace(",", " ").split()]
        return sorted({normalize_stock_code(item) for item in items if item.strip()})
    files = sorted(DATA_DIR.glob("*_daily.parquet"))
    return sorted({normalize_stock_code(path.name.split("_", 1)[0]) for path in files})


def existing_good_codes(
    period: str,
    min_file_size: int = DEFAULT_MIN_FILE_SIZE_BYTES,
    output_root: Path = MIN_KLINE_DIR,
    start_date: str = DEFAULT_COLD_START_DATE,
    end_date: str = DEFAULT_COLD_END_DATE,
    segment_mode: str = "month",
) -> set[str]:
    period_dir = output_root / f"{normalize_period(period)}m"
    if not period_dir.exists():
        return set()
    progress = load_progress(progress_path(output_root, period))
    segments = split_segments(start_date, end_date, mode=segment_mode)
    codes = {
        normalize_stock_code(filename.stem)
        for filename in period_dir.iterdir()
        if filename.name.endswith(".parquet") and filename.stat().st_size >= min_file_size
    }
    return {
        code
        for code in codes
        if all(segment_done(code, period, output_root, segment, progress) for segment in segments)
    }


def query_jq_quota() -> dict[str, object]:
    """Return a normalized JoinQuant quota snapshot."""
    from jqdatasdk import get_query_count

    raw = get_query_count()
    if isinstance(raw, dict):
        total = _first_number(raw, ("total", "limit", "daily_total", "count"))
        spare = _first_number(raw, ("spare", "left", "remaining", "available"))
    else:
        total = _first_number(vars(raw), ("total", "limit", "daily_total", "count")) if hasattr(raw, "__dict__") else None
        spare = _first_number(vars(raw), ("spare", "left", "remaining", "available")) if hasattr(raw, "__dict__") else None
    return {"raw": raw, "total": total, "spare": spare}


def _first_number(mapping: dict[str, object], keys: Sequence[str]) -> int | None:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            try:
                return int(float(mapping[key]))
            except (TypeError, ValueError):
                continue
    return None


def quota_is_exhausted(quota: dict[str, object], buffer_rows: int = QUOTA_STOP_BUFFER_ROWS) -> bool:
    spare = quota.get("spare")
    if spare is None:
        return False
    return int(spare) <= max(0, int(buffer_rows))


def fetch_one_by_segments(
    code: str,
    period: str,
    start_date: str,
    end_date: str,
    output_root: Path,
    segment_mode: str = "month",
    quota_stop_buffer_rows: int = QUOTA_STOP_BUFFER_ROWS,
    progress: dict[str, object] | None = None,
    progress_file: Path | None = None,
    force: bool = False,
) -> dict[str, object]:
    segments = split_segments(start_date, end_date, mode=segment_mode)
    path = minute_parquet_path_for_write(code, period=period, output_root=output_root)
    total_fetched = 0
    total_written = 0
    skipped_segments = 0
    stopped_by_quota = False
    state = progress if progress is not None else {}

    for idx, (segment_start, segment_end) in enumerate(segments, start=1):
        segment = (segment_start, segment_end)
        if not force and segment_done(code, period, output_root, segment, state):
            skipped_segments += 1
            continue

        quota_before = query_jq_quota()
        if quota_is_exhausted(quota_before, quota_stop_buffer_rows):
            print_quota(code, idx, len(segments), quota_before, "额度不足，停止该股票后续切片")
            stopped_by_quota = True
            break

        df = get_stock_min_data(code, period=period, start_date=segment_start, end_date=segment_end)
        written = write_minute_parquet(df, path, code=code, period=period, merge_existing=True)
        total_fetched += len(df)
        total_written = written
        mark_segment_done(state, code, segment, len(df), written)
        if progress_file is not None:
            save_progress(progress_file, state)
        quota_after = query_jq_quota()
        print_quota(code, idx, len(segments), quota_after, f"fetched={len(df)} written={written}")
        if quota_is_exhausted(quota_after, quota_stop_buffer_rows):
            stopped_by_quota = True
            break

    return {
        "code": normalize_stock_code(code),
        "symbol": prefixed_symbol(code),
        "period": f"{normalize_period(period)}m",
        "path": str(path),
        "segments": len(segments),
        "skipped_segments": skipped_segments,
        "fetched_rows": total_fetched,
        "written_rows": total_written,
        "stopped_by_quota": stopped_by_quota,
        "status": "quota_stopped" if stopped_by_quota else ("saved" if total_fetched else "already_done" if skipped_segments == len(segments) else "empty"),
    }


def print_quota(code: str, idx: int, total: int, quota: dict[str, object], note: str) -> None:
    total_quota = quota.get("total")
    spare = quota.get("spare")
    if total_quota is None and spare is None:
        quota_text = f"quota={quota.get('raw')}"
    else:
        quota_text = f"spare={spare} total={total_quota}"
    print(f"[jq-min-batch] {code} segment={idx}/{total} {note}; {quota_text}")


def batch_fetch_historical_min(
    period: str = "5",
    start_date: str | None = None,
    end_date: str | None = None,
    codes: Sequence[str] | None = None,
    code_file: str | Path | None = None,
    limit: int | None = None,
    min_file_size: int = DEFAULT_MIN_FILE_SIZE_BYTES,
    force: bool = False,
    output_root: str | Path = MIN_KLINE_DIR,
    segment_mode: str = "month",
    quota_stop_buffer_rows: int = QUOTA_STOP_BUFFER_ROWS,
) -> dict[str, object]:
    init_jq()
    safe_period = normalize_period(period)
    output_path = Path(output_root)
    start = start_date or DEFAULT_COLD_START_DATE
    end = end_date or DEFAULT_COLD_END_DATE
    universe = load_universe(codes=codes, code_file=code_file)
    if limit:
        universe = universe[: max(0, int(limit))]

    segments = split_segments(start, end, mode=segment_mode)
    progress_file = progress_path(output_path, safe_period)
    progress = {} if force else load_progress(progress_file)
    completed = set() if force else {
        code
        for code in universe
        if all(segment_done(code, safe_period, output_path, segment, progress) for segment in segments)
    }
    pending_count = sum(1 for code in universe if code not in completed)
    skipped = 0

    print(
        f"[jq-min-batch] universe={len(universe)} pending={pending_count} precompleted={len(universe) - pending_count} "
        f"period={safe_period}m range={start} -> {end} segment={segment_mode}"
    )
    print("[jq-min-batch] 数据源=JoinQuant；无代理清洗；无 sleep；按额度停止；Parquet 分段写入。")

    results: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    stopped_by_quota = False
    failure_log = output_path / f"{safe_period}m" / f"jq_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    failure_log.parent.mkdir(parents=True, exist_ok=True)

    for code in tqdm(universe, desc=f"fetch jq {safe_period}m min-kline", unit="stock"):
        if not force and code in completed:
            skipped += 1
            continue

        quota_before = query_jq_quota()
        if quota_is_exhausted(quota_before, quota_stop_buffer_rows):
            print("今日额度已耗尽，请明天继续")
            stopped_by_quota = True
            break

        try:
            result = fetch_one_by_segments(
                code,
                safe_period,
                start,
                end,
                output_path,
                segment_mode=segment_mode,
                quota_stop_buffer_rows=quota_stop_buffer_rows,
                progress=progress,
                progress_file=progress_file,
                force=force,
            )
            results.append(result)
            if result.get("stopped_by_quota"):
                print("今日额度已耗尽，请明天继续")
                stopped_by_quota = True
                break
        except Exception as exc:
            error_text = str(exc)
            if _looks_like_quota_error(error_text):
                print(f"[jq-min-batch][QUOTA] {error_text}")
                print("今日额度已耗尽，请明天继续")
                stopped_by_quota = True
                break
            error_item = {"code": code, "symbol": prefixed_symbol(code), "error": error_text, "time": datetime.now().isoformat(timespec="seconds")}
            errors.append(error_item)
            with failure_log.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(error_item, ensure_ascii=False) + "\n")
            print(f"[jq-min-batch][ERROR] {code}: {error_text}")

    summary_path = output_path / f"{safe_period}m" / f"jq_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary = {
        "period": f"{safe_period}m",
        "start_date": start,
        "end_date": end,
        "segment_mode": segment_mode,
        "universe": len(universe),
        "skipped": skipped,
        "success": len(results),
        "failed": len(errors),
        "stopped_by_quota": stopped_by_quota,
        "progress_path": str(progress_file),
        "output_dir": str(output_path / f"{safe_period}m"),
        "failure_log": str(failure_log) if errors else "",
        "errors": errors[:100],
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def _looks_like_quota_error(error_text: str) -> bool:
    lower = error_text.lower()
    keywords = ("quota", "query count", "count exceeded", "超过", "额度", "流量")
    return any(keyword in lower for keyword in keywords)


def progress_path(output_root: str | Path, period: str) -> Path:
    return Path(output_root) / f"{normalize_period(period)}m" / PROGRESS_FILENAME


def load_progress(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def save_progress(path: Path, progress: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def segment_done(
    code: str,
    period: str,
    output_root: str | Path,
    segment: tuple[str, str],
    progress: dict[str, object] | None = None,
) -> bool:
    safe_code = normalize_stock_code(code)
    key = segment_key(segment)
    code_state = (progress or {}).get(safe_code, {})
    if isinstance(code_state, dict):
        item = code_state.get(key)
        if isinstance(item, dict) and item.get("status") in {"saved", "empty"}:
            return True
    return any(path_has_jq_segment(path, segment) for path in minute_parquet_paths(safe_code, period, output_root))


def mark_segment_done(
    progress: dict[str, object],
    code: str,
    segment: tuple[str, str],
    fetched_rows: int,
    written_rows: int,
) -> None:
    safe_code = normalize_stock_code(code)
    code_state = progress.setdefault(safe_code, {})
    if not isinstance(code_state, dict):
        code_state = {}
        progress[safe_code] = code_state
    code_state[segment_key(segment)] = {
        "status": "saved" if fetched_rows else "empty",
        "fetched_rows": int(fetched_rows),
        "written_rows": int(written_rows),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def segment_key(segment: tuple[str, str]) -> str:
    return f"{segment[0]}|{segment[1]}"


def minute_parquet_paths(code: str, period: str, output_root: str | Path = MIN_KLINE_DIR) -> list[Path]:
    safe_code = normalize_stock_code(code)
    period_dir = Path(output_root) / f"{normalize_period(period)}m"
    prefix = "sh" if safe_code.startswith(("5", "6", "9")) else "sz"
    paths = [period_dir / f"{safe_code}.parquet", period_dir / f"{prefix}{safe_code}.parquet"]
    unique: list[Path] = []
    for path in paths:
        if path not in unique:
            unique.append(path)
    return unique


def minute_parquet_path_for_write(code: str, period: str, output_root: str | Path = MIN_KLINE_DIR) -> Path:
    paths = minute_parquet_paths(code, period, output_root)
    for path in paths:
        if path.exists():
            return path
    return minute_parquet_path(code, period=period, output_root=output_root)


def path_has_jq_segment(path: Path, segment: tuple[str, str]) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        df = pd.read_parquet(path, columns=["datetime", "source"])
    except Exception:
        return False
    if df.empty or "source" not in df.columns:
        return False
    start = parse_cli_datetime(segment[0], is_end=False)
    end = parse_cli_datetime(segment[1], is_end=True)
    dt = pd.to_datetime(df["datetime"], errors="coerce")
    source = df["source"].fillna("").astype(str)
    mask = (source == "jqdatasdk.get_price") & (dt >= start) & (dt <= end)
    return int(mask.sum()) >= MIN_EXISTING_JQ_ROWS_PER_SEGMENT


def split_segments(start_date: str, end_date: str, mode: str = "month") -> list[tuple[str, str]]:
    if mode == "day":
        return split_day_segments(start_date, end_date)
    if mode == "month":
        return split_month_segments(start_date, end_date)
    raise ValueError("segment_mode 只支持 month 或 day")


def split_month_segments(start_date: str, end_date: str) -> list[tuple[str, str]]:
    start_dt = parse_cli_datetime(start_date, is_end=False)
    end_dt = parse_cli_datetime(end_date, is_end=True)
    if start_dt > end_dt:
        raise ValueError(f"开始时间不能晚于结束时间：{start_date} > {end_date}")

    segments: list[tuple[str, str]] = []
    cursor = start_dt
    while cursor <= end_dt:
        next_month = first_day_next_month(cursor.date())
        month_end = datetime.combine(next_month - timedelta(days=1), dt_time(15, 0))
        segment_end = min(month_end, end_dt)
        segments.append((cursor.strftime("%Y-%m-%d %H:%M:%S"), segment_end.strftime("%Y-%m-%d %H:%M:%S")))
        cursor = datetime.combine(next_month, dt_time(9, 30))
    return segments


def split_day_segments(start_date: str, end_date: str) -> list[tuple[str, str]]:
    start_dt = parse_cli_datetime(start_date, is_end=False)
    end_dt = parse_cli_datetime(end_date, is_end=True)
    if start_dt > end_dt:
        raise ValueError(f"开始时间不能晚于结束时间：{start_date} > {end_date}")

    segments: list[tuple[str, str]] = []
    cursor = start_dt
    while cursor.date() <= end_dt.date():
        day_start = max(cursor, datetime.combine(cursor.date(), dt_time(9, 30)))
        day_end = min(end_dt, datetime.combine(cursor.date(), dt_time(15, 0)))
        if day_start <= day_end:
            segments.append((day_start.strftime("%Y-%m-%d %H:%M:%S"), day_end.strftime("%Y-%m-%d %H:%M:%S")))
        cursor = datetime.combine(cursor.date() + timedelta(days=1), dt_time(9, 30))
    return segments


def parse_cli_datetime(value: str, is_end: bool) -> datetime:
    text = str(value).strip()
    if len(text) == 10:
        parsed = datetime.fromisoformat(text)
        return datetime.combine(parsed.date(), dt_time(15, 0) if is_end else dt_time(9, 30))
    return datetime.fromisoformat(text)


def first_day_next_month(day: date) -> date:
    if day.month == 12:
        return date(day.year + 1, 1, 1)
    return date(day.year, day.month + 1, 1)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="聚宽半年历史分时数据批量下载器（JoinQuant + Parquet）")
    parser.add_argument("--period", default="5", choices=["1", "5", "15", "30", "60"])
    parser.add_argument("--start-date", help="开始时间，默认今天向前约190天")
    parser.add_argument("--end-date", help="结束时间，默认今天 15:00")
    parser.add_argument("--code", action="append", dest="codes", help="指定股票代码，可重复传入")
    parser.add_argument("--code-file", help="股票代码文件，一行多个均可，逗号或空格分隔")
    parser.add_argument("--limit", type=int, help="只抓前 N 只，用于小规模测试")
    parser.add_argument("--min-file-size", type=int, default=DEFAULT_MIN_FILE_SIZE_BYTES, help="断点续传判定的最小文件大小")
    parser.add_argument("--force", action="store_true", help="忽略断点续传，强制重新抓取")
    parser.add_argument("--segment", choices=["month", "day"], default="month", help="切片粒度；默认按月，调试可按日")
    parser.add_argument("--quota-buffer", type=int, default=QUOTA_STOP_BUFFER_ROWS, help="剩余额度低于该行数时自动停止")
    args = parser.parse_args(argv)

    try:
        summary = batch_fetch_historical_min(
            period=args.period,
            start_date=args.start_date,
            end_date=args.end_date,
            codes=args.codes,
            code_file=args.code_file,
            limit=args.limit,
            min_file_size=args.min_file_size,
            force=args.force,
            segment_mode=args.segment,
            quota_stop_buffer_rows=args.quota_buffer,
        )
    except RuntimeError as exc:
        raise SystemExit(f"[jq-min-batch][ERROR] {exc}") from exc
    print(summary)


if __name__ == "__main__":
    main()
