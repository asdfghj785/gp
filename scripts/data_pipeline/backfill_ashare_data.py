from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional, Sequence, Union

import pandas as pd
from tqdm import tqdm

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import DATA_DIR, MIN_KLINE_DIR, SQLITE_PATH
from quant_core.data_pipeline.fetch_minute_data import minute_parquet_path, normalize_stock_code, write_minute_parquet
from quant_core.data_pipeline.tencent_engine import get_tencent_daily, get_tencent_m5
from quant_core.storage import upsert_daily_rows, upsert_minute_5m_rows
from quant_core.utils.stock_filter import get_core_universe, get_st_name_map


def load_codes(codes: Optional[Sequence[str]] = None, code_file: Optional[Union[str, Path]] = None) -> list[str]:
    items: list[str] = []
    if codes:
        items.extend(codes)
    if code_file:
        text = Path(code_file).read_text(encoding="utf-8")
        items.extend(item for line in text.splitlines() for item in line.replace(",", " ").split())
    return sorted({normalize_stock_code(item) for item in items if str(item).strip()})


def backfill_ashare_data(
    universe_mode: str = "core",
    codes: Optional[Sequence[str]] = None,
    code_file: Optional[Union[str, Path]] = None,
    limit: Optional[int] = None,
    daily_count: int = 1000,
    m5_count: int = 3000,
    sleep_seconds: float = 0.03,
    retries: int = 2,
    skip_daily: bool = False,
    skip_m5: bool = False,
    replace_daily: bool = False,
) -> dict[str, object]:
    started_at = datetime.now().isoformat(timespec="seconds")
    explicit_codes = load_codes(codes=codes, code_file=code_file)
    mode = normalize_universe_mode(universe_mode)
    name_map = latest_name_map()
    st_name_map = get_st_name_map(include_daily_picks=True)
    name_map.update(st_name_map)
    universe = explicit_codes or build_universe(mode=mode, st_name_map=st_name_map)
    if limit:
        universe = universe[: max(0, int(limit))]

    results: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    daily_fetched_total = 0
    daily_inserted_total = 0
    daily_parquet_appended_total = 0
    m5_fetched_total = 0
    m5_db_upserted_total = 0
    m5_total_rows_after_merge_sum = 0

    source_tag = "nonst" if mode == "core" else mode
    for index, code in enumerate(tqdm(universe, desc=f"{mode} ashare/tencent backfill", unit="stock"), start=1):
        name = name_map.get(code, "")
        item: dict[str, object] = {"code": code, "name": name}
        if skip_daily:
            item.update({"daily_skipped": True, "daily_fetched_rows": 0, "daily_inserted_rows": 0, "daily_parquet_appended_rows": 0})
        else:
            try:
                daily = retry(lambda: fetch_daily_frame(code, name=name, count=daily_count, source_tag=source_tag), retries=retries)
                daily_inserted = (
                    upsert_daily_rows(daily, source=f"tencent_daily_qfq_{source_tag}_backfill")
                    if replace_daily
                    else upsert_missing_daily_rows(code, daily, source_tag=source_tag)
                )
                daily_parquet_appended = append_missing_daily_parquet(code, daily)
                item.update(
                    {
                        "daily_fetched_rows": int(len(daily)),
                        "daily_inserted_rows": int(daily_inserted),
                        "daily_parquet_appended_rows": int(daily_parquet_appended),
                        "daily_path": str(DATA_DIR / f"{code}_daily.parquet"),
                    }
                )
                daily_fetched_total += int(len(daily))
                daily_inserted_total += int(daily_inserted)
                daily_parquet_appended_total += int(daily_parquet_appended)
            except Exception as exc:
                item["daily_error"] = str(exc)

        if skip_m5:
            item.update({"m5_skipped": True, "m5_fetched_rows": 0, "m5_total_rows_after_merge": 0})
        else:
            try:
                m5 = retry(lambda: fetch_m5_frame(code, count=m5_count, source_tag=source_tag), retries=retries)
                m5_db_upserted = upsert_minute_5m_rows(m5, code=code) if not m5.empty else 0
                m5_path = minute_path_for_write(code)
                m5_total = write_minute_parquet(m5, m5_path, code=code, period="5", merge_existing=True) if not m5.empty else 0
                item.update(
                    {
                        "m5_fetched_rows": int(len(m5)),
                        "m5_db_upserted_rows": int(m5_db_upserted),
                        "m5_total_rows_after_merge": int(m5_total),
                        "m5_path": str(m5_path),
                        "m5_start": min_text(m5.get("datetime")) if not m5.empty else "",
                        "m5_end": max_text(m5.get("datetime")) if not m5.empty else "",
                    }
                )
                m5_fetched_total += int(len(m5))
                m5_db_upserted_total += int(m5_db_upserted)
                m5_total_rows_after_merge_sum += int(m5_total)
            except Exception as exc:
                item["m5_error"] = str(exc)

        if "daily_error" in item or "m5_error" in item:
            errors.append(item)
        results.append(item)
        tqdm.write(
            f"[backfill:{mode}] {index}/{len(universe)} {code} "
            f"daily_fetch={item.get('daily_fetched_rows', 0)} daily+{item.get('daily_parquet_appended_rows', 0)} "
            f"db+{item.get('daily_inserted_rows', 0)} m5={item.get('m5_fetched_rows', 0)} "
            f"m5_db={item.get('m5_db_upserted_rows', 0)} m5_total={item.get('m5_total_rows_after_merge', 0)}"
        )
        if index < len(universe):
            time.sleep(max(0.0, float(sleep_seconds)))

    finished_at = datetime.now().isoformat(timespec="seconds")
    summary = {
        "source": f"ashare_tencent_{source_tag}_backfill",
        "started_at": started_at,
        "finished_at": finished_at,
        "universe_mode": mode,
        "universe": len(universe),
        "daily_count": daily_count,
        "m5_count": m5_count,
        "skip_daily": skip_daily,
        "skip_m5": skip_m5,
        "replace_daily": replace_daily,
        "success": len(results) - len(errors),
        "failed": len(errors),
        "daily_fetched_rows": daily_fetched_total,
        "daily_inserted_rows": daily_inserted_total,
        "daily_parquet_appended_rows": daily_parquet_appended_total,
        "m5_fetched_rows": m5_fetched_total,
        "m5_db_upserted_rows": m5_db_upserted_total,
        "m5_total_rows_after_merge_sum": m5_total_rows_after_merge_sum,
        "m5_nonempty_codes": sum(1 for row in results if int(row.get("m5_fetched_rows") or 0) > 0),
        "daily_nonempty_codes": sum(1 for row in results if int(row.get("daily_fetched_rows") or 0) > 0),
        "results": results,
        "errors": errors[:50],
        "note": (
            "Ashare package is not required for this workstation path; this uses the Tencent endpoints used by "
            "the local Ashare-compatible pipeline. When replace_daily=true, daily qfq bars overwrite the SQLite "
            "stock_daily row for the same code/date; parquet files are only appended for compatibility. 5m bars "
            "are realtime/raw price bars and are merged into stock_minute_5m by datetime."
        ),
    }
    summary_path = MIN_KLINE_DIR / "5m" / f"ashare_backfill_{mode}_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def normalize_universe_mode(value: str) -> str:
    mode = str(value or "core").strip().lower().replace("-", "_")
    aliases = {"nonst": "core", "non_st": "core", "mainboard": "core", "st_only": "st", "all_mainboard": "all"}
    mode = aliases.get(mode, mode)
    if mode not in {"core", "st", "all"}:
        raise ValueError("universe must be one of: core, st, all")
    return mode


def build_universe(mode: str, st_name_map: dict[str, str]) -> list[str]:
    core = set(get_core_universe())
    st = set(st_name_map)
    if mode == "core":
        return sorted(core)
    if mode == "st":
        return sorted(st)
    return sorted(core | st)


def latest_name_map() -> dict[str, str]:
    if not SQLITE_PATH.exists():
        return {}
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            rows = conn.execute(
                """
                SELECT code, name
                FROM (
                    SELECT code, name,
                           ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
                    FROM stock_daily
                    WHERE code IS NOT NULL AND code != ''
                )
                WHERE rn = 1
                """
            ).fetchall()
    except Exception:
        return {}
    out: dict[str, str] = {}
    for code, name in rows:
        try:
            clean = normalize_stock_code(code)
        except Exception:
            continue
        out[clean] = str(name or "").strip()
    return out


def fetch_daily_frame(code: str, name: str = "", count: int = 1000, source_tag: str = "core") -> pd.DataFrame:
    df = get_tencent_daily(code, count=count, adjust="qfq")
    if df.empty:
        return df
    out = df.copy()
    out["name"] = name or code
    out["turnover"] = None
    out["volume_ratio"] = None
    out["source"] = f"tencent.daily.qfq.{source_tag}_backfill"
    return out


def fetch_m5_frame(code: str, count: int = 3000, source_tag: str = "core") -> pd.DataFrame:
    df = get_tencent_m5(code, count=count)
    if df.empty:
        return df
    out = df.copy()
    out["money"] = 0.0
    out["source"] = f"tencent.m5.{source_tag}_backfill"
    return out


def retry(action: Callable[[], pd.DataFrame], retries: int = 2) -> pd.DataFrame:
    attempts = max(1, int(retries))
    last_error: Optional[Exception] = None
    for index in range(attempts):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            if index + 1 < attempts:
                time.sleep(0.3 * (index + 1))
    if last_error is not None:
        raise last_error
    return pd.DataFrame()


def upsert_missing_daily_rows(code: str, fresh: pd.DataFrame, source_tag: str = "core") -> int:
    if fresh.empty:
        return 0
    missing = fresh[fresh["date"].astype(str).isin(missing_daily_dates_in_db(code, fresh["date"].astype(str).tolist()))].copy()
    if missing.empty:
        return 0
    return upsert_daily_rows(missing, source=f"tencent_daily_qfq_{source_tag}_backfill")


def missing_daily_dates_in_db(code: str, dates: list[str]) -> set[str]:
    if not SQLITE_PATH.exists() or not dates:
        return set(dates)
    placeholders = ",".join(["?"] * len(dates))
    with sqlite3.connect(SQLITE_PATH) as conn:
        rows = conn.execute(
            f"SELECT date FROM stock_daily WHERE code = ? AND date IN ({placeholders})",
            (code, *dates),
        ).fetchall()
    existing = {str(row[0]) for row in rows}
    return set(dates) - existing


def append_missing_daily_parquet(code: str, fresh: pd.DataFrame) -> int:
    if fresh.empty:
        return 0
    path = DATA_DIR / f"{code}_daily.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    new_rows = fresh.copy()
    if path.exists():
        existing = pd.read_parquet(path)
        existing_dates = set(normalize_date_series(existing["date"]).dropna().astype(str))
        new_rows = new_rows[~new_rows["date"].astype(str).isin(existing_dates)].copy()
        if new_rows.empty:
            return 0
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
                category=FutureWarning,
            )
            merged = pd.concat([existing, new_rows], ignore_index=True, sort=False)
    else:
        merged = new_rows
    merged["_sort_date"] = normalize_date_series(merged["date"])
    merged = merged.sort_values("_sort_date").drop(columns=["_sort_date"]).reset_index(drop=True)
    merged.to_parquet(path, engine="pyarrow", index=False)
    return int(len(new_rows))


def normalize_date_series(values: pd.Series) -> pd.Series:
    text = values.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    yyyymmdd = text.str.fullmatch(r"\d{8}", na=False)
    parsed.loc[yyyymmdd] = pd.to_datetime(text.loc[yyyymmdd], format="%Y%m%d", errors="coerce")
    parsed.loc[~yyyymmdd] = pd.to_datetime(text.loc[~yyyymmdd], errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d")


def minute_path_for_write(code: str) -> Path:
    safe = normalize_stock_code(code)
    prefixed = minute_parquet_path(safe, period="5", output_root=MIN_KLINE_DIR)
    plain = MIN_KLINE_DIR / "5m" / f"{safe}.parquet"
    if prefixed.exists():
        return prefixed
    if plain.exists():
        return plain
    return prefixed


def min_text(values: pd.Series) -> str:
    value = pd.to_datetime(values, errors="coerce").min()
    return "" if pd.isna(value) else value.strftime("%Y-%m-%d %H:%M:%S")


def max_text(values: pd.Series) -> str:
    value = pd.to_datetime(values, errors="coerce").max()
    return "" if pd.isna(value) else value.strftime("%Y-%m-%d %H:%M:%S")


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="补全核心/ST 股票日线与 5m 热数据（Ashare/Tencent 兼容链路）")
    parser.add_argument("--universe", default="core", choices=["core", "nonst", "non_st", "st", "all"], help="补全股票池，core/nonst 为非 ST 主板")
    parser.add_argument("--code", action="append", dest="codes", help="显式股票代码，可重复传入")
    parser.add_argument("--code-file", help="股票代码文件，支持空格/逗号/换行分隔")
    parser.add_argument("--limit", type=int, help="只补前 N 只，用于测试")
    parser.add_argument("--daily-count", type=int, default=1000, help="腾讯 qfq 日线最大请求根数")
    parser.add_argument("--m5-count", type=int, default=3000, help="腾讯 5m 最大请求根数；接口通常最多返回约 320 根")
    parser.add_argument("--sleep", type=float, default=0.03, help="每只股票之间休眠秒数")
    parser.add_argument("--retries", type=int, default=2, help="单接口失败重试次数")
    parser.add_argument("--skip-daily", action="store_true", help="只补 5m，不补日线")
    parser.add_argument("--skip-m5", action="store_true", help="只补日线，不补 5m")
    parser.add_argument("--replace-daily", action="store_true", help="日线按 Ashare/Tencent qfq 结果覆盖同 code/date 旧源记录")
    args = parser.parse_args(argv)
    print(json.dumps(
        backfill_ashare_data(
            universe_mode=args.universe,
            codes=args.codes,
            code_file=args.code_file,
            limit=args.limit,
            daily_count=args.daily_count,
            m5_count=args.m5_count,
            sleep_seconds=args.sleep,
            retries=args.retries,
            skip_daily=args.skip_daily,
            skip_m5=args.skip_m5,
            replace_daily=args.replace_daily,
        ),
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
