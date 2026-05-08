from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from tqdm import tqdm

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import MIN_KLINE_DIR, SQLITE_PATH
from quant_core.storage import init_db, upsert_minute_5m_rows


ALLOWED_SOURCE_PREFIXES = ("tencent.m5",)


def rebuild_minute_5m_sqlite(
    minute_dir: Path = MIN_KLINE_DIR / "5m",
    reset: bool = False,
    limit: Optional[int] = None,
) -> dict[str, object]:
    init_db()
    started_at = datetime.now().isoformat(timespec="seconds")
    if reset:
        with sqlite3.connect(SQLITE_PATH) as conn:
            conn.execute("DELETE FROM stock_minute_5m")
            conn.commit()

    files = sorted(minute_dir.glob("*.parquet"))
    if limit:
        files = files[: max(0, int(limit))]

    imported_rows = 0
    eligible_rows = 0
    scanned_rows = 0
    imported_files = 0
    skipped_files = 0
    source_counts: dict[str, int] = {}
    errors: list[dict[str, str]] = []

    for path in tqdm(files, desc="sqlite minute 5m rebuild", unit="file"):
        try:
            df = pd.read_parquet(path)
            scanned_rows += int(len(df))
            if df.empty:
                skipped_files += 1
                continue
            if "source" not in df.columns:
                skipped_files += 1
                continue
            source = df["source"].fillna("unknown").astype(str)
            for key, value in source.value_counts().to_dict().items():
                source_counts[str(key)] = source_counts.get(str(key), 0) + int(value)
            mask = source.map(lambda value: value.startswith(ALLOWED_SOURCE_PREFIXES))
            clean = df.loc[mask].copy()
            if clean.empty:
                skipped_files += 1
                continue
            code = path.name
            if "_" in code:
                code = code.split("_", 1)[0]
            code = "".join(ch for ch in code if ch.isdigit())[-6:]
            written = upsert_minute_5m_rows(clean, code=code)
            eligible_rows += int(len(clean))
            imported_rows += int(written)
            imported_files += 1
        except Exception as exc:
            errors.append({"file": str(path), "error": str(exc)})

    with sqlite3.connect(SQLITE_PATH) as conn:
        total_rows = conn.execute("SELECT COUNT(*) FROM stock_minute_5m").fetchone()[0]
        total_codes = conn.execute("SELECT COUNT(DISTINCT code) FROM stock_minute_5m").fetchone()[0]
        min_dt, max_dt = conn.execute("SELECT MIN(datetime), MAX(datetime) FROM stock_minute_5m").fetchone()
        db_sources = conn.execute(
            "SELECT source, COUNT(*) FROM stock_minute_5m GROUP BY source ORDER BY COUNT(*) DESC"
        ).fetchall()

    summary = {
        "source": "sqlite_stock_minute_5m_rebuild",
        "started_at": started_at,
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "minute_dir": str(minute_dir),
        "reset": reset,
        "files_seen": len(files),
        "files_imported": imported_files,
        "files_skipped": skipped_files,
        "scanned_rows": scanned_rows,
        "eligible_ashare_tencent_rows": eligible_rows,
        "imported_rows": imported_rows,
        "errors_count": len(errors),
        "errors": errors[:50],
        "source_counts_seen": dict(sorted(source_counts.items(), key=lambda item: item[1], reverse=True)[:30]),
        "database": str(SQLITE_PATH),
        "db_rows": int(total_rows),
        "db_codes": int(total_codes),
        "db_min_datetime": min_dt,
        "db_max_datetime": max_dt,
        "db_source_counts": {str(key): int(value) for key, value in db_sources},
        "note": "Only source values starting with tencent.m5 are imported. JoinQuant/AkShare/5min legacy rows remain on disk for audit or future simulation, but are not production DB rows.",
    }
    summary_path = minute_dir / f"sqlite_minute_5m_rebuild_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="从 Ashare/Tencent 兼容 5m Parquet 重建统一 SQLite 分钟表")
    parser.add_argument("--minute-dir", default=str(MIN_KLINE_DIR / "5m"))
    parser.add_argument("--reset", action="store_true", help="清空 stock_minute_5m 后重建")
    parser.add_argument("--limit", type=int, help="只处理前 N 个文件，用于测试")
    args = parser.parse_args(argv)
    print(json.dumps(
        rebuild_minute_5m_sqlite(Path(args.minute_dir), reset=args.reset, limit=args.limit),
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
