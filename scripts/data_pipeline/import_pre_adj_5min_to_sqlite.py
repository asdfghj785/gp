from __future__ import annotations

import argparse
import io
import json
import sqlite3
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional, Sequence

import pandas as pd

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import SQLITE_PATH
from quant_core.storage import MINUTE_5M_COLUMNS, normalize_minute_5m_frame, upsert_minute_5m_rows


DEFAULT_SOURCE_ROOT = Path("/Users/eudis/5min/organized_5min_pre_adj/sh_sz")
SOURCE_LABEL = "local_5min_pre_adj"


def normalize_code(value: object) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def normalize_date(value: object) -> str:
    text = str(value or "").strip().replace("-", "")
    return text[:8]


def load_codes(codes: Optional[Sequence[str]], code_file: Optional[str]) -> set[str]:
    items: list[str] = []
    if codes:
        items.extend(codes)
    if code_file:
        text = Path(code_file).read_text(encoding="utf-8")
        items.extend(token for line in text.splitlines() for token in line.replace(",", " ").split())
    return {normalize_code(item) for item in items if normalize_code(item)}


def iter_zip_paths(root: Path, start_date: str, end_date: str) -> list[Path]:
    start = normalize_date(start_date)
    end = normalize_date(end_date)
    paths: list[Path] = []
    for path in sorted(root.glob("*/*_5min.zip")):
        day = path.name.split("_", 1)[0]
        if len(day) == 8 and start <= day <= end:
            paths.append(path)
    return paths


def member_code(member: str) -> str:
    return normalize_code(Path(member).stem)


def read_member_frame(zf: zipfile.ZipFile, member: str) -> pd.DataFrame:
    raw = zf.read(member)
    df = pd.read_csv(io.BytesIO(raw), encoding="utf-8-sig")
    if df.empty:
        return df
    out = pd.DataFrame(
        {
            "datetime": df.get("时间"),
            "code": df.get("代码"),
            "open": df.get("开盘价"),
            "high": df.get("最高价"),
            "low": df.get("最低价"),
            "close": df.get("收盘价"),
            "volume": df.get("成交量"),
            "amount": df.get("成交额"),
            "source": SOURCE_LABEL,
        }
    )
    return out


def filter_missing_rows(df: pd.DataFrame, db_path: Path) -> pd.DataFrame:
    if df.empty:
        return df
    code = normalize_code(df["code"].iloc[0])
    datetimes = pd.to_datetime(df["datetime"], errors="coerce").dropna().dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
    if not datetimes:
        return df.iloc[0:0].copy()
    placeholders = ",".join(["?"] * len(datetimes))
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT datetime FROM stock_minute_5m WHERE code = ? AND datetime IN ({placeholders})",
            (code, *datetimes),
        ).fetchall()
    existing = {str(row[0]) for row in rows}
    parsed = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.loc[~parsed.isin(existing)].copy()


def insert_missing_minute_5m_rows(df: pd.DataFrame, chunk_size: int = 50000) -> int:
    rows_df = normalize_minute_5m_frame(df, source=SOURCE_LABEL)
    if rows_df.empty:
        return 0
    rows = [
        tuple(None if pd.isna(value) else value for value in record)
        for record in rows_df[MINUTE_5M_COLUMNS].itertuples(index=False, name=None)
    ]
    placeholders = ",".join(["?"] * len(MINUTE_5M_COLUMNS))
    sql = f"""
        INSERT OR IGNORE INTO stock_minute_5m ({",".join(MINUTE_5M_COLUMNS)})
        VALUES ({placeholders})
    """
    inserted = 0
    with sqlite3.connect(SQLITE_PATH) as conn:
        before = conn.total_changes
        for start in range(0, len(rows), max(1, int(chunk_size))):
            conn.executemany(sql, rows[start : start + chunk_size])
        inserted = conn.total_changes - before
    return int(inserted)


def import_pre_adj_5min(
    source_root: Path = DEFAULT_SOURCE_ROOT,
    start_date: str = "2026-02-25",
    end_date: str = "2026-05-06",
    codes: Optional[Iterable[str]] = None,
    overwrite: bool = False,
    zip_batch: bool = True,
) -> dict[str, object]:
    wanted_codes = {normalize_code(code) for code in (codes or []) if normalize_code(code)}
    started_at = datetime.now().isoformat(timespec="seconds")
    zip_paths = iter_zip_paths(source_root, start_date, end_date)
    imported_rows = 0
    scanned_rows = 0
    imported_members = 0
    scanned_members = 0
    errors: list[dict[str, str]] = []
    touched_codes: set[str] = set()

    for zip_path in zip_paths:
        try:
            with zipfile.ZipFile(zip_path) as zf:
                members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
                if wanted_codes:
                    members = [name for name in members if member_code(name) in wanted_codes]
                if zip_batch:
                    frames: list[pd.DataFrame] = []
                    for member in members:
                        scanned_members += 1
                        try:
                            frame = read_member_frame(zf, member)
                            if frame.empty:
                                continue
                            scanned_rows += int(len(frame))
                            frames.append(frame)
                            touched_codes.add(member_code(member))
                        except Exception as exc:
                            errors.append({"zip": str(zip_path), "member": member, "error": str(exc)})
                    if frames:
                        merged = pd.concat(frames, ignore_index=True, sort=False)
                        inserted = (
                            upsert_minute_5m_rows(merged, source=SOURCE_LABEL)
                            if overwrite
                            else insert_missing_minute_5m_rows(merged)
                        )
                        imported_rows += int(inserted)
                        if inserted:
                            imported_members += len(frames)
                    continue
                for member in members:
                    scanned_members += 1
                    try:
                        frame = read_member_frame(zf, member)
                        if frame.empty:
                            continue
                        scanned_rows += int(len(frame))
                        if not overwrite:
                            frame = filter_missing_rows(frame, SQLITE_PATH)
                        if frame.empty:
                            continue
                        inserted = upsert_minute_5m_rows(frame, source=SOURCE_LABEL)
                        imported_rows += int(inserted)
                        imported_members += 1
                        touched_codes.add(member_code(member))
                    except Exception as exc:
                        errors.append({"zip": str(zip_path), "member": member, "error": str(exc)})
        except Exception as exc:
            errors.append({"zip": str(zip_path), "member": "", "error": str(exc)})

    finished_at = datetime.now().isoformat(timespec="seconds")
    summary = {
        "source": SOURCE_LABEL,
        "source_root": str(source_root),
        "started_at": started_at,
        "finished_at": finished_at,
        "start_date": start_date,
        "end_date": end_date,
        "zip_count": len(zip_paths),
        "code_filter_count": len(wanted_codes),
        "scanned_members": scanned_members,
        "imported_members": imported_members,
        "scanned_rows": scanned_rows,
        "imported_rows": imported_rows,
        "touched_codes": len(touched_codes),
        "overwrite": overwrite,
        "zip_batch": zip_batch,
        "errors_count": len(errors),
        "errors": errors[:50],
    }
    summary_path = BASE_DIR / "data" / "min_kline" / "5m" / f"local_pre_adj_sqlite_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Import local pre-adjusted 5m zip data into stock_minute_5m.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--start-date", default="2026-02-25")
    parser.add_argument("--end-date", default="2026-05-06")
    parser.add_argument("--code", action="append", dest="codes")
    parser.add_argument("--code-file")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing code/datetime rows; default only inserts missing rows.")
    parser.add_argument("--no-zip-batch", action="store_true", help="Use slower member-by-member import path.")
    args = parser.parse_args(argv)
    code_set = load_codes(args.codes, args.code_file)
    print(json.dumps(
        import_pre_adj_5min(
            source_root=Path(args.source_root),
            start_date=args.start_date,
            end_date=args.end_date,
            codes=code_set,
            overwrite=args.overwrite,
            zip_batch=not args.no_zip_batch,
        ),
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
