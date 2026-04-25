from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .config import DATA_DIR, PROFIT_TARGET_PCT, SQLITE_PATH, ensure_dirs


DAILY_COLUMNS = [
    "code",
    "name",
    "date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change_pct",
    "volume",
    "amount",
    "turnover",
    "volume_ratio",
    "source",
    "ingested_at",
]


def connect() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS stock_daily (
                code TEXT NOT NULL,
                name TEXT,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                pre_close REAL,
                change_pct REAL,
                volume REAL,
                amount REAL,
                turnover REAL,
                volume_ratio REAL,
                source TEXT NOT NULL DEFAULT 'unknown',
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (code, date)
            );

            CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily(date);
            CREATE INDEX IF NOT EXISTS idx_stock_daily_code ON stock_daily(code);

            CREATE TABLE IF NOT EXISTS validation_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                scope TEXT NOT NULL,
                status TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                issues_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS prediction_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                strategy TEXT NOT NULL,
                rows_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_picks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                selection_date TEXT NOT NULL UNIQUE,
                target_date TEXT NOT NULL,
                selected_at TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                win_rate REAL NOT NULL,
                selection_price REAL NOT NULL,
                selection_change REAL,
                model_status TEXT,
                status TEXT NOT NULL DEFAULT 'pending_open',
                open_price REAL,
                open_checked_at TEXT,
                open_premium REAL,
                success INTEGER,
                raw_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_daily_picks_target_date ON daily_picks(target_date);
            CREATE INDEX IF NOT EXISTS idx_daily_picks_status ON daily_picks(status);

            CREATE TABLE IF NOT EXISTS market_sync_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                sync_date TEXT,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                fetched_rows INTEGER NOT NULL DEFAULT 0,
                valid_rows INTEGER NOT NULL DEFAULT 0,
                inserted_rows INTEGER NOT NULL DEFAULT 0,
                updated_rows INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                summary_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_market_sync_runs_finished_at ON market_sync_runs(finished_at);
            CREATE INDEX IF NOT EXISTS idx_market_sync_runs_status ON market_sync_runs(status);
            """
        )


def normalize_daily_frame(df: pd.DataFrame, source: str = "parquet") -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    out = df.copy()
    rename_candidates = {
        "pctChg": "change_pct",
        "turn": "turnover",
        "量比": "volume_ratio",
        "最新价": "close",
        "涨跌幅": "change_pct",
        "换手率": "turnover",
        "昨收": "pre_close",
        "今开": "open",
        "最高": "high",
        "最低": "low",
        "名称": "name",
        "代码": "code",
    }
    out = out.rename(columns={k: v for k, v in rename_candidates.items() if k in out.columns})
    out = _coalesce_duplicate_columns(out)

    if "code" not in out.columns:
        out["code"] = None
    out["code"] = out["code"].astype(str).str.extract(r"(\d{6})")[0]
    if "symbol" in out.columns:
        symbol_code = out["symbol"].astype(str).str.extract(r"(\d{6})")[0]
        out["code"] = out["code"].fillna(symbol_code)
    known_codes = out["code"].dropna().unique()
    if len(known_codes) == 1:
        out["code"] = out["code"].fillna(known_codes[0])

    if "date" not in out.columns:
        out["date"] = datetime.now().strftime("%Y-%m-%d")
    out["date"] = _parse_mixed_dates(out["date"])

    if "name" not in out.columns:
        out["name"] = ""
    if "pre_close" not in out.columns and "close" in out.columns and "change_pct" in out.columns:
        pct = pd.to_numeric(out["change_pct"], errors="coerce")
        close = pd.to_numeric(out["close"], errors="coerce")
        out["pre_close"] = close / (1 + pct / 100)

    for col in ["open", "high", "low", "close", "pre_close", "change_pct", "volume", "amount", "turnover", "volume_ratio"]:
        if col not in out.columns:
            out[col] = None
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["source"] = source
    out["ingested_at"] = datetime.now().isoformat(timespec="seconds")
    out = out[DAILY_COLUMNS]
    return out.dropna(subset=["code", "date"]).copy()


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.duplicated().any():
        return df
    merged: dict[str, pd.Series] = {}
    for col in dict.fromkeys(df.columns):
        subset = df.loc[:, df.columns == col]
        if subset.shape[1] == 1:
            merged[col] = subset.iloc[:, 0]
        else:
            merged[col] = subset.bfill(axis=1).iloc[:, 0]
    return pd.DataFrame(merged)


def _parse_mixed_dates(values: pd.Series) -> pd.Series:
    text = values.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    yyyymmdd = text.str.fullmatch(r"\d{8}", na=False)
    parsed.loc[yyyymmdd] = pd.to_datetime(text.loc[yyyymmdd], format="%Y%m%d", errors="coerce")
    parsed.loc[~yyyymmdd] = pd.to_datetime(text.loc[~yyyymmdd], errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d")


def upsert_daily_rows(df: pd.DataFrame, source: str = "unknown") -> int:
    init_db()
    rows_df = normalize_daily_frame(df, source=source)
    if rows_df.empty:
        return 0
    rows = [
        tuple(None if pd.isna(value) else value for value in record)
        for record in rows_df[DAILY_COLUMNS].itertuples(index=False, name=None)
    ]
    placeholders = ",".join(["?"] * len(DAILY_COLUMNS))
    assignments = ",".join([f"{col}=excluded.{col}" for col in DAILY_COLUMNS if col not in {"code", "date"}])
    sql = f"""
        INSERT INTO stock_daily ({",".join(DAILY_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT(code, date) DO UPDATE SET {assignments}
    """
    with connect() as conn:
        conn.executemany(sql, rows)
    return len(rows)


def import_parquet_files(
    data_dir: Path = DATA_DIR,
    codes: Iterable[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    init_db()
    wanted = {str(code).zfill(6) for code in codes} if codes else None
    files = sorted(data_dir.glob("*_daily.parquet"))
    if wanted:
        files = [path for path in files if path.name[:6] in wanted]
    if limit:
        files = files[:limit]

    imported_rows = 0
    failed: list[dict[str, str]] = []
    for path in files:
        try:
            df = pd.read_parquet(path)
            imported_rows += upsert_daily_rows(df, source="parquet")
        except Exception as exc:
            failed.append({"file": str(path), "error": str(exc)})

    return {
        "files_seen": len(files),
        "rows_imported": imported_rows,
        "failed_count": len(failed),
        "failed": failed[:50],
        "database": str(SQLITE_PATH),
    }


def database_overview() -> dict[str, Any]:
    init_db()
    parquet_files = list(DATA_DIR.glob("*_daily.parquet"))
    with connect() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS rows_count,
                   COUNT(DISTINCT code) AS stock_count,
                   MIN(date) AS min_date,
                   MAX(date) AS max_date
            FROM stock_daily
            """
        ).fetchone()
        latest_report = conn.execute(
            "SELECT id, created_at, scope, status, summary_json FROM validation_reports ORDER BY id DESC LIMIT 1"
        ).fetchone()
        latest_sync = conn.execute(
            """
            SELECT id, started_at, finished_at, sync_date, status, source, fetched_rows,
                   valid_rows, inserted_rows, updated_rows, error, summary_json
            FROM market_sync_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    overview = dict(row)
    overview["parquet_files"] = len(parquet_files)
    overview["database"] = str(SQLITE_PATH)
    overview["latest_report"] = dict(latest_report) if latest_report else None
    if overview["latest_report"]:
        overview["latest_report"]["summary"] = json.loads(overview["latest_report"].pop("summary_json"))
    overview["latest_sync"] = _decode_sync_row(latest_sync)
    return overview


def recent_daily_rows(code: str, limit: int = 120) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT code, name, date, open, high, low, close, pre_close, change_pct,
                   volume, amount, turnover, volume_ratio, source
            FROM stock_daily
            WHERE code = ?
            ORDER BY date DESC
            LIMIT ?
            """,
            (code, limit),
        ).fetchall()
    return [dict(row) for row in rows][::-1]


def save_validation_report(scope: str, status: str, summary: dict[str, Any], issues: list[dict[str, Any]]) -> int:
    init_db()
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO validation_reports (created_at, scope, status, summary_json, issues_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                datetime.now().isoformat(timespec="seconds"),
                scope,
                status,
                json.dumps(summary, ensure_ascii=False),
                json.dumps(issues[:1000], ensure_ascii=False),
            ),
        )
        return int(cursor.lastrowid)


def list_validation_reports(limit: int = 20) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT id, created_at, scope, status, summary_json FROM validation_reports ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    reports = []
    for row in rows:
        item = dict(row)
        item["summary"] = json.loads(item.pop("summary_json"))
        reports.append(item)
    return reports


def save_prediction_snapshot(strategy: str, rows: list[dict[str, Any]]) -> int:
    init_db()
    with connect() as conn:
        cursor = conn.execute(
            "INSERT INTO prediction_snapshots (created_at, strategy, rows_json) VALUES (?, ?, ?)",
            (datetime.now().isoformat(timespec="seconds"), strategy, json.dumps(rows, ensure_ascii=False)),
        )
        return int(cursor.lastrowid)


def latest_prediction_snapshot() -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            "SELECT id, created_at, strategy, rows_json FROM prediction_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["rows"] = json.loads(item.pop("rows_json"))
    item["model_status"] = "ready" if item["strategy"] in {"xgboost_intraday", "xgboost_regressor"} else item["strategy"]
    return item


def save_daily_pick(pick: dict[str, Any]) -> int:
    init_db()
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO daily_picks (
                selection_date, target_date, selected_at, code, name, win_rate,
                selection_price, selection_change, model_status, status, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(selection_date) DO NOTHING
            """,
            (
                pick["selection_date"],
                pick["target_date"],
                pick["selected_at"],
                pick["code"],
                pick["name"],
                pick["win_rate"],
                pick["selection_price"],
                pick.get("selection_change"),
                pick.get("model_status"),
                pick.get("status", "pending_open"),
                json.dumps(pick.get("raw", pick), ensure_ascii=False),
            ),
        )
        return int(cursor.lastrowid or 0) if cursor.rowcount else 0


def update_daily_pick_open(selection_date: str, open_price: float, checked_at: str) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute("SELECT * FROM daily_picks WHERE selection_date = ?", (selection_date,)).fetchone()
        if not row:
            return None
        item = dict(row)
        premium = (open_price / float(item["selection_price"]) - 1) * 100 if item["selection_price"] else None
        success = 1 if premium is not None and premium > PROFIT_TARGET_PCT else 0
        conn.execute(
            """
            UPDATE daily_picks
            SET open_price = ?, open_checked_at = ?, open_premium = ?, success = ?, status = 'open_checked'
            WHERE selection_date = ?
            """,
            (open_price, checked_at, premium, success, selection_date),
        )
    return get_daily_pick(selection_date)


def get_daily_pick(selection_date: str) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute("SELECT * FROM daily_picks WHERE selection_date = ?", (selection_date,)).fetchone()
    return _decode_daily_pick(row)


def pending_daily_picks(target_date: str | None = None) -> list[dict[str, Any]]:
    init_db()
    params: tuple[Any, ...]
    where = "status = 'pending_open'"
    if target_date:
        where += " AND target_date <= ?"
        params = (target_date,)
    else:
        params = ()
    with connect() as conn:
        rows = conn.execute(f"SELECT * FROM daily_picks WHERE {where} ORDER BY selection_date ASC", params).fetchall()
    return [_decode_daily_pick(row) for row in rows if row]


def latest_daily_picks(limit: int = 10) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT * FROM daily_picks ORDER BY selection_date DESC LIMIT ?", (limit,)).fetchall()
    return [_decode_daily_pick(row) for row in rows if row]


def _decode_daily_pick(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None
    item = dict(row)
    item["success"] = None if item["success"] is None else bool(item["success"])
    item["raw"] = json.loads(item.pop("raw_json"))
    item["strategy_type"] = (item.get("raw") or {}).get("winner", {}).get("strategy_type")
    return item


def save_market_sync_run(report: dict[str, Any]) -> int:
    init_db()
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO market_sync_runs (
                started_at, finished_at, sync_date, status, source, fetched_rows,
                valid_rows, inserted_rows, updated_rows, error, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report["started_at"],
                report["finished_at"],
                report.get("sync_date"),
                report["status"],
                report.get("source", "sina_hs_a"),
                int(report.get("fetched_rows", 0)),
                int(report.get("valid_rows", 0)),
                int(report.get("inserted_rows", 0)),
                int(report.get("updated_rows", 0)),
                report.get("error"),
                json.dumps(report.get("summary", report), ensure_ascii=False),
            ),
        )
        return int(cursor.lastrowid)


def latest_market_sync_run() -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT id, started_at, finished_at, sync_date, status, source, fetched_rows,
                   valid_rows, inserted_rows, updated_rows, error, summary_json
            FROM market_sync_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    return _decode_sync_row(row)


def list_market_sync_runs(limit: int = 20) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, started_at, finished_at, sync_date, status, source, fetched_rows,
                   valid_rows, inserted_rows, updated_rows, error, summary_json
            FROM market_sync_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [_decode_sync_row(row) for row in rows if row]


def count_existing_daily_keys(keys: list[tuple[str, str]]) -> int:
    if not keys:
        return 0
    init_db()
    count = 0
    with connect() as conn:
        for code, day in keys:
            row = conn.execute("SELECT 1 FROM stock_daily WHERE code = ? AND date = ? LIMIT 1", (code, day)).fetchone()
            if row:
                count += 1
    return count


def _decode_sync_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None
    item = dict(row)
    item["summary"] = json.loads(item.pop("summary_json"))
    return item
