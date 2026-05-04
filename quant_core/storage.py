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
LIVE_DAILY_SOURCES = {"sina_close_sync", "sina_snapshot", "sina_open_check"}


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

            CREATE TABLE IF NOT EXISTS v3_sniper_locks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                selection_date TEXT NOT NULL UNIQUE,
                locked_at TEXT NOT NULL,
                top_k INTEGER NOT NULL DEFAULT 5,
                payload_json TEXT NOT NULL,
                created_by TEXT NOT NULL DEFAULT 'v3_sniper_1450'
            );

            CREATE TABLE IF NOT EXISTS v3_sniper_followups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                selection_date TEXT NOT NULL,
                code TEXT NOT NULL,
                horizon INTEGER NOT NULL,
                trade_date TEXT NOT NULL,
                close REAL,
                return_pct REAL,
                change_pct REAL,
                checked_at TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'stock_daily',
                UNIQUE(selection_date, code, horizon)
            );

            CREATE TABLE IF NOT EXISTS daily_picks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                selection_date TEXT NOT NULL,
                target_date TEXT NOT NULL,
                selected_at TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                strategy_type TEXT NOT NULL DEFAULT '尾盘突破',
                win_rate REAL NOT NULL,
                selection_price REAL NOT NULL,
                selection_change REAL,
                snapshot_time TEXT,
                snapshot_price REAL,
                snapshot_vol_ratio REAL,
                is_shadow_test INTEGER NOT NULL DEFAULT 1,
                model_status TEXT,
                status TEXT NOT NULL DEFAULT 'pending_open',
                open_price REAL,
                open_checked_at TEXT,
                open_premium REAL,
                t3_max_gain_pct REAL DEFAULT NULL,
                suggested_position REAL DEFAULT NULL,
                tier TEXT,
                success INTEGER,
                is_closed INTEGER NOT NULL DEFAULT 0,
                close_date TEXT,
                close_price REAL,
                close_return_pct REAL,
                close_reason TEXT,
                close_checked_at TEXT,
                raw_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_daily_picks_target_date ON daily_picks(target_date);
            CREATE INDEX IF NOT EXISTS idx_daily_picks_status ON daily_picks(status);
            CREATE INDEX IF NOT EXISTS idx_v3_sniper_followups_selection ON v3_sniper_followups(selection_date);
            CREATE INDEX IF NOT EXISTS idx_v3_sniper_followups_code ON v3_sniper_followups(code);

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
        _ensure_daily_picks_multi_strategy_schema(conn)
        _ensure_column(conn, "daily_picks", "strategy_type", "TEXT NOT NULL DEFAULT '尾盘突破'")
        _ensure_column(conn, "daily_picks", "t3_max_gain_pct", "REAL DEFAULT NULL")
        _ensure_column(conn, "daily_picks", "suggested_position", "REAL DEFAULT NULL")
        _ensure_column(conn, "daily_picks", "tier", "TEXT")
        _ensure_column(conn, "daily_picks", "snapshot_time", "TEXT")
        _ensure_column(conn, "daily_picks", "snapshot_price", "REAL")
        _ensure_column(conn, "daily_picks", "snapshot_vol_ratio", "REAL")
        _ensure_column(conn, "daily_picks", "is_shadow_test", "INTEGER NOT NULL DEFAULT 1")
        _ensure_column(conn, "daily_picks", "is_closed", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "daily_picks", "close_date", "TEXT")
        _ensure_column(conn, "daily_picks", "close_price", "REAL")
        _ensure_column(conn, "daily_picks", "close_return_pct", "REAL")
        _ensure_column(conn, "daily_picks", "close_reason", "TEXT")
        _ensure_column(conn, "daily_picks", "close_checked_at", "TEXT")
        _ensure_column(conn, "daily_picks", "push_sent_at", "TEXT")
        _ensure_column(conn, "daily_picks", "push_status", "TEXT")
        _ensure_column(conn, "daily_picks", "push_message_id", "TEXT")
        _ensure_column(conn, "daily_picks", "push_error", "TEXT")
        conn.execute(
            """
            UPDATE daily_picks
            SET strategy_type = COALESCE(
                NULLIF(json_extract(raw_json, '$.winner.strategy_type'), ''),
                strategy_type,
                '尾盘突破'
            )
            WHERE strategy_type IS NULL OR strategy_type = ''
            """
        )
        conn.execute(
            """
            UPDATE daily_picks
            SET is_shadow_test = 0
            WHERE json_extract(raw_json, '$.source') = 'historical_production_replay'
            """
        )
        conn.execute(
            """
            UPDATE daily_picks
            SET tier = COALESCE(NULLIF(tier, ''), NULLIF(json_extract(raw_json, '$.winner.selection_tier'), ''), 'base')
            WHERE tier IS NULL OR tier = ''
            """
        )
        conn.execute(
            """
            UPDATE daily_picks
            SET suggested_position = json_extract(raw_json, '$.winner.suggested_position')
            WHERE suggested_position IS NULL
              AND json_extract(raw_json, '$.winner.suggested_position') IS NOT NULL
            """
        )
        conn.execute("DROP INDEX IF EXISTS uidx_daily_picks_date_strategy")
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uidx_daily_picks_date_strategy_code
            ON daily_picks(selection_date, strategy_type, code)
            """
        )
        conn.executescript(
            """
            CREATE TRIGGER IF NOT EXISTS trg_daily_picks_snapshot_time_immutable
            BEFORE UPDATE OF snapshot_time ON daily_picks
            WHEN OLD.snapshot_time IS NOT NULL AND NEW.snapshot_time IS NOT OLD.snapshot_time
            BEGIN
                SELECT RAISE(ABORT, 'snapshot_time is immutable once written');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_daily_picks_snapshot_price_immutable
            BEFORE UPDATE OF snapshot_price ON daily_picks
            WHEN OLD.snapshot_price IS NOT NULL AND NEW.snapshot_price IS NOT OLD.snapshot_price
            BEGIN
                SELECT RAISE(ABORT, 'snapshot_price is immutable once written');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_daily_picks_snapshot_vol_ratio_immutable
            BEFORE UPDATE OF snapshot_vol_ratio ON daily_picks
            WHEN OLD.snapshot_vol_ratio IS NOT NULL AND NEW.snapshot_vol_ratio IS NOT OLD.snapshot_vol_ratio
            BEGIN
                SELECT RAISE(ABORT, 'snapshot_vol_ratio is immutable once written');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_v3_sniper_locks_immutable_update
            BEFORE UPDATE ON v3_sniper_locks
            BEGIN
                SELECT RAISE(ABORT, 'v3_sniper_locks are immutable once written');
            END;

            CREATE TRIGGER IF NOT EXISTS trg_v3_sniper_locks_immutable_delete
            BEFORE DELETE ON v3_sniper_locks
            BEGIN
                SELECT RAISE(ABORT, 'v3_sniper_locks cannot be deleted');
            END;
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
    rows_df = _filter_live_trading_dates(rows_df, source=source)
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


def _filter_live_trading_dates(rows_df: pd.DataFrame, source: str) -> pd.DataFrame:
    if rows_df.empty or source not in LIVE_DAILY_SOURCES or "date" not in rows_df.columns:
        return rows_df

    unique_dates = sorted(str(item) for item in rows_df["date"].dropna().unique().tolist())
    if not unique_dates:
        return rows_df

    max_day = pd.to_datetime(unique_dates[-1], errors="coerce")
    if pd.isna(max_day):
        return rows_df.iloc[0:0].copy()

    try:
        from quant_core.data_pipeline.trading_calendar import trading_days_on_or_before

        valid_days = {
            item.isoformat()
            for item in trading_days_on_or_before(max_day.date(), lookback_days=max(90, len(unique_dates) + 30))
        }
    except Exception as exc:
        raise RuntimeError(f"交易日校验失败，拒绝写入实时日线: {exc}") from exc

    return rows_df[rows_df["date"].astype(str).isin(valid_days)].copy()


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


def save_v3_sniper_lock(payload: dict[str, Any], created_by: str = "v3_sniper_1450") -> dict[str, Any]:
    init_db()
    selection_date = str(payload.get("prediction_date") or datetime.now().date().isoformat())
    locked_at = str(payload.get("locked_at") or datetime.now().isoformat(timespec="seconds"))
    top_k = int(payload.get("top_k") or 5)
    stored_payload = dict(payload)
    stored_payload["prediction_date"] = selection_date
    stored_payload["locked"] = True
    stored_payload["locked_at"] = locked_at
    stored_payload["lock_source"] = created_by
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO v3_sniper_locks (selection_date, locked_at, top_k, payload_json, created_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                selection_date,
                locked_at,
                top_k,
                json.dumps(stored_payload, ensure_ascii=False),
                created_by,
            ),
        )
        inserted = int(cursor.rowcount or 0) > 0
        row = conn.execute(
            """
            SELECT id, selection_date, locked_at, top_k, payload_json, created_by
            FROM v3_sniper_locks
            WHERE selection_date = ?
            LIMIT 1
            """,
            (selection_date,),
        ).fetchone()
    item = _decode_v3_sniper_lock(row)
    if item:
        item["inserted"] = inserted
    return item or {"inserted": False, "payload": stored_payload}


def get_v3_sniper_lock(selection_date: str) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT id, selection_date, locked_at, top_k, payload_json, created_by
            FROM v3_sniper_locks
            WHERE selection_date = ?
            LIMIT 1
            """,
            (selection_date,),
        ).fetchone()
    return _decode_v3_sniper_lock(row)


def latest_v3_sniper_lock() -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT id, selection_date, locked_at, top_k, payload_json, created_by
            FROM v3_sniper_locks
            ORDER BY selection_date DESC
            LIMIT 1
            """
        ).fetchone()
    return _decode_v3_sniper_lock(row)


def list_v3_sniper_locks(limit: int = 20) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT id, selection_date, locked_at, top_k, payload_json, created_by
            FROM v3_sniper_locks
            ORDER BY selection_date DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [item for item in (_decode_v3_sniper_lock(row) for row in rows) if item]


def v3_sniper_future_closes(code: str, selection_date: str, limit: int = 3) -> list[dict[str, Any]]:
    init_db()
    clean_code = str(code).zfill(6)[-6:]
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT code, name, date, open, high, low, close, pre_close, change_pct,
                   volume, amount, turnover, volume_ratio
            FROM stock_daily
            WHERE code = ? AND date > ?
              AND strftime('%w', date) NOT IN ('0', '6')
            ORDER BY date ASC
            LIMIT ?
            """,
            (clean_code, selection_date, int(limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def v3_sniper_followup_rows(code: str, selection_date: str, limit: int = 3) -> list[dict[str, Any]]:
    init_db()
    clean_code = str(code).zfill(6)[-6:]
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT selection_date, code, horizon, trade_date AS date, close, return_pct,
                   change_pct, checked_at, source
            FROM v3_sniper_followups
            WHERE selection_date = ? AND code = ?
            ORDER BY horizon ASC
            LIMIT ?
            """,
            (selection_date, clean_code, int(limit)),
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_v3_sniper_followups(limit_locks: int = 120) -> dict[str, Any]:
    """Materialize V3 Top-5 T+1/T+2/T+3 closes without mutating lock rows."""
    init_db()
    locks = list_v3_sniper_locks(limit=limit_locks)
    checked_at = datetime.now().isoformat(timespec="seconds")
    rows_to_write: list[tuple[Any, ...]] = []
    for lock in locks:
        selection_date = str(lock.get("selection_date") or "")
        payload = dict(lock.get("payload") or {})
        top_k = int(lock.get("top_k") or payload.get("top_k") or 5)
        signals = list(payload.get("rows") or [])[:top_k]
        for signal in signals:
            code = str(signal.get("code") or "").zfill(6)[-6:]
            if len(code) != 6:
                continue
            base_close = _safe_float(signal.get("close"))
            closes = v3_sniper_future_closes(code, selection_date, limit=3)
            for index, row in enumerate(closes[:3], start=1):
                close = _safe_float(row.get("close"))
                return_pct = None
                if base_close and close:
                    return_pct = round((float(close) / float(base_close) - 1) * 100, 2)
                rows_to_write.append(
                    (
                        selection_date,
                        code,
                        index,
                        row.get("date"),
                        close,
                        return_pct,
                        _safe_float(row.get("change_pct")),
                        checked_at,
                        "stock_daily",
                    )
                )

    if not rows_to_write:
        return {"locks_seen": len(locks), "rows_upserted": 0, "checked_at": checked_at}

    with connect() as conn:
        conn.executemany(
            """
            INSERT INTO v3_sniper_followups (
                selection_date, code, horizon, trade_date, close, return_pct,
                change_pct, checked_at, source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(selection_date, code, horizon) DO UPDATE SET
                trade_date = excluded.trade_date,
                close = excluded.close,
                return_pct = excluded.return_pct,
                change_pct = excluded.change_pct,
                checked_at = excluded.checked_at,
                source = excluded.source
            """,
            rows_to_write,
        )
    return {"locks_seen": len(locks), "rows_upserted": len(rows_to_write), "checked_at": checked_at}


def _decode_v3_sniper_lock(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None
    item = dict(row)
    payload = json.loads(item.pop("payload_json"))
    payload.setdefault("prediction_date", item["selection_date"])
    payload.setdefault("locked_at", item["locked_at"])
    payload.setdefault("top_k", item["top_k"])
    payload["locked"] = True
    payload["lock_id"] = item["id"]
    payload["lock_source"] = item.get("created_by") or "v3_sniper_1450"
    item["payload"] = payload
    return item


def _safe_float(value: Any) -> float | None:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(num):
        return None
    return num


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
    item["model_status"] = "ready" if item["strategy"] in {"xgboost_intraday", "xgboost_regressor", "triple_xgboost_regressor", "quad_xgboost_regressor"} else item["strategy"]
    return item


def save_daily_pick(pick: dict[str, Any]) -> int:
    init_db()
    strategy_type = pick.get("strategy_type") or (pick.get("raw") or {}).get("winner", {}).get("strategy_type") or "尾盘突破"
    t3_max_gain_pct = pick.get("t3_max_gain_pct")
    raw_payload = _daily_pick_raw_payload_with_theme_contract(pick)
    raw_winner = raw_payload.get("winner", {}) if isinstance(raw_payload, dict) else {}
    suggested_position = pick.get("suggested_position", raw_winner.get("suggested_position") if isinstance(raw_winner, dict) else None)
    tier = pick.get("tier") or pick.get("selection_tier")
    if not tier and isinstance(raw_winner, dict):
        tier = raw_winner.get("selection_tier")
    selected_at = str(pick["selected_at"])
    snapshot_time = pick.get("snapshot_time") or (selected_at.split("T", 1)[1] if "T" in selected_at else selected_at)
    snapshot_price = pick.get("snapshot_price", pick.get("selection_price"))
    snapshot_vol_ratio = pick.get("snapshot_vol_ratio")
    if snapshot_vol_ratio is None:
        snapshot_vol_ratio = (pick.get("raw") or {}).get("winner", {}).get("volume_ratio")
    with connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO daily_picks (
                selection_date, target_date, selected_at, code, name, strategy_type, win_rate,
                selection_price, selection_change, snapshot_time, snapshot_price, snapshot_vol_ratio,
                is_shadow_test, model_status, status, t3_max_gain_pct, suggested_position, tier, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(selection_date, strategy_type, code) DO NOTHING
            """,
            (
                pick["selection_date"],
                pick["target_date"],
                selected_at,
                pick["code"],
                pick["name"],
                strategy_type,
                pick["win_rate"],
                pick["selection_price"],
                pick.get("selection_change"),
                snapshot_time,
                snapshot_price,
                snapshot_vol_ratio,
                1 if pick.get("is_shadow_test", True) else 0,
                pick.get("model_status"),
                pick.get("status", "pending_open"),
                t3_max_gain_pct,
                suggested_position,
                tier,
                json.dumps(raw_payload, ensure_ascii=False),
            ),
        )
        return int(cursor.lastrowid or 0) if cursor.rowcount else 0


def _daily_pick_raw_payload_with_theme_contract(pick: dict[str, Any]) -> dict[str, Any]:
    raw = dict(pick.get("raw") or pick)
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    winner = dict(winner)
    raw["winner"] = winner

    core_theme = _theme_text(
        pick.get("core_theme"),
        pick.get("theme_name"),
        winner.get("core_theme"),
        winner.get("theme_name"),
    )
    momentum = _optional_float(
        pick.get("theme_momentum_3d")
        if pick.get("theme_momentum_3d") is not None
        else pick.get("theme_momentum")
        if pick.get("theme_momentum") is not None
        else pick.get("theme_pct_chg_3")
        if pick.get("theme_pct_chg_3") is not None
        else winner.get("theme_momentum_3d")
        if winner.get("theme_momentum_3d") is not None
        else winner.get("theme_momentum")
        if winner.get("theme_momentum") is not None
        else winner.get("theme_pct_chg_3")
    )
    if momentum is None:
        momentum = 0.0

    raw["core_theme"] = core_theme
    raw["theme_momentum_3d"] = momentum
    winner["core_theme"] = core_theme
    winner["theme_name"] = _theme_text(winner.get("theme_name"), core_theme)
    winner["theme_momentum_3d"] = momentum
    winner["theme_momentum"] = _optional_float(winner.get("theme_momentum")) if winner.get("theme_momentum") is not None else momentum
    winner["theme_pct_chg_3"] = _optional_float(winner.get("theme_pct_chg_3")) if winner.get("theme_pct_chg_3") is not None else momentum
    return raw


def _theme_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() not in {"nan", "none", "-"}:
            return text
    return "-"


def clear_daily_picks() -> int:
    init_db()
    with connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS count FROM daily_picks").fetchone()
        count = int(row["count"] or 0) if row else 0
        conn.execute("DELETE FROM daily_picks")
        conn.execute("DELETE FROM sqlite_sequence WHERE name = 'daily_picks'")
    return count


def list_unpushed_daily_picks(selection_date: str) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM daily_picks
            WHERE selection_date = ?
              AND COALESCE(is_shadow_test, 0) = 1
              AND COALESCE(push_status, '') != 'sent'
            ORDER BY strategy_type ASC, id ASC
            """,
            (selection_date,),
        ).fetchall()
    return [_decode_daily_pick(row) for row in rows if row]


def mark_daily_picks_push_result(
    selection_date: str,
    status: str,
    pushed_at: str | None = None,
    message_id: str | None = None,
    error: str | None = None,
    pick_ids: list[int] | None = None,
) -> int:
    init_db()
    timestamp = pushed_at or datetime.now().isoformat(timespec="seconds")
    params: list[Any] = [status, timestamp if status == "sent" else None, message_id, error, selection_date]
    where = "selection_date = ? AND COALESCE(is_shadow_test, 0) = 1"
    if pick_ids:
        placeholders = ",".join("?" for _ in pick_ids)
        where += f" AND id IN ({placeholders})"
        params.extend(int(item) for item in pick_ids)
    with connect() as conn:
        cursor = conn.execute(
            f"""
            UPDATE daily_picks
            SET push_status = ?, push_sent_at = COALESCE(?, push_sent_at),
                push_message_id = ?, push_error = ?
            WHERE {where}
            """,
            tuple(params),
        )
        return int(cursor.rowcount or 0)


def update_daily_pick_open(
    selection_date: str,
    open_price: float,
    checked_at: str,
    exit_signal: dict[str, Any] | None = None,
    close_position: bool = False,
    strategy_type: str | None = None,
    code: str | None = None,
    pick_id: int | None = None,
) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        where, params = _daily_pick_identity_where(selection_date, strategy_type=strategy_type, code=code, pick_id=pick_id)
        row = conn.execute(f"SELECT * FROM daily_picks WHERE {where} ORDER BY id LIMIT 1", params).fetchone()
        if not row:
            return None
        item = dict(row)
        premium = (open_price / float(item["selection_price"]) - 1) * 100 if item["selection_price"] else None
        raw = json.loads(item.get("raw_json") or "{}")
        if exit_signal is not None:
            raw["exit_sentinel"] = exit_signal
        success = 1 if premium is not None and premium > PROFIT_TARGET_PCT else 0
        if close_position:
            conn.execute(
                """
                UPDATE daily_picks
                SET open_price = ?, open_checked_at = ?, open_premium = ?, success = ?,
                    status = 'open_checked', is_closed = 1, close_date = ?,
                    close_price = ?, close_return_pct = ?, close_reason = ?,
                    close_checked_at = ?, raw_json = ?
                WHERE id = ?
                """,
                (
                    open_price,
                    checked_at,
                    premium,
                    success,
                    checked_at[:10],
                    open_price,
                    premium,
                    (exit_signal or {}).get("action") if exit_signal else "开盘闭环",
                    checked_at,
                    json.dumps(raw, ensure_ascii=False),
                    int(item["id"]),
                ),
            )
        else:
            conn.execute(
                """
                UPDATE daily_picks
                SET open_price = ?, open_checked_at = ?, open_premium = ?, success = ?, status = 'open_checked', raw_json = ?
                WHERE id = ?
                """,
                (open_price, checked_at, premium, success, json.dumps(raw, ensure_ascii=False), int(item["id"])),
            )
    return get_daily_pick(selection_date, strategy_type=strategy_type or item.get("strategy_type"), code=code or item.get("code"), pick_id=int(item["id"]))


def update_daily_pick_t3_gain(
    selection_date: str,
    t3_max_gain_pct: float,
    checked_at: str | None = None,
    strategy_type: str | None = None,
    code: str | None = None,
    pick_id: int | None = None,
) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        where, params = _daily_pick_identity_where(selection_date, strategy_type=strategy_type, code=code, pick_id=pick_id)
        row = conn.execute(f"SELECT * FROM daily_picks WHERE {where} ORDER BY id LIMIT 1", params).fetchone()
        if not row:
            return None
        item = dict(row)
        raw = json.loads(item.get("raw_json") or "{}")
        raw["t3_result"] = {
            "t3_max_gain_pct": float(t3_max_gain_pct),
            "checked_at": checked_at or datetime.now().isoformat(timespec="seconds"),
        }
        conn.execute(
            """
            UPDATE daily_picks
            SET t3_max_gain_pct = ?, status = 't3_checked', raw_json = ?
            WHERE id = ?
            """,
            (float(t3_max_gain_pct), json.dumps(raw, ensure_ascii=False), int(item["id"])),
        )
    return get_daily_pick(selection_date, strategy_type=strategy_type or item.get("strategy_type"), code=code or item.get("code"), pick_id=int(item["id"]))


def mark_daily_pick_closed(
    selection_date: str,
    close_price: float,
    close_return_pct: float,
    close_reason: str,
    checked_at: str | None = None,
    close_signal: dict[str, Any] | None = None,
    strategy_type: str | None = None,
    code: str | None = None,
    pick_id: int | None = None,
) -> dict[str, Any] | None:
    init_db()
    timestamp = checked_at or datetime.now().isoformat(timespec="seconds")
    with connect() as conn:
        where, params = _daily_pick_identity_where(selection_date, strategy_type=strategy_type, code=code, pick_id=pick_id)
        row = conn.execute(f"SELECT * FROM daily_picks WHERE {where} ORDER BY id LIMIT 1", params).fetchone()
        if not row:
            return None
        item = dict(row)
        raw = json.loads(item.get("raw_json") or "{}")
        if close_signal is not None:
            raw["close_signal"] = close_signal
        conn.execute(
            """
            UPDATE daily_picks
            SET is_closed = 1, status = 'closed', success = ?,
                close_date = ?, close_price = ?,
                close_return_pct = ?, close_reason = ?, close_checked_at = ?,
                raw_json = ?
            WHERE id = ?
            """,
            (
                1 if float(close_return_pct) > 0 else 0,
                timestamp[:10],
                float(close_price),
                float(close_return_pct),
                close_reason,
                timestamp,
                json.dumps(raw, ensure_ascii=False),
                int(item["id"]),
            ),
        )
    return get_daily_pick(selection_date, strategy_type=strategy_type or item.get("strategy_type"), code=code or item.get("code"), pick_id=int(item["id"]))


def open_position_picks(today: str | None = None) -> list[dict[str, Any]]:
    init_db()
    current = today or datetime.now().date().isoformat()
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM daily_picks
            WHERE COALESCE(is_closed, 0) = 0
              AND selection_date < ?
              AND (
                    (
                        strategy_type = '尾盘突破'
                        AND status = 'pending_open'
                        AND target_date <= ?
                    )
                    OR (
                        strategy_type IN ('中线超跌反转', '右侧主升浪', '全局动量狙击')
                        AND target_date <= ?
                    )
                  )
            ORDER BY selection_date ASC
            """,
            (current, current, current),
        ).fetchall()
    return [_decode_daily_pick(row) for row in rows if row]


def stock_daily_row(code: str, trade_date: str) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT code, name, date, open, high, low, close, pre_close, change_pct,
                   volume, amount, turnover, volume_ratio
            FROM stock_daily
            WHERE code = ? AND date = ?
            LIMIT 1
            """,
            (str(code).zfill(6), trade_date),
        ).fetchone()
    return dict(row) if row else None


def get_daily_pick(
    selection_date: str,
    strategy_type: str | None = None,
    code: str | None = None,
    pick_id: int | None = None,
) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        where, params = _daily_pick_identity_where(selection_date, strategy_type=strategy_type, code=code, pick_id=pick_id)
        row = conn.execute(f"SELECT * FROM daily_picks WHERE {where} ORDER BY id LIMIT 1", params).fetchone()
    return _decode_daily_pick(row)


def get_daily_picks(selection_date: str) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM daily_picks WHERE selection_date = ? ORDER BY strategy_type ASC, id ASC",
            (selection_date,),
        ).fetchall()
    return [_decode_daily_pick(row) for row in rows if row]


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


def latest_daily_picks(limit: int = 10, shadow_only: bool = False) -> list[dict[str, Any]]:
    init_db()
    with connect() as conn:
        if shadow_only:
            rows = conn.execute(
                "SELECT * FROM daily_picks WHERE COALESCE(is_shadow_test, 0) = 1 ORDER BY selection_date DESC, id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM daily_picks ORDER BY selection_date DESC, id ASC LIMIT ?", (limit,)).fetchall()
    return [_decode_daily_pick(row) for row in rows if row]


def _decode_daily_pick(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if not row:
        return None
    item = dict(row)
    item["success"] = None if item["success"] is None else bool(item["success"])
    item["is_closed"] = bool(item.get("is_closed") or 0)
    item["raw"] = json.loads(item.pop("raw_json"))
    item["strategy_type"] = item.get("strategy_type") or (item.get("raw") or {}).get("winner", {}).get("strategy_type") or "尾盘突破"
    _attach_pick_display_fields(item)
    return item


def _attach_pick_display_fields(item: dict[str, Any]) -> None:
    raw = item.get("raw") or {}
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    sentinel = raw.get("exit_sentinel") if isinstance(raw.get("exit_sentinel"), dict) else {}
    close_signal = raw.get("close_signal") if isinstance(raw.get("close_signal"), dict) else {}

    item["expected_premium"] = _optional_float(winner.get("expected_premium"))
    item["predicted_open_premium"] = item["expected_premium"]
    item["expected_t3_max_gain_pct"] = _optional_float(winner.get("expected_t3_max_gain_pct"))
    item["composite_score"] = _optional_float(winner.get("composite_score"))
    item["sort_score"] = _optional_float(winner.get("sort_score"))
    item["score_threshold"] = _optional_float(winner.get("score_threshold"))
    item["score_floor"] = _optional_float(winner.get("score_floor"))
    item["selection_tier"] = item.get("tier") or winner.get("selection_tier") or "base"
    item["tier"] = item["selection_tier"]
    item["risk_warning"] = winner.get("risk_warning") or ""
    item["position_probability"] = _optional_float(winner.get("position_probability"))
    item["suggested_position"] = _optional_float(item.get("suggested_position"))
    if item["suggested_position"] is None:
        item["suggested_position"] = _optional_float(winner.get("suggested_position"))
    item["sentiment_bonus"] = _optional_float(winner.get("sentiment_bonus"))
    item["market_gate_mode"] = winner.get("market_gate_mode") or ""
    item["core_theme"] = _theme_text(item.get("core_theme"), raw.get("core_theme"), winner.get("core_theme"), winner.get("theme_name"))
    theme_momentum = _optional_float(
        item.get("theme_momentum_3d")
        if item.get("theme_momentum_3d") is not None
        else raw.get("theme_momentum_3d")
        if raw.get("theme_momentum_3d") is not None
        else winner.get("theme_momentum_3d")
        if winner.get("theme_momentum_3d") is not None
        else winner.get("theme_momentum")
        if winner.get("theme_momentum") is not None
        else winner.get("theme_pct_chg_3")
    )
    item["theme_momentum_3d"] = 0.0 if theme_momentum is None else theme_momentum
    item["theme_name"] = item["core_theme"]
    item["theme_pct_chg_3"] = item["theme_momentum_3d"]
    item["theme_momentum"] = item["theme_momentum_3d"]

    actual = _optional_float(item.get("open_premium"))
    expected = item.get("expected_premium")
    item["premium_error_abs"] = abs(actual - expected) if actual is not None and expected is not None else None
    item["exit_action"] = sentinel.get("action") or _exit_action_from_premium(actual)
    item["exit_instruction"] = sentinel.get("instruction") or _exit_instruction_from_premium(actual)
    item["exit_level"] = sentinel.get("level") or _exit_level_from_action(item.get("exit_action"))
    item["exit_pushed_at"] = sentinel.get("pushed_at")
    item["exit_push_status"] = sentinel.get("push_status")
    item["close_action"] = close_signal.get("action") or item.get("close_reason")
    item["close_instruction"] = close_signal.get("instruction") or ""
    item["close_push_status"] = close_signal.get("push_status")


def _optional_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _exit_action_from_premium(premium: float | None) -> str | None:
    if premium is None:
        return None
    if premium < 0:
        return "核按钮"
    if premium < 3.0:
        return "落袋为安"
    return "超预期锁仓"


def _exit_instruction_from_premium(premium: float | None) -> str | None:
    if premium is None:
        return None
    if premium < 0:
        return "逻辑证伪，按跌停价挂单卖出，斩断亏损。"
    if premium < 3.0:
        return "符合套利预期，按开盘价挂单止盈。"
    return "强势高开，勿早盘秒卖，等待盘中冲高或封板。"


def _exit_level_from_action(action: str | None) -> str | None:
    if action == "核按钮":
        return "danger"
    if action == "超预期锁仓":
        return "strong"
    if action == "落袋为安":
        return "profit"
    return None


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _ensure_daily_picks_multi_strategy_schema(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='daily_picks'"
    ).fetchone()
    table_sql = str(row["sql"] or "") if row else ""
    if "selection_date TEXT NOT NULL UNIQUE" not in table_sql:
        return

    conn.execute("ALTER TABLE daily_picks RENAME TO daily_picks_legacy_single_date")
    conn.execute(
        """
        CREATE TABLE daily_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            selection_date TEXT NOT NULL,
            target_date TEXT NOT NULL,
            selected_at TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            win_rate REAL NOT NULL,
            selection_price REAL NOT NULL,
            selection_change REAL,
            snapshot_time TEXT,
            snapshot_price REAL,
            snapshot_vol_ratio REAL,
            is_shadow_test INTEGER NOT NULL DEFAULT 1,
            model_status TEXT,
            status TEXT NOT NULL DEFAULT 'pending_open',
            open_price REAL,
            open_checked_at TEXT,
            open_premium REAL,
            success INTEGER,
            raw_json TEXT NOT NULL,
            strategy_type TEXT NOT NULL DEFAULT '尾盘突破',
            t3_max_gain_pct REAL DEFAULT NULL,
            suggested_position REAL DEFAULT NULL,
            tier TEXT,
            is_closed INTEGER NOT NULL DEFAULT 0,
            close_date TEXT,
            close_price REAL,
            close_return_pct REAL,
            close_reason TEXT,
            close_checked_at TEXT
        )
        """
    )
    old_cols = {row["name"] for row in conn.execute("PRAGMA table_info(daily_picks_legacy_single_date)").fetchall()}
    new_cols = [row["name"] for row in conn.execute("PRAGMA table_info(daily_picks)").fetchall()]
    common_cols = [col for col in new_cols if col in old_cols]
    col_sql = ", ".join(common_cols)
    conn.execute(
        f"""
        INSERT INTO daily_picks ({col_sql})
        SELECT {col_sql}
        FROM daily_picks_legacy_single_date
        """
    )
    conn.execute("DROP TABLE daily_picks_legacy_single_date")


def _daily_pick_identity_where(
    selection_date: str,
    strategy_type: str | None = None,
    code: str | None = None,
    pick_id: int | None = None,
) -> tuple[str, tuple[Any, ...]]:
    if pick_id is not None:
        return "id = ?", (int(pick_id),)
    where = ["selection_date = ?"]
    params: list[Any] = [selection_date]
    if strategy_type:
        where.append("strategy_type = ?")
        params.append(strategy_type)
    if code:
        where.append("code = ?")
        params.append(str(code).zfill(6))
    return " AND ".join(where), tuple(params)


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
