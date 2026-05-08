from __future__ import annotations

import argparse
import contextlib
import io
import json
import math
import shutil
import sqlite3
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import GLOBAL_DAILY_META_PATH, GLOBAL_DAILY_MODEL_PATH, GLOBAL_MIN_SCORE, SQLITE_PATH
from quant_core.engine.predictor import (
    GLOBAL_SNIPER_PRODUCTION_VERSION,
    GLOBAL_SNIPER_SELECTION_MODE,
    _latest_historical_playback_trade_date,
    prepare_historical_playback_candidates,
    scan_market,
)
from scripts.backtest.simulate_sentinel_5m import (
    BuyRecord,
    PROJECT_HOT_5M_DIR,
    human_exit_reason,
    load_trading_dates,
    sell_strategy_label,
    simulate_one,
)


BACKUP_DIR = BASE_DIR / "data" / "core_db" / "backups"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "v6_2026_historical_replay_latest.json"
PICKS_PATH = BASE_DIR / "scripts" / "research" / "v6_2026_historical_replay_picks.csv"
TRADES_PATH = BASE_DIR / "scripts" / "research" / "v6_2026_historical_replay_trades.csv"

STRATEGY_TYPE = "全局动量狙击"
MODEL_LABEL = "V6.0 极寒爆发大脑"
SOURCE = "v6_extreme_burst_2026_historical_replay"


def norm_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def finite_float(value: Any, default: float | None = None) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except Exception:
            pass
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def date_text(value: Any) -> str:
    text = str(value or "")[:10]
    return text if len(text) == 10 and text[4] == "-" and text[7] == "-" else ""


def production_model_contract() -> dict[str, Any]:
    return {
        "version": GLOBAL_SNIPER_PRODUCTION_VERSION,
        "label": MODEL_LABEL,
        "path": str(GLOBAL_DAILY_MODEL_PATH),
        "meta_path": str(GLOBAL_DAILY_META_PATH),
        "threshold": float(GLOBAL_MIN_SCORE),
        "selection_mode": GLOBAL_SNIPER_SELECTION_MODE,
    }


def row_probability(row: dict[str, Any]) -> float:
    return finite_float(row.get("global_probability"), None) or (finite_float(row.get("win_rate"), 0.0) or 0.0) / 100.0


def row_cost(row: dict[str, Any]) -> float:
    return finite_float(row.get("snapshot_price"), None) or finite_float(row.get("price"), 0.0) or 0.0


def row_to_record(row: dict[str, Any], idx: int, raw: dict[str, Any]) -> BuyRecord | None:
    trade_date = date_text(row.get("date") or row.get("selection_date"))
    code = norm_code(row.get("code"))
    cost = row_cost(row)
    if not trade_date or not code or cost <= 0:
        return None
    return BuyRecord(
        pick_id=2_026_000_000 + idx,
        code=code,
        name=str(row.get("name") or code),
        buy_date=trade_date,
        selected_at=f"{trade_date}T14:50:00",
        cost_price=float(cost),
        strategy_type=STRATEGY_TYPE,
        tier=str(row.get("selection_tier") or "base"),
        target_date=date_text(row.get("t3_exit_date") or row.get("target_date")),
        win_rate=finite_float(row.get("win_rate"), 0.0) or 0.0,
        selection_change=finite_float(row.get("change"), 0.0) or 0.0,
        snapshot_vol_ratio=finite_float(row.get("volume_ratio"), 0.0) or 0.0,
        suggested_position=finite_float(row.get("suggested_position"), None),
        open_price=finite_float(row.get("next_open"), None),
        open_premium=finite_float(row.get("open_premium"), None),
        open_checked_at="",
        close_date="",
        close_price=None,
        close_return_pct=None,
        close_reason="",
        raw=raw,
    )


def build_raw(row: dict[str, Any], sentinel: dict[str, Any], generated_at: str, start_date: str, end_date: str) -> dict[str, Any]:
    probability = row_probability(row)
    cost = row_cost(row)
    production_model = production_model_contract()
    winner = {
        **row,
        "code": norm_code(row.get("code")),
        "name": str(row.get("name") or norm_code(row.get("code"))),
        "strategy_type": STRATEGY_TYPE,
        "price": round(cost, 4),
        "win_rate": round(probability * 100.0, 4),
        "global_probability": round(probability, 8),
        "global_probability_pct": round(probability * 100.0, 4),
        "selection_score": round(probability, 8),
        "score_threshold": float(GLOBAL_MIN_SCORE),
        "selection_tier": row.get("selection_tier") or "base",
        "model_version": GLOBAL_SNIPER_PRODUCTION_VERSION,
        "selection_mode": GLOBAL_SNIPER_SELECTION_MODE,
        "production_model": production_model,
        "coverage_status": sentinel.get("coverage_status"),
        "sentinel_5m": sentinel,
        "sell_strategy": sentinel.get("sell_strategy"),
        "exit_policy": sentinel.get("sell_strategy"),
        "t3_settlement_price": sentinel.get("exit_price"),
        "t3_settlement_return_pct": sentinel.get("yield_pct"),
        "t3_max_gain_pct": sentinel.get("highest_gain_pct"),
    }
    raw = {
        "source": SOURCE,
        "schema_version": "daily_picks.v6_2026_historical_replay.v1",
        "generated_at": generated_at,
        "replay_source": "scripts/research/replay_v6_2026_daily_picks.py",
        "date_range": {"start": start_date, "end": end_date},
        "production_model": production_model,
        "model_version": GLOBAL_SNIPER_PRODUCTION_VERSION,
        "selection_mode": GLOBAL_SNIPER_SELECTION_MODE,
        "threshold": float(GLOBAL_MIN_SCORE),
        "top_k": 1,
        "winner": winner,
        "sentinel_5m": sentinel,
    }
    if sentinel.get("is_closed"):
        raw["close_signal"] = {
            "source": "sentinel_5m_backtest",
            "action": sentinel.get("sell_strategy"),
            "level": "sentinel_5m",
            "instruction": sentinel.get("sell_strategy"),
            "sell_strategy": sentinel.get("sell_strategy"),
            "exit_policy": sentinel.get("sell_strategy"),
            "coverage_status": sentinel.get("coverage_status"),
            "exit_reason": sentinel.get("exit_reason"),
            "close_time": sentinel.get("exit_time"),
            "close_price": sentinel.get("exit_price"),
            "close_return_pct": sentinel.get("yield_pct"),
            "bars_replayed": sentinel.get("bars_replayed"),
            "highest_price": sentinel.get("highest_price"),
            "highest_gain_pct": sentinel.get("highest_gain_pct"),
            "warning": sentinel.get("warning"),
        }
    return json_safe(raw)


def build_replay_rows(start_date: str, end_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prepared = prepare_historical_playback_candidates(start_date=start_date, end_date=end_date)
    candidates = prepared.get("candidates", pd.DataFrame())
    trading_dates = [day for day in prepared.get("trading_dates", []) if start_date <= str(day) <= end_date]
    rows: list[dict[str, Any]] = []
    empty_days: list[str] = []
    blocked_days: list[str] = []
    for trade_date in trading_dates:
        with contextlib.redirect_stdout(io.StringIO()):
            payload = scan_market(
                limit=3,
                cache_prediction=False,
                persist_snapshot=False,
                target_date=trade_date,
                historical_candidates=candidates,
            )
        global_rows = [item for item in payload.get("rows", []) if item.get("strategy_type") == STRATEGY_TYPE]
        if not global_rows:
            if (payload.get("market_gate") or {}).get("blocked"):
                blocked_days.append(trade_date)
            else:
                empty_days.append(trade_date)
            continue
        pick = dict(global_rows[0])
        pick["selection_date"] = trade_date
        pick["date"] = trade_date
        rows.append(pick)
    meta = {
        "prepared_start": prepared.get("start_date"),
        "prepared_end": prepared.get("end_date"),
        "model_status": prepared.get("model_status"),
        "trading_days": len(trading_dates),
        "empty_days": empty_days,
        "blocked_days": blocked_days,
        "repaired_pre_close_count": prepared.get("repaired_pre_close_count"),
        "repaired_volume_ratio_count": prepared.get("repaired_volume_ratio_count"),
    }
    return rows, meta


def settle_rows(picks: list[dict[str, Any]], end_date: str, generated_at: str, start_date: str) -> list[dict[str, Any]]:
    trading_dates = [day for day in load_trading_dates(SQLITE_PATH) if day <= end_date]
    settled: list[dict[str, Any]] = []
    for idx, row in enumerate(picks, start=1):
        sentinel_seed = {
            "coverage_status": "pending",
            "sell_strategy": "V5.6 5m卖出闭环待回放",
        }
        raw_seed = build_raw(row, sentinel_seed, generated_at, start_date, end_date)
        record = row_to_record(row, idx, raw_seed)
        if record is None:
            continue
        with contextlib.redirect_stdout(io.StringIO()):
            result = simulate_one(record, PROJECT_HOT_5M_DIR, trading_dates, replay_end_date=end_date)
        is_closed = result.yield_pct is not None
        sentinel = {
            "source": "sentinel_5m_backtest",
            "coverage_status": result.coverage_status,
            "exit_reason": result.exit_reason,
            "exit_time": result.exit_time,
            "exit_price": result.exit_price,
            "yield_pct": result.yield_pct,
            "highest_price": result.highest_price,
            "highest_gain_pct": result.highest_gain_pct,
            "bars_replayed": result.bars_replayed,
            "t3_date": result.t3_date,
            "warning": result.warning,
            "sell_strategy": sell_strategy_label(result),
            "close_reason": human_exit_reason(result.exit_reason),
            "is_closed": is_closed,
        }
        raw = build_raw(row, sentinel, generated_at, start_date, end_date)
        trade_date = date_text(row.get("date") or row.get("selection_date"))
        probability = row_probability(row)
        cost = row_cost(row)
        close_date = date_text(result.exit_time) if result.exit_time else ""
        settled.append(
            {
                "selection_date": trade_date,
                "target_date": result.t3_date or date_text(row.get("t3_exit_date") or row.get("target_date")),
                "selected_at": f"{trade_date}T14:50:00",
                "code": norm_code(row.get("code")),
                "name": str(row.get("name") or norm_code(row.get("code"))),
                "win_rate": round(probability * 100.0, 4),
                "selection_price": round(cost, 4),
                "selection_change": finite_float(row.get("change"), None),
                "snapshot_time": "14:50:00",
                "snapshot_price": round(cost, 4),
                "snapshot_vol_ratio": finite_float(row.get("volume_ratio"), None),
                "is_shadow_test": 0 if is_closed else 1,
                "model_status": f"{SOURCE}; probability={probability:.6f}; coverage={result.coverage_status}",
                "status": "closed" if is_closed else "open",
                "open_price": finite_float(row.get("next_open"), None),
                "open_checked_at": "",
                "open_premium": finite_float(row.get("open_premium"), None),
                "success": (1 if result.yield_pct > 0 else 0) if is_closed else None,
                "raw_json": json.dumps(raw, ensure_ascii=False),
                "strategy_type": STRATEGY_TYPE,
                "t3_max_gain_pct": result.highest_gain_pct,
                "is_closed": 1 if is_closed else 0,
                "close_date": close_date if is_closed else None,
                "close_price": result.exit_price if is_closed else None,
                "close_return_pct": result.yield_pct if is_closed else None,
                "close_reason": human_exit_reason(result.exit_reason) if is_closed else None,
                "close_checked_at": result.exit_time if is_closed else None,
                "suggested_position": finite_float(row.get("suggested_position"), None),
                "tier": row.get("selection_tier") or "base",
                "probability": probability,
                "coverage_status": result.coverage_status,
                "exit_reason": result.exit_reason,
                "exit_time": result.exit_time,
                "exit_price": result.exit_price,
                "yield_pct": result.yield_pct,
                "highest_gain_pct": result.highest_gain_pct,
                "bars_replayed": result.bars_replayed,
                "warning": result.warning,
            }
        )
    return settled


def export_existing_rows(conn: sqlite3.Connection, start_date: str, end_date: str, timestamp: str) -> tuple[Path, int]:
    rows = conn.execute(
        """
        SELECT *
        FROM daily_picks
        WHERE strategy_type = ?
          AND selection_date >= ?
          AND selection_date <= ?
        ORDER BY selection_date, id
        """,
        (STRATEGY_TYPE, start_date, end_date),
    ).fetchall()
    path = BACKUP_DIR / f"daily_picks_global_{start_date}_to_{end_date}_before_v6_replay_{timestamp}.json"
    path.write_text(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2), encoding="utf-8")
    return path, len(rows)


def write_daily_picks(rows: list[dict[str, Any]], start_date: str, end_date: str, dry_run: bool) -> dict[str, Any]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = BACKUP_DIR / f"quant_workstation_before_v6_2026_replay_{timestamp}.sqlite3"
    shutil.copy2(SQLITE_PATH, backup_path)
    with sqlite3.connect(SQLITE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        export_path, existing_count = export_existing_rows(conn, start_date, end_date, timestamp)
        deleted = 0
        inserted = 0
        if not dry_run:
            cursor = conn.execute(
                """
                DELETE FROM daily_picks
                WHERE strategy_type = ?
                  AND selection_date >= ?
                  AND selection_date <= ?
                """,
                (STRATEGY_TYPE, start_date, end_date),
            )
            deleted = int(cursor.rowcount or 0)
            for row in rows:
                conn.execute(
                    """
                    INSERT INTO daily_picks (
                        selection_date, target_date, selected_at, code, name,
                        win_rate, selection_price, selection_change,
                        snapshot_time, snapshot_price, snapshot_vol_ratio,
                        is_shadow_test, model_status, status,
                        open_price, open_checked_at, open_premium, success,
                        raw_json, strategy_type, t3_max_gain_pct,
                        is_closed, close_date, close_price, close_return_pct,
                        close_reason, close_checked_at, suggested_position, tier
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["selection_date"],
                        row["target_date"],
                        row["selected_at"],
                        row["code"],
                        row["name"],
                        row["win_rate"],
                        row["selection_price"],
                        row["selection_change"],
                        row["snapshot_time"],
                        row["snapshot_price"],
                        row["snapshot_vol_ratio"],
                        row["is_shadow_test"],
                        row["model_status"],
                        row["status"],
                        row["open_price"],
                        row["open_checked_at"],
                        row["open_premium"],
                        row["success"],
                        row["raw_json"],
                        row["strategy_type"],
                        row["t3_max_gain_pct"],
                        row["is_closed"],
                        row["close_date"],
                        row["close_price"],
                        row["close_return_pct"],
                        row["close_reason"],
                        row["close_checked_at"],
                        row["suggested_position"],
                        row["tier"],
                    ),
                )
                inserted += 1
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
    return {
        "backup_path": str(backup_path),
        "export_path": str(export_path),
        "existing_count": existing_count,
        "deleted_count": deleted,
        "inserted_count": inserted,
        "dry_run": dry_run,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay production V6 global sniper picks for 2026 and backfill daily_picks.")
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--end-date", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start_date = date_text(args.start_date)
    end_date = date_text(args.end_date) or (_latest_historical_playback_trade_date() or "")
    if not start_date or not end_date:
        raise RuntimeError(f"invalid date range: {args.start_date} -> {args.end_date}")
    if end_date < start_date:
        raise RuntimeError(f"end_date {end_date} is before start_date {start_date}")

    generated_at = datetime.now().isoformat(timespec="seconds")
    print(f"========== V6 2026 Historical Replay ==========")
    print(f"Date Range : {start_date} -> {end_date}")
    picks, replay_meta = build_replay_rows(start_date, end_date)
    print(f"V6 Picks   : {len(picks)} / trading_days={replay_meta['trading_days']}")
    settled = settle_rows(picks, end_date, generated_at, start_date)
    print(f"Settled    : {len(settled)}")

    pd.DataFrame(picks).to_csv(PICKS_PATH, index=False)
    pd.DataFrame(
        [
            {key: value for key, value in row.items() if key != "raw_json"}
            for row in settled
        ]
    ).to_csv(TRADES_PATH, index=False)

    write_meta = write_daily_picks(settled, start_date, end_date, dry_run=bool(args.dry_run))
    closed = [row for row in settled if row["is_closed"]]
    yields = pd.Series([row["close_return_pct"] for row in closed], dtype="float64") if closed else pd.Series(dtype="float64")
    coverage_counts = Counter(str(row["coverage_status"]) for row in settled)
    month_counts = Counter(str(row["selection_date"])[:7] for row in settled)
    report = {
        "created_at": generated_at,
        "source": "scripts/research/replay_v6_2026_daily_picks.py",
        "date_range": {"start": start_date, "end": end_date},
        "model_version": GLOBAL_SNIPER_PRODUCTION_VERSION,
        "selection_mode": GLOBAL_SNIPER_SELECTION_MODE,
        "threshold": float(GLOBAL_MIN_SCORE),
        "replay_meta": replay_meta,
        "selected_picks": len(picks),
        "insert_rows": len(settled),
        "closed_rows": len(closed),
        "open_rows": len(settled) - len(closed),
        "coverage_counts": dict(coverage_counts),
        "month_counts": dict(sorted(month_counts.items())),
        "closed_summary": {
            "win_rate_pct": round(float((yields > 0).mean() * 100.0), 4) if len(yields) else 0.0,
            "mean_yield_pct": round(float(yields.mean()), 4) if len(yields) else 0.0,
            "median_yield_pct": round(float(yields.median()), 4) if len(yields) else 0.0,
        },
        "picks_path": str(PICKS_PATH),
        "trades_path": str(TRADES_PATH),
        "write": write_meta,
    }
    REPORT_PATH.write_text(json.dumps(json_safe(report), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(json_safe(report), ensure_ascii=False, indent=2))
    print("================================================")


if __name__ == "__main__":
    main()
