from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Optional, Union

from quant_core.config import (
    BREAKOUT_MIN_SCORE,
    GLOBAL_MIN_SCORE,
    PAUSED_STRATEGY_TYPES,
    PRODUCTION_STRATEGY_TYPES,
    PRODUCTION_TOTAL_PICK_LIMIT,
    RISK_CONTROL_PROFILES,
    SQLITE_PATH,
    ST_BREAKOUT_MIN_SCORE,
)


SIGNATURE_SCHEMA_VERSION = "sentinel_daily_picks_contract.v1"
PICK_MODE = "daily_strategy_top1"
SELL_STRATEGY_SCHEMA_VERSION = "sentinel_5m_sell_contract.v2"
REGULAR_INTRADAY_DISASTER_STOP_PCT = 0.06
SNIPER_INTRADAY_DISASTER_STOP_PCT = 0.04
REGULAR_EOD_STRUCTURAL_STOP_PCT = 0.03
SNIPER_EOD_STRUCTURAL_STOP_PCT = 0.015
TRAILING_ARM_PCT = 0.04
TRAILING_PULLBACK_PCT = 0.02
DEFAULT_BUY_TIME = "14:50"
EOD_STRUCTURAL_STOP_TIMES = ["14:50", "14:55"]


def active_strategy_types() -> list[str]:
    paused = set(PAUSED_STRATEGY_TYPES)
    return [strategy for strategy in PRODUCTION_STRATEGY_TYPES if strategy not in paused]


def strategy_contract() -> dict[str, Any]:
    return {
        "schema_version": SIGNATURE_SCHEMA_VERSION,
        "pick_mode": PICK_MODE,
        "active_strategy_types": active_strategy_types(),
        "paused_strategy_types": list(PAUSED_STRATEGY_TYPES),
        "production_total_pick_limit": int(PRODUCTION_TOTAL_PICK_LIMIT),
        "global_min_score": float(GLOBAL_MIN_SCORE),
        "breakout_min_score": float(BREAKOUT_MIN_SCORE),
        "st_breakout_min_score": float(ST_BREAKOUT_MIN_SCORE),
        "sell_strategy_contract": sell_strategy_contract(),
    }


def sell_strategy_contract() -> dict[str, Any]:
    return {
        "schema_version": SELL_STRATEGY_SCHEMA_VERSION,
        "regular_intraday_disaster_stop_pct": float(REGULAR_INTRADAY_DISASTER_STOP_PCT),
        "sniper_intraday_disaster_stop_pct": float(SNIPER_INTRADAY_DISASTER_STOP_PCT),
        "regular_eod_structural_stop_pct": float(REGULAR_EOD_STRUCTURAL_STOP_PCT),
        "sniper_eod_structural_stop_pct": float(SNIPER_EOD_STRUCTURAL_STOP_PCT),
        "trailing_arm_pct": float(TRAILING_ARM_PCT),
        "trailing_pullback_pct": float(TRAILING_PULLBACK_PCT),
        "default_buy_time": DEFAULT_BUY_TIME,
        "eod_structural_stop_times": list(EOD_STRUCTURAL_STOP_TIMES),
        "risk_control_profiles": {
            name: {key: float(value) for key, value in profile.items()}
            for name, profile in RISK_CONTROL_PROFILES.items()
        },
    }


def daily_picks_signature(
    start_date: str,
    end_date: str,
    db_path: Union[str, Path] = SQLITE_PATH,
) -> dict[str, Any]:
    start = str(start_date or "")[:10]
    end = str(end_date or "")[:10]
    contract = strategy_contract()
    rows = _daily_pick_identity_rows(start, end, Path(db_path), contract["active_strategy_types"])
    body = {
        "schema_version": SIGNATURE_SCHEMA_VERSION,
        "start_date": start,
        "end_date": end,
        "contract": contract,
        "rows": rows,
    }
    digest = hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    dates = [row["selection_date"] for row in rows]
    ids = [row["id"] for row in rows]
    return {
        "schema_version": SIGNATURE_SCHEMA_VERSION,
        "hash": digest,
        "start_date": start,
        "end_date": end,
        "row_count": len(rows),
        "min_selection_date": min(dates) if dates else "",
        "max_selection_date": max(dates) if dates else "",
        "min_pick_id": min(ids) if ids else None,
        "max_pick_id": max(ids) if ids else None,
        "contract": contract,
    }


def validate_sentinel_payload(
    payload: dict[str, Any],
    db_path: Union[str, Path] = SQLITE_PATH,
) -> tuple[bool, dict[str, Any]]:
    cached = payload.get("daily_picks_signature")
    if not isinstance(cached, dict):
        return False, {
            "status": "invalid",
            "reason": "Sentinel 缓存缺少 daily_picks_signature，禁止使用旧口径缓存。",
            "cached": None,
            "current": None,
        }
    current = daily_picks_signature(
        str(payload.get("start_date") or cached.get("start_date") or ""),
        str(payload.get("end_date") or cached.get("end_date") or ""),
        db_path=db_path,
    )
    mismatches: list[str] = []
    for key in ("schema_version", "hash", "row_count", "start_date", "end_date"):
        if cached.get(key) != current.get(key):
            mismatches.append(key)
    cached_contract = cached.get("contract") if isinstance(cached.get("contract"), dict) else {}
    current_contract = current.get("contract") if isinstance(current.get("contract"), dict) else {}
    if cached_contract != current_contract:
        mismatches.append("contract")
    if mismatches:
        return False, {
            "status": "invalid",
            "reason": f"Sentinel 缓存与当前 daily_picks/策略契约不一致：{', '.join(mismatches)}。",
            "cached": cached,
            "current": current,
            "mismatches": mismatches,
        }
    return True, {
        "status": "valid",
        "reason": "Sentinel 缓存与当前 daily_picks/策略契约一致。",
        "cached": cached,
        "current": current,
        "mismatches": [],
    }


def _daily_pick_identity_rows(
    start_date: str,
    end_date: str,
    db_path: Path,
    strategies: list[str],
) -> list[dict[str, Any]]:
    if not strategies:
        return []
    placeholders = ",".join("?" for _ in strategies)
    params: list[Any] = [start_date, end_date, *strategies]
    with sqlite3.connect(str(db_path)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT id, selection_date, target_date, selected_at, code, name, strategy_type,
                   win_rate, selection_change, selection_price, snapshot_time, snapshot_price,
                   snapshot_vol_ratio, suggested_position, tier
            FROM daily_picks
            WHERE selection_date >= ?
              AND selection_date <= ?
              AND strategy_type IN ({placeholders})
            ORDER BY selection_date ASC, strategy_type ASC, code ASC, id ASC
            """,
            params,
        ).fetchall()
    return [_normalize_identity_row(row) for row in rows]


def _normalize_identity_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "selection_date": str(row["selection_date"] or "")[:10],
        "target_date": str(row["target_date"] or "")[:10],
        "selected_at": str(row["selected_at"] or ""),
        "code": _normalize_code(row["code"]),
        "name": str(row["name"] or ""),
        "strategy_type": str(row["strategy_type"] or ""),
        "win_rate": _round_optional(row["win_rate"]),
        "selection_change": _round_optional(row["selection_change"]),
        "selection_price": _round_optional(row["selection_price"]),
        "snapshot_time": str(row["snapshot_time"] or ""),
        "snapshot_price": _round_optional(row["snapshot_price"]),
        "snapshot_vol_ratio": _round_optional(row["snapshot_vol_ratio"]),
        "suggested_position": _round_optional(row["suggested_position"]),
        "tier": str(row["tier"] or ""),
    }


def _normalize_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else str(value or "")


def _round_optional(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return round(parsed, 8)
