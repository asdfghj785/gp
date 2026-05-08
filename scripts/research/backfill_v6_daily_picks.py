from __future__ import annotations

import json
import math
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path("/Users/eudis/ths")
DB_PATH = BASE_DIR / "data" / "core_db" / "quant_workstation.sqlite3"
PICKS_PATH = BASE_DIR / "scripts" / "research" / "rebuild_v6_global_sniper_picks.csv"
TRADES_PATH = BASE_DIR / "scripts" / "research" / "rebuild_v6_global_sniper_trades.csv"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "rebuild_v6_global_sniper_latest.json"
BACKUP_DIR = BASE_DIR / "data" / "core_db" / "backups"

STRATEGY_TYPE = "全局动量狙击"
MODEL_VERSION = "v6_0_extreme_burst"
MODEL_LABEL = "V6.0 极寒爆发大脑"
SELECTION_MODE = "v6_extreme_top1_p60"
THRESHOLD = 0.60
MODEL_PATH = BASE_DIR / "models" / "production" / "xgboost_global_sniper_v6_0_extreme_burst.json"
META_PATH = BASE_DIR / "models" / "production" / "xgboost_global_sniper_v6_0_extreme_burst.meta.json"


def norm_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else ""


def finite_float(value: Any, default: float | None = None) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(parsed):
        return default
    return parsed


def close_reason(reason: str) -> str:
    text = str(reason or "")
    if "动态追踪止盈" in text:
        return "5m动态追踪止盈"
    if "盘中暴雷止损" in text:
        return "5m盘中暴雷止损：敢死队-4%"
    if "尾盘破位" in text:
        return "5m尾盘破位卖出：敢死队-1.5%"
    if "T+3" in text:
        return "5m T+3强制平仓"
    return text or "V6回填结算"


def suggested_position(probability: float) -> float:
    kelly_win_loss_ratio = 1.5
    half_kelly_factor = 0.5
    base_min = 0.10
    base_max = 0.30
    probability = max(0.0, min(1.0, float(probability)))
    kelly_fraction = probability - (1.0 - probability) / kelly_win_loss_ratio
    half_kelly = max(0.0, kelly_fraction * half_kelly_factor)
    return round(max(base_min, min(base_max, half_kelly)), 4)


def load_inputs() -> tuple[pd.DataFrame, dict[str, Any]]:
    if not PICKS_PATH.exists():
        raise FileNotFoundError(PICKS_PATH)
    if not TRADES_PATH.exists():
        raise FileNotFoundError(TRADES_PATH)
    picks = pd.read_csv(PICKS_PATH, dtype={"code": "string"})
    trades = pd.read_csv(TRADES_PATH, dtype={"code": "string"})
    picks["code"] = picks["code"].map(norm_code)
    trades["code"] = trades["code"].map(norm_code)
    picks["date"] = pd.to_datetime(picks["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    trades["date"] = pd.to_datetime(trades["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    merged = trades.merge(
        picks[["date", "code", "close", "entry_price", "buy5m_entry_price"]],
        on=["date", "code"],
        how="left",
        suffixes=("", "_pick"),
    )
    merged = merged.dropna(subset=["date", "code", "cost_price", "exit_price", "yield_pct"])
    report = json.loads(REPORT_PATH.read_text(encoding="utf-8")) if REPORT_PATH.exists() else {}
    if len(merged) != 195:
        raise RuntimeError(f"expected 195 V6 trades, got {len(merged)}")
    return merged, report


def export_legacy_rows(conn: sqlite3.Connection, timestamp: str) -> tuple[Path, int]:
    rows = conn.execute(
        """
        SELECT *
        FROM daily_picks
        WHERE strategy_type = ?
        ORDER BY selection_date, id
        """,
        (STRATEGY_TYPE,),
    ).fetchall()
    export_path = BACKUP_DIR / f"daily_picks_legacy_global_export_{timestamp}.json"
    export_path.write_text(
        json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return export_path, len(rows)


def build_raw(row: pd.Series, report: dict[str, Any], position: float) -> dict[str, Any]:
    probability = finite_float(row.get("probability"), 0.0) or 0.0
    future_pct = finite_float(row.get("future_max_return_3d"), 0.0) or 0.0
    cost = finite_float(row.get("cost_price"), 0.0) or 0.0
    exit_price = finite_float(row.get("exit_price"), 0.0) or 0.0
    yield_pct = finite_float(row.get("yield_pct"), 0.0) or 0.0
    highest_gain_pct = finite_float(row.get("highest_gain_pct"), None)
    production_model = {
        "version": MODEL_VERSION,
        "label": MODEL_LABEL,
        "path": str(MODEL_PATH),
        "meta_path": str(META_PATH),
        "threshold": THRESHOLD,
        "selection_mode": SELECTION_MODE,
    }
    signal = {
        "source": "sentinel_5m_backtest",
        "coverage_status": "covered",
        "exit_reason": str(row.get("exit_reason") or ""),
        "close_reason": close_reason(str(row.get("exit_reason") or "")),
        "sell_strategy": close_reason(str(row.get("exit_reason") or "")),
        "exit_policy": close_reason(str(row.get("exit_reason") or "")),
        "close_time": str(row.get("exit_time") or ""),
        "close_price": round(exit_price, 4),
        "close_return_pct": round(yield_pct, 4),
        "highest_gain_pct": round(highest_gain_pct, 4) if highest_gain_pct is not None else None,
        "bars_replayed": int(finite_float(row.get("bars_replayed"), 0) or 0),
    }
    winner = {
        "code": norm_code(row.get("code")),
        "name": str(row.get("name") or norm_code(row.get("code"))),
        "strategy_type": STRATEGY_TYPE,
        "price": round(cost, 4),
        "win_rate": round(probability * 100.0, 4),
        "expected_premium": round(max(4.0, min(9.0, 4.0 + max(0.0, probability - THRESHOLD) * 20.0)), 4),
        "expected_t3_max_gain_pct": round(max(4.0, min(9.0, 4.0 + max(0.0, probability - THRESHOLD) * 20.0)), 4),
        "global_probability": round(probability, 8),
        "global_probability_pct": round(probability * 100.0, 4),
        "composite_score": round(probability * 100.0, 4),
        "sort_score": round(probability * 100.0, 4),
        "selection_score": round(probability, 8),
        "score_threshold": THRESHOLD,
        "score_floor": THRESHOLD,
        "selection_tier": "base",
        "suggested_position": position,
        "position_probability": round(probability, 8),
        "model_version": MODEL_VERSION,
        "selection_mode": SELECTION_MODE,
        "production_model": production_model,
        "label_extreme_burst": int(finite_float(row.get("label_extreme_burst"), 0) or 0),
        "future_max_return_3d": round(future_pct, 4),
        "t3_max_gain_pct": round(highest_gain_pct, 4) if highest_gain_pct is not None else None,
        "t3_settlement_price": round(exit_price, 4),
        "t3_settlement_return_pct": round(yield_pct, 4),
        "sell_strategy": signal["sell_strategy"],
        "exit_policy": signal["exit_policy"],
        "coverage_status": "covered",
        "exit_category": signal["exit_reason"],
        "bars_replayed": signal["bars_replayed"],
        "risk_warning": "V6.0 极寒爆发回填账本；旧全局狙击已物理移除",
    }
    return {
        "source": "v6_extreme_burst_daily_picks_backfill",
        "schema_version": "daily_picks.v6_extreme_burst_backfill.v1",
        "replay_source": "scripts/research/rebuild_v6_global_sniper.py",
        "pick_detail_path": str(PICKS_PATH),
        "trade_detail_path": str(TRADES_PATH),
        "report_path": str(REPORT_PATH),
        "production_model": production_model,
        "selection_mode": SELECTION_MODE,
        "threshold": THRESHOLD,
        "top_k": 1,
        "label_rule": report.get("label_rule"),
        "selection": report.get("selection"),
        "sentinel_summary": report.get("sentinel_summary"),
        "winner": winner,
        "close_signal": signal,
        "sentinel_5m": signal,
    }


def main() -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = BACKUP_DIR / f"quant_workstation_before_v6_daily_picks_{timestamp}.sqlite3"
    shutil.copy2(DB_PATH, backup_path)

    trades, report = load_inputs()
    inserted = 0
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        export_path, legacy_count = export_legacy_rows(conn, timestamp)
        conn.execute("DELETE FROM daily_picks WHERE strategy_type = ?", (STRATEGY_TYPE,))
        for item in trades.itertuples(index=False):
            row = pd.Series(item._asdict())
            trade_date = str(row["date"])[:10]
            probability = finite_float(row.get("probability"), 0.0) or 0.0
            cost = finite_float(row.get("cost_price"), 0.0) or 0.0
            exit_price = finite_float(row.get("exit_price"), 0.0) or 0.0
            yield_pct = finite_float(row.get("yield_pct"), 0.0) or 0.0
            position = suggested_position(probability)
            raw = build_raw(row, report, position)
            close_time = str(row.get("exit_time") or "")
            close_date = close_time[:10] if close_time else str(row.get("t3_date") or "")[:10]
            reason = close_reason(str(row.get("exit_reason") or ""))
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
                    trade_date,
                    str(row.get("t3_date") or "")[:10],
                    f"{trade_date}T14:50:00",
                    norm_code(row.get("code")),
                    str(row.get("name") or norm_code(row.get("code"))),
                    round(probability * 100.0, 4),
                    round(cost, 4),
                    None,
                    "14:50:00",
                    round(cost, 4),
                    None,
                    0,
                    "v6_extreme_burst_backfill; win_rate=75.90%; mean_yield=8.8322%",
                    "closed",
                    None,
                    None,
                    None,
                    1 if yield_pct > 0 else 0,
                    json.dumps(raw, ensure_ascii=False),
                    STRATEGY_TYPE,
                    finite_float(row.get("highest_gain_pct"), None),
                    1,
                    close_date,
                    round(exit_price, 4),
                    round(yield_pct, 4),
                    reason,
                    close_time,
                    position,
                    "base",
                ),
            )
            inserted += 1
        conn.commit()

    print(json.dumps({
        "status": "ok",
        "backup_path": str(backup_path),
        "legacy_export_path": str(export_path),
        "deleted_legacy_global_rows": legacy_count,
        "inserted_v6_rows": inserted,
        "model_version": MODEL_VERSION,
        "selection_mode": SELECTION_MODE,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
