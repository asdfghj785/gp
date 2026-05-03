from __future__ import annotations

from datetime import date, datetime
from typing import Any

from quant_core.data_pipeline.market import fetch_sina_snapshot
from quant_core.data_pipeline.trading_calendar import is_trading_day, next_trading_day, nth_trading_day
from quant_core.engine.predictor import attach_pick_theme_fields, scan_market
from .storage import (
    get_daily_picks,
    latest_daily_picks,
    pending_daily_picks,
    save_daily_pick,
    update_daily_pick_open,
    upsert_daily_rows,
)


SWING_STRATEGY_TYPES = {"中线超跌反转", "右侧主升浪", "全局动量狙击"}


def is_weekday(day: date | None = None) -> bool:
    return is_trading_day(day or date.today())


def next_weekday(day: date | None = None) -> date:
    return next_trading_day(day or date.today())


def nth_weekday(day: date, n: int) -> date:
    return nth_trading_day(day, n)


def save_today_top_pick(limit: int = 12, force: bool = False) -> dict[str, Any]:
    today = date.today()
    if not force and not is_weekday(today):
        return {"status": "skipped", "reason": "非工作日不保存 14:50 推送标的", "selection_date": today.isoformat()}

    scan = scan_market(limit=limit, persist_snapshot=True, cache_prediction=False, async_persist=False)
    rows = scan.get("rows", [])
    if not rows:
        raise RuntimeError("实时预测没有返回候选股票，无法保存 14:50 推送标的")

    return save_pushed_top_picks(rows, scan, force=force)


def save_pushed_top_pick(winner: dict[str, Any], scan: dict[str, Any], force: bool = False) -> dict[str, Any]:
    result = save_pushed_top_picks([winner], scan, force=force)
    if result.get("saved"):
        return {"status": result["status"], "pick": result["saved"][0]}
    if result.get("existing"):
        return {"status": "exists", "reason": "今日 14:50 推送标的已锁定，不覆盖修改", "pick": result["existing"][0]}
    return result


def save_pushed_top_picks(winners: list[dict[str, Any]], scan: dict[str, Any], force: bool = False) -> dict[str, Any]:
    today = date.today()
    if not force and not is_weekday(today):
        return {"status": "skipped", "reason": "非工作日不保存 14:50 推送标的", "selection_date": today.isoformat()}

    selected_at = datetime.now().isoformat(timespec="seconds")
    saved: list[dict[str, Any]] = []
    existing: list[dict[str, Any]] = []
    for winner in winners:
        pick = _pick_from_winner(winner, scan, selected_at)
        inserted_id = save_daily_pick(pick)
        day_picks = get_daily_picks(today.isoformat())
        matched = next(
            (
                item
                for item in day_picks
                if item.get("strategy_type") == pick["strategy_type"] and item.get("code") == pick["code"]
            ),
            None,
        )
        if inserted_id == 0:
            matched = matched or next((item for item in day_picks if item.get("strategy_type") == pick["strategy_type"]), None)
            if matched:
                existing.append(matched)
        elif matched:
            saved.append(matched)
    status = "saved" if saved else "exists" if existing else "noop"
    return {
        "status": status,
        "selection_date": today.isoformat(),
        "saved": saved,
        "existing": existing,
        "count": len(saved),
        "existing_count": len(existing),
    }


def _pick_from_winner(winner: dict[str, Any], scan: dict[str, Any], selected_at: str) -> dict[str, Any]:
    today = date.today()
    winner = _winner_theme_contract(dict(winner))
    strategy_type = winner.get("strategy_type", "尾盘突破")
    target_date = nth_trading_day(today, 3) if strategy_type in SWING_STRATEGY_TYPES else next_trading_day(today)
    return {
        "selection_date": today.isoformat(),
        "target_date": target_date.isoformat(),
        "selected_at": selected_at,
        "code": winner["code"],
        "name": winner["name"],
        "strategy_type": strategy_type,
        "win_rate": float(winner["win_rate"]),
        "selection_price": float(winner["price"]),
        "selection_change": float(winner["change"]),
        "snapshot_time": selected_at.split("T", 1)[1] if "T" in selected_at else selected_at,
        "snapshot_price": float(winner["price"]),
        "snapshot_vol_ratio": float(winner.get("volume_ratio") or 0),
        "core_theme": winner["core_theme"],
        "theme_momentum_3d": winner["theme_momentum_3d"],
        "is_shadow_test": True,
        "t3_max_gain_pct": None,
        "model_status": scan.get("model_status", ""),
        "status": "pending_open",
        "raw": {
            "source": "pushplus_1450",
            "winner": winner,
            "scan_id": scan.get("id"),
            "scan_created_at": scan.get("created_at"),
            "strategy": scan.get("strategy"),
            "market_gate": scan.get("market_gate"),
            "intraday_snapshot": scan.get("intraday_snapshot"),
        },
    }


def _winner_theme_contract(winner: dict[str, Any]) -> dict[str, Any]:
    core_theme = str(winner.get("core_theme") or winner.get("theme_name") or "-").strip() or "-"
    momentum = winner.get("theme_momentum_3d", winner.get("theme_momentum", winner.get("theme_pct_chg_3", 0.0)))
    try:
        momentum_value = float(momentum)
    except (TypeError, ValueError):
        momentum_value = 0.0
    winner["core_theme"] = core_theme
    winner["theme_name"] = winner.get("theme_name") or core_theme
    winner["theme_momentum_3d"] = momentum_value
    winner["theme_momentum"] = winner.get("theme_momentum", momentum_value)
    winner["theme_pct_chg_3"] = winner.get("theme_pct_chg_3", momentum_value)
    return winner


def update_pending_open_results(force: bool = False) -> dict[str, Any]:
    today = date.today()
    if not force and not is_weekday(today):
        return {"status": "skipped", "reason": "非工作日不更新开盘结果", "updated": []}

    pending = pending_daily_picks(target_date=today.isoformat())
    pending = [pick for pick in pending if pick.get("strategy_type") not in SWING_STRATEGY_TYPES]
    if not pending:
        return {"status": "noop", "updated": []}

    snapshot = fetch_sina_snapshot()
    if snapshot.empty:
        raise RuntimeError("实时行情源返回空数据，无法更新开盘价")
    upsert_daily_rows(snapshot, source="sina_open_check")
    live_by_code = snapshot.set_index("code").to_dict(orient="index")
    checked_at = datetime.now().isoformat(timespec="seconds")

    updated: list[dict[str, Any]] = []
    missing: list[str] = []
    for pick in pending:
        live = live_by_code.get(pick["code"])
        if not live:
            missing.append(pick["code"])
            continue
        open_price = float(live.get("open") or 0)
        if open_price <= 0:
            missing.append(pick["code"])
            continue
        result = update_daily_pick_open(
            pick["selection_date"],
            open_price,
            checked_at,
            strategy_type=pick.get("strategy_type"),
            code=pick.get("code"),
            pick_id=pick.get("id"),
        )
        if result:
            updated.append(result)

    return {"status": "updated", "updated": updated, "missing": missing}


def list_daily_pick_results(limit: int = 10, shadow_only: bool = False) -> dict[str, Any]:
    return {"rows": [attach_pick_theme_fields(row) for row in latest_daily_picks(limit=limit, shadow_only=shadow_only)]}
