from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from .market import fetch_sina_snapshot
from .predictor import scan_market
from .storage import (
    latest_daily_picks,
    pending_daily_picks,
    save_daily_pick,
    update_daily_pick_open,
    upsert_daily_rows,
)


def is_weekday(day: date | None = None) -> bool:
    current = day or date.today()
    return current.weekday() < 5


def next_weekday(day: date | None = None) -> date:
    current = day or date.today()
    target = current + timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return target


def save_today_top_pick(limit: int = 10, force: bool = False) -> dict[str, Any]:
    today = date.today()
    if not force and not is_weekday(today):
        return {"status": "skipped", "reason": "非工作日不保存 14:50 推送标的", "selection_date": today.isoformat()}

    scan = scan_market(limit=limit, persist_snapshot=True, cache_prediction=False, async_persist=False)
    rows = scan.get("rows", [])
    if not rows:
        raise RuntimeError("实时预测没有返回候选股票，无法保存 14:50 推送标的")

    return save_pushed_top_pick(rows[0], scan, force=force)


def save_pushed_top_pick(winner: dict[str, Any], scan: dict[str, Any], force: bool = False) -> dict[str, Any]:
    today = date.today()
    if not force and not is_weekday(today):
        return {"status": "skipped", "reason": "非工作日不保存 14:50 推送标的", "selection_date": today.isoformat()}

    selected_at = datetime.now().isoformat(timespec="seconds")
    pick = _pick_from_winner(winner, scan, selected_at)
    inserted_id = save_daily_pick(pick)
    saved = latest_daily_picks(limit=1)[0]
    if inserted_id == 0 and saved and saved.get("selection_date") == today.isoformat():
        return {"status": "exists", "reason": "今日 14:50 推送标的已锁定，不覆盖修改", "pick": saved}
    return {"status": "saved", "pick": saved}


def _pick_from_winner(winner: dict[str, Any], scan: dict[str, Any], selected_at: str) -> dict[str, Any]:
    today = date.today()
    return {
        "selection_date": today.isoformat(),
        "target_date": next_weekday(today).isoformat(),
        "selected_at": selected_at,
        "code": winner["code"],
        "name": winner["name"],
        "strategy_type": winner.get("strategy_type", "尾盘突破"),
        "win_rate": float(winner["win_rate"]),
        "selection_price": float(winner["price"]),
        "selection_change": float(winner["change"]),
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


def update_pending_open_results(force: bool = False) -> dict[str, Any]:
    today = date.today()
    if not force and not is_weekday(today):
        return {"status": "skipped", "reason": "非工作日不更新开盘结果", "updated": []}

    pending = pending_daily_picks(target_date=today.isoformat())
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
        result = update_daily_pick_open(pick["selection_date"], open_price, checked_at)
        if result:
            updated.append(result)

    return {"status": "updated", "updated": updated, "missing": missing}


def list_daily_pick_results(limit: int = 10) -> dict[str, Any]:
    return {"rows": latest_daily_picks(limit=limit)}
