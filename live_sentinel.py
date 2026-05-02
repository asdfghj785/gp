from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.data_pipeline.tencent_engine import get_tencent_realtime, tencent_symbol
from quant_core.data_pipeline.trading_calendar import is_trading_day, nth_trading_day, trading_day_count_after
from quant_core.execution.pushplus_tasks import send_pushplus
from quant_core.storage import connect, init_db, latest_daily_picks, mark_daily_pick_closed


LEDGER_PATH = BASE_DIR / "shadow_ledger.json"
DEFAULT_INTERVAL_SECONDS = 30
MARKET_OPEN = clock_time(9, 15)
CONTINUOUS_TRADING_START = clock_time(9, 30)
LUNCH_START = clock_time(11, 30)
LUNCH_END = clock_time(13, 0)
MARKET_CLOSE = clock_time(15, 0)
TENCENT_MKLINE_URL = "http://ifzq.gtimg.cn/appstock/app/kline/mkline"
TENCENT_REALTIME_URL = "http://qt.gtimg.cn/q={symbol}"
INTRADAY_1M_COUNT = 240
VOLUME_DIVERGENCE_MULTIPLIER = 3.0
ORDER_BOOK_IMBALANCE_THRESHOLD = -80.0
VWAP_FAKE_DUMP_HOLD_RATIO = 0.995
ANTI_NUCLEAR_CONFIRM_SECONDS = 180
ANTI_NUCLEAR_MAX_LOSS_PCT = -1.0
SWING_STRATEGY_TYPES = {"中线超跌反转", "右侧主升浪"}
NON_SWING_STRATEGY_TYPES = {"尾盘突破", "首阴低吸"}


def load_ledger(path: Path = LEDGER_PATH) -> dict[str, Any]:
    if not path.exists():
        ledger = {"positions": [], "closed_positions": []}
        save_ledger(ledger, path)
        return ledger

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        ledger = {"positions": [], "closed_positions": []}
        save_ledger(ledger, path)
        return ledger

    try:
        raw = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"账本 JSON 解析失败：{path} / {exc}") from exc

    if isinstance(raw, list):
        ledger = {"positions": raw, "closed_positions": []}
    elif isinstance(raw, dict):
        ledger = raw
        ledger.setdefault("positions", [])
        ledger.setdefault("closed_positions", [])
    else:
        raise RuntimeError("shadow_ledger.json 根节点必须是数组或对象")

    changed = migrate_position_state(ledger)
    if changed:
        save_ledger(ledger, path)
    return ledger


def save_ledger(ledger: dict[str, Any], path: Path = LEDGER_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(ledger, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def migrate_position_state(ledger: dict[str, Any]) -> bool:
    changed = False
    for position in open_positions(ledger):
        buy_price = safe_float(position.get("buy_price"))
        highest_price = safe_float(position.get("highest_price"))
        if buy_price <= 0:
            continue
        if highest_price <= 0:
            position["highest_price"] = round(buy_price, 4)
            changed = True
        elif highest_price < buy_price:
            position["highest_price"] = round(buy_price, 4)
            changed = True
        if not isinstance(position.get("volume_alert_triggered"), bool):
            position["volume_alert_triggered"] = False
            changed = True
        if position.get("volume_alert_date") and position.get("volume_alert_date") != date.today().isoformat():
            position["volume_alert_triggered"] = False
            changed = True
        if not isinstance(position.get("order_book_alert_triggered"), bool):
            position["order_book_alert_triggered"] = False
            changed = True
        if (
            position.get("order_book_alert_date")
            and position.get("order_book_alert_date") != date.today().isoformat()
        ):
            position["order_book_alert_triggered"] = False
            changed = True
    return changed


def open_positions(ledger: dict[str, Any]) -> list[dict[str, Any]]:
    positions = ledger.get("positions")
    if not isinstance(positions, list):
        ledger["positions"] = []
    return ledger["positions"]


def add_position(
    code: str,
    name: str,
    buy_price: float,
    buy_date: Optional[str] = None,
    target_date: Optional[str] = None,
    path: Path = LEDGER_PATH,
) -> dict[str, Any]:
    if buy_price <= 0:
        raise ValueError("buy_price 必须大于 0")

    ledger = load_ledger(path)
    clean_code = normalize_code(code)
    now = datetime.now()
    position = {
        "code": clean_code,
        "name": name or clean_code,
        "buy_price": round(float(buy_price), 4),
        "highest_price": round(float(buy_price), 4),
        "volume_alert_triggered": False,
        "order_book_alert_triggered": False,
        "buy_date": buy_date or now.date().isoformat(),
        "buy_time": now.isoformat(timespec="seconds"),
        "target_date": target_date or t_plus_3_date(buy_date or now.date().isoformat()),
    }
    positions = open_positions(ledger)
    positions[:] = [item for item in positions if normalize_code(item.get("code")) != clean_code]
    positions.append(position)
    save_ledger(ledger, path)
    return position


def seed_from_latest_1450_picks(path: Path = LEDGER_PATH, monitor_date: Optional[str] = None) -> dict[str, Any]:
    current_day = monitor_date or date.today().isoformat()
    if not is_trading_day(parse_date(current_day) or date.today()):
        return {"status": "skipped", "reason": "非交易日不重建实时巡逻账本", "monitor_date": current_day}

    picks = previous_1450_picks(current_day)
    ledger = load_ledger(path)
    closed_keys = {
        (
            normalize_code(item.get("code")),
            str(item.get("source_selection_date") or item.get("buy_date") or ""),
            str(item.get("monitor_date") or ""),
        )
        for item in ledger.get("closed_positions", [])
        if isinstance(item, dict) and item.get("code")
    }

    seeded: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    for pick in picks:
        code = normalize_code(pick.get("code"))
        source_date = str(pick.get("selection_date") or "")
        key = (code, source_date, current_day)
        if key in closed_keys:
            skipped.append({"code": code, "name": pick.get("name"), "reason": "already_closed_today"})
            continue
        buy_price = safe_float(pick.get("snapshot_price") or pick.get("selection_price") or pick.get("price"))
        if buy_price <= 0:
            skipped.append({"code": code, "name": pick.get("name"), "reason": "missing_snapshot_price"})
            continue
        position = {
            "code": code,
            "name": str(pick.get("name") or code),
            "buy_price": round(buy_price, 4),
            "highest_price": round(buy_price, 4),
            "volume_alert_triggered": False,
            "order_book_alert_triggered": False,
            "buy_date": source_date,
            "buy_time": str(pick.get("selected_at") or pick.get("snapshot_time") or ""),
            "target_date": str(pick.get("target_date") or t_plus_3_date(source_date)),
            "strategy_type": pick.get("strategy_type") or "",
            "source": "daily_picks_1450",
            "source_selection_date": source_date,
            "source_pick_id": pick.get("id"),
            "monitor_date": current_day,
        }
        positions.append(position)
        seeded.append({"code": code, "name": position["name"], "buy_price": position["buy_price"]})

    ledger["positions"] = positions
    ledger["last_seeded_at"] = datetime.now().isoformat(timespec="seconds")
    ledger["last_seeded_selection_date"] = picks[0].get("selection_date") if picks else ""
    ledger["monitor_date"] = current_day
    save_ledger(ledger, path)
    return {
        "status": "seeded" if seeded else "empty",
        "monitor_date": current_day,
        "selection_date": ledger["last_seeded_selection_date"],
        "count": len(seeded),
        "seeded": seeded,
        "skipped": skipped,
    }


def previous_1450_picks(current_day: str) -> list[dict[str, Any]]:
    rows = latest_daily_picks(limit=300, shadow_only=True)
    previous_dates = sorted(
        {
            str(row.get("selection_date") or "")
            for row in rows
            if str(row.get("selection_date") or "") and str(row.get("selection_date") or "") < current_day
        },
        reverse=True,
    )
    if not previous_dates:
        return []
    selection_date = previous_dates[0]
    return [
        row
        for row in rows
        if str(row.get("selection_date") or "") == selection_date and not bool(row.get("is_closed"))
    ]


def run_loop(
    path: Path = LEDGER_PATH,
    interval_seconds: int = DEFAULT_INTERVAL_SECONDS,
    once: bool = False,
    send_push: bool = True,
    dry_run: bool = False,
    seed_latest_picks: bool = False,
) -> dict[str, Any]:
    summary: dict[str, Any] = {"checked_rounds": 0, "events": []}
    if seed_latest_picks and not dry_run:
        summary["seed"] = seed_from_latest_1450_picks(path)
    while True:
        if not once:
            wait_seconds = seconds_until_scan_window()
            if wait_seconds is None:
                summary["stopped_at"] = datetime.now().isoformat(timespec="seconds")
                summary["stop_reason"] = "market_closed"
                return summary
            if wait_seconds > 0:
                summary["events"].append(
                    {
                        "status": "sleeping",
                        "reason": scan_pause_reason(),
                        "seconds": wait_seconds,
                        "time": datetime.now().isoformat(timespec="seconds"),
                    }
                )
                time.sleep(wait_seconds)
                continue

        result = check_positions(path=path, send_push=send_push, dry_run=dry_run)
        summary["checked_rounds"] += 1
        summary["events"].extend(result.get("events", []))

        if once or not open_positions(result["ledger"]):
            return summary
        time.sleep(max(1, int(interval_seconds)))


def check_positions(path: Path = LEDGER_PATH, send_push: bool = True, dry_run: bool = False) -> dict[str, Any]:
    ledger = load_ledger(path)
    if not is_trading_day(date.today()):
        return {"status": "skipped", "reason": "非交易日不执行实时巡逻", "count": 0, "events": [], "ledger": ledger}

    positions = list(open_positions(ledger))
    events: list[dict[str, Any]] = []

    for position in positions:
        try:
            event = check_one_position(position, ledger, path, send_push=send_push, dry_run=dry_run)
            if event:
                events.append(event)
        except Exception as exc:
            events.append(
                {
                    "status": "error",
                    "code": position.get("code"),
                    "name": position.get("name"),
                    "error": str(exc),
                    "checked_at": datetime.now().isoformat(timespec="seconds"),
                }
            )

    return {"status": "checked", "count": len(positions), "events": events, "ledger": ledger}


def check_one_position(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    send_push: bool = True,
    dry_run: bool = False,
) -> Optional[dict[str, Any]]:
    code = normalize_code(position.get("code"))
    name = str(position.get("name") or code)
    buy_price = safe_float(position.get("buy_price"))
    highest_price = safe_float(position.get("highest_price")) or buy_price
    if buy_price <= 0:
        raise RuntimeError(f"{name}({code}) 缺少有效 buy_price")

    external_close_event = retire_if_source_pick_already_closed(position, ledger, path, dry_run=dry_run)
    if external_close_event:
        return external_close_event

    quote = get_tencent_realtime(code)
    current_price = realtime_price(quote)
    if current_price <= 0:
        raise RuntimeError(f"{name}({code}) 实时价格无效")

    updated_highest = False
    if current_price > highest_price:
        highest_price = current_price
        position["highest_price"] = round(highest_price, 4)
        position["highest_price_updated_at"] = datetime.now().isoformat(timespec="seconds")
        updated_highest = True
        if not dry_run:
            save_ledger(ledger, path)

    current_gain_pct = gain_pct(current_price, buy_price)
    highest_gain_pct = gain_pct(highest_price, buy_price)
    drawdown_pct = gain_pct(current_price, highest_price)
    anti_nuclear_event = check_anti_nuclear_pool(
        position=position,
        ledger=ledger,
        path=path,
        current_price=current_price,
        buy_price=buy_price,
        send_push=send_push,
        dry_run=dry_run,
    )
    try:
        volume_alert_event = check_volume_divergence(
            position=position,
            ledger=ledger,
            path=path,
            send_push=send_push,
            dry_run=dry_run,
        )
    except Exception as exc:
        volume_alert_event = {
            "status": "volume_alert_error",
            "code": code,
            "name": name,
            "error": str(exc),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
    try:
        order_book_alert_event = check_order_book_imbalance(
            position=position,
            ledger=ledger,
            path=path,
            send_push=send_push,
            dry_run=dry_run,
        )
    except Exception as exc:
        order_book_alert_event = {
            "status": "order_book_alert_error",
            "code": code,
            "name": name,
            "error": str(exc),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }

    if current_price <= buy_price * 0.97:
        event = close_position(
            ledger=ledger,
            position=position,
            path=path,
            reason="initial_stop",
            title=f"【初始止损报警】{name} 跌破买入价 3% 防线",
            message=(
                f"【初始止损报警】{name}({code}) 当前价 {current_price:.2f}，"
                f"相对买入价 {buy_price:.2f} 亏损约 {abs(current_gain_pct):.2f}%，建议立即止损。"
            ),
            current_price=current_price,
            highest_price=highest_price,
            buy_price=buy_price,
            current_gain_pct=current_gain_pct,
            highest_gain_pct=highest_gain_pct,
            drawdown_pct=drawdown_pct,
            quote=quote,
            send_push=send_push,
            dry_run=dry_run,
        )
        if volume_alert_event:
            event["volume_alert"] = volume_alert_event
        if order_book_alert_event:
            event["order_book_alert"] = order_book_alert_event
        if anti_nuclear_event:
            event["anti_nuclear"] = anti_nuclear_event
        return event

    trailing_active = highest_price >= buy_price * 1.05
    trailing_hit = current_price <= highest_price * 0.97
    if trailing_active and trailing_hit:
        event = close_position(
            ledger=ledger,
            position=position,
            path=path,
            reason="trailing_take_profit",
            title=f"【追踪止盈触发】{name} 从最高点回撤 3%",
            message=(
                f"【追踪止盈触发】{name}从最高点回撤 {abs(drawdown_pct):.2f}%，"
                f"当前盈利约 {current_gain_pct:.2f}%，建议落袋为安！"
            ),
            current_price=current_price,
            highest_price=highest_price,
            buy_price=buy_price,
            current_gain_pct=current_gain_pct,
            highest_gain_pct=highest_gain_pct,
            drawdown_pct=drawdown_pct,
            quote=quote,
            send_push=send_push,
            dry_run=dry_run,
        )
        if volume_alert_event:
            event["volume_alert"] = volume_alert_event
        if order_book_alert_event:
            event["order_book_alert"] = order_book_alert_event
        if anti_nuclear_event:
            event["anti_nuclear"] = anti_nuclear_event
        return event

    if is_t_plus_3_timeout(position):
        event = close_position(
            ledger=ledger,
            position=position,
            path=path,
            reason="t3_timeout",
            title=f"【T+3 超时清仓】{name} 持仓周期到期",
            message=(
                f"【T+3 超时清仓】{name}({code}) 持仓已到 T+3，"
                f"当前盈利约 {current_gain_pct:.2f}%，按原规则清仓。"
            ),
            current_price=current_price,
            highest_price=highest_price,
            buy_price=buy_price,
            current_gain_pct=current_gain_pct,
            highest_gain_pct=highest_gain_pct,
            drawdown_pct=drawdown_pct,
            quote=quote,
            send_push=send_push,
            dry_run=dry_run,
        )
        if volume_alert_event:
            event["volume_alert"] = volume_alert_event
        if order_book_alert_event:
            event["order_book_alert"] = order_book_alert_event
        if anti_nuclear_event:
            event["anti_nuclear"] = anti_nuclear_event
        return event

    event = {
        "status": "holding",
        "code": code,
        "name": name,
        "current_price": round(current_price, 4),
        "buy_price": round(buy_price, 4),
        "highest_price": round(highest_price, 4),
        "current_gain_pct": round(current_gain_pct, 4),
        "highest_gain_pct": round(highest_gain_pct, 4),
        "drawdown_pct": round(drawdown_pct, 4),
        "updated_highest": updated_highest,
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }
    if volume_alert_event:
        event["volume_alert"] = volume_alert_event
    if order_book_alert_event:
        event["order_book_alert"] = order_book_alert_event
    if anti_nuclear_event:
        event["anti_nuclear"] = anti_nuclear_event
    return event


def retire_if_source_pick_already_closed(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    dry_run: bool = False,
) -> Optional[dict[str, Any]]:
    if str(position.get("source") or "") != "daily_picks_1450":
        return None
    pick_id = safe_int(position.get("source_pick_id"))
    if pick_id <= 0:
        return None

    closed_pick = closed_daily_pick_snapshot(pick_id)
    if not closed_pick:
        return None

    code = normalize_code(position.get("code"))
    name = str(position.get("name") or code)
    now = datetime.now().isoformat(timespec="seconds")
    event = {
        "status": "already_closed",
        "code": code,
        "name": name,
        "reason": closed_pick.get("close_reason") or "daily_pick_closed",
        "close_price": round(safe_float(closed_pick.get("close_price")), 4),
        "close_return_pct": round(safe_float(closed_pick.get("close_return_pct")), 4),
        "closed_at": closed_pick.get("close_checked_at") or "",
        "checked_at": now,
        "dry_run": dry_run,
    }
    if dry_run:
        return event

    positions = open_positions(ledger)
    positions[:] = [
        item
        for item in positions
        if safe_int(item.get("source_pick_id")) != pick_id
    ]
    closed_positions = ledger.setdefault("closed_positions", [])
    if isinstance(closed_positions, list) and not any(
        safe_int(item.get("source_pick_id")) == pick_id for item in closed_positions if isinstance(item, dict)
    ):
        closed = dict(position)
        closed.update(
            {
                "exit_reason": "external_daily_pick_close",
                "exit_price": event["close_price"],
                "exit_gain_pct": event["close_return_pct"],
                "closed_at": event["closed_at"] or now,
                "daily_pick_sync": {
                    "status": "already_closed",
                    "pick_id": pick_id,
                    "close_price": event["close_price"],
                    "close_return_pct": event["close_return_pct"],
                    "close_reason": event["reason"],
                },
            }
        )
        closed_positions.append(closed)
    save_ledger(ledger, path)
    return event


def closed_daily_pick_snapshot(pick_id: int) -> Optional[dict[str, Any]]:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT id, code, name, strategy_type, is_closed, close_price,
                   close_return_pct, close_reason, close_checked_at
            FROM daily_picks
            WHERE id = ?
            """,
            (int(pick_id),),
        ).fetchone()
    if not row or not bool(row["is_closed"]):
        return None
    return dict(row)


def check_volume_divergence(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    send_push: bool,
    dry_run: bool,
) -> Optional[dict[str, Any]]:
    if bool(position.get("volume_alert_triggered")):
        return None

    code = normalize_code(position.get("code"))
    name = str(position.get("name") or code)
    df = fetch_intraday_1m_df(code)
    signal = volume_divergence_signal(df)
    if not signal["triggered"]:
        return None

    now = datetime.now()
    title = f"【异动报警】{name}({code}) 分钟级天量滞涨"
    message = (
        f"【异动报警】{name} ({code}) 出现分钟级天量滞涨！"
        "近3分钟放出平日3倍巨量但价格未涨，疑似主力派发，请警惕回落风险！"
    )
    content = f"""## {message}

- 标的：{name}({code})
- 今日 1m 均量：{signal['avg_vol']:.2f}
- 近 3 分钟总量：{signal['recent_3m_vol']:.2f}
- 近 3 分钟均量倍数：{signal['recent_avg_multiple']:.2f}x
- 近 3 分钟价格变化：{signal['price_change']:.4f}
- 检查时间：{now.isoformat(timespec="seconds")}
"""
    push_result = send_pushplus(title, content) if send_push and not dry_run else {"status": "dry_run"}

    if not dry_run:
        position["volume_alert_triggered"] = True
        position["volume_alert_date"] = now.date().isoformat()
        position["volume_alert_at"] = now.isoformat(timespec="seconds")
        position["volume_alert_snapshot"] = {
            "avg_vol": round(signal["avg_vol"], 4),
            "recent_3m_vol": round(signal["recent_3m_vol"], 4),
            "recent_avg_multiple": round(signal["recent_avg_multiple"], 4),
            "price_change": round(signal["price_change"], 4),
        }
        save_ledger(ledger, path)

    return {
        "status": "volume_alert",
        "code": code,
        "name": name,
        "avg_vol": round(signal["avg_vol"], 4),
        "recent_3m_vol": round(signal["recent_3m_vol"], 4),
        "recent_avg_multiple": round(signal["recent_avg_multiple"], 4),
        "price_change": round(signal["price_change"], 4),
        "pushplus": push_result,
        "dry_run": dry_run,
    }


def check_order_book_imbalance(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    send_push: bool,
    dry_run: bool,
) -> Optional[dict[str, Any]]:
    if bool(position.get("order_book_alert_triggered")):
        return None

    code = normalize_code(position.get("code"))
    name = str(position.get("name") or code)
    snapshot = fetch_order_book_snapshot(code)
    signal = order_book_imbalance_signal(snapshot)
    if not signal["triggered"]:
        return None

    now = datetime.now()
    current_price = safe_float(snapshot.get("current_price"))
    buy_price = safe_float(position.get("buy_price"))
    if should_verify_fake_dump(position, current_price):
        vwap_context = vwap_verification_context(code, current_price)
        verification = verify_fake_dump(
            {
                "code": code,
                "name": name,
                "current_price": current_price,
                "vwap": vwap_context.get("vwap"),
                "trigger": "order_book_imbalance",
                "weibi": signal["weibi"],
            }
        )
        if verification.get("fake_dump"):
            return start_anti_nuclear_observation(
                position=position,
                ledger=ledger,
                path=path,
                snapshot=snapshot,
                signal=signal,
                verification=verification,
                buy_price=buy_price,
                checked_at=now,
                dry_run=dry_run,
            )

    title = f"【盘口抢跑预警】{name}({code}) 委比极限反转"
    message = (
        f"【盘口抢跑预警】{name} ({code}) 卖一至卖五突然涌现巨量抛单，"
        f"实时委比暴跌至 {signal['weibi']:.2f}%！"
        "买盘承接断裂，主力疑似大单压盘，请立即准备抢跑离场！"
    )
    content = f"""## {message}

- 标的：{name}({code})
- 买一至买五总挂单量：{signal['total_buy_vol']:.2f}
- 卖一至卖五总挂单量：{signal['total_sell_vol']:.2f}
- 实时委比：{signal['weibi']:.2f}%
- 盘口时间：{snapshot.get("quote_time") or "-"}
- 检查时间：{now.isoformat(timespec="seconds")}
"""
    push_result = send_pushplus(title, content) if send_push and not dry_run else {"status": "dry_run"}

    if not dry_run:
        position["order_book_alert_triggered"] = True
        position["order_book_alert_date"] = now.date().isoformat()
        position["order_book_alert_at"] = now.isoformat(timespec="seconds")
        position["order_book_alert_snapshot"] = {
            "total_buy_vol": round(signal["total_buy_vol"], 4),
            "total_sell_vol": round(signal["total_sell_vol"], 4),
            "weibi": round(signal["weibi"], 4),
            "buy_volumes": [round(item, 4) for item in snapshot["buy_volumes"]],
            "sell_volumes": [round(item, 4) for item in snapshot["sell_volumes"]],
            "quote_time": snapshot.get("quote_time") or "",
        }
        save_ledger(ledger, path)

    return {
        "status": "order_book_alert",
        "code": code,
        "name": name,
        "total_buy_vol": round(signal["total_buy_vol"], 4),
        "total_sell_vol": round(signal["total_sell_vol"], 4),
        "weibi": round(signal["weibi"], 4),
        "pushplus": push_result,
        "dry_run": dry_run,
    }


def should_verify_fake_dump(position: dict[str, Any], current_price: float) -> bool:
    buy_price = safe_float(position.get("buy_price"))
    if buy_price > 0 and current_price <= buy_price * 0.97:
        return False
    return current_price > 0


def vwap_verification_context(code: str, current_price: float) -> dict[str, Any]:
    try:
        df = fetch_intraday_1m_df(code)
        vwap = calculate_realtime_vwap(df)
    except Exception as exc:
        return {"vwap": 0.0, "error": str(exc)}
    return {
        "vwap": vwap,
        "current_price": current_price,
        "row_count": int(len(df)),
    }


def calculate_realtime_vwap(df: pd.DataFrame) -> float:
    if df.empty or "volume" not in df.columns:
        return 0.0
    frame = df.copy()
    price_col = "close" if "close" in frame.columns else "price" if "price" in frame.columns else ""
    if not price_col:
        return 0.0
    prices = pd.to_numeric(frame[price_col], errors="coerce").fillna(0.0)
    volumes = pd.to_numeric(frame["volume"], errors="coerce").fillna(0.0)
    valid = (prices > 0) & (volumes > 0)
    total_volume = float(volumes[valid].sum())
    if total_volume <= 0:
        return 0.0
    return float((prices[valid] * volumes[valid]).sum() / total_volume)


def verify_fake_dump(stock_data: dict[str, Any]) -> dict[str, Any]:
    current_price = safe_float(stock_data.get("current_price") or stock_data.get("price"))
    vwap = safe_float(stock_data.get("vwap"))
    threshold_price = vwap * VWAP_FAKE_DUMP_HOLD_RATIO if vwap > 0 else 0.0
    fake_dump = current_price > 0 and threshold_price > 0 and current_price >= threshold_price
    return {
        "fake_dump": bool(fake_dump),
        "status": "FAKE_DUMP" if fake_dump else "REAL_DUMP",
        "reason": "price_holds_vwap" if fake_dump else "vwap_breakdown_or_missing",
        "current_price": round(current_price, 4),
        "vwap": round(vwap, 4),
        "vwap_threshold": round(threshold_price, 4),
        "vwap_deviation_pct": round(gain_pct(current_price, vwap), 4) if vwap > 0 else 0.0,
        "trigger": stock_data.get("trigger") or "",
        "weibi": round(safe_float(stock_data.get("weibi")), 4),
    }


def start_anti_nuclear_observation(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    snapshot: dict[str, Any],
    signal: dict[str, Any],
    verification: dict[str, Any],
    buy_price: float,
    checked_at: datetime,
    dry_run: bool,
) -> dict[str, Any]:
    code = normalize_code(position.get("code"))
    current_price = safe_float(verification.get("current_price")) or safe_float(snapshot.get("current_price"))
    current_gain = gain_pct(current_price, buy_price)
    deadline = checked_at + timedelta(seconds=ANTI_NUCLEAR_CONFIRM_SECONDS)
    observation = {
        "status": "FAKE_DUMP",
        "started_at": checked_at.isoformat(timespec="seconds"),
        "deadline_at": deadline.isoformat(timespec="seconds"),
        "initial_price": round(current_price, 4),
        "initial_gain_pct": round(current_gain, 4),
        "vwap": verification.get("vwap"),
        "vwap_threshold": verification.get("vwap_threshold"),
        "vwap_deviation_pct": verification.get("vwap_deviation_pct"),
        "weibi": round(signal["weibi"], 4),
        "total_buy_vol": round(signal["total_buy_vol"], 4),
        "total_sell_vol": round(signal["total_sell_vol"], 4),
        "quote_time": snapshot.get("quote_time") or "",
        "push_sent": False,
    }

    if not dry_run:
        position["anti_nuclear_status"] = "FAKE_DUMP"
        position["anti_nuclear_date"] = checked_at.date().isoformat()
        position["anti_nuclear_observation"] = observation
        position["order_book_alert_triggered"] = True
        position["order_book_alert_date"] = checked_at.date().isoformat()
        position["order_book_alert_at"] = checked_at.isoformat(timespec="seconds")
        position["order_book_alert_type"] = "FAKE_DUMP_INTERCEPTED"
        position["order_book_alert_snapshot"] = {
            "total_buy_vol": round(signal["total_buy_vol"], 4),
            "total_sell_vol": round(signal["total_sell_vol"], 4),
            "weibi": round(signal["weibi"], 4),
            "buy_volumes": [round(item, 4) for item in snapshot["buy_volumes"]],
            "sell_volumes": [round(item, 4) for item in snapshot["sell_volumes"]],
            "quote_time": snapshot.get("quote_time") or "",
            "vwap_verification": verification,
        }
        save_ledger(ledger, path)

    return {
        "status": "fake_dump_intercepted",
        "code": code,
        "name": position.get("name") or code,
        "current_price": round(current_price, 4),
        "current_gain_pct": round(current_gain, 4),
        "vwap": verification.get("vwap"),
        "vwap_threshold": verification.get("vwap_threshold"),
        "weibi": round(signal["weibi"], 4),
        "deadline_at": observation["deadline_at"],
        "dry_run": dry_run,
    }


def check_anti_nuclear_pool(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    current_price: float,
    buy_price: float,
    send_push: bool,
    dry_run: bool,
) -> Optional[dict[str, Any]]:
    if str(position.get("anti_nuclear_status") or "") != "FAKE_DUMP":
        return None
    observation = position.get("anti_nuclear_observation")
    if not isinstance(observation, dict):
        return None
    if bool(observation.get("push_sent")):
        return None

    now = datetime.now()
    deadline = parse_datetime(observation.get("deadline_at"))
    if deadline and now < deadline:
        return {
            "status": "anti_nuclear_monitoring",
            "code": normalize_code(position.get("code")),
            "deadline_at": observation.get("deadline_at"),
            "current_price": round(current_price, 4),
            "current_gain_pct": round(gain_pct(current_price, buy_price), 4),
        }

    current_gain = gain_pct(current_price, buy_price)
    if current_gain <= -3.0:
        return finish_anti_nuclear_observation(
            position=position,
            ledger=ledger,
            path=path,
            status="FAILED_HARD_STOP_ZONE",
            current_price=current_price,
            current_gain_pct=current_gain,
            push_result={"status": "skipped_hard_stop_zone"},
            dry_run=dry_run,
        )
    if current_gain < ANTI_NUCLEAR_MAX_LOSS_PCT:
        return finish_anti_nuclear_observation(
            position=position,
            ledger=ledger,
            path=path,
            status="FAILED_PRICE_WEAK",
            current_price=current_price,
            current_gain_pct=current_gain,
            push_result={"status": "skipped_price_weak"},
            dry_run=dry_run,
        )

    title, content = build_anti_nuclear_push(position, observation, current_price, current_gain, now)
    push_result = send_pushplus(title, content) if send_push and not dry_run else {"status": "dry_run"}
    return finish_anti_nuclear_observation(
        position=position,
        ledger=ledger,
        path=path,
        status="CONFIRMED_FAKE_DUMP",
        current_price=current_price,
        current_gain_pct=current_gain,
        push_result=push_result,
        dry_run=dry_run,
    )


def finish_anti_nuclear_observation(
    position: dict[str, Any],
    ledger: dict[str, Any],
    path: Path,
    status: str,
    current_price: float,
    current_gain_pct: float,
    push_result: dict[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    now = datetime.now().isoformat(timespec="seconds")
    if not dry_run:
        observation = position.get("anti_nuclear_observation")
        if isinstance(observation, dict):
            observation["confirmed_at"] = now
            observation["confirmed_price"] = round(current_price, 4)
            observation["confirmed_gain_pct"] = round(current_gain_pct, 4)
            observation["final_status"] = status
            observation["push_sent"] = status == "CONFIRMED_FAKE_DUMP"
            observation["push_status"] = push_result
        position["anti_nuclear_status"] = status
        save_ledger(ledger, path)

    return {
        "status": status.lower(),
        "code": normalize_code(position.get("code")),
        "name": position.get("name") or normalize_code(position.get("code")),
        "current_price": round(current_price, 4),
        "current_gain_pct": round(current_gain_pct, 4),
        "pushplus": push_result,
        "dry_run": dry_run,
    }


def build_anti_nuclear_push(
    position: dict[str, Any],
    observation: dict[str, Any],
    current_price: float,
    current_gain_pct: float,
    checked_at: datetime,
) -> tuple[str, str]:
    code = normalize_code(position.get("code"))
    name = str(position.get("name") or code)
    title = f"【反推捕捉：极强承接！】{name}({code})"
    content = f"""## 【反推捕捉：极强承接！】{name} ({code}) 刚刚遭遇盘口核按钮，但股价死守 VWAP！

识别为[虚假出货/主力洗盘]。系统已拦截卖出指令，该股洗盘后存在极大反包概率，建议死拿！

- 标的：{name}({code})
- 命中策略：{position.get("strategy_type") or "-"}
- 盘口委比：{safe_float(observation.get("weibi")):.2f}%
- 触发价：{safe_float(observation.get("initial_price")):.2f}
- 当前价：{current_price:.2f}
- 当前盈亏：{current_gain_pct:.2f}%
- 实时 VWAP：{safe_float(observation.get("vwap")):.2f}
- VWAP 防线：{safe_float(observation.get("vwap_threshold")):.2f}
- VWAP 偏离：{safe_float(observation.get("vwap_deviation_pct")):.2f}%
- 观察开始：{observation.get("started_at") or "-"}
- 确认时间：{checked_at.isoformat(timespec="seconds")}
"""
    return title, content


def close_position(
    ledger: dict[str, Any],
    position: dict[str, Any],
    path: Path,
    reason: str,
    title: str,
    message: str,
    current_price: float,
    highest_price: float,
    buy_price: float,
    current_gain_pct: float,
    highest_gain_pct: float,
    drawdown_pct: float,
    quote: dict[str, Any],
    send_push: bool,
    dry_run: bool,
) -> dict[str, Any]:
    code = normalize_code(position.get("code"))
    now = datetime.now().isoformat(timespec="seconds")
    settlement = close_settlement_context(
        quote=quote,
        checked_at=now,
        trigger_price=current_price,
        buy_price=buy_price,
    )
    settlement_price = safe_float(settlement.get("settlement_price")) or current_price
    settlement_gain_pct = safe_float(settlement.get("settlement_gain_pct"))
    settlement_basis = str(settlement.get("settlement_basis") or "realtime")
    display_message = message
    if settlement_basis == "morning_auction_open":
        display_message = (
            f"{message}\n\n早盘竞价触发，影子账本按当日开盘价 "
            f"{settlement_price:.2f} 结算，结算盈亏 {settlement_gain_pct:.2f}%。"
        )
    content = build_push_content(
        position=position,
        reason=reason,
        message=display_message,
        current_price=current_price,
        settlement_price=settlement_price,
        settlement_gain_pct=settlement_gain_pct,
        settlement_basis=settlement_basis,
        highest_price=highest_price,
        buy_price=buy_price,
        current_gain_pct=current_gain_pct,
        highest_gain_pct=highest_gain_pct,
        drawdown_pct=drawdown_pct,
        quote=quote,
        checked_at=now,
    )
    push_result = send_pushplus(title, content) if send_push and not dry_run else {"status": "dry_run"}
    daily_pick_sync = sync_daily_pick_close(
        position=position,
        reason=reason,
        title=title,
        message=display_message,
        current_price=current_price,
        settlement_price=settlement_price,
        settlement_gain_pct=settlement_gain_pct,
        settlement_basis=settlement_basis,
        highest_price=highest_price,
        buy_price=buy_price,
        current_gain_pct=current_gain_pct,
        highest_gain_pct=highest_gain_pct,
        drawdown_pct=drawdown_pct,
        quote=quote,
        checked_at=now,
        dry_run=dry_run,
    )

    closed = dict(position)
    closed.update(
        {
            "exit_reason": reason,
            "exit_price": round(settlement_price, 4),
            "exit_gain_pct": round(settlement_gain_pct, 4),
            "trigger_price": round(current_price, 4),
            "trigger_gain_pct": round(current_gain_pct, 4),
            "settlement_basis": settlement_basis,
            "highest_price": round(highest_price, 4),
            "highest_gain_pct": round(highest_gain_pct, 4),
            "drawdown_pct": round(drawdown_pct, 4),
            "closed_at": now,
            "push_status": push_result,
            "daily_pick_sync": daily_pick_sync,
        }
    )

    if not dry_run:
        positions = open_positions(ledger)
        positions[:] = [item for item in positions if normalize_code(item.get("code")) != code]
        closed_positions = ledger.setdefault("closed_positions", [])
        if isinstance(closed_positions, list):
            closed_positions.append(closed)
        save_ledger(ledger, path)

    return {
        "status": "closed",
        "code": code,
        "name": position.get("name") or code,
        "reason": reason,
        "current_price": round(current_price, 4),
        "settlement_price": round(settlement_price, 4),
        "settlement_gain_pct": round(settlement_gain_pct, 4),
        "settlement_basis": settlement_basis,
        "buy_price": round(buy_price, 4),
        "highest_price": round(highest_price, 4),
        "current_gain_pct": round(current_gain_pct, 4),
        "highest_gain_pct": round(highest_gain_pct, 4),
        "drawdown_pct": round(drawdown_pct, 4),
        "pushplus": push_result,
        "daily_pick_sync": daily_pick_sync,
        "dry_run": dry_run,
    }


def sync_daily_pick_close(
    position: dict[str, Any],
    reason: str,
    title: str,
    message: str,
    current_price: float,
    settlement_price: float,
    settlement_gain_pct: float,
    settlement_basis: str,
    highest_price: float,
    buy_price: float,
    current_gain_pct: float,
    highest_gain_pct: float,
    drawdown_pct: float,
    quote: dict[str, Any],
    checked_at: str,
    dry_run: bool,
) -> dict[str, Any]:
    selection_date = str(position.get("source_selection_date") or position.get("buy_date") or "")[:10]
    code = normalize_code(position.get("code"))
    pick_id = safe_int(position.get("source_pick_id"))
    strategy_type = str(position.get("strategy_type") or "") or None
    if not selection_date:
        return {"status": "skipped", "reason": "missing_selection_date"}

    close_signal = {
        "source": "live_sentinel",
        "action": reason,
        "title": title,
        "instruction": message,
        "current_price": round(current_price, 4),
        "settlement_price": round(settlement_price, 4),
        "settlement_gain_pct": round(settlement_gain_pct, 4),
        "settlement_basis": settlement_basis,
        "buy_price": round(buy_price, 4),
        "highest_price": round(highest_price, 4),
        "current_gain_pct": round(current_gain_pct, 4),
        "highest_gain_pct": round(highest_gain_pct, 4),
        "drawdown_pct": round(drawdown_pct, 4),
        "quote_time": f"{quote.get('date') or ''} {quote.get('time') or ''}".strip(),
        "checked_at": checked_at,
    }

    if dry_run:
        return {
            "status": "dry_run",
            "selection_date": selection_date,
            "code": code,
            "close_price": round(settlement_price, 4),
            "close_return_pct": round(settlement_gain_pct, 4),
        }

    try:
        updated = mark_daily_pick_closed(
            selection_date,
            settlement_price,
            settlement_gain_pct,
            reason,
            checked_at=checked_at,
            close_signal=close_signal,
            strategy_type=strategy_type,
            code=code,
            pick_id=pick_id,
        )
    except Exception as exc:
        return {"status": "failed", "selection_date": selection_date, "code": code, "error": str(exc)}

    if not updated:
        return {"status": "not_found", "selection_date": selection_date, "code": code, "pick_id": pick_id}
    return {
        "status": "synced",
        "selection_date": selection_date,
        "code": code,
        "pick_id": updated.get("id"),
        "close_price": updated.get("close_price"),
        "close_return_pct": updated.get("close_return_pct"),
        "close_reason": updated.get("close_reason"),
    }


def build_push_content(
    position: dict[str, Any],
    reason: str,
    message: str,
    current_price: float,
    highest_price: float,
    buy_price: float,
    current_gain_pct: float,
    highest_gain_pct: float,
    drawdown_pct: float,
    quote: dict[str, Any],
    checked_at: str,
    settlement_price: Optional[float] = None,
    settlement_gain_pct: Optional[float] = None,
    settlement_basis: str = "realtime",
) -> str:
    code = normalize_code(position.get("code"))
    name = str(position.get("name") or code)
    strategy_type = str(position.get("strategy_type") or "").strip()
    target_label = "T+3 日期" if supports_t_plus_3_timeout(position) else "目标日期"
    settlement_lines = ""
    close_price = safe_float(settlement_price)
    close_gain = safe_float(settlement_gain_pct)
    if close_price > 0 and settlement_basis != "realtime":
        settlement_label = "结算价（开盘价）" if settlement_basis == "morning_auction_open" else "结算价"
        settlement_lines = f"- {settlement_label}：{close_price:.2f}\n- 结算盈亏：{close_gain:.2f}%\n"
    return f"""## {message}

- 标的：{name}({code})
- 命中策略：{strategy_type or "-"}
- 触发规则：{reason}
- 买入价：{buy_price:.2f}
- 盘中最高价：{highest_price:.2f}
- 触发价：{current_price:.2f}
{settlement_lines}
- 最高盈利：{highest_gain_pct:.2f}%
- 当前盈利：{current_gain_pct:.2f}%
- 较最高点回撤：{abs(drawdown_pct):.2f}%
- 买入日期：{position.get("buy_date") or "-"}
- {target_label}：{position.get("target_date") or "-"}
- 行情时间：{quote.get("date") or "-"} {quote.get("time") or "-"}
- 检查时间：{checked_at}
"""


def realtime_price(quote: dict[str, Any]) -> float:
    for key in ("current_price", "price", "auction_price", "open"):
        value = safe_float(quote.get(key))
        if value > 0:
            return value
    return 0.0


def close_settlement_context(
    quote: dict[str, Any],
    checked_at: str,
    trigger_price: float,
    buy_price: float,
) -> dict[str, Any]:
    settlement_price = safe_float(trigger_price)
    settlement_basis = "realtime"
    if is_morning_auction_quote(quote, checked_at):
        open_price = quote_open_price(quote)
        if open_price > 0:
            settlement_price = open_price
            settlement_basis = "morning_auction_open"
        else:
            settlement_basis = "morning_auction_open_unavailable"

    return {
        "settlement_price": settlement_price,
        "settlement_gain_pct": gain_pct(settlement_price, buy_price),
        "settlement_basis": settlement_basis,
    }


def quote_open_price(quote: dict[str, Any]) -> float:
    for key in ("open", "open_price"):
        value = safe_float(quote.get(key))
        if value > 0:
            return value
    return 0.0


def is_morning_auction_quote(quote: dict[str, Any], checked_at: str) -> bool:
    quote_time = parse_quote_clock_time(quote) or parse_iso_clock_time(checked_at)
    if not quote_time:
        return False
    return quote_time < CONTINUOUS_TRADING_START


def parse_quote_clock_time(quote: dict[str, Any]) -> Optional[clock_time]:
    value = str(quote.get("time") or "").strip()
    if not value:
        return None
    try:
        return clock_time.fromisoformat(value[:8])
    except ValueError:
        return None


def parse_iso_clock_time(value: str) -> Optional[clock_time]:
    try:
        return datetime.fromisoformat(str(value)).time()
    except ValueError:
        return None


def fetch_intraday_1m_df(code: str, count: int = INTRADAY_1M_COUNT) -> pd.DataFrame:
    symbol = tencent_symbol(code)
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        TENCENT_MKLINE_URL,
        params={"param": f"{symbol},m1,,{max(3, int(count))}"},
        timeout=8,
        proxies={},
    )
    response.raise_for_status()
    payload = response.json()
    rows = (((payload.get("data") or {}).get(symbol) or {}).get("m1") or [])
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < 6:
            continue
        normalized.append(
            {
                "datetime": pd.to_datetime(str(row[0]), format="%Y%m%d%H%M", errors="coerce"),
                "open": safe_float(row[1]),
                "close": safe_float(row[2]),
                "high": safe_float(row[3]),
                "low": safe_float(row[4]),
                "volume": safe_float(row[5]),
            }
        )
    df = pd.DataFrame(normalized)
    if df.empty:
        return pd.DataFrame(columns=["datetime", "open", "close", "high", "low", "volume"])
    df = df.dropna(subset=["datetime"])
    for col in ["open", "close", "high", "low", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    today = datetime.now().date()
    df = df[df["datetime"].dt.date == today]
    return df.sort_values("datetime").reset_index(drop=True)


def fetch_order_book_snapshot(code: str) -> dict[str, Any]:
    clean_code = normalize_code(code)
    symbol = tencent_symbol(clean_code)
    session = requests.Session()
    session.trust_env = False
    response = session.get(TENCENT_REALTIME_URL.format(symbol=symbol), timeout=5, proxies={})
    response.raise_for_status()
    text = response.content.decode("gbk", errors="ignore")
    start = text.find('"')
    end = text.rfind('"')
    if start < 0 or end <= start:
        raise RuntimeError(f"{clean_code} 五档盘口响应格式异常")

    fields = text[start + 1 : end].split("~")
    if len(fields) < 29:
        raise RuntimeError(f"{clean_code} 五档盘口字段不足：{len(fields)}")

    buy_levels = parse_order_book_levels(fields, [(9, 10), (11, 12), (13, 14), (15, 16), (17, 18)])
    sell_levels = parse_order_book_levels(fields, [(19, 20), (21, 22), (23, 24), (25, 26), (27, 28)])
    return {
        "code": clean_code,
        "symbol": symbol,
        "name": fields[1] if len(fields) > 1 else clean_code,
        "current_price": safe_float(fields[3] if len(fields) > 3 else 0),
        "quote_time": fields[30] if len(fields) > 30 else "",
        "buy_levels": buy_levels,
        "sell_levels": sell_levels,
        "buy_volumes": [level["volume"] for level in buy_levels],
        "sell_volumes": [level["volume"] for level in sell_levels],
    }


def parse_order_book_levels(fields: list[str], pairs: list[tuple[int, int]]) -> list[dict[str, float]]:
    levels: list[dict[str, float]] = []
    for price_index, volume_index in pairs:
        price = safe_float(fields[price_index] if len(fields) > price_index else 0)
        volume = safe_float(fields[volume_index] if len(fields) > volume_index else 0)
        levels.append({"price": price, "volume": volume})
    return levels


def volume_divergence_signal(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty or len(df) < 3 or "volume" not in df.columns:
        return {"triggered": False, "reason": "insufficient_1m_rows"}

    avg_vol = float(pd.to_numeric(df["volume"], errors="coerce").fillna(0.0).mean())
    recent = df.tail(3)
    recent_3m_vol = float(pd.to_numeric(recent["volume"], errors="coerce").fillna(0.0).sum())
    first_open = safe_float(recent.iloc[0].get("open"))
    last_close = safe_float(recent.iloc[-1].get("close"))
    price_change = last_close - first_open
    threshold = (avg_vol * VOLUME_DIVERGENCE_MULTIPLIER) * 3
    recent_avg_multiple = recent_3m_vol / (avg_vol * 3) if avg_vol > 0 else 0.0
    triggered = avg_vol > 0 and recent_3m_vol > threshold and price_change <= 0
    return {
        "triggered": bool(triggered),
        "avg_vol": avg_vol,
        "recent_3m_vol": recent_3m_vol,
        "recent_avg_multiple": recent_avg_multiple,
        "price_change": price_change,
        "threshold": threshold,
    }


def order_book_imbalance_signal(snapshot: dict[str, Any]) -> dict[str, Any]:
    buy_volumes = [safe_float(value) for value in snapshot.get("buy_volumes", [])]
    sell_volumes = [safe_float(value) for value in snapshot.get("sell_volumes", [])]
    total_buy_vol = sum(buy_volumes)
    total_sell_vol = sum(sell_volumes)
    denominator = total_buy_vol + total_sell_vol
    if denominator <= 0:
        return {
            "triggered": False,
            "reason": "empty_order_book",
            "total_buy_vol": total_buy_vol,
            "total_sell_vol": total_sell_vol,
            "weibi": 0.0,
        }

    weibi = ((total_buy_vol - total_sell_vol) / denominator) * 100.0
    return {
        "triggered": weibi <= ORDER_BOOK_IMBALANCE_THRESHOLD,
        "total_buy_vol": total_buy_vol,
        "total_sell_vol": total_sell_vol,
        "weibi": weibi,
        "threshold": ORDER_BOOK_IMBALANCE_THRESHOLD,
    }


def seconds_until_scan_window(now: Optional[datetime] = None) -> Optional[int]:
    current = now or datetime.now()
    current_time = current.time()
    if current_time >= MARKET_CLOSE:
        return None
    if current_time < MARKET_OPEN:
        return max(1, int((datetime.combine(current.date(), MARKET_OPEN) - current).total_seconds()))
    if LUNCH_START <= current_time < LUNCH_END:
        return max(1, int((datetime.combine(current.date(), LUNCH_END) - current).total_seconds()))
    return 0


def scan_pause_reason(now: Optional[datetime] = None) -> str:
    current_time = (now or datetime.now()).time()
    if current_time < MARKET_OPEN:
        return "waiting_for_0915"
    if LUNCH_START <= current_time < LUNCH_END:
        return "lunch_break"
    if current_time >= MARKET_CLOSE:
        return "market_closed"
    return "active"


def is_t_plus_3_timeout(position: dict[str, Any], today: Optional[date] = None) -> bool:
    if not supports_t_plus_3_timeout(position):
        return False

    current_day = today or date.today()
    target_date = parse_date(position.get("target_date"))
    if target_date:
        return current_day >= target_date

    buy_date = parse_date(position.get("buy_date"))
    if not buy_date:
        return False
    return weekday_count_after(buy_date, current_day) >= 3


def supports_t_plus_3_timeout(position: dict[str, Any]) -> bool:
    strategy_type = str(position.get("strategy_type") or "").strip()
    if strategy_type:
        if strategy_type in NON_SWING_STRATEGY_TYPES:
            return False
        return strategy_type in SWING_STRATEGY_TYPES
    # Legacy manually added positions had no strategy_type; their target_date was T+3.
    return str(position.get("source") or "") != "daily_picks_1450"


def t_plus_3_date(buy_date: str) -> str:
    start = parse_date(buy_date) or date.today()
    return nth_trading_day(start, 3).isoformat()


def weekday_count_after(start: date, end: date) -> int:
    return trading_day_count_after(start, end)


def parse_date(value: Any) -> Optional[date]:
    if not value:
        return None
    text = str(value)[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def normalize_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) < 6:
        raise ValueError(f"非法股票代码：{value}")
    return digits[-6:]


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def gain_pct(price: float, base: float) -> float:
    if base <= 0:
        return 0.0
    return (price / base - 1.0) * 100.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="盘中持仓巡逻兵：硬止损 + 动态追踪止盈 + T+3 超时")
    parser.add_argument("--ledger", default=str(LEDGER_PATH), help="shadow_ledger.json 路径")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS, help="循环间隔秒数")
    parser.add_argument("--once", action="store_true", help="只检查一轮")
    parser.add_argument("--no-push", action="store_true", help="不发送 PushPlus")
    parser.add_argument("--dry-run", action="store_true", help="干跑，不写回账本、不发送 PushPlus")
    parser.add_argument("--from-yesterday-picks", action="store_true", help="启动时用上一交易日 14:50 推送标的重建监控账本")
    parser.add_argument("--seed-only", action="store_true", help="只从上一交易日 14:50 推送标的重建账本，不进入巡逻")

    subparsers = parser.add_subparsers(dest="command")
    add = subparsers.add_parser("add", help="登记一笔初始买入，并将 highest_price 初始化为 buy_price")
    add.add_argument("--code", required=True)
    add.add_argument("--name", default="")
    add.add_argument("--buy-price", required=True, type=float)
    add.add_argument("--buy-date", default=None)
    add.add_argument("--target-date", default=None)
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    ledger_path = Path(args.ledger).expanduser()

    if args.command == "add":
        position = add_position(
            code=args.code,
            name=args.name,
            buy_price=args.buy_price,
            buy_date=args.buy_date,
            target_date=args.target_date,
            path=ledger_path,
        )
        print(json.dumps({"status": "added", "position": position}, ensure_ascii=False, indent=2))
        return

    if args.seed_only:
        result = seed_from_latest_1450_picks(ledger_path)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    result = run_loop(
        path=ledger_path,
        interval_seconds=args.interval,
        once=args.once,
        send_push=not args.no_push,
        dry_run=args.dry_run,
        seed_latest_picks=args.from_yesterday_picks,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
