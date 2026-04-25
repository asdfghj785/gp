from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from typing import Any

import requests

from .config import PUSHPLUS_TOKEN
from .market import fetch_sina_quote
from .storage import connect, init_db, update_daily_pick_open


PUSHPLUS_URL = "http://www.pushplus.plus/send"


def run_exit_sentinel(today: str | None = None, send_push: bool = True) -> dict[str, Any]:
    """Judge yesterday's locked Top1 at the 09:25 auction price and close the daily pick."""
    target_day = today or date.today().isoformat()
    pick = _latest_pending_pick(target_day)
    if pick is None:
        result = {"status": "noop", "reason": "没有需要 09:26 审判的待开盘标的", "target_date": target_day}
        print(result)
        return result

    quote = fetch_sina_quote(pick["code"])
    open_price = float(quote.get("auction_price") or quote.get("open") or 0)
    if open_price <= 0:
        raise RuntimeError(f"新浪行情没有返回 {pick['code']} 的有效集合竞价价格")

    selection_price = float(pick["selection_price"] or 0)
    if selection_price <= 0:
        raise RuntimeError(f"daily_picks 中 {pick['code']} 缺少有效昨日锁定价")

    open_premium = (open_price / selection_price - 1) * 100
    instruction = build_exit_instruction(pick, quote, open_premium)
    pushed_at = datetime.now().isoformat(timespec="seconds")
    push_status: dict[str, Any]
    if send_push:
        push_status = _send_pushplus(instruction["title"], instruction["content"])
    else:
        push_status = {"status": "dry_run"}

    exit_signal = {
        "action": instruction["action"],
        "level": instruction["level"],
        "instruction": instruction["instruction"],
        "title": instruction["title"],
        "content": instruction["content"],
        "open_price": round(open_price, 4),
        "open_premium": round(open_premium, 4),
        "quote_time": f"{quote.get('date') or ''} {quote.get('time') or ''}".strip(),
        "pushed_at": pushed_at,
        "push_status": push_status.get("status") or push_status.get("code") or "sent",
    }
    updated = update_daily_pick_open(pick["selection_date"], open_price, pushed_at, exit_signal=exit_signal)
    result = {
        "status": "updated",
        "target_date": target_day,
        "selection_date": pick["selection_date"],
        "code": pick["code"],
        "name": pick["name"],
        "open_price": round(open_price, 4),
        "open_premium": round(open_premium, 4),
        "action": instruction["action"],
        "pushplus": push_status,
        "pick": updated,
    }
    print(result)
    return result


def build_exit_instruction(pick: dict[str, Any], quote: dict[str, Any], open_premium: float) -> dict[str, str]:
    code = str(pick["code"])
    name = str(pick["name"])
    price_text = f"{float(quote.get('auction_price') or quote.get('open') or 0):.2f}"
    premium_text = f"{open_premium:.2f}%"
    expected = _safe_float((pick.get("raw") or {}).get("winner", {}).get("expected_premium"))
    score = _safe_float((pick.get("raw") or {}).get("winner", {}).get("composite_score"))

    if open_premium < 0:
        action = "核按钮"
        level = "danger"
        title = f"🚨【核按钮警告】{name} 低开 {premium_text}"
        instruction = "逻辑证伪，请立刻按跌停价挂单卖出，斩断亏损！"
    elif open_premium < 3.0:
        action = "落袋为安"
        level = "profit"
        title = f"💰【落袋为安】{name} 高开 {premium_text}"
        instruction = "符合套利预期，请立刻按开盘价挂单止盈！"
    else:
        action = "超预期锁仓"
        level = "strong"
        title = f"🚀【超预期锁仓】{name} 强势高开 {premium_text}"
        instruction = "主力极度看好，请勿早盘秒卖，等待盘中冲高或封板！"

    content = f"""**{title}**

昨日标的: {name}({code})
命中策略: {pick.get('strategy_type') or '尾盘突破'}
昨日14:50锁定价: {float(pick['selection_price']):.2f}
今日09:25集合竞价价: {price_text}
实际开盘溢价: {premium_text}
昨日预期溢价: {expected:.2f}%
综合评分: {score:.2f}
行情时间: {quote.get('date') or '-'} {quote.get('time') or '-'}

操作指令: {instruction}
"""
    return {
        "action": action,
        "level": level,
        "title": title,
        "instruction": instruction,
        "content": content,
    }


def _latest_pending_pick(target_day: str) -> dict[str, Any] | None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT *
            FROM daily_picks
            WHERE status = 'pending_open' AND target_date <= ?
            ORDER BY target_date DESC, selection_date DESC
            LIMIT 1
            """,
            (target_day,),
        ).fetchone()
    if not row:
        return None
    item = dict(row)
    item["raw"] = json.loads(item.pop("raw_json"))
    return item


def _send_pushplus(title: str, content: str) -> dict[str, Any]:
    if not PUSHPLUS_TOKEN:
        return {"status": "skipped_missing_token"}
    response = requests.post(
        PUSHPLUS_URL,
        json={"token": PUSHPLUS_TOKEN, "title": title, "content": content, "template": "markdown"},
        timeout=12,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != 200:
        raise RuntimeError(f"PushPlus 返回失败: {payload}")
    payload["status"] = "sent"
    return payload


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="09:26 集合竞价审判哨兵")
    parser.add_argument("--date", help="目标开盘日期，默认今天")
    parser.add_argument("--no-push", action="store_true", help="只回填数据库，不发送 PushPlus")
    args = parser.parse_args()
    run_exit_sentinel(today=args.date, send_push=not args.no_push)


if __name__ == "__main__":
    main()
