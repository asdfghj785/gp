from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from quant_core.data_pipeline.market import fetch_realtime_quote
from quant_core.data_pipeline.trading_calendar import is_trading_day
from quant_core.storage import connect, init_db
from quant_core.execution.pushplus_tasks import send_pushplus


BREAKOUT_STRATEGY = "尾盘突破"
SWING_STRATEGY_TYPES = {"中线超跌反转", "右侧主升浪", "全局动量狙击"}
SWING_BREAKDOWN_THRESHOLD_PCT = -4.0
AUCTION_WARNING_LOW_PCT = -5.0
AUCTION_WARNING_HIGH_PCT = 5.0
STAGE_LABELS = {
    "preopen": "09:16 竞价预热观察",
    "audit": "09:21 撤单关闭策略审计",
    "final": "09:25 终极哨兵",
}


def run_exit_sentinel(
    today: str | None = None,
    send_push: bool = True,
    dry_run: bool = False,
    include_today: bool = False,
    use_close_as_open: bool = False,
    persist: bool | None = None,
    stage: str = "final",
) -> dict[str, Any]:
    """Run the multi-stage auction sentinel for all unfinished daily_picks positions."""
    stage = _normalize_stage(stage)
    target_day = today or date.today().isoformat()
    checked_at = datetime.now().isoformat(timespec="seconds")
    if not is_trading_day(date.fromisoformat(target_day[:10])):
        result = {"status": "skipped", "reason": "非交易日不执行开盘哨兵", "stage": stage, "target_date": target_day}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return result

    should_persist = (not dry_run) if persist is None else bool(persist)
    if stage != "final":
        should_persist = False
    simulation_mode = include_today or use_close_as_open
    picks = _load_unclosed_picks(target_day, include_today=include_today)

    if not picks:
        stage_label = STAGE_LABELS[stage]
        content = f"## {stage_label}\n\n{target_day} 没有未完结持仓需要审判。"
        push_status = _send_pushplus(f"{stage_label}：无持仓", content) if send_push else {"status": "dry_run"}
        result = {"status": "noop", "stage": stage, "target_date": target_day, "count": 0, "pushplus": push_status}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return result

    actions: list[dict[str, Any]] = []
    silent: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for pick in picks:
        try:
            if stage == "final":
                result = _judge_one_pick(
                    pick,
                    target_day,
                    checked_at,
                    dry_run=(dry_run or not should_persist),
                    use_close_as_open=use_close_as_open,
                )
            else:
                result = _judge_auction_audit_pick(pick, target_day, stage=stage)
        except Exception as exc:  # Keep one bad quote from blocking the whole morning run.
            result = {
                "id": pick.get("id"),
                "selection_date": pick.get("selection_date"),
                "strategy_type": pick.get("strategy_type"),
                "code": pick.get("code"),
                "name": pick.get("name"),
                "status": "error",
                "error": str(exc),
            }
            errors.append(result)
            continue

        if result["status"] == "silent":
            silent.append(result)
        else:
            actions.append(result)

    title, content = _build_push_message(
        target_day,
        actions,
        silent,
        errors,
        simulation_mode=simulation_mode,
        stage=stage,
    )
    should_send = send_push and (stage == "final" or bool(actions) or bool(errors))
    push_status = _send_pushplus(title, content) if should_send else {"status": "skipped_no_action"}
    payload = {
        "status": "updated",
        "stage": stage,
        "target_date": target_day,
        "count": len(picks),
        "action_count": len(actions),
        "silent_count": len(silent),
        "error_count": len(errors),
        "actions": actions,
        "silent": silent,
        "errors": errors,
        "pushplus": push_status,
        "dry_run": dry_run,
        "simulation_mode": simulation_mode,
        "persisted": should_persist,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return payload


def _load_unclosed_picks(target_day: str, include_today: bool = False) -> list[dict[str, Any]]:
    init_db()
    date_operator = "<=" if include_today else "<"
    with connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM daily_picks
            WHERE COALESCE(is_closed, 0) = 0
              AND COALESCE(is_shadow_test, 0) = 1
              AND selection_date {date_operator} ?
            ORDER BY selection_date ASC, strategy_type ASC, id ASC
            """,
            (target_day,),
        ).fetchall()
    return [_decode_pick(row) for row in rows]


def _judge_one_pick(
    pick: dict[str, Any],
    target_day: str,
    checked_at: str,
    dry_run: bool = False,
    use_close_as_open: bool = False,
) -> dict[str, Any]:
    quote = fetch_realtime_quote(str(pick["code"]), prefer_auction=not use_close_as_open)
    open_price = _close_proxy_price(quote) if use_close_as_open else _valid_open_price(quote)
    base_price = _base_snapshot_price(pick)
    open_premium = (open_price / base_price - 1) * 100
    strategy_type = str(pick.get("strategy_type") or BREAKOUT_STRATEGY)

    if strategy_type == BREAKOUT_STRATEGY:
        action = _breakout_action(open_premium)
        result = {
            "id": pick.get("id"),
            "status": "action",
            "selection_date": pick.get("selection_date"),
            "target_date": target_day,
            "strategy_type": strategy_type,
            "code": pick.get("code"),
            "name": pick.get("name"),
            "snapshot_price": round(base_price, 4),
            "open_price": round(open_price, 4),
            "open_premium": round(open_premium, 4),
            "quote_time": _quote_time(quote),
            "price_mode": "15:00收盘价模拟开盘" if use_close_as_open else "09:25开盘价",
            **action,
        }
        if not dry_run:
            _update_pick_open(pick, open_price, open_premium, checked_at, result, close_position=True)
        return result

    if strategy_type in SWING_STRATEGY_TYPES:
        if open_premium < SWING_BREAKDOWN_THRESHOLD_PCT:
            result = {
                "id": pick.get("id"),
                "status": "action",
                "selection_date": pick.get("selection_date"),
                "target_date": target_day,
                "strategy_type": strategy_type,
                "code": pick.get("code"),
                "name": pick.get("name"),
                "snapshot_price": round(base_price, 4),
                "open_price": round(open_price, 4),
                "open_premium": round(open_premium, 4),
                "quote_time": _quote_time(quote),
                "price_mode": "15:00收盘价模拟开盘" if use_close_as_open else "09:25开盘价",
                "level": "danger",
                "action": "波段破位警告",
                "title": "🚨【波段破位警告】",
                "instruction": "遭遇极端下杀，洗盘过度逻辑破位，请立刻市价止损出局！",
            }
            if not dry_run:
                _update_pick_open(pick, open_price, open_premium, checked_at, result, close_position=True)
            return result

        result = {
            "id": pick.get("id"),
            "status": "silent",
            "selection_date": pick.get("selection_date"),
            "target_date": target_day,
            "strategy_type": strategy_type,
            "code": pick.get("code"),
            "name": pick.get("name"),
            "snapshot_price": round(base_price, 4),
            "open_price": round(open_price, 4),
            "open_premium": round(open_premium, 4),
            "quote_time": _quote_time(quote),
            "price_mode": "15:00收盘价模拟开盘" if use_close_as_open else "09:25开盘价",
            "action": "静默洗盘",
            "instruction": "开盘波动在正常洗盘区间，保持静默，等待 14:45 波段巡逻兵指令。",
        }
        if not dry_run:
            _update_pick_open(pick, open_price, open_premium, checked_at, result, close_position=False)
        return result

    result = {
        "id": pick.get("id"),
        "status": "silent",
        "selection_date": pick.get("selection_date"),
        "target_date": target_day,
        "strategy_type": strategy_type,
        "code": pick.get("code"),
        "name": pick.get("name"),
        "snapshot_price": round(base_price, 4),
        "open_price": round(open_price, 4),
        "open_premium": round(open_premium, 4),
        "quote_time": _quote_time(quote),
        "price_mode": "15:00收盘价模拟开盘" if use_close_as_open else "09:25开盘价",
        "action": "未知策略静默",
        "instruction": "未知策略类型，仅回填开盘数据，不发送交易动作。",
    }
    if not dry_run:
        _update_pick_open(pick, open_price, open_premium, checked_at, result, close_position=False)
    return result


def _breakout_action(open_premium: float) -> dict[str, str]:
    if open_premium < 0:
        return {
            "level": "danger",
            "action": "核按钮",
            "title": "🔴【核按钮】",
            "instruction": "逻辑证伪，立刻按跌停价挂单卖出，斩断亏损！操作指南：请立即在券商 App 以‘跌停价’挂委卖单，利用时间优先原则在 09:30 第一秒出货！",
        }
    if open_premium < 3.0:
        return {
            "level": "profit",
            "action": "落袋为安",
            "title": "🟢【落袋为安】",
            "instruction": "符合预期，开盘止盈，将隔夜套利兑现。",
        }
    return {
        "level": "strong",
        "action": "超预期锁仓",
        "title": "🚀【超预期锁仓】",
        "instruction": "强势高开超预期，请勿早盘秒卖，等待盘中冲高或封板。",
    }


def _judge_auction_audit_pick(pick: dict[str, Any], target_day: str, stage: str) -> dict[str, Any]:
    quote = fetch_realtime_quote(str(pick["code"]), prefer_auction=True)
    match_price = _auction_match_price(quote)
    base_price = _base_snapshot_price(pick)
    virtual_premium = (match_price / base_price - 1) * 100
    strategy_type = str(pick.get("strategy_type") or BREAKOUT_STRATEGY)

    base = {
        "id": pick.get("id"),
        "selection_date": pick.get("selection_date"),
        "target_date": target_day,
        "stage": stage,
        "strategy_type": strategy_type,
        "code": pick.get("code"),
        "name": pick.get("name"),
        "snapshot_price": round(base_price, 4),
        "open_price": round(match_price, 4),
        "open_premium": round(virtual_premium, 4),
        "quote_time": _quote_time(quote),
        "price_mode": "竞价虚拟匹配价",
    }
    if virtual_premium < AUCTION_WARNING_LOW_PCT:
        return {
            **base,
            "status": "action",
            "level": "danger",
            "action": "早盘风控预警",
            "title": "⚠️【早盘风控预警】",
            "instruction": "撤单已关闭！当前虚拟价极差，建议主理人停止一切买入操作，并做好 09:25 后挂低价抢跑的心理准备。",
        }
    if virtual_premium > AUCTION_WARNING_HIGH_PCT:
        return {
            **base,
            "status": "action",
            "level": "strong",
            "action": "早盘超预期提示",
            "title": "🚀【早盘超预期提示】",
            "instruction": "主力竞价积极，关注 09:25 最终定值。",
        }
    return {
        **base,
        "status": "silent",
        "action": "竞价正常",
        "instruction": "竞价虚拟溢价未触发 ±5% 预警阈值，仅记录观察，不回填数据库。",
    }


def _update_pick_open(
    pick: dict[str, Any],
    open_price: float,
    open_premium: float,
    checked_at: str,
    signal: dict[str, Any],
    close_position: bool,
) -> None:
    raw = pick.get("raw") if isinstance(pick.get("raw"), dict) else {}
    raw["exit_sentinel"] = {
        "action": signal.get("action"),
        "level": signal.get("level"),
        "instruction": signal.get("instruction"),
        "title": signal.get("title"),
        "open_price": round(open_price, 4),
        "open_premium": round(open_premium, 4),
        "quote_time": signal.get("quote_time"),
        "checked_at": checked_at,
    }
    success = 1 if open_premium > 0 else 0
    with connect() as conn:
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
                    open_premium,
                    success,
                    checked_at[:10],
                    open_price,
                    open_premium,
                    signal.get("action") or "开盘哨兵",
                    checked_at,
                    json.dumps(raw, ensure_ascii=False),
                    int(pick["id"]),
                ),
            )
        else:
            conn.execute(
                """
                UPDATE daily_picks
                SET open_price = ?, open_checked_at = ?, open_premium = ?,
                    status = 'open_checked', raw_json = ?
                WHERE id = ?
                """,
                (
                    open_price,
                    checked_at,
                    open_premium,
                    json.dumps(raw, ensure_ascii=False),
                    int(pick["id"]),
                ),
            )


def _build_push_message(
    target_day: str,
    actions: list[dict[str, Any]],
    silent: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    simulation_mode: bool = False,
    stage: str = "final",
) -> tuple[str, str]:
    stage = _normalize_stage(stage)
    stage_label = STAGE_LABELS[stage]
    if simulation_mode:
        title = f"早盘哨兵模拟报告：{len(actions) + len(silent)} 只标的"
    elif stage == "audit" and actions:
        title = f"09:21 策略审计：触发 {len(actions)} 条预警"
    elif stage == "preopen" and actions:
        title = f"09:16 竞价预热：触发 {len(actions)} 条预警"
    elif actions:
        title = f"09:25 终极哨兵：触发 {len(actions)} 条动作"
    elif errors:
        title = f"{stage_label}：{len(errors)} 条异常"
    else:
        title = f"{stage_label}：无异常"

    heading = "早盘哨兵模拟报告" if simulation_mode else stage_label
    lines = [f"## {heading}", "", f"日期：{target_day}", ""]
    if simulation_mode:
        lines.extend(
            [
                "模式：实战模拟推送",
                "价格口径：临时使用今日 15:00 收盘价/当前价作为伪 09:25 开盘价。",
                "安全说明：本次真实发送 PushPlus，但不回填 daily_picks，避免污染明天真实结算。",
                "",
            ]
            )

    if stage != "final" and not simulation_mode:
        lines.extend(
            [
                "数据库策略：09:25 前仅推送竞价风险，不回填 open_price/open_premium。",
                "",
            ]
        )

    if actions:
        lines.append("### 触发动作")
        for item in actions:
            lines.extend(
                [
                    f"- {item['title']} **{item['name']}({item['code']})**",
                    f"  - 命中策略：{item['strategy_type']}",
                    f"  - 14:50 快照价：{item['snapshot_price']:.2f}",
                    f"  - {item.get('price_mode') or '09:25开盘价'}：{item['open_price']:.2f}",
                    f"  - 溢价：{item['open_premium']:.2f}%",
                    f"  - 操作指令：{item['instruction']}",
                    "",
                ]
            )
    else:
        lines.append("今日早盘哨兵无异常，全部波段标的正常洗盘。")
        lines.append("")

    if silent:
        lines.append("### 静默持仓")
        for item in silent:
            lines.append(
                f"- **波段持仓/静默洗盘**：{item['name']}({item['code']}) / {item['strategy_type']}："
                f"开盘溢价 {item['open_premium']:.2f}%，保持静默。"
            )
        lines.append("")

    if errors:
        lines.append("### 数据异常")
        for item in errors:
            lines.append(
                f"- {item.get('name') or '-'}({item.get('code') or '-'}) / "
                f"{item.get('strategy_type') or '-'}：{item.get('error')}"
            )
        lines.append("")

    return title, "\n".join(lines).strip()


def _send_pushplus(title: str, content: str) -> dict[str, Any]:
    try:
        result = send_pushplus(title, content)
    except Exception as exc:
        print(f"[PushPlus][ERROR] 推送异常：{exc}", file=sys.stderr)
        return {"status": "failed", "error": str(exc)}
    if result.get("status") != "sent":
        print(f"[PushPlus][ERROR] 推送失败响应：{json.dumps(result, ensure_ascii=False)}", file=sys.stderr)
    return result


def _decode_pick(row: Any) -> dict[str, Any]:
    item = dict(row)
    try:
        item["raw"] = json.loads(item.get("raw_json") or "{}")
    except json.JSONDecodeError:
        item["raw"] = {}
    item["strategy_type"] = item.get("strategy_type") or "尾盘突破"
    return item


def _valid_open_price(quote: dict[str, Any]) -> float:
    open_price = _safe_float(quote.get("open") or quote.get("auction_price") or quote.get("current_price"))
    if open_price <= 0:
        raise RuntimeError("行情接口未返回有效 09:25/开盘价")
    return open_price


def _auction_match_price(quote: dict[str, Any]) -> float:
    match_price = _safe_float(quote.get("auction_price") or quote.get("current_price") or quote.get("open"))
    if match_price <= 0:
        raise RuntimeError("行情接口未返回有效竞价虚拟匹配价")
    return match_price


def _close_proxy_price(quote: dict[str, Any]) -> float:
    close_price = _safe_float(quote.get("current_price") or quote.get("auction_price") or quote.get("open"))
    if close_price <= 0:
        raise RuntimeError("行情接口未返回有效收盘/当前价，无法模拟早盘审计")
    return close_price


def _base_snapshot_price(pick: dict[str, Any]) -> float:
    value = _safe_float(pick.get("snapshot_price") or pick.get("selection_price"))
    if value <= 0:
        raise RuntimeError("daily_picks 缺少有效 14:50 快照价")
    return value


def _quote_time(quote: dict[str, Any]) -> str:
    return f"{quote.get('date') or ''} {quote.get('time') or ''}".strip()


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _parse_bool(value: str | bool | None) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"无法解析布尔值: {value}")


def _normalize_stage(stage: str | None) -> str:
    value = (stage or "final").strip().lower()
    aliases = {
        "916": "preopen",
        "09:16": "preopen",
        "pre": "preopen",
        "preopen": "preopen",
        "921": "audit",
        "09:21": "audit",
        "audit": "audit",
        "strategic-audit": "audit",
        "925": "final",
        "09:25": "final",
        "final": "final",
        "sentinel": "final",
    }
    if value not in aliases:
        raise argparse.ArgumentTypeError(f"未知竞价阶段: {stage}")
    return aliases[value]


def main() -> None:
    parser = argparse.ArgumentParser(description="V2.2 三阶段竞价哨兵")
    parser.add_argument("--date", help="目标开盘日期，默认今天")
    parser.add_argument("--stage", default="final", type=_normalize_stage, help="竞价阶段：preopen(09:16), audit(09:21), final(09:25)")
    parser.add_argument("--no-push", action="store_true", help="只回填数据库，不发送 PushPlus")
    parser.add_argument("--dry-run", nargs="?", const=True, default=False, type=_parse_bool, help="干跑：不回填数据库，也不发送 PushPlus；支持 --dry-run=false")
    parser.add_argument("--simulate-today-close", action="store_true", help="实战模拟：读取当天锁定标的，用今日收盘/当前价作为伪开盘价，只推送不回填")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="启动后等待指定秒数，用于 09:21:05 / 09:25:05 精准触发")
    args = parser.parse_args()

    if args.sleep_seconds > 0:
        import time

        time.sleep(args.sleep_seconds)

    target_day = args.date or date.today().isoformat()
    simulate_today_close = bool(args.simulate_today_close)
    if not args.date and not args.dry_run and not simulate_today_close:
        same_day_picks = _load_unclosed_picks(target_day, include_today=True)
        previous_picks = _load_unclosed_picks(target_day, include_today=False)
        if same_day_picks and not previous_picks:
            simulate_today_close = True

    run_exit_sentinel(
        today=target_day,
        send_push=(not args.no_push and not args.dry_run),
        dry_run=args.dry_run,
        include_today=simulate_today_close,
        use_close_as_open=simulate_today_close,
        persist=(args.stage == "final" and not simulate_today_close and not args.dry_run),
        stage=args.stage,
    )


if __name__ == "__main__":
    main()
