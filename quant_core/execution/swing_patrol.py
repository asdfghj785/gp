from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from quant_core.data_pipeline.market import fetch_realtime_quote
from quant_core.data_pipeline.trading_calendar import is_trading_day, trading_day_count_after
from quant_core.storage import mark_daily_pick_closed, open_position_picks, stock_daily_row
from quant_core.execution.pushplus_tasks import send_pushplus


REVERSAL_STRATEGY = "中线超跌反转"
MAIN_WAVE_STRATEGY = "右侧主升浪"
GLOBAL_MOMENTUM_STRATEGY = "全局动量狙击"
SWING_STRATEGY_TYPES = {REVERSAL_STRATEGY, MAIN_WAVE_STRATEGY, GLOBAL_MOMENTUM_STRATEGY}


def run_swing_patrol(today: str | None = None, send_push: bool = True) -> dict[str, Any]:
    current_day = today or date.today().isoformat()
    if not is_trading_day(datetime.fromisoformat(current_day[:10]).date()):
        result = {"status": "skipped", "reason": "非交易日不执行 14:45 波段巡逻", "date": current_day}
        print(result)
        return result

    picks = [
        pick
        for pick in open_position_picks(today=current_day)
        if pick.get("strategy_type") in SWING_STRATEGY_TYPES
        and str(pick.get("selection_date") or "") < current_day <= str(pick.get("target_date") or "")
    ]
    if not picks:
        result = {"status": "noop", "reason": "没有需要 14:45 巡逻的 T+3 波段持仓", "date": current_day}
        if send_push:
            result["pushplus"] = _send_pushplus(
                f"14:45波段巡逻：{current_day} 无持仓",
                f"## 14:45 波段巡逻报告\n\n{current_day} 没有需要巡逻的 T+3 波段持仓。",
            )
        print(result)
        return result

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for pick in picks:
        try:
            results.append(_patrol_one_pick(pick, current_day))
        except Exception as exc:
            errors.append(
                {
                    "status": "error",
                    "selection_date": pick.get("selection_date"),
                    "code": pick.get("code"),
                    "name": pick.get("name"),
                    "strategy_type": pick.get("strategy_type"),
                    "error": str(exc),
                }
            )

    title, content = _build_patrol_report(current_day, results, errors)
    push_status = _send_pushplus(title, content) if send_push else {"status": "dry_run"}
    result = {
        "status": "checked",
        "date": current_day,
        "count": len(results) + len(errors),
        "result_count": len(results),
        "error_count": len(errors),
        "results": results,
        "errors": errors,
        "pushplus": push_status,
    }
    print(result)
    return result


def _patrol_one_pick(pick: dict[str, Any], current_day: str) -> dict[str, Any]:
    quote = fetch_realtime_quote(str(pick["code"]))
    current_price = _safe_float(quote.get("current_price")) or _safe_float(quote.get("auction_price")) or _safe_float(quote.get("open"))
    if current_price <= 0:
        raise RuntimeError(f"实时行情没有返回 {pick['code']} 的有效 14:45 当前价")

    selection_price = _safe_float(pick.get("selection_price"))
    if selection_price <= 0:
        raise RuntimeError(f"daily_picks 中 {pick['code']} 缺少有效锁定价")

    current_gain = (current_price / selection_price - 1) * 100
    anchor_open = _anchor_open_price(pick)
    holding_day = _holding_trading_day(str(pick["selection_date"]), current_day)
    decision = _build_decision(pick, quote, current_price, current_gain, anchor_open, holding_day, current_day)
    if decision is None:
        return {
            "status": "holding",
            "selection_date": pick["selection_date"],
            "code": pick["code"],
            "name": pick["name"],
            "strategy_type": pick.get("strategy_type"),
            "holding_day": holding_day,
            "current_price": round(current_price, 4),
            "current_gain": round(current_gain, 4),
            "anchor_open": round(anchor_open, 4),
            "quote_time": f"{quote.get('date') or ''} {quote.get('time') or ''}".strip(),
            "reason": "未触发止盈、止损或 T+3 清退",
        }

    checked_at = f"{current_day}T14:45:00"
    close_signal = {
        "action": decision["action"],
        "level": decision["level"],
        "instruction": decision["instruction"],
        "title": decision["title"],
        "content": decision["content"],
        "current_price": round(current_price, 4),
        "current_gain": round(current_gain, 4),
        "anchor_open": round(anchor_open, 4),
        "holding_day": holding_day,
        "quote_time": f"{quote.get('date') or ''} {quote.get('time') or ''}".strip(),
        "pushed_at": checked_at,
        "push_status": "pending",
    }
    updated = mark_daily_pick_closed(
        str(pick["selection_date"]),
        current_price,
        current_gain,
        decision["action"],
        checked_at=checked_at,
        close_signal=close_signal,
        strategy_type=pick.get("strategy_type"),
        code=pick.get("code"),
        pick_id=pick.get("id"),
    )
    return {
        "status": "closed",
        "selection_date": pick["selection_date"],
        "code": pick["code"],
        "name": pick["name"],
        "strategy_type": pick.get("strategy_type"),
        "holding_day": holding_day,
        "current_price": round(current_price, 4),
        "current_gain": round(current_gain, 4),
        "anchor_open": round(anchor_open, 4),
        "quote_time": f"{quote.get('date') or ''} {quote.get('time') or ''}".strip(),
        "action": decision["action"],
        "level": decision["level"],
        "instruction": decision["instruction"],
        "pick": updated,
    }


def _build_decision(
    pick: dict[str, Any],
    quote: dict[str, Any],
    current_price: float,
    current_gain: float,
    anchor_open: float,
    holding_day: int,
    current_day: str,
) -> dict[str, str] | None:
    if current_gain >= 5.0:
        return _instruction(
            pick,
            quote,
            current_price,
            current_gain,
            anchor_open,
            holding_day,
            current_day,
            action="波段自动止盈",
            level="profit",
            title=f"💰【波段自动止盈】{pick['name']} 已达到 {current_gain:.2f}% 涨幅",
            instruction=f"标的 {pick['name']} 已达到 5% 预期涨幅！请立刻按现价挂单卖出，将利润锁进保险箱！",
        )
    if pick.get("strategy_type") == MAIN_WAVE_STRATEGY and anchor_open > 0 and current_price < anchor_open:
        return _instruction(
            pick,
            quote,
            current_price,
            current_gain,
            anchor_open,
            holding_day,
            current_day,
            action="防线击穿止损",
            level="danger",
            title=f"🔪【防线击穿止损】{pick['name']} A字杀跌破T日开盘价",
            instruction=f"标的 {pick['name']} 跌破 T 日开盘价，右侧突破演变为 A 字杀假突破！请立刻清仓换股！",
        )
    if anchor_open > 0 and current_price < anchor_open:
        return _instruction(
            pick,
            quote,
            current_price,
            current_gain,
            anchor_open,
            holding_day,
            current_day,
            action="防线击穿止损",
            level="danger",
            title=f"🔪【防线击穿止损】{pick['name']} 跌破主力成本线",
            instruction=f"标的 {pick['name']} 跌破主力成本线，逻辑证伪！请立刻清仓换股！",
        )
    if holding_day >= 3:
        return _instruction(
            pick,
            quote,
            current_price,
            current_gain,
            anchor_open,
            holding_day,
            current_day,
            action="波段期满清退",
            level="time",
            title=f"⏰【波段期满清退】{pick['name']} T+3 到期",
            instruction=f"标的 {pick['name']} 潜伏已达 3 天上限，当前涨幅 {current_gain:.2f}%。为提高资金利用率，请于尾盘直接清仓！",
        )
    return None


def _instruction(
    pick: dict[str, Any],
    quote: dict[str, Any],
    current_price: float,
    current_gain: float,
    anchor_open: float,
    holding_day: int,
    current_day: str,
    action: str,
    level: str,
    title: str,
    instruction: str,
) -> dict[str, str]:
    expected = _safe_float((pick.get("raw") or {}).get("winner", {}).get("expected_t3_max_gain_pct"))
    content = f"""**{title}**

持仓标的: {pick['name']}({pick['code']})
命中策略: {pick.get('strategy_type') or '波段策略'}
买入观察日: {pick['selection_date']}
当前交易日: {current_day}，T+{holding_day}
14:50锁定价: {float(pick['selection_price']):.2f}
14:45当前价: {current_price:.2f}
累计实际涨幅: {current_gain:.2f}%
主力成本锚定开盘价: {anchor_open:.2f}
预期T+3最大涨幅: {expected:.2f}%
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


def _anchor_open_price(pick: dict[str, Any]) -> float:
    row = stock_daily_row(str(pick["code"]), str(pick["selection_date"]))
    if row and _safe_float(row.get("open")) > 0:
        return _safe_float(row.get("open"))
    raw_winner = (pick.get("raw") or {}).get("winner") if isinstance((pick.get("raw") or {}).get("winner"), dict) else {}
    for key in ("open", "今开", "selection_open"):
        value = _safe_float(raw_winner.get(key))
        if value > 0:
            return value
    return _safe_float(pick.get("selection_price"))


def _build_patrol_report(
    current_day: str,
    results: list[dict[str, Any]],
    errors: list[dict[str, Any]],
) -> tuple[str, str]:
    closed = [item for item in results if item.get("status") == "closed"]
    holding = [item for item in results if item.get("status") == "holding"]
    title = f"14:45波段巡逻报告：持仓{len(holding)} / 指令{len(closed)}"
    lines = [
        "## 14:45 波段巡逻报告",
        "",
        f"- 日期：{current_day}",
        f"- 巡逻标的：{len(results) + len(errors)} 只",
        f"- 继续持仓：{len(holding)} 只",
        f"- 触发指令：{len(closed)} 只",
        f"- 异常：{len(errors)} 只",
        "",
    ]

    for item in closed:
        lines.extend(
            [
                f"### {_action_icon(str(item.get('level') or ''))} {item.get('action')}：{item.get('name')}({item.get('code')})",
                f"- 命中策略：{item.get('strategy_type') or '波段策略'}",
                f"- 买入观察日：{item.get('selection_date')}，当前 T+{item.get('holding_day')}",
                f"- 14:45 当前价：{_fmt_price(item.get('current_price'))}",
                f"- 累计涨幅：{_fmt_pct(item.get('current_gain'))}",
                f"- 主力成本线：{_fmt_price(item.get('anchor_open'))}",
                f"- 行情时间：{item.get('quote_time') or '-'}",
                f"- 操作指令：{item.get('instruction') or '-'}",
                "",
            ]
        )

    for item in holding:
        lines.extend(
            [
                f"### 🟦 持仓观察：{item.get('name')}({item.get('code')})",
                f"- 命中策略：{item.get('strategy_type') or '波段策略'}",
                f"- 买入观察日：{item.get('selection_date')}，当前 T+{item.get('holding_day')}",
                f"- 14:45 当前价：{_fmt_price(item.get('current_price'))}",
                f"- 累计涨幅：{_fmt_pct(item.get('current_gain'))}",
                f"- 主力成本线：{_fmt_price(item.get('anchor_open'))}",
                f"- 行情时间：{item.get('quote_time') or '-'}",
                f"- 状态：{item.get('reason') or '继续观察'}",
                "",
            ]
        )

    for item in errors:
        lines.extend(
            [
                f"### ⚠️ 巡逻异常：{item.get('name')}({item.get('code')})",
                f"- 命中策略：{item.get('strategy_type') or '波段策略'}",
                f"- 买入观察日：{item.get('selection_date')}",
                f"- 错误：{item.get('error')}",
                "",
            ]
        )

    if not results and not errors:
        lines.append("今日无可展示巡逻结果。")
    return title, "\n".join(lines).strip()


def _action_icon(level: str) -> str:
    if level == "profit":
        return "💰"
    if level == "danger":
        return "🔪"
    if level == "time":
        return "⏰"
    return "📌"


def _fmt_price(value: Any) -> str:
    number = _safe_float(value)
    return "-" if number <= 0 else f"{number:.2f}"


def _fmt_pct(value: Any) -> str:
    return f"{_safe_float(value):.2f}%"


def _holding_trading_day(selection_date: str, current_day: str) -> int:
    start = datetime.fromisoformat(selection_date[:10]).date()
    end = datetime.fromisoformat(current_day[:10]).date()
    return trading_day_count_after(start, end)


def _send_pushplus(title: str, content: str) -> dict[str, Any]:
    return send_pushplus(title, content)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="14:45 T+3 波段巡逻兵")
    parser.add_argument("--date", help="巡逻日期，默认今天")
    parser.add_argument("--no-push", action="store_true", help="只打印和回填，不发送 PushPlus")
    args = parser.parse_args()
    run_swing_patrol(today=args.date, send_push=not args.no_push)


if __name__ == "__main__":
    main()
