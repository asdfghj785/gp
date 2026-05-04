from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

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
        result = {"status": "skipped", "reason": "非交易日不执行 T+3 收盘结算", "date": current_day}
        print(result)
        return result

    picks = [
        pick
        for pick in open_position_picks(today=current_day)
        if pick.get("strategy_type") in SWING_STRATEGY_TYPES
        and str(pick.get("selection_date") or "") < current_day
        and str(pick.get("target_date") or "") <= current_day
    ]
    if not picks:
        result = {"status": "noop", "reason": "没有到期需要 T+3 收盘结算的波段持仓", "date": current_day}
        if send_push:
            result["pushplus"] = _send_pushplus(
                f"T+3收盘结算：{current_day} 无到期持仓",
                f"## T+3 收盘结算报告\n\n{current_day} 没有到期需要按 15:00 收盘价结算的 T+3 波段持仓。",
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
    selection_price = _safe_float(pick.get("selection_price"))
    if selection_price <= 0:
        raise RuntimeError(f"daily_picks 中 {pick['code']} 缺少有效锁定价")

    target_date = str(pick.get("target_date") or current_day)[:10]
    settlement_date = target_date or current_day
    holding_day = _holding_trading_day(str(pick["selection_date"]), settlement_date)
    if current_day < settlement_date:
        return {
            "status": "holding",
            "selection_date": pick["selection_date"],
            "code": pick["code"],
            "name": pick["name"],
            "strategy_type": pick.get("strategy_type"),
            "holding_day": holding_day,
            "target_date": settlement_date,
            "reason": "未到目标交易日，盘中止盈止损已屏蔽，继续等待 T+3 15:00 收盘结算",
        }

    daily = stock_daily_row(str(pick["code"]), settlement_date)
    close_price = _safe_float((daily or {}).get("close"))
    if close_price <= 0:
        return {
            "status": "holding",
            "selection_date": pick["selection_date"],
            "code": pick["code"],
            "name": pick["name"],
            "strategy_type": pick.get("strategy_type"),
            "holding_day": holding_day,
            "target_date": settlement_date,
            "reason": "目标交易日 15:00 收盘价尚未落库，暂不使用盘中价结算",
        }

    close_return = (close_price / selection_price - 1) * 100
    checked_at = f"{settlement_date}T15:00:00"
    close_signal = {
        "action": "T+3收盘结算",
        "level": "time",
        "instruction": "T+3 策略到期，仅按目标交易日 15:00 收盘价结算，不使用盘中止盈、止损或追踪卖出。",
        "title": f"【T+3收盘结算】{pick['name']} 到期闭环",
        "close_price": round(close_price, 4),
        "close_return_pct": round(close_return, 4),
        "holding_day": holding_day,
        "settlement_basis": "stock_daily.close@15:00",
        "pushed_at": checked_at,
        "push_status": "pending",
    }
    updated = mark_daily_pick_closed(
        str(pick["selection_date"]),
        close_price,
        close_return,
        "T+3收盘结算",
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
        "target_date": settlement_date,
        "close_price": round(close_price, 4),
        "close_return_pct": round(close_return, 4),
        "settlement_basis": "stock_daily.close@15:00",
        "action": "T+3收盘结算",
        "level": "time",
        "instruction": close_signal["instruction"],
        "pick": updated,
    }


def _build_patrol_report(
    current_day: str,
    results: list[dict[str, Any]],
    errors: list[dict[str, Any]],
) -> tuple[str, str]:
    closed = [item for item in results if item.get("status") == "closed"]
    holding = [item for item in results if item.get("status") == "holding"]
    title = f"T+3收盘结算报告：等待{len(holding)} / 闭环{len(closed)}"
    lines = [
        "## T+3 收盘结算报告",
        "",
        f"- 日期：{current_day}",
        f"- 到期标的：{len(results) + len(errors)} 只",
        f"- 等待收盘价落库：{len(holding)} 只",
        f"- 收盘闭环：{len(closed)} 只",
        f"- 异常：{len(errors)} 只",
        "- 规则：只按 T+3 目标交易日 15:00 收盘价结算，不做盘中止盈、止损或追踪卖出",
        "",
    ]

    for item in closed:
        lines.extend(
            [
                f"### {_action_icon(str(item.get('level') or ''))} {item.get('action')}：{item.get('name')}({item.get('code')})",
                f"- 命中策略：{item.get('strategy_type') or '波段策略'}",
                f"- 买入观察日：{item.get('selection_date')}，当前 T+{item.get('holding_day')}",
                f"- 目标结算日：{item.get('target_date') or '-'}",
                f"- 15:00 收盘价：{_fmt_price(item.get('close_price'))}",
                f"- 收盘收益：{_fmt_pct(item.get('close_return_pct'))}",
                f"- 结算口径：{item.get('settlement_basis') or 'stock_daily.close@15:00'}",
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
                f"- 目标结算日：{item.get('target_date') or '-'}",
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
    parser = argparse.ArgumentParser(description="T+3 收盘结算器：仅按目标交易日 15:00 日线 close 闭环")
    parser.add_argument("--date", help="结算日期，默认今天")
    parser.add_argument("--no-push", action="store_true", help="只打印和回填，不发送 PushPlus")
    args = parser.parse_args()
    run_swing_patrol(today=args.date, send_push=not args.no_push)


if __name__ == "__main__":
    main()
