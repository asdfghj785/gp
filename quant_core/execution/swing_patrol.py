from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from quant_core.config import BASE_DIR
from quant_core.data_pipeline.trading_calendar import is_trading_day, trading_day_count_after
from quant_core.sentinel_cache import validate_sentinel_payload
from quant_core.storage import mark_daily_pick_closed, open_position_picks, sentinel_5m_exit_picks, stock_daily_row
from quant_core.execution.pushplus_tasks import send_pushplus


REVERSAL_STRATEGY = "中线超跌反转"
MAIN_WAVE_STRATEGY = "右侧主升浪"
GLOBAL_MOMENTUM_STRATEGY = "全局动量狙击"
BREAKOUT_STRATEGY = "尾盘突破"
ST_BREAKOUT_STRATEGY = "尾盘突破-ST特情"
SWING_STRATEGY_TYPES = {REVERSAL_STRATEGY, MAIN_WAVE_STRATEGY, GLOBAL_MOMENTUM_STRATEGY}
SENTINEL_5M_STRATEGY_TYPES = {GLOBAL_MOMENTUM_STRATEGY, BREAKOUT_STRATEGY, ST_BREAKOUT_STRATEGY}
STRICT_5M_EXIT_START_DATE = "2024-04-09"
CLOSABLE_SENTINEL_COVERAGE_STATUSES = {"covered", "daily_t3_fallback", "next_open_fallback"}
SENTINEL_5M_SCRIPT = BASE_DIR / "scripts" / "backtest" / "simulate_sentinel_5m.py"
SENTINEL_5M_CACHE_DIR = BASE_DIR / "data" / "strategy_cache"


def run_swing_patrol(
    today: str | None = None,
    send_push: bool = True,
    allow_backtest_writeback: bool = False,
) -> dict[str, Any]:
    current_day = today or date.today().isoformat()
    if not is_trading_day(datetime.fromisoformat(current_day[:10]).date()):
        result = {"status": "skipped", "reason": "非交易日不执行 T+3 收盘结算", "date": current_day}
        print(result)
        return result

    sentinel_picks = [
        pick
        for pick in sentinel_5m_exit_picks(STRICT_5M_EXIT_START_DATE, today=current_day)
        if _needs_sentinel_5m_sync(pick)
    ]
    legacy_picks = [
        pick
        for pick in open_position_picks(today=current_day)
        if pick.get("strategy_type") in SWING_STRATEGY_TYPES
        and str(pick.get("selection_date") or "") < STRICT_5M_EXIT_START_DATE
        and str(pick.get("selection_date") or "") < current_day
        and str(pick.get("target_date") or "") <= current_day
    ]
    if not sentinel_picks and not legacy_picks:
        result = {"status": "noop", "reason": "没有需要 5m 回放体检或旧口径 T+3 结算的持仓", "date": current_day}
        if send_push:
            result["pushplus"] = _send_pushplus(
                f"5m回放体检：{current_day} 无待处理持仓",
                f"## 5m 回放体检报告\n\n{current_day} 没有需要按 V5.6 5m 规则回放体检或旧口径 T+3 结算的持仓。",
            )
        print(result)
        return result

    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    if sentinel_picks:
        try:
            results.extend(
                _sync_sentinel_5m_picks(
                    sentinel_picks,
                    current_day,
                    allow_backtest_writeback=allow_backtest_writeback,
                )
            )
        except Exception as exc:
            for pick in sentinel_picks:
                errors.append(
                    {
                        "status": "error",
                        "selection_date": pick.get("selection_date"),
                        "code": pick.get("code"),
                        "name": pick.get("name"),
                        "strategy_type": pick.get("strategy_type"),
                        "error": f"5m 回放同步失败：{exc}",
                    }
                )

    for pick in legacy_picks:
        try:
            results.append(_patrol_one_pick_daily_close(pick, current_day))
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
        "sentinel_5m_count": len(sentinel_picks),
        "legacy_daily_t3_count": len(legacy_picks),
        "allow_backtest_writeback": allow_backtest_writeback,
        "replay_trigger_count": len([item for item in results if item.get("status") == "replay_triggered"]),
        "results": results,
        "errors": errors,
        "pushplus": push_status,
    }
    print(result)
    return result


def _needs_sentinel_5m_sync(pick: dict[str, Any]) -> bool:
    selection_date = str(pick.get("selection_date") or "")[:10]
    strategy_type = str(pick.get("strategy_type") or "")
    if selection_date < STRICT_5M_EXIT_START_DATE or strategy_type not in SENTINEL_5M_STRATEGY_TYPES:
        return False
    raw = pick.get("raw") if isinstance(pick.get("raw"), dict) else {}
    close_signal = raw.get("close_signal") if isinstance(raw.get("close_signal"), dict) else {}
    source = str(close_signal.get("source") or "")
    if source == "live_sentinel":
        return False
    return True


def _sync_sentinel_5m_picks(
    picks: list[dict[str, Any]],
    current_day: str,
    allow_backtest_writeback: bool = False,
) -> list[dict[str, Any]]:
    start_date = min(str(pick.get("selection_date") or current_day)[:10] for pick in picks)
    payload = _refresh_sentinel_5m_payload(start_date, current_day)
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise RuntimeError("5m 回放缓存缺少 rows")

    by_pick_id: dict[int, dict[str, Any]] = {}
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        pick_id = _safe_int(row.get("pick_id"))
        if pick_id:
            by_pick_id[pick_id] = row
        key = _sentinel_match_key(row)
        if key:
            by_key[key] = row

    results: list[dict[str, Any]] = []
    for pick in picks:
        pick_id = _safe_int(pick.get("id"))
        row = by_pick_id.get(pick_id) if pick_id else None
        if row is None:
            key = _sentinel_match_key(pick)
            row = by_key.get(key) if key else None
        results.append(
            _apply_sentinel_5m_row(
                pick,
                row,
                current_day,
                allow_backtest_writeback=allow_backtest_writeback,
            )
        )
    return results


def _refresh_sentinel_5m_payload(start_date: str, current_day: str) -> dict[str, Any]:
    if not SENTINEL_5M_SCRIPT.exists():
        raise RuntimeError(f"5m 回放脚本不存在：{SENTINEL_5M_SCRIPT}")
    SENTINEL_5M_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = _sentinel_5m_cache_path(start_date, current_day)
    completed = subprocess.run(
        [
            sys.executable,
            str(SENTINEL_5M_SCRIPT),
            "--start-date",
            start_date,
            "--end-date",
            current_day,
            "--output-json",
            str(cache_path),
        ],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=900,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "5m 回放脚本失败："
            f"returncode={completed.returncode}; stdout_tail={(completed.stdout or '')[-1200:]}; "
            f"stderr_tail={(completed.stderr or '')[-1200:]}"
        )
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"5m 回放缓存格式非法：{cache_path}")
    valid_cache, validation = validate_sentinel_payload(payload)
    if not valid_cache:
        raise RuntimeError(f"5m 回放缓存与当前账本不一致：{validation.get('reason')}")
    return payload


def _apply_sentinel_5m_row(
    pick: dict[str, Any],
    row: dict[str, Any] | None,
    current_day: str,
    allow_backtest_writeback: bool = False,
) -> dict[str, Any]:
    selection_date = str(pick.get("selection_date") or "")[:10]
    holding_day = _holding_trading_day(selection_date, current_day)
    base = {
        "selection_date": selection_date,
        "code": pick.get("code"),
        "name": pick.get("name"),
        "strategy_type": pick.get("strategy_type"),
        "holding_day": holding_day,
        "target_date": pick.get("target_date"),
        "settlement_basis": "sentinel_5m_backtest",
    }
    if row is None:
        return {
            **base,
            "status": "holding",
            "reason": "严格 5m 口径：没有匹配到 5m 回放结果，禁止日线 T+3 兜底",
        }

    coverage_status = str(row.get("coverage_status") or "")
    if coverage_status not in CLOSABLE_SENTINEL_COVERAGE_STATUSES or not row.get("is_closed"):
        return {
            **base,
            "status": "holding",
            "coverage_status": coverage_status,
            "bars_replayed": row.get("bars_replayed"),
            "reason": _sentinel_holding_reason(row),
        }

    close_price = _safe_float(row.get("close_price"))
    close_return_pct = _safe_float(row.get("close_return_pct"))
    if close_price <= 0 or row.get("close_return_pct") is None:
        return {
            **base,
            "status": "holding",
            "coverage_status": coverage_status,
            "reason": "严格 5m 口径：5m 已覆盖但缺少有效结算价或收益，禁止日线 T+3 兜底",
        }

    close_time = str(row.get("close_time") or row.get("close_date") or current_day)
    sell_strategy = str(row.get("sell_strategy") or row.get("close_reason") or row.get("exit_reason") or "V5.6 5m卖出闭环")
    close_reason = str(row.get("close_reason") or sell_strategy)
    if not allow_backtest_writeback:
        return {
            **base,
            "status": "replay_triggered",
            "coverage_status": coverage_status,
            "close_price": round(close_price, 4),
            "close_return_pct": round(close_return_pct, 4),
            "close_time": close_time,
            "close_reason": close_reason,
            "action": sell_strategy,
            "level": "sentinel_5m_replay",
            "instruction": row.get("exit_policy") or sell_strategy,
            "writeback": False,
            "reason": "5m回放触发卖出信号；未写真实账本，真实卖出只以 live_sentinel 实时触发为准",
        }
    close_signal = {
        "source": "sentinel_5m_backtest",
        "action": sell_strategy,
        "level": "sentinel_5m",
        "instruction": row.get("exit_policy") or sell_strategy,
        "sell_strategy": sell_strategy,
        "exit_policy": row.get("exit_policy") or sell_strategy,
        "close_time": close_time,
        "close_price": round(close_price, 4),
        "close_return_pct": round(close_return_pct, 4),
        "settlement_basis": "sentinel_5m_backtest",
        "coverage_status": coverage_status,
        "exit_category": row.get("exit_category"),
        "exit_reason": row.get("exit_reason"),
        "bars_replayed": row.get("bars_replayed"),
        "highest_price": row.get("highest_price"),
        "highest_gain_pct": row.get("highest_gain_pct"),
        "warning": row.get("warning"),
        "pushed_at": close_time,
        "push_status": "pending",
    }
    updated = mark_daily_pick_closed(
        selection_date,
        close_price,
        close_return_pct,
        close_reason,
        checked_at=close_time,
        close_signal=close_signal,
        strategy_type=pick.get("strategy_type"),
        code=pick.get("code"),
        pick_id=pick.get("id"),
    )
    return {
        **base,
        "status": "closed",
        "coverage_status": coverage_status,
        "close_price": round(close_price, 4),
        "close_return_pct": round(close_return_pct, 4),
        "close_time": close_time,
        "close_reason": close_reason,
        "action": sell_strategy,
        "level": "sentinel_5m",
        "instruction": close_signal["instruction"],
        "pick": updated,
    }


def _sentinel_holding_reason(row: dict[str, Any]) -> str:
    coverage_status = str(row.get("coverage_status") or "")
    if coverage_status == "missing_5m":
        return "缺失 5m 数据且没有可用兜底结算价，继续等待真实账本闭环"
    if coverage_status == "open_or_incomplete":
        if _safe_int(row.get("bars_replayed")) > 0:
            return str(row.get("warning") or "5m 未完整覆盖且没有可用兜底结算价，继续等待真实账本闭环")
        return "5m 未覆盖到结算点且没有可用兜底结算价，继续等待真实账本闭环"
    if coverage_status == "daily_t3_fallback":
        return "无完整 5m：全局动量狙击按 T+3 收盘价结算"
    if coverage_status == "next_open_fallback":
        return "无完整 5m：尾盘突破/ST特情按 T+1 开盘价结算"
    return str(row.get("warning") or row.get("exit_reason") or "严格 5m 口径：继续等待 5m 闭环")


def _sentinel_match_key(row: dict[str, Any]) -> tuple[str, str, str] | None:
    selection_date = str(row.get("selection_date") or row.get("date") or "")[:10]
    code = _normalize_code(row.get("code"))
    strategy_type = str(row.get("strategy_type") or "").strip()
    if not selection_date or not code or not strategy_type:
        return None
    return selection_date, code, strategy_type


def _normalize_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:].zfill(6) if digits else str(value or "")


def _sentinel_5m_cache_path(start_date: str, current_day: str) -> Path:
    start_key = start_date.replace("-", "")
    end_key = current_day.replace("-", "")
    return SENTINEL_5M_CACHE_DIR / f"sentinel_5m_backtest_{start_key}_{end_key}.json"


def _patrol_one_pick_daily_close(pick: dict[str, Any], current_day: str) -> dict[str, Any]:
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
    replay_triggered = [item for item in results if item.get("status") == "replay_triggered"]
    holding = [item for item in results if item.get("status") == "holding"]
    title = f"5m回放体检报告：等待{len(holding)} / 回放触发{len(replay_triggered)} / 真实闭环{len(closed)}"
    lines = [
        "## 5m 回放体检报告",
        "",
        f"- 日期：{current_day}",
        f"- 检查标的：{len(results) + len(errors)} 只",
        f"- 等待 5m 继续观察：{len(holding)} 只",
        f"- 5m 回放触发但未写真实账本：{len(replay_triggered)} 只",
        f"- 真实闭环/旧口径结算：{len(closed)} 只",
        f"- 异常：{len(errors)} 只",
        f"- 规则：{STRICT_5M_EXIT_START_DATE} 起 15:35 只做 5m 回放体检；真实卖出以 live_sentinel 盘中触发为准",
        "",
    ]

    for item in replay_triggered:
        lines.append(
            "- 回放触发："
            f"{item.get('code')} {item.get('name')} | {item.get('strategy_type') or '波段策略'} | "
            f"T+{item.get('holding_day')} | {item.get('action') or '-'} | "
            f"{_fmt_price(item.get('close_price'))} | {_fmt_pct(item.get('close_return_pct'))} | 未写真实账本"
        )

    for item in closed:
        lines.append(
            "- 真实闭环："
            f"{item.get('code')} {item.get('name')} | {item.get('strategy_type') or '波段策略'} | "
            f"T+{item.get('holding_day')} | {item.get('action') or '-'} | "
            f"{_fmt_price(item.get('close_price'))} | {_fmt_pct(item.get('close_return_pct'))}"
        )

    for item in holding:
        lines.append(
            "- 等待："
            f"{item.get('code')} {item.get('name')} | {item.get('strategy_type') or '波段策略'} | "
            f"{item.get('selection_date')} T+{item.get('holding_day')} -> {item.get('target_date') or '-'} | "
            f"{_short_text(item.get('reason') or '继续观察', 80)}"
        )

    for item in errors:
        lines.append(
            "- 异常："
            f"{item.get('code')} {item.get('name')} | {item.get('strategy_type') or '波段策略'} | "
            f"{_short_text(item.get('error') or '-', 100)}"
        )

    if not results and not errors:
        lines.append("今日无可展示巡逻结果。")
    return title, "\n".join(lines).strip()


def _fmt_price(value: Any) -> str:
    number = _safe_float(value)
    return "-" if number <= 0 else f"{number:.2f}"


def _fmt_pct(value: Any) -> str:
    return f"{_safe_float(value):.2f}%"


def _short_text(value: Any, limit: int) -> str:
    text = str(value or "").replace("\n", " ").strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


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


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="V5.6 5m 回放体检器：默认不把回放结果写入真实账本")
    parser.add_argument("--date", help="结算日期，默认今天")
    parser.add_argument("--no-push", action="store_true", help="不发送 PushPlus")
    parser.add_argument(
        "--allow-backtest-writeback",
        action="store_true",
        help="危险开关：允许把 sentinel_5m_backtest 回放结果写入真实账本，默认禁止",
    )
    args = parser.parse_args()
    run_swing_patrol(
        today=args.date,
        send_push=not args.no_push,
        allow_backtest_writeback=args.allow_backtest_writeback,
    )


if __name__ == "__main__":
    main()
