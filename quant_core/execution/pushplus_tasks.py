from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from quant_core.config import PRODUCTION_TOTAL_PICK_LIMIT, PUSHPLUS_TOKEN, check_push_config
from quant_core.ai_agent.agent_gateway import attach_ai_interview, run_1446_ai_interview
from quant_core.data_pipeline.trading_calendar import is_trading_day
from quant_core.daily_pick import save_pushed_top_picks
from quant_core.engine.predictor import scan_market
from quant_core.execution.mac_sniper import aim_and_fire, read_trade_panel_snapshot
from quant_core.execution.position_sizer import (
    InsufficientFundsError,
    build_broker_confirmed_trade_record,
    calculate_order,
    sync_shadow_account_from_broker,
)
from quant_core.sniper_status import get_sniper_status
from quant_core.storage import (
    database_overview,
    get_daily_picks,
    latest_prediction_snapshot,
    latest_daily_picks,
    list_unpushed_daily_picks,
    mark_daily_picks_push_result,
)


PUSHPLUS_URL = "http://www.pushplus.plus/send"
BASE_DIR = Path(__file__).resolve().parent
PUSHPLUS_RETRY_COUNT = 3
PUSHPLUS_RETRY_DELAY_SECONDS = 2
PUSHPLUS_MAX_CONTENT_LENGTH = 3500
MAC_SNIPER_APP_NAME = os.getenv("QUANT_MAC_SNIPER_APP", "同花顺").strip() or "同花顺"
PUSH_STRATEGY_PRIORITY = {
    "全局动量狙击": 4,
    "右侧主升浪": 3,
    "中线超跌反转": 2,
    "尾盘突破": 1,
    "首阴低吸": 0,
}


def send_pushplus(title: str, content: str) -> dict[str, Any]:
    config = check_push_config(print_warning=True)
    if not config["ok"]:
        return {"status": "skipped_missing_token", "error": config["reason"]}

    token = _pushplus_token()
    if not token:
        return {"status": "skipped_missing_token", "error": "缺少 PUSHPLUS_TOKEN，无法发送 PushPlus"}

    chunks = _split_markdown(content, PUSHPLUS_MAX_CONTENT_LENGTH)
    results = []
    for index, chunk in enumerate(chunks, start=1):
        chunk_title = title if len(chunks) == 1 else f"{title} ({index}/{len(chunks)})"
        results.append(_send_pushplus_once(token, chunk_title, chunk))

    failed = [item for item in results if item.get("status") != "sent"]
    if failed:
        return {"status": "failed", "parts": results, "error": failed[-1].get("error") or "PushPlus 分段发送失败"}
    if len(results) == 1:
        return results[0]
    return {
        "status": "sent",
        "multipart": True,
        "parts": results,
        "data": ",".join(str(item.get("data", "")) for item in results if item.get("data")),
        "attempt": max(int(item.get("attempt", 1)) for item in results),
    }


def _send_pushplus_once(token: str, title: str, content: str) -> dict[str, Any]:
    last_error = ""
    for attempt in range(1, PUSHPLUS_RETRY_COUNT + 1):
        try:
            response = requests.post(
                PUSHPLUS_URL,
                json={"token": token, "title": title, "content": content, "template": "markdown"},
                timeout=12,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") == 200:
                payload["status"] = "sent"
                payload["attempt"] = attempt
                return payload
            last_error = f"PushPlus 返回失败: {payload}; response_text={response.text[:500]}"
            print(f"[PushPlus][ERROR] attempt={attempt} {last_error}")
        except Exception as exc:
            response_text = ""
            response = locals().get("response")
            if response is not None:
                response_text = getattr(response, "text", "")[:500]
            last_error = f"{exc}; response_text={response_text}" if response_text else str(exc)
            print(f"[PushPlus][ERROR] attempt={attempt} {last_error}")
        if attempt < PUSHPLUS_RETRY_COUNT:
            time.sleep(PUSHPLUS_RETRY_DELAY_SECONDS)
    return {"status": "failed", "attempts": PUSHPLUS_RETRY_COUNT, "error": last_error}


def heartbeat() -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    overview = database_overview()
    cached = latest_prediction_snapshot()
    cached_rows = len(cached.get("rows", [])) if cached else 0
    latest_report = overview.get("latest_report") or {}
    latest_report_summary = latest_report.get("summary") or {}

    content = f"""项目代码运行正常
时间: {now}
工作目录: /Users/eudis/ths
数据库股票数: {overview.get('stock_count')}
数据库K线数: {overview.get('rows_count')}
数据日期范围: {overview.get('min_date')} 至 {overview.get('max_date')}
预测缓存条数: {cached_rows}
最新数据验证: {latest_report.get('status', '无')} / 错误 {latest_report_summary.get('error_count', '-')} / 警告 {latest_report_summary.get('warning_count', '-')}
今日14:50将推送实时最高预期溢价股票。
"""
    result = send_pushplus("量化项目心跳：运行正常", content)
    print({"status": "sent", "task": "heartbeat", "time": now, "pushplus": result})
    return result


def top_pick() -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today()
    if not is_trading_day(today):
        result = {"status": "skipped", "reason": "非交易日不执行 14:50 推送", "selection_date": today.isoformat()}
        print(result)
        return result

    locked_rows = _limit_top_push_rows(get_daily_picks(date.today().isoformat()))
    if locked_rows:
        locked_lines = "\n".join(_pick_line(row, exists=True) for row in locked_rows) or "- 今日记录存在，但没有可展示标的。"
        content = f"""14:50 分策略 Top1 推送标的已锁定
时间: {now}
标的数量: {len(locked_rows)}
{locked_lines}

今日记录已存在，系统不会重新扫描或覆盖修改。
"""
        result = send_pushplus(f"14:50 分策略Top1候选已锁定: {len(locked_rows)}只", content)
        _mark_push(date.today().isoformat(), locked_rows, result)
        sniper_result = _trigger_mac_sniper(locked_rows)
        print(_compact_task_result("sent_locked", "top_pick", now, locked_rows, result, mac_sniper=sniper_result))
        return {"pushplus": result, "mac_sniper": sniper_result, "daily_pick": {"status": "exists", "picks": locked_rows}}

    scan = scan_market(limit=PRODUCTION_TOTAL_PICK_LIMIT, persist_snapshot=False, cache_prediction=False, async_persist=False)
    rows = scan.get("rows", [])
    gate = scan.get("market_gate") or {}
    intraday = scan.get("intraday_snapshot") or {}
    if gate.get("blocked"):
        reasons = "；".join(gate.get("reasons") or ["大盘风控触发"])
        content = f"""14:50 实时策略结果
时间: {now}
状态: 空仓
模式: {gate.get('mode')}
成交额: {gate.get('market_amount_yi', 0):.0f} 亿
14:30快照: {intraday.get('status')}
原因: {reasons}
模型状态: {scan.get('model_status')}
"""
        result = send_pushplus("14:50尾盘策略：大盘风控空仓", content)
        print({"status": "sent", "task": "top_pick", "time": now, "market_gate": gate, "pushplus": result})
        return result
    if not rows:
        content = f"""14:50 实时策略结果
时间: {now}
状态: 空仓
模式: {gate.get('mode')}
14:30快照: {intraday.get('status')} | 拦截: {intraday.get('trapped_count', 0)}
原因: 没有达到四大军团动态底线且通过物理风控的候选股。
模型状态: {scan.get('model_status')}
"""
        result = send_pushplus("14:50尾盘策略：无强信号空仓", content)
        print({"status": "sent", "task": "top_pick", "time": now, "pushplus": result})
        return result
    ai_interview = run_1446_ai_interview(
        [str(row.get("code") or "") for row in rows],
        [str(row.get("name") or "") for row in rows],
        rows,
    )
    rows = attach_ai_interview(rows, ai_interview)
    saved = save_pushed_top_picks(rows, scan, force=False)
    pick_lines = "\n".join(_pick_line(winner) for winner in rows) or "- 本次扫描无可展示标的。"
    ai_block = str(ai_interview.get("markdown") or "").strip()
    content = f"""14:50 实时分策略 Top1 候选
时间: {now}
标的数量: {len(rows)}
{pick_lines}

{ai_block}

市场模式: {gate.get('mode')}
成交额: {gate.get('market_amount_yi', 0):.0f} 亿
14:30快照: {intraday.get('status')} | 匹配: {intraday.get('matched_count', 0)} | 拦截: {intraday.get('trapped_count', 0)}
模型状态: {scan.get('model_status')}

过滤规则: 已排除创业板、北交所、科创板、ST/退市；当前启用军团各取 Top1，总输出上限 {PRODUCTION_TOTAL_PICK_LIMIT} 只，未达基准时按当日合规池 99 分位动态下探 1 只并标红风偏；雷暴或下跌缩量空仓；14:30后尾盘拉升超过阈值、近3日断头铡刀按策略风控剔除。
"""
    result = send_pushplus(f"14:50 分策略Top1候选: {len(rows)}只", content)
    day_rows = _limit_top_push_rows(get_daily_picks(date.today().isoformat()))
    _mark_push(date.today().isoformat(), day_rows, result)
    sniper_result = _trigger_mac_sniper(rows)
    print(_compact_task_result("sent", "top_pick", now, day_rows, result, daily_pick=saved, mac_sniper=sniper_result))
    return {"pushplus": result, "mac_sniper": sniper_result, "daily_pick": saved}


def resend_today(send_push: bool = True) -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today().isoformat()
    rows = _limit_top_push_rows(list_unpushed_daily_picks(today))
    if not rows:
        result = {"status": "noop", "reason": "今日没有未推送的影子测试标的", "selection_date": today}
        print(result)
        return result

    pick_lines = "\n".join(_pick_line(row, exists=True) for row in rows) or "- 今日无可展示标的。"
    content = f"""14:50 影子测试标的补发
时间: {now}
补发数量: {len(rows)}
{pick_lines}

说明: 本消息来自 resend-today，只补发数据库中今日 is_shadow_test=1 且 push_status 未标记为 sent 的记录，不重新扫描、不覆盖 14:50 快照。
"""
    result = send_pushplus(f"14:50影子测试补发: {len(rows)}只", content) if send_push else {"status": "dry_run"}
    if send_push:
        _mark_push(today, rows, result)
    print(_compact_task_result("resent", "resend_today", now, rows, result))
    return {"pushplus": result, "daily_pick": {"status": "resent", "picks": rows}}


def preview_trade_markdown(selection_date: str = "latest", limit: int = PRODUCTION_TOTAL_PICK_LIMIT) -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = _preview_rows(selection_date, limit=limit)
    if not rows:
        result = {"status": "noop", "reason": "没有可预览的 daily_picks 记录", "selection_date": selection_date}
        print(result)
        return result

    preview_date = str(rows[0].get("selection_date") or selection_date)
    pick_lines = "\n".join(_pick_line(row, exists=True) for row in rows)
    content = f"""14:50 一键狙击通道预览
时间: {now}
预览日期: {preview_date}
标的数量: {len(rows)}

{pick_lines}
"""
    print(content)
    return {"status": "dry_run", "selection_date": preview_date, "count": len(rows), "content": content}


def _pick_line(row: dict[str, Any], exists: bool = False) -> str:
    strategy_type = str(row.get("strategy_type") or "未知")
    name = str(row.get("name") or "-")
    code = _normalize_a_share_code(row.get("code") or "-")
    position = _suggested_position(row)
    position_text = f"{position * 100:.0f}%" if position is not None else "-"
    risk_warning = _risk_warning(row) or "无"
    return (
        f"### 🔴 {strategy_type} | {name}\n"
        "- **买入代码** (长按下方数字复制):\n"
        f"`{code}`\n"
        f"- **凯利仓位**: 💰 **{position_text}** 💰\n"
        f"- **风险提示**: {risk_warning}"
    )


def _trigger_mac_sniper(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not get_sniper_status():
        return {"status": "skipped", "reason": "mac_sniper_disabled"}
    if not rows:
        return {"status": "skipped", "reason": "empty_rows"}
    top = rows[0]
    code = _normalize_a_share_code(top.get("code") or "")
    if not code or len(code) != 6:
        return {"status": "skipped", "reason": "missing_code", "raw_code": top.get("code")}

    current_price = _safe_float(top.get("snapshot_price") or top.get("selection_price") or top.get("price"))
    if current_price <= 0:
        return {"status": "skipped", "reason": "missing_price", "code": code}

    position_pct = _suggested_position(top)
    if position_pct is None or position_pct <= 0:
        return {"status": "skipped", "reason": "missing_position_pct", "code": code}

    try:
        sizing = calculate_order(code, current_price, position_pct)
    except InsufficientFundsError as exc:
        print(f"⚠️ [物理狙击跳过] {exc}")
        return {"status": "skipped", "reason": "insufficient_funds", "code": code, "error": str(exc)}
    except Exception as exc:
        print(f"⚠️ [物理狙击算股异常] {exc}")
        return {"status": "failed", "reason": "position_sizing_failed", "code": code, "error": str(exc)}

    shares = int(sizing["shares"])
    try:
        print(f"🎯 [物理狙击] 正在拉起同花顺全自动买入: {code} | {shares}股 | 参考价 {current_price:.2f}")
        sniper_result = aim_and_fire(code, app_name=MAC_SNIPER_APP_NAME, shares=shares, limit_price=current_price)
        payload = {
            "status": sniper_result.get("status"),
            "code": code,
            "app_name": MAC_SNIPER_APP_NAME,
            "shares": shares,
            "current_price": current_price,
            "position_pct": position_pct,
            "sizing": sizing,
            "mac_sniper": sniper_result,
        }
        if sniper_result.get("status") == "broker_confirmed":
            try:
                trade_record = build_broker_confirmed_trade_record(
                    code,
                    top.get("name") or "",
                    current_price,
                    shares,
                    position_pct,
                    "pushplus_tasks.top_pick",
                    mac_sniper_result=sniper_result,
                    metadata={
                        "selection_date": top.get("selection_date") or date.today().isoformat(),
                        "name": top.get("name"),
                        "strategy_type": top.get("strategy_type"),
                        "source": "pushplus_tasks.top_pick",
                    },
                )
                payload["shadow_account"] = sync_shadow_account_from_broker(
                    sniper_result.get("after_snapshot") or read_trade_panel_snapshot(MAC_SNIPER_APP_NAME),
                    trade_record=trade_record,
                )
                payload["trade_record"] = trade_record
            except Exception as exc:
                payload["status"] = "broker_confirmed_account_sync_failed"
                payload["shadow_account_error"] = str(exc)
                print(f"⚠️ [影子资金池异常] 买入已确认但本地同步失败: {exc}")
        elif sniper_result.get("status") == "submitted_unverified":
            print("⚠️ [物理狙击未记账] 买入提交后未在持仓表确认成交，本地流水未写入")
        return payload
    except Exception as exc:
        print(f"⚠️ [物理狙击异常] 无法拉起终端: {exc}")
        return {
            "status": "failed",
            "code": code,
            "app_name": MAC_SNIPER_APP_NAME,
            "shares": shares,
            "error": str(exc),
        }


def _limit_top_push_rows(rows: list[dict[str, Any]], limit: int = PRODUCTION_TOTAL_PICK_LIMIT) -> list[dict[str, Any]]:
    try:
        cap = int(limit)
    except (TypeError, ValueError):
        cap = int(PRODUCTION_TOTAL_PICK_LIMIT)
    cap = max(1, min(int(PRODUCTION_TOTAL_PICK_LIMIT), cap))
    return sorted(list(rows or []), key=_push_row_sort_key, reverse=True)[:cap]


def _push_row_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    winner = _winner_payload(row)
    strategy_type = str(row.get("strategy_type") or winner.get("strategy_type") or "")
    score = (
        row.get("global_probability")
        or winner.get("global_probability")
        or row.get("composite_score")
        or winner.get("composite_score")
        or row.get("expected_premium")
        or winner.get("expected_premium")
    )
    expected = (
        row.get("expected_t3_max_gain_pct")
        or winner.get("expected_t3_max_gain_pct")
        or row.get("expected_premium")
        or winner.get("expected_premium")
    )
    return (
        float(PUSH_STRATEGY_PRIORITY.get(strategy_type, 0)),
        _safe_float(score),
        _safe_float(expected),
        -_safe_float(row.get("id")),
    )


def _winner_payload(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    if isinstance(raw, dict):
        winner = raw.get("winner")
        if isinstance(winner, dict):
            return winner
    return {}


def _preview_rows(selection_date: str, limit: int = PRODUCTION_TOTAL_PICK_LIMIT) -> list[dict[str, Any]]:
    requested = str(selection_date or "latest").strip()
    if requested and requested.lower() != "latest":
        return _limit_top_push_rows(get_daily_picks(requested[:10]), limit=limit)

    latest_rows = latest_daily_picks(limit=max(limit, 30), shadow_only=False)
    if not latest_rows:
        return []
    latest_date = str(latest_rows[0].get("selection_date") or "")[:10]
    if not latest_date:
        return _limit_top_push_rows(latest_rows, limit=limit)
    return _limit_top_push_rows(get_daily_picks(latest_date), limit=limit)


def _normalize_a_share_code(code: Any) -> str:
    text = str(code or "").strip().upper()
    if "." in text:
        text = text.split(".", 1)[0]
    for prefix in ("SH", "SZ", "BJ"):
        if text.startswith(prefix):
            text = text[len(prefix) :]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[-6:].zfill(6) if digits else text


def _suggested_position(row: dict[str, Any]):
    value = row.get("suggested_position")
    if value is None:
        raw = row.get("raw")
        if isinstance(raw, dict):
            winner = raw.get("winner")
            if isinstance(winner, dict):
                value = winner.get("suggested_position")
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, min(1.0, value))


def _risk_warning(row: dict[str, Any]) -> str:
    warning = str(row.get("risk_warning") or "").strip()
    if warning:
        return warning
    raw = row.get("raw")
    if isinstance(raw, dict):
        winner = raw.get("winner")
        if isinstance(winner, dict):
            return str(winner.get("risk_warning") or "").strip()
    return ""


def _ai_summary(row: dict[str, Any]) -> str:
    ai = row.get("ai_interview")
    if not isinstance(ai, dict):
        raw = row.get("raw")
        if isinstance(raw, dict):
            winner = raw.get("winner")
            if isinstance(winner, dict):
                ai = winner.get("ai_interview")
    if not isinstance(ai, dict):
        return ""
    risk = str(ai.get("risk_level") or "-")
    hint = str(ai.get("action_hint") or "-")
    verdict = str(ai.get("verdict") or "").strip()
    reason = str(ai.get("reason") or "").strip()
    detail = verdict or reason
    if len(detail) > 90:
        detail = detail[:87] + "..."
    return f"风险{risk} / {hint} / {detail}".strip()


def _pushplus_token() -> str:
    token = (PUSHPLUS_TOKEN or os.getenv("PUSHPLUS_TOKEN") or "").strip()
    if token:
        return token
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        if key.strip() == "PUSHPLUS_TOKEN":
            return value.strip().strip('"').strip("'")
    return ""


def _split_markdown(content: str, max_length: int) -> list[str]:
    if len(content) <= max_length:
        return [content]
    lines = content.splitlines()
    chunks: list[str] = []
    current: list[str] = []
    current_length = 0
    for line in lines:
        line_length = len(line) + 1
        if current and current_length + line_length > max_length:
            chunks.append("\n".join(current).strip())
            current = []
            current_length = 0
        if line_length > max_length:
            for start in range(0, len(line), max_length):
                if current:
                    chunks.append("\n".join(current).strip())
                    current = []
                    current_length = 0
                chunks.append(line[start : start + max_length])
            continue
        current.append(line)
        current_length += line_length
    if current:
        chunks.append("\n".join(current).strip())
    return [chunk for chunk in chunks if chunk]


def _mark_push(selection_date: str, rows: list[dict[str, Any]], push_result: dict[str, Any]) -> None:
    status = str(push_result.get("status") or "unknown")
    message_id = str(push_result.get("data") or "") or None
    error = str(push_result.get("error") or "") or None
    pick_ids = [int(row["id"]) for row in rows if row.get("id") is not None]
    mark_daily_picks_push_result(selection_date, status, message_id=message_id, error=error, pick_ids=pick_ids)


def _compact_task_result(
    status: str,
    task: str,
    now: str,
    rows: list[dict[str, Any]],
    pushplus: dict[str, Any],
    daily_pick: dict[str, Any] | None = None,
    mac_sniper: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "task": task,
        "time": now,
        "count": len(rows),
        "picks": [
            {
                "id": row.get("id"),
                "code": row.get("code"),
                "name": row.get("name"),
                "strategy_type": row.get("strategy_type"),
                "snapshot_price": row.get("snapshot_price") or row.get("selection_price") or row.get("price"),
                "suggested_position": row.get("suggested_position"),
            }
            for row in rows
        ],
        "pushplus": pushplus,
    }
    if daily_pick is not None:
        payload["daily_pick"] = daily_pick
    if mac_sniper is not None:
        payload["mac_sniper"] = mac_sniper
    return payload


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _fmt(value: Any) -> str:
    return f"{_safe_float(value):.2f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="PushPlus 定时通知任务")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("heartbeat", help="发送项目运行正常心跳")
    top_parser = sub.add_parser("top-pick", help="推送实时最高预期溢价股票")
    top_parser.add_argument("--dry-run", action="store_true", help="只打印一键狙击 Markdown，不发送微信、不扫描、不写库")
    top_parser.add_argument("--preview-date", default="latest", help="dry-run 预览日期，默认 latest 使用最近一次出票")
    resend_parser = sub.add_parser("resend-today", help="补发今日未标记为已推送的影子测试标的")
    resend_parser.add_argument("--dry-run", action="store_true", help="只检查补发候选，不发送微信、不写推送状态")
    args = parser.parse_args()

    if args.command == "heartbeat":
        heartbeat()
    elif args.command == "top-pick":
        if args.dry_run:
            preview_trade_markdown(selection_date=args.preview_date)
        else:
            top_pick()
    elif args.command == "resend-today":
        resend_today(send_push=not args.dry_run)


if __name__ == "__main__":
    main()
