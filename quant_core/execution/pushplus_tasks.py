from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

from quant_core.config import PUSHPLUS_TOKEN, check_push_config
from quant_core.daily_pick import save_pushed_top_picks
from quant_core.engine.predictor import scan_market
from quant_core.storage import (
    database_overview,
    get_daily_picks,
    latest_prediction_snapshot,
    list_unpushed_daily_picks,
    mark_daily_picks_push_result,
)


PUSHPLUS_URL = "http://www.pushplus.plus/send"
BASE_DIR = Path(__file__).resolve().parent
PUSHPLUS_RETRY_COUNT = 3
PUSHPLUS_RETRY_DELAY_SECONDS = 2
PUSHPLUS_MAX_CONTENT_LENGTH = 3500


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
    locked_rows = get_daily_picks(date.today().isoformat())
    if locked_rows:
        locked_lines = "\n".join(_pick_line(row, exists=True) for row in locked_rows) or "- 今日记录存在，但没有可展示标的。"
        content = f"""14:50 推送标的已锁定
时间: {now}
标的数量: {len(locked_rows)}
{locked_lines}

今日记录已存在，系统不会重新扫描或覆盖修改。
"""
        result = send_pushplus(f"14:50多轨候选已锁定: {len(locked_rows)}只", content)
        _mark_push(date.today().isoformat(), locked_rows, result)
        print(_compact_task_result("sent_locked", "top_pick", now, locked_rows, result))
        return {"pushplus": result, "daily_pick": {"status": "exists", "picks": locked_rows}}

    scan = scan_market(limit=10, persist_snapshot=False, cache_prediction=False, async_persist=False)
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
原因: 没有达到回归预期溢价、综合评分门槛且预期溢价为正的候选股。
模型状态: {scan.get('model_status')}
"""
        result = send_pushplus("14:50尾盘策略：无强信号空仓", content)
        print({"status": "sent", "task": "top_pick", "time": now, "pushplus": result})
        return result
    saved = save_pushed_top_picks(rows, scan, force=False)
    pick_lines = "\n".join(_pick_line(winner) for winner in rows) or "- 本次扫描无可展示标的。"
    content = f"""14:50 实时多轨独立候选
时间: {now}
标的数量: {len(rows)}
{pick_lines}

市场模式: {gate.get('mode')}
成交额: {gate.get('market_amount_yi', 0):.0f} 亿
14:30快照: {intraday.get('status')} | 匹配: {intraday.get('matched_count', 0)} | 拦截: {intraday.get('trapped_count', 0)}
模型状态: {scan.get('model_status')}

过滤规则: 已排除创业板、北交所、科创板、ST/退市；三大军团各自独立选 Top1；雷暴或下跌缩量空仓；14:30后尾盘拉升超过阈值、近3日断头铡刀按策略风控剔除。
"""
    result = send_pushplus(f"14:50多轨候选: {len(rows)}只", content)
    day_rows = get_daily_picks(date.today().isoformat())
    _mark_push(date.today().isoformat(), day_rows, result)
    print(_compact_task_result("sent", "top_pick", now, day_rows, result, daily_pick=saved))
    return {"pushplus": result, "daily_pick": saved}


def resend_today(send_push: bool = True) -> dict[str, Any]:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today = date.today().isoformat()
    rows = list_unpushed_daily_picks(today)
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


def _pick_line(row: dict[str, Any], exists: bool = False) -> str:
    strategy_type = str(row.get("strategy_type") or "未知")
    name = str(row.get("name") or "-")
    code = str(row.get("code") or "-")
    price = _safe_float(row.get("snapshot_price") or row.get("selection_price") or row.get("price"))
    change = _safe_float(row.get("selection_change") if exists else row.get("change"))
    expected = _safe_float(
        row.get("expected_t3_max_gain_pct")
        if strategy_type in {"中线超跌反转", "右侧主升浪"}
        else row.get("expected_premium")
    )
    if expected == 0:
        expected = _safe_float(row.get("expected_premium") or row.get("composite_score"))
    score = _safe_float(row.get("composite_score"))
    target_date = row.get("target_date") or "-"
    snapshot_time = row.get("snapshot_time") or "14:50"
    metric_label = "T+3预期最大涨幅" if strategy_type in {"中线超跌反转", "右侧主升浪"} else "预期开盘溢价"
    return (
        f"- 【{strategy_type}】{name}({code}) "
        f"14:50价 {_fmt(price)} / 涨跌 {_fmt(change)}% / "
        f"{metric_label} {_fmt(expected)}% / 评分 {_fmt(score)} / "
        f"快照 {snapshot_time} / 目标日 {target_date}"
    )


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
            }
            for row in rows
        ],
        "pushplus": pushplus,
    }
    if daily_pick is not None:
        payload["daily_pick"] = daily_pick
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
    sub.add_parser("top-pick", help="推送实时最高预期溢价股票")
    resend_parser = sub.add_parser("resend-today", help="补发今日未标记为已推送的影子测试标的")
    resend_parser.add_argument("--dry-run", action="store_true", help="只检查补发候选，不发送微信、不写推送状态")
    args = parser.parse_args()

    if args.command == "heartbeat":
        heartbeat()
    elif args.command == "top-pick":
        top_pick()
    elif args.command == "resend-today":
        resend_today(send_push=not args.dry_run)


if __name__ == "__main__":
    main()
