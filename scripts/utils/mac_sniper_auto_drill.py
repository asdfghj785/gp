#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from quant_core.execution.mac_sniper import aim_and_fire, read_trade_panel_snapshot
from quant_core.execution.position_sizer import (
    InsufficientFundsError,
    calculate_order,
    load_shadow_account,
    normalize_stock_code,
)


DEFAULT_CODE = "002747"
DEFAULT_NAME = "埃斯顿"
DEFAULT_APP_NAME = "同花顺"
DEFAULT_POSITION_PCT = 0.25
DEFAULT_MAX_ATTEMPTS = 5
LOG_DIR = ROOT_DIR / "logs"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mac Sniper 全自动休市试射诊断：同步交易页、算股、填单、提交并判定链路是否打通。"
    )
    parser.add_argument("--code", default=DEFAULT_CODE, help="6 位 A 股代码，默认 002747")
    parser.add_argument("--name", default=DEFAULT_NAME, help="股票名称，仅用于报告展示")
    parser.add_argument("--app-name", default=DEFAULT_APP_NAME, help="目标 App 名称，默认 同花顺")
    parser.add_argument("--cash", type=float, default=None, help="覆盖测试资金；默认读取 data/shadow_account.json")
    parser.add_argument("--position-pct", type=float, default=DEFAULT_POSITION_PCT, help="仓位比例，默认 0.25")
    parser.add_argument("--price", type=float, default=None, help="覆盖委托价；默认从同花顺交易页/持仓表读取")
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS, help="最大重试次数；0 表示一直重试")
    parser.add_argument("--retry-delay", type=float, default=1.5, help="失败后重试等待秒数")
    parser.add_argument(
        "--strict-broker-confirm",
        action="store_true",
        help="只接受持仓数量增加作为成功；默认休市允许 submitted_unverified 作为链路成功。",
    )
    parser.add_argument("--json", action="store_true", help="只输出最终 JSON")
    return parser.parse_args()


def run_drill(args: argparse.Namespace) -> dict[str, Any]:
    code = normalize_stock_code(args.code)
    cash = resolve_cash(args.cash)
    attempts: list[dict[str, Any]] = []
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    attempt_no = 0

    while args.max_attempts <= 0 or attempt_no < args.max_attempts:
        attempt_no += 1
        attempt_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        attempt: dict[str, Any] = {
            "attempt": attempt_no,
            "started_at": attempt_started,
            "code": code,
            "name": args.name,
            "cash": cash,
            "position_pct": args.position_pct,
        }
        attempts.append(attempt)

        try:
            snapshot = read_trade_panel_snapshot(args.app_name)
            price, price_source = resolve_price(snapshot, code, args.price)
            sizing = calculate_order(code, price, args.position_pct, available_cash_override=cash)
            attempt.update(
                {
                    "broker_snapshot": compact_snapshot(snapshot, code),
                    "price": price,
                    "price_source": price_source,
                    "order": sizing,
                }
            )
        except InsufficientFundsError as exc:
            attempt.update({"status": "failed", "stage": "sizing", "error": str(exc)})
            break
        except Exception as exc:
            attempt.update({"status": "failed", "stage": "preflight", "error": str(exc)})
            if not should_retry(args, attempt_no):
                break
            time.sleep(args.retry_delay)
            continue

        if not args.json:
            print(
                f"[MacSniperDrill] attempt={attempt_no} code={code} "
                f"price={price:.3f}({price_source}) shares={sizing['shares']} cash={cash:.2f}"
            )

        result = aim_and_fire(args.code, app_name=args.app_name, shares=int(sizing["shares"]), limit_price=price)
        decision = classify_result(result, strict_broker_confirm=args.strict_broker_confirm)
        attempt.update({"mac_sniper": compact_mac_result(result), "decision": decision, "status": decision["status"]})

        if decision["success"]:
            final = {
                "status": "success",
                "success_kind": decision["kind"],
                "message": decision["message"],
                "started_at": started_at,
                "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "name": args.name,
                "attempts": attempts,
            }
            write_report(final)
            return final

        if not should_retry(args, attempt_no):
            break
        time.sleep(args.retry_delay)

    final = {
        "status": "failed",
        "message": "Mac Sniper 全自动试射未达到成功判定",
        "started_at": started_at,
        "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "code": code,
        "name": args.name,
        "attempts": attempts,
    }
    write_report(final)
    return final


def resolve_cash(override_cash: Optional[float]) -> float:
    if override_cash is not None:
        if override_cash < 0:
            raise ValueError("cash 不能为负数")
        return round(float(override_cash), 2)
    account = load_shadow_account()
    cash = float(account.get("available_cash") or 0)
    if cash < 0:
        raise ValueError("shadow_account.available_cash 不能为负数")
    return round(cash, 2)


def resolve_price(snapshot: dict[str, Any], code: str, override_price: Optional[float]) -> tuple[float, str]:
    if override_price is not None:
        price = float(override_price)
        if price <= 0:
            raise ValueError("price 必须大于 0")
        return round(price, 4), "manual_override"

    order_form = snapshot.get("order_form") or {}
    form_code = safe_code(order_form.get("code"))
    for key in ("current_price", "limit_price"):
        price = safe_float(order_form.get(key))
        if price and (not form_code or form_code == code):
            return round(price, 4), f"order_form.{key}"

    for position in snapshot.get("positions") or []:
        if safe_code(position.get("code")) != code:
            continue
        for key in ("market_price", "cost_price"):
            price = safe_float(position.get(key))
            if price:
                return round(price, 4), f"positions.{code}.{key}"

    raise ValueError("无法从同花顺交易页读取当前价；请先让交易面板显示目标股票，或临时传入 --price")


def classify_result(result: dict[str, Any], strict_broker_confirm: bool = False) -> dict[str, Any]:
    status = result.get("status")
    if status == "broker_confirmed":
        return {
            "success": True,
            "status": "success",
            "kind": "broker_confirmed",
            "message": "同花顺持仓数量已增加，券商侧成交确认。",
        }

    if status == "broker_alert":
        alert = result.get("broker_alert") or {}
        if alert.get("present") and alert.get("dismissed"):
            return {
                "success": True,
                "status": "success",
                "kind": "broker_alert_recorded",
                "message": f"券商弹窗已记录并自动确认：{alert.get('message') or '无弹窗文本'}",
            }

    if status == "submitted_unverified" and not strict_broker_confirm:
        verification = result.get("broker_verification") or {}
        if not result.get("stderr") and verification.get("reason") == "position_quantity_not_increased":
            return {
                "success": True,
                "status": "success",
                "kind": "off_hours_submitted_unfilled",
                "message": "休市试射链路已完整填单并提交；持仓未增加，未写本地成交流水。",
            }

    return {
        "success": False,
        "status": "retryable_failed" if status in {"failed", "timeout", "submitted_unverified"} else "failed",
        "kind": str(status or "unknown"),
        "message": failure_message(result),
    }


def failure_message(result: dict[str, Any]) -> str:
    if result.get("stderr"):
        return str(result["stderr"])
    if result.get("error"):
        return str(result["error"])
    if result.get("hint"):
        return str(result["hint"])
    verification = result.get("broker_verification") or {}
    if verification.get("reason"):
        return str(verification["reason"])
    return "未知失败"


def compact_snapshot(snapshot: dict[str, Any], code: str) -> dict[str, Any]:
    positions = []
    for item in snapshot.get("positions") or []:
        if safe_code(item.get("code")) == code:
            positions.append(item)
    account = snapshot.get("account") or {}
    return {
        "status": snapshot.get("status"),
        "target_window": snapshot.get("target_window"),
        "available_cash": account.get("available_cash"),
        "order_form": snapshot.get("order_form") or {},
        "target_positions": positions,
        "position_count": snapshot.get("position_count"),
    }


def compact_mac_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "code": result.get("code"),
        "shares": result.get("shares"),
        "limit_price": result.get("limit_price"),
        "returncode": result.get("returncode"),
        "stdout": result.get("stdout"),
        "stderr": result.get("stderr"),
        "hint": result.get("hint"),
        "before_snapshot_error": result.get("before_snapshot_error"),
        "after_snapshot_error": result.get("after_snapshot_error"),
        "preexisting_broker_alert": result.get("preexisting_broker_alert"),
        "broker_alert": result.get("broker_alert"),
        "broker_verification": result.get("broker_verification"),
        "after_snapshot": compact_snapshot(result.get("after_snapshot") or {}, safe_code(result.get("code")) or ""),
    }


def should_retry(args: argparse.Namespace, attempt_no: int) -> bool:
    return args.max_attempts <= 0 or attempt_no < args.max_attempts


def write_report(payload: dict[str, Any]) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    report_path = LOG_DIR / f"mac_sniper_auto_drill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    payload["report_path"] = str(report_path)
    return report_path


def safe_float(value: Any) -> Optional[float]:
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def safe_code(value: Any) -> str:
    try:
        return normalize_stock_code(str(value))
    except Exception:
        return ""


def main() -> int:
    args = parse_args()
    result = run_drill(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
