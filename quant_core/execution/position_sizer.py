from __future__ import annotations

import argparse
import json
import math
import os
from datetime import datetime
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

from quant_core.config import BASE_DIR


SHADOW_ACCOUNT_PATH = Path(os.getenv("QUANT_SHADOW_ACCOUNT_PATH", str(BASE_DIR / "data" / "shadow_account.json")))
DEFAULT_AVAILABLE_CASH = float(os.getenv("QUANT_SHADOW_ACCOUNT_INITIAL_CASH", "30000"))
RESERVE_RATE = 0.995


class InsufficientFundsError(RuntimeError):
    pass


def calculate_shares(
    target_code: str,
    current_price: float,
    position_pct: float,
    account_path: Optional[Path] = None,
    available_cash_override: Optional[float] = None,
) -> int:
    return int(
        calculate_order(
            target_code,
            current_price,
            position_pct,
            account_path=account_path,
            available_cash_override=available_cash_override,
        )["shares"]
    )


def calculate_order(
    target_code: str,
    current_price: float,
    position_pct: float,
    account_path: Optional[Path] = None,
    available_cash_override: Optional[float] = None,
) -> dict[str, Any]:
    code = normalize_stock_code(target_code)
    price = normalize_positive_float(current_price, "current_price")
    pct = normalize_position_pct(position_pct)
    if available_cash_override is None:
        account = load_shadow_account(account_path)
        available_cash = normalize_non_negative_float(account.get("available_cash"), "available_cash")
    else:
        available_cash = normalize_non_negative_float(available_cash_override, "available_cash")

    target_amount = available_cash * pct * RESERVE_RATE
    shares = math.floor(target_amount / price / 100) * 100
    if shares < 100:
        raise InsufficientFundsError(
            f"资金不足：available_cash={available_cash:.2f}, position_pct={pct:.4f}, "
            f"price={price:.4f}, target_amount={target_amount:.2f}, shares={shares}"
        )

    estimated_cost = round(shares * price, 2)
    return {
        "code": code,
        "current_price": price,
        "position_pct": pct,
        "available_cash": round(available_cash, 2),
        "reserve_rate": RESERVE_RATE,
        "target_amount": round(target_amount, 2),
        "shares": shares,
        "estimated_cost": estimated_cost,
    }


def reserve_shadow_order(
    target_code: str,
    current_price: float,
    shares: int,
    position_pct: Optional[float] = None,
    account_path: Optional[Path] = None,
    metadata: Optional[dict[str, Any]] = None,
    mac_sniper_result: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    code = normalize_stock_code(target_code)
    price = normalize_positive_float(current_price, "current_price")
    clean_shares = normalize_board_lot(shares)
    estimated_cost = round(clean_shares * price, 2)

    account = load_shadow_account(account_path)
    available_cash = normalize_non_negative_float(account.get("available_cash"), "available_cash")
    if available_cash + 1e-6 < estimated_cost:
        raise InsufficientFundsError(
            f"影子资金池扣款失败：available_cash={available_cash:.2f}, estimated_cost={estimated_cost:.2f}"
        )

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sequence = len(account.get("locked_orders") or []) + len(account.get("trade_records") or []) + 1
    order_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{code}-{sequence:04d}"
    clean_metadata = metadata or {}
    core_theme = str(clean_metadata.get("core_theme") or clean_metadata.get("theme_name") or "-").strip() or "-"
    try:
        theme_momentum_3d = float(
            clean_metadata.get("theme_momentum_3d", clean_metadata.get("theme_momentum", clean_metadata.get("theme_pct_chg_3", 0.0)))
        )
    except (TypeError, ValueError):
        theme_momentum_3d = 0.0
    order = {
        "order_id": order_id,
        "code": code,
        "name": clean_metadata.get("name") or "",
        "core_theme": core_theme,
        "theme_momentum_3d": theme_momentum_3d,
        "shares": clean_shares,
        "reserved_price": price,
        "estimated_cost": estimated_cost,
        "position_pct": normalize_position_pct(position_pct) if position_pct is not None else None,
        "status": "pending_buy",
        "reserved_at": now,
        "metadata": clean_metadata,
    }
    trade_record = {
        "order_id": order_id,
        "code": code,
        "name": clean_metadata.get("name") or "",
        "core_theme": core_theme,
        "theme_momentum_3d": theme_momentum_3d,
        "shares": clean_shares,
        "reference_price": price,
        "estimated_cost": estimated_cost,
        "position_pct": order["position_pct"],
        "source": clean_metadata.get("source") or "unknown",
        "status": "local_fired",
        "fired_at": now,
        "mac_sniper": mac_sniper_result or {},
        "metadata": clean_metadata,
    }
    account["available_cash"] = round(available_cash - estimated_cost, 2)
    account.setdefault("locked_orders", [])
    account.setdefault("trade_records", [])
    account["locked_orders"].append(order)
    account["trade_records"].insert(0, trade_record)
    account["updated_at"] = now
    write_shadow_account(account, account_path)
    return {
        "status": "reserved",
        "available_cash": account["available_cash"],
        "reserved_order": order,
        "trade_record": trade_record,
    }


def sync_shadow_account_from_broker(
    broker_snapshot: dict[str, Any],
    trade_record: Optional[dict[str, Any]] = None,
    account_path: Optional[Path] = None,
) -> dict[str, Any]:
    if not isinstance(broker_snapshot, dict):
        raise ValueError("broker_snapshot 格式非法")
    account_data = broker_snapshot.get("account") or {}
    available_cash = normalize_non_negative_float(account_data.get("available_cash"), "broker.available_cash")
    broker_market_value = normalize_positive_or_zero(account_data.get("market_value"))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    locked_orders = []
    for position in broker_snapshot.get("positions") or []:
        try:
            code = normalize_stock_code(position.get("code"))
            shares = int(round(float(position.get("actual_quantity") or 0)))
        except (TypeError, ValueError):
            continue
        if shares <= 0:
            continue
        market_price = normalize_positive_or_zero(position.get("market_price"))
        cost_price = normalize_positive_or_zero(position.get("cost_price"))
        market_value = normalize_positive_or_zero(position.get("market_value"))
        reference_price = cost_price or market_price
        estimated_cost = market_value or round(shares * reference_price, 2)
        if reference_price <= 0 or estimated_cost <= 0:
            continue
        locked_orders.append(
            {
                "order_id": f"broker-sync-{code}",
                "code": code,
                "name": position.get("name") or "",
                "shares": shares,
                "reserved_price": round(reference_price, 4),
                "estimated_cost": round(estimated_cost, 2),
                "position_pct": None,
                "status": "open",
                "reserved_at": now,
                "metadata": {
                    "source": "broker_sync",
                    "market_price": market_price,
                    "cost_price": cost_price,
                    "broker_position": position,
                },
            }
        )

    account = load_shadow_account(account_path)
    verified_records = [
        record
        for record in (account.get("trade_records") or [])
        if str(record.get("status") or "") == "broker_confirmed"
    ]
    if trade_record:
        clean_record = deepcopy(trade_record)
        clean_record["status"] = "broker_confirmed"
        clean_record.setdefault("confirmed_at", now)
        verified_records.insert(0, clean_record)

    parse_warning = ""
    if not locked_orders and broker_market_value > 0:
        parse_warning = (
            f"同花顺账户总市值 {broker_market_value:.2f}，但本次未解析到持仓明细；"
            "已同步可用资金，保留原本地持仓，避免被空列表覆盖。"
        )

    account["available_cash"] = round(available_cash, 2)
    if not parse_warning:
        account["locked_orders"] = locked_orders
    account["trade_records"] = verified_records
    account["updated_at"] = now
    account["broker_snapshot"] = {
        "synced_at": now,
        "account": account_data,
        "order_form": broker_snapshot.get("order_form") or {},
        "position_count": len(locked_orders),
        "parse_warning": parse_warning,
    }
    write_shadow_account(account, account_path)
    summary = shadow_account_summary(account)
    if parse_warning:
        summary["broker_sync_warning"] = parse_warning
    return summary


def build_broker_confirmed_trade_record(
    target_code: str,
    name: str,
    current_price: float,
    shares: int,
    position_pct: Optional[float],
    source: str,
    mac_sniper_result: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    code = normalize_stock_code(target_code)
    price = normalize_positive_float(current_price, "current_price")
    clean_shares = normalize_board_lot(shares)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    order_id = f"{datetime.now().strftime('%Y%m%d%H%M%S')}-{code}-broker"
    return {
        "order_id": order_id,
        "code": code,
        "name": name or "",
        "shares": clean_shares,
        "reference_price": price,
        "estimated_cost": round(clean_shares * price, 2),
        "position_pct": normalize_position_pct(position_pct) if position_pct is not None else None,
        "source": source,
        "status": "broker_confirmed",
        "fired_at": now,
        "confirmed_at": now,
        "mac_sniper": mac_sniper_result or {},
        "metadata": metadata or {},
    }


def set_available_cash(available_cash: float, account_path: Optional[Path] = None) -> dict[str, Any]:
    cash = normalize_non_negative_float(available_cash, "available_cash")
    account = load_shadow_account(account_path)
    account["available_cash"] = round(cash, 2)
    account["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_shadow_account(account, account_path)
    return shadow_account_summary(account)


def shadow_account_summary(account: Optional[dict[str, Any]] = None, account_path: Optional[Path] = None) -> dict[str, Any]:
    raw = deepcopy(account if account is not None else load_shadow_account(account_path))
    positions = aggregate_positions(raw.get("locked_orders") or [])
    locked_capital = round(sum(float(item.get("estimated_cost") or 0) for item in positions), 2)
    records = raw.get("trade_records") or []
    return {
        "available_cash": round(normalize_non_negative_float(raw.get("available_cash"), "available_cash"), 2),
        "locked_capital": locked_capital,
        "total_shadow_equity": round(normalize_non_negative_float(raw.get("available_cash"), "available_cash") + locked_capital, 2),
        "positions": positions,
        "position_count": len(positions),
        "trade_records": records,
        "trade_record_count": len(records),
        "updated_at": raw.get("updated_at"),
        "raw": raw,
    }


def aggregate_positions(locked_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for order in locked_orders:
        status = str(order.get("status") or "").strip()
        if status not in {"pending_buy", "open"}:
            continue
        code = normalize_stock_code(order.get("code"))
        shares = int(order.get("shares") or 0)
        price = float(order.get("reserved_price") or 0)
        cost = float(order.get("estimated_cost") or shares * price)
        if shares <= 0 or cost <= 0:
            continue
        item = grouped.setdefault(
            code,
            {
                "code": code,
                "name": order.get("name") or (order.get("metadata") or {}).get("name") or "",
                "shares": 0,
                "estimated_cost": 0.0,
                "cost_basis": 0.0,
                "avg_price": 0.0,
                "status": status,
                "latest_reserved_at": order.get("reserved_at"),
                "orders": [],
            },
        )
        item["shares"] += shares
        item["estimated_cost"] = round(float(item["estimated_cost"]) + cost, 2)
        item["cost_basis"] = round(float(item.get("cost_basis") or 0.0) + shares * price, 2)
        item["avg_price"] = round(item["cost_basis"] / item["shares"], 4)
        item["latest_reserved_at"] = max(str(item.get("latest_reserved_at") or ""), str(order.get("reserved_at") or ""))
        item["orders"].append(order)
    return sorted(grouped.values(), key=lambda item: str(item.get("latest_reserved_at") or ""), reverse=True)


def load_shadow_account(account_path: Optional[Path] = None) -> dict[str, Any]:
    path = account_path or SHADOW_ACCOUNT_PATH
    if not path.exists():
        account = default_shadow_account()
        write_shadow_account(account, path)
        return account

    with path.open("r", encoding="utf-8") as handle:
        account = json.load(handle)
    if not isinstance(account, dict):
        raise ValueError(f"影子资金池格式非法：{path}")

    account["available_cash"] = normalize_non_negative_float(account.get("available_cash"), "available_cash")
    if not isinstance(account.get("locked_orders"), list):
        account["locked_orders"] = []
    if not isinstance(account.get("trade_records"), list):
        account["trade_records"] = []
    return account


def write_shadow_account(account: dict[str, Any], account_path: Optional[Path] = None) -> None:
    path = account_path or SHADOW_ACCOUNT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(json.dumps(account, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def default_shadow_account() -> dict[str, Any]:
    return {
        "available_cash": round(DEFAULT_AVAILABLE_CASH, 2),
        "locked_orders": [],
        "trade_records": [],
        "updated_at": None,
    }


def normalize_stock_code(stock_code: str) -> str:
    digits = "".join(ch for ch in str(stock_code or "") if ch.isdigit())
    if len(digits) < 6:
        raise ValueError(f"非法股票代码：{stock_code}")
    return digits[-6:]


def normalize_positive_float(value: Any, name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 不是有效数字：{value}") from exc
    if not math.isfinite(out) or out <= 0:
        raise ValueError(f"{name} 必须大于 0：{value}")
    return out


def normalize_non_negative_float(value: Any, name: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 不是有效数字：{value}") from exc
    if not math.isfinite(out) or out < 0:
        raise ValueError(f"{name} 必须大于等于 0：{value}")
    return out


def normalize_positive_or_zero(value: Any) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(out) or out <= 0:
        return 0.0
    return out


def normalize_position_pct(value: Any) -> float:
    pct = normalize_positive_float(value, "position_pct")
    if pct > 1:
        pct = pct / 100.0
    if pct <= 0 or pct > 1:
        raise ValueError(f"position_pct 必须落在 (0, 1] 或 (0, 100]：{value}")
    return pct


def normalize_board_lot(shares: int) -> int:
    try:
        clean = int(shares)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"shares 不是有效整数：{shares}") from exc
    if clean < 100:
        raise InsufficientFundsError(f"shares 小于 100 股，放弃交易：{shares}")
    if clean % 100 != 0:
        raise ValueError(f"shares 必须是 100 股整数倍：{shares}")
    return clean


def main() -> None:
    parser = argparse.ArgumentParser(description="V5.0 影子资金池算股引擎")
    parser.add_argument("code", help="6 位 A 股代码，例如 002747")
    parser.add_argument("--price", type=float, required=True, help="当前参考成交价")
    parser.add_argument("--position-pct", type=float, required=True, help="目标仓位比例，例如 0.25")
    args = parser.parse_args()
    print(json.dumps(calculate_order(args.code, args.price, args.position_pct), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
