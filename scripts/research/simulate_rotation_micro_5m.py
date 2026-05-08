from __future__ import annotations

import argparse
import json
import math
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


BASE_DIR = Path("/Users/eudis/ths")
DATASET_PATH = BASE_DIR / "data" / "dataset" / "global_sniper_v5_7_dataset.csv"
REPORT_PATH = BASE_DIR / "scripts" / "research" / "simulate_rotation_micro_5m_latest.json"
TRADE_DETAIL_PATH = BASE_DIR / "scripts" / "research" / "simulate_rotation_micro_5m_trades.csv"
EQUITY_PATH = BASE_DIR / "scripts" / "research" / "simulate_rotation_micro_5m_equity.csv"

DEFAULT_START_DATE = "2025-01-02"
DEFAULT_END_DATE = "2025-12-31"
INITIAL_CAPITAL = 200_000.0
MAX_POSITION_CASH = 100_000.0
BUY_COST_RATE = 0.0005
SELL_COST_RATE = 0.0015
SWITCH_BUFFER = 0.05
MAX_HOLDINGS = 2
LOT_SIZE = 100

CODE_RE = re.compile(r"(\d{6})")


@dataclass
class Position:
    code: str
    name: str
    shares: int
    cost_price: float
    last_price: float
    score: float
    score_date: str


@dataclass(frozen=True)
class PendingOrder:
    action: str
    code: str
    name: str
    score: float
    decision_date: str
    reason: str
    switch_id: str = ""
    threshold_score: Optional[float] = None


@dataclass(frozen=True)
class Trade:
    trade_date: str
    action: str
    code: str
    name: str
    price: float
    shares: int
    gross_amount: float
    friction_cost: float
    cash_after: float
    position_count_after: int
    reason: str
    decision_date: str
    score: Optional[float] = None
    threshold_score: Optional[float] = None


@dataclass
class Account:
    cash: float
    positions: dict[str, Position]
    total_friction_cost: float = 0.0
    buy_count: int = 0
    sell_count: int = 0
    switch_count: int = 0
    ma20_stop_count: int = 0
    skipped_order_count: int = 0


def normalize_code(value: Any) -> str:
    text = str(value or "")
    match = CODE_RE.search(text)
    return match.group(1) if match else text.zfill(6)[-6:]


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def finite_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def read_panel(dataset_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    if not dataset_path.exists():
        raise FileNotFoundError(f"V5.7 5m 宽表不存在：{dataset_path}")

    header = pd.read_csv(dataset_path, nrows=0).columns.tolist()
    required = {"code", "date", "open", "close", "tail_accel", "intra_volatility"}
    missing = sorted(required - set(header))
    if missing:
        raise ValueError(f"V5.7 5m 宽表缺少必要字段：{missing}")

    optional = [
        "name",
        "entry_price",
        "buy5m_entry_price",
        "ma20_bias",
        "ma20",
        "MA20",
    ]
    usecols = [col for col in [*required, *optional] if col in header]
    frame = pd.read_csv(
        dataset_path,
        usecols=usecols,
        dtype={"code": "string"},
        low_memory=False,
    )
    frame["code"] = frame["code"].map(normalize_code)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame = frame.dropna(subset=["code", "date"]).copy()
    frame = frame[(frame["date"] >= start_date) & (frame["date"] <= end_date)].copy()
    if frame.empty:
        raise SystemExit(f"区间 {start_date} -> {end_date} 在宽表中没有记录。")

    if "name" not in frame.columns:
        frame["name"] = frame["code"]
    else:
        frame["name"] = frame["name"].fillna(frame["code"]).astype(str)

    numeric_cols = [
        "open",
        "close",
        "tail_accel",
        "intra_volatility",
        "entry_price",
        "buy5m_entry_price",
        "ma20_bias",
        "ma20",
        "MA20",
    ]
    for col in numeric_cols:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame["micro_score"] = frame["tail_accel"] * 0.6 - frame["intra_volatility"] * 0.4
    frame["ma_check_price"] = pd.Series(np.nan, index=frame.index, dtype="float64")
    for col in ("buy5m_entry_price", "entry_price", "close"):
        if col in frame.columns:
            frame["ma_check_price"] = frame["ma_check_price"].combine_first(frame[col])

    if "ma20" in frame.columns:
        frame["ma20_line"] = frame["ma20"]
    elif "MA20" in frame.columns:
        frame["ma20_line"] = frame["MA20"]
    elif "ma20_bias" in frame.columns:
        denom = 1.0 + frame["ma20_bias"] / 100.0
        frame["ma20_line"] = np.where(denom > 0, frame["close"] / denom, np.nan)
    else:
        frame["ma20_line"] = np.nan

    frame = frame.replace([np.inf, -np.inf], np.nan)
    frame = frame.dropna(subset=["micro_score"]).copy()
    frame = frame.drop_duplicates(["date", "code"], keep="last")
    frame = frame.sort_values(["date", "micro_score", "code"], ascending=[True, False, True])
    return frame.reset_index(drop=True)


def row_price(row: pd.Series, column: str) -> Optional[float]:
    value = finite_float(row.get(column))
    if value is None or value <= 0:
        return None
    return value


def buy_position(
    account: Account,
    order: PendingOrder,
    price: float,
    max_position_cash: float,
    buy_cost_rate: float,
) -> Optional[Trade]:
    if len(account.positions) >= MAX_HOLDINGS or order.code in account.positions:
        account.skipped_order_count += 1
        return None
    budget = min(float(max_position_cash), account.cash)
    shares = int((budget / (price * (1.0 + buy_cost_rate))) // LOT_SIZE) * LOT_SIZE
    if shares <= 0:
        account.skipped_order_count += 1
        return None

    gross = float(shares * price)
    friction = gross * buy_cost_rate
    total_cost = gross + friction
    if total_cost > account.cash + 1e-6:
        account.skipped_order_count += 1
        return None

    account.cash -= total_cost
    account.total_friction_cost += friction
    account.buy_count += 1
    account.positions[order.code] = Position(
        code=order.code,
        name=order.name,
        shares=shares,
        cost_price=price,
        last_price=price,
        score=float(order.score),
        score_date=order.decision_date,
    )
    return Trade(
        trade_date="",
        action="BUY",
        code=order.code,
        name=order.name,
        price=round(price, 4),
        shares=int(shares),
        gross_amount=round(gross, 2),
        friction_cost=round(friction, 2),
        cash_after=round(account.cash, 2),
        position_count_after=len(account.positions),
        reason=order.reason,
        decision_date=order.decision_date,
        score=round(float(order.score), 8),
        threshold_score=round(float(order.threshold_score), 8) if order.threshold_score is not None else None,
    )


def sell_position(
    account: Account,
    position: Position,
    price: float,
    sell_cost_rate: float,
    reason: str,
    decision_date: str,
    threshold_score: Optional[float] = None,
) -> Trade:
    shares = int(position.shares)
    gross = float(shares * price)
    friction = gross * sell_cost_rate
    proceeds = gross - friction
    account.cash += proceeds
    account.total_friction_cost += friction
    account.sell_count += 1
    if reason == "SWITCH_SELL":
        account.switch_count += 1
    if reason == "MA20_STOP":
        account.ma20_stop_count += 1
    del account.positions[position.code]
    return Trade(
        trade_date="",
        action="SELL",
        code=position.code,
        name=position.name,
        price=round(price, 4),
        shares=shares,
        gross_amount=round(gross, 2),
        friction_cost=round(friction, 2),
        cash_after=round(account.cash, 2),
        position_count_after=len(account.positions),
        reason=reason,
        decision_date=decision_date,
        score=round(float(position.score), 8) if math.isfinite(position.score) else None,
        threshold_score=round(float(threshold_score), 8) if threshold_score is not None else None,
    )


def with_trade_date(trade: Trade, trade_date: str) -> dict[str, Any]:
    row = asdict(trade)
    row["trade_date"] = trade_date
    return row


def execute_pending_orders(
    account: Account,
    pending_orders: list[PendingOrder],
    trade_date: str,
    by_code: pd.DataFrame,
    buy_cost_rate: float,
    sell_cost_rate: float,
    max_position_cash: float,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    executed_switch_sells: set[str] = set()
    sell_orders = [order for order in pending_orders if order.action == "SELL"]
    buy_orders = [order for order in pending_orders if order.action == "BUY"]

    for order in sell_orders:
        position = account.positions.get(order.code)
        if position is None:
            account.skipped_order_count += 1
            continue
        if order.code not in by_code.index:
            account.skipped_order_count += 1
            continue
        price = row_price(by_code.loc[order.code], "open")
        if price is None:
            account.skipped_order_count += 1
            continue
        trade = sell_position(
            account=account,
            position=position,
            price=price,
            sell_cost_rate=sell_cost_rate,
            reason=order.reason,
            decision_date=order.decision_date,
            threshold_score=order.threshold_score,
        )
        trades.append(with_trade_date(trade, trade_date))
        if order.switch_id:
            executed_switch_sells.add(order.switch_id)

    for order in buy_orders:
        if order.switch_id and order.switch_id not in executed_switch_sells:
            account.skipped_order_count += 1
            continue
        if order.code not in by_code.index:
            account.skipped_order_count += 1
            continue
        price = row_price(by_code.loc[order.code], "open")
        if price is None:
            account.skipped_order_count += 1
            continue
        trade = buy_position(
            account=account,
            order=order,
            price=price,
            max_position_cash=max_position_cash,
            buy_cost_rate=buy_cost_rate,
        )
        if trade is not None:
            trades.append(with_trade_date(trade, trade_date))

    return trades


def run_ma20_stops(
    account: Account,
    trade_date: str,
    by_code: pd.DataFrame,
    sell_cost_rate: float,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    for code in list(account.positions):
        position = account.positions.get(code)
        if position is None or code not in by_code.index:
            continue
        row = by_code.loc[code]
        ma_check_price = row_price(row, "ma_check_price")
        ma20_line = row_price(row, "ma20_line")
        if ma_check_price is None or ma20_line is None:
            continue
        if ma_check_price < ma20_line:
            trade = sell_position(
                account=account,
                position=position,
                price=ma_check_price,
                sell_cost_rate=sell_cost_rate,
                reason="MA20_STOP",
                decision_date=trade_date,
            )
            trades.append(with_trade_date(trade, trade_date))
    return trades


def mark_positions_to_close(account: Account, trade_date: str, by_code: pd.DataFrame) -> None:
    for code, position in list(account.positions.items()):
        if code not in by_code.index:
            continue
        row = by_code.loc[code]
        close = row_price(row, "close")
        if close is not None:
            position.last_price = close
        score = finite_float(row.get("micro_score"))
        if score is not None:
            position.score = score
            position.score_date = trade_date


def portfolio_value(account: Account) -> float:
    return float(account.cash + sum(pos.shares * pos.last_price for pos in account.positions.values()))


def build_next_orders(
    account: Account,
    trade_date: str,
    ranking: pd.DataFrame,
    switch_buffer: float,
) -> list[PendingOrder]:
    orders: list[PendingOrder] = []
    if len(account.positions) < MAX_HOLDINGS:
        planned_codes = set(account.positions)
        for row in ranking.itertuples(index=False):
            code = str(row.code)
            if code in planned_codes:
                continue
            orders.append(
                PendingOrder(
                    action="BUY",
                    code=code,
                    name=str(getattr(row, "name", "") or code),
                    score=float(row.micro_score),
                    decision_date=trade_date,
                    reason="FILL_TOP_SLOT",
                )
            )
            planned_codes.add(code)
            if len(planned_codes) >= MAX_HOLDINGS:
                break
        return orders

    worst = min(account.positions.values(), key=lambda item: item.score)
    threshold = worst.score * (1.0 + switch_buffer)
    for row in ranking.itertuples(index=False):
        code = str(row.code)
        if code in account.positions:
            continue
        candidate_score = float(row.micro_score)
        if candidate_score <= threshold:
            return orders
        switch_id = f"{trade_date}:{worst.code}->{code}"
        orders.append(
            PendingOrder(
                action="SELL",
                code=worst.code,
                name=worst.name,
                score=worst.score,
                decision_date=trade_date,
                reason="SWITCH_SELL",
                switch_id=switch_id,
                threshold_score=threshold,
            )
        )
        orders.append(
            PendingOrder(
                action="BUY",
                code=code,
                name=str(getattr(row, "name", "") or code),
                score=candidate_score,
                decision_date=trade_date,
                reason="SWITCH_BUY",
                switch_id=switch_id,
                threshold_score=threshold,
            )
        )
        return orders
    return orders


def positions_snapshot(account: Account) -> str:
    if not account.positions:
        return ""
    parts = []
    for pos in sorted(account.positions.values(), key=lambda item: item.code):
        parts.append(f"{pos.code}:{pos.shares}@{pos.last_price:.3f}/score={pos.score:.6f}")
    return ";".join(parts)


def simulate_rotation(
    panel: pd.DataFrame,
    initial_capital: float,
    max_position_cash: float,
    buy_cost_rate: float,
    sell_cost_rate: float,
    switch_buffer: float,
) -> dict[str, Any]:
    account = Account(cash=float(initial_capital), positions={})
    trade_rows: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    pending_orders: list[PendingOrder] = []
    day_count = 0

    for trade_date, day in panel.groupby("date", sort=True):
        day_count += 1
        ranking = day.sort_values(["micro_score", "code"], ascending=[False, True])
        by_code = day.set_index("code", drop=False)

        trade_rows.extend(
            execute_pending_orders(
                account=account,
                pending_orders=pending_orders,
                trade_date=str(trade_date),
                by_code=by_code,
                buy_cost_rate=buy_cost_rate,
                sell_cost_rate=sell_cost_rate,
                max_position_cash=max_position_cash,
            )
        )
        pending_orders = []

        trade_rows.extend(
            run_ma20_stops(
                account=account,
                trade_date=str(trade_date),
                by_code=by_code,
                sell_cost_rate=sell_cost_rate,
            )
        )
        mark_positions_to_close(account, str(trade_date), by_code)

        top = ranking.iloc[0]
        equity_rows.append(
            {
                "date": str(trade_date),
                "equity": round(portfolio_value(account), 2),
                "cash": round(account.cash, 2),
                "position_count": len(account.positions),
                "positions": positions_snapshot(account),
                "top_code": str(top["code"]),
                "top_name": str(top.get("name", "")),
                "top_micro_score": round(float(top["micro_score"]), 8),
                "total_friction_cost": round(account.total_friction_cost, 2),
                "buy_count": account.buy_count,
                "sell_count": account.sell_count,
                "switch_count": account.switch_count,
                "ma20_stop_count": account.ma20_stop_count,
            }
        )

        pending_orders = build_next_orders(
            account=account,
            trade_date=str(trade_date),
            ranking=ranking,
            switch_buffer=switch_buffer,
        )

    final_asset = portfolio_value(account)
    return_pct = (final_asset / initial_capital - 1.0) * 100.0
    return {
        "portfolio": {
            "initial_capital": round(float(initial_capital), 2),
            "final_asset": round(final_asset, 2),
            "total_return_pct": round(return_pct, 4),
            "cash": round(account.cash, 2),
            "holding_market_value": round(final_asset - account.cash, 2),
            "open_position_count": len(account.positions),
            "open_positions": [asdict(pos) for pos in account.positions.values()],
            "total_turnover_count": int(account.sell_count),
            "total_trade_count": int(account.buy_count + account.sell_count),
            "buy_count": int(account.buy_count),
            "sell_count": int(account.sell_count),
            "switch_count": int(account.switch_count),
            "ma20_stop_count": int(account.ma20_stop_count),
            "skipped_order_count": int(account.skipped_order_count),
            "total_friction_cost": round(float(account.total_friction_cost), 2),
        },
        "trade_rows": trade_rows,
        "equity_rows": equity_rows,
        "day_count": day_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="5m 微观特征动态轮动沙盘模拟器。")
    parser.add_argument("--dataset-path", default=str(DATASET_PATH))
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    parser.add_argument("--max-position-cash", type=float, default=MAX_POSITION_CASH)
    parser.add_argument("--buy-cost-rate", type=float, default=BUY_COST_RATE)
    parser.add_argument("--sell-cost-rate", type=float, default=SELL_COST_RATE)
    parser.add_argument("--switch-buffer", type=float, default=SWITCH_BUFFER)
    parser.add_argument("--report-path", default=str(REPORT_PATH))
    parser.add_argument("--trade-detail-path", default=str(TRADE_DETAIL_PATH))
    parser.add_argument("--equity-path", default=str(EQUITY_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    started = time.perf_counter()
    dataset_path = Path(args.dataset_path)

    print("========== 5m Micro Rotation Simulator ==========")
    print(f"Dataset          : {dataset_path}")
    print(f"Date Range       : {args.start_date} -> {args.end_date}")
    print(f"Initial Capital  : {float(args.initial_capital):,.2f}")
    print(f"Max Position Cash: {float(args.max_position_cash):,.2f}")
    print(f"Buy/Sell Cost    : {float(args.buy_cost_rate) * 100:.3f}% / {float(args.sell_cost_rate) * 100:.3f}%")
    print(f"Switch Buffer    : {float(args.switch_buffer) * 100:.2f}%")

    panel = read_panel(dataset_path, args.start_date, args.end_date)
    actual_start_date = str(panel["date"].min())
    actual_end_date = str(panel["date"].max())
    print(
        f"Panel Loaded     : rows={len(panel):,} days={panel['date'].nunique():,} "
        f"codes={panel['code'].nunique():,}"
    )
    if actual_start_date > args.start_date or actual_end_date < args.end_date:
        print(f"[Warn] 可用宽表实际覆盖: {actual_start_date} -> {actual_end_date}")
    result = simulate_rotation(
        panel=panel,
        initial_capital=float(args.initial_capital),
        max_position_cash=float(args.max_position_cash),
        buy_cost_rate=float(args.buy_cost_rate),
        sell_cost_rate=float(args.sell_cost_rate),
        switch_buffer=float(args.switch_buffer),
    )
    elapsed = time.perf_counter() - started

    report_path = Path(args.report_path)
    trade_path = Path(args.trade_detail_path)
    equity_path = Path(args.equity_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    trade_path.parent.mkdir(parents=True, exist_ok=True)
    equity_path.parent.mkdir(parents=True, exist_ok=True)

    portfolio = result["portfolio"]
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "scripts/research/simulate_rotation_micro_5m.py",
        "dataset_path": str(dataset_path),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "scoring_formula": "Micro_Score = tail_accel * 0.6 - intra_volatility * 0.4",
        "ma20_stop_note": "14:50 price uses buy5m_entry_price/entry_price fallback; MA20 is direct ma20/MA20 when present, otherwise approximated from close and ma20_bias.",
        "params": {
            "initial_capital": float(args.initial_capital),
            "max_position_cash": float(args.max_position_cash),
            "max_holdings": MAX_HOLDINGS,
            "buy_cost_rate": float(args.buy_cost_rate),
            "sell_cost_rate": float(args.sell_cost_rate),
            "round_trip_cost_rate": float(args.buy_cost_rate) + float(args.sell_cost_rate),
            "switch_buffer": float(args.switch_buffer),
            "lot_size": LOT_SIZE,
        },
        "dataset": {
            "row_count": int(len(panel)),
            "day_count": int(panel["date"].nunique()),
            "code_count": int(panel["code"].nunique()),
            "actual_start_date": actual_start_date,
            "actual_end_date": actual_end_date,
        },
        "portfolio": portfolio,
        "elapsed_seconds": round(elapsed, 3),
    }
    report_path.write_text(json.dumps(json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")
    pd.DataFrame(result["trade_rows"]).to_csv(trade_path, index=False)
    pd.DataFrame(result["equity_rows"]).to_csv(equity_path, index=False)

    print("\n---------- Rotation Health Report ----------")
    print(f"最终总资金收益率 : {portfolio['total_return_pct']:.2f}%")
    print(f"最终总资产       : {portfolio['final_asset']:,.2f}")
    print(f"总换手次数       : {portfolio['total_turnover_count']} (按持仓卖出/换仓事件计)")
    print(f"总买卖订单       : {portfolio['total_trade_count']} (buy={portfolio['buy_count']}, sell={portfolio['sell_count']})")
    print(f"摩擦成本消耗     : {portfolio['total_friction_cost']:,.2f}")
    print(f"MA20 强平次数    : {portfolio['ma20_stop_count']}")
    print(f"阈值换股次数     : {portfolio['switch_count']}")
    print(f"未成交/跳过订单  : {portfolio['skipped_order_count']}")
    print(f"Report Path      : {report_path}")
    print(f"Trade Detail     : {trade_path}")
    print(f"Equity Curve     : {equity_path}")
    print(f"Elapsed Seconds  : {elapsed:.3f}")
    print("================================================\n")


if __name__ == "__main__":
    main()
