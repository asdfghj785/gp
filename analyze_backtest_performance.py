#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, DefaultDict, Iterable, Optional

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "data" / "core_db" / "quant_workstation.sqlite3"
DEFAULT_OUTPUT_PATH = BASE_DIR / "equity_curve.png"
DEFAULT_START_DATE = date(2024, 4, 8)
DEFAULT_INITIAL_CASH = 30000.0
DEFAULT_COST_RATE = 0.002
STOP_LOSS_TRIGGER_PCT = -3.0
STOP_LOSS_EXIT_PCT = -3.5
EPSILON = 1e-8


@dataclass
class Pick:
    id: int
    pick_date: date
    exit_date: date
    code: str
    name: str
    strategy_type: str
    tier: str
    position_pct: float
    entry_price: float
    t3_close_return_pct: float
    close_return_source: str
    win_rate: Optional[float]


@dataclass
class Position:
    pick: Pick
    principal: float
    buy_date: date
    exit_date: date


@dataclass
class ClosedTrade:
    pick: Pick
    principal: float
    proceeds: float
    pnl: float
    cost: float
    gross_return_pct: float
    net_return_pct: float
    exit_reason: str
    intraday_low_pct: Optional[float]
    buy_date: date
    exit_date: date


def main() -> None:
    args = parse_args()
    db_path = Path(args.db).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date) if args.end_date else date.today()
    initial_cash = float(args.initial_cash)
    cost_rate = float(args.cost_rate)

    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    picks, load_stats = load_picks(db_path, start_date, end_date)
    trading_days = load_trading_days(db_path, start_date, end_date, picks)
    if not trading_days:
        raise RuntimeError("No trading days found in stock_daily/daily_picks for the requested range.")
    price_lows = load_price_lows(db_path, {pick.code for pick in picks}, trading_days[0], trading_days[-1])

    result = run_broker_simulation(
        picks=picks,
        trading_days=trading_days,
        price_lows=price_lows,
        initial_cash=initial_cash,
        cost_rate=cost_rate,
    )
    metrics = calculate_metrics(result, initial_cash)
    render_report(
        db_path=db_path,
        output_path=output_path,
        start_date=start_date,
        requested_end_date=end_date,
        trading_days=trading_days,
        load_stats=load_stats,
        result=result,
        metrics=metrics,
        initial_cash=initial_cash,
        cost_rate=cost_rate,
    )
    plot_equity_curve(result["equity_curve"], output_path, metrics)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="V4.4 daily_picks broker-style compounding backtest from local SQLite."
    )
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to quant_workstation.sqlite3.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat(), help="Backtest start date.")
    parser.add_argument("--end-date", default=None, help="Backtest end date. Defaults to today.")
    parser.add_argument("--initial-cash", type=float, default=DEFAULT_INITIAL_CASH, help="Initial account cash.")
    parser.add_argument(
        "--cost-rate",
        type=float,
        default=DEFAULT_COST_RATE,
        help="Round-trip friction rate charged on every completed trade.",
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="PNG path for equity curve.")
    return parser.parse_args()


def load_picks(db_path: Path, start_date: date, end_date: date) -> tuple[list[Pick], dict[str, Any]]:
    rows = query_rows(
        db_path,
        """
        SELECT id, selection_date, target_date, code, name, strategy_type, tier,
               win_rate, selection_price, snapshot_price, close_return_pct, close_reason,
               suggested_position, raw_json
        FROM daily_picks
        WHERE selection_date >= ? AND selection_date <= ?
        ORDER BY selection_date ASC, id ASC
        """,
        (start_date.isoformat(), end_date.isoformat()),
    )

    picks: list[Pick] = []
    stats: dict[str, Any] = {
        "raw_rows": len(rows),
        "loaded_rows": 0,
        "skipped_bad_date": 0,
        "skipped_missing_price": 0,
        "skipped_missing_position": 0,
        "skipped_missing_return": 0,
        "return_sources": defaultdict(int),
    }

    for row in rows:
        raw = parse_json(row.get("raw_json"))
        winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
        pick_date = optional_iso_date(row.get("selection_date"))
        if pick_date is None:
            stats["skipped_bad_date"] += 1
            continue

        exit_date = optional_iso_date(winner.get("t3_exit_date")) or optional_iso_date(row.get("target_date"))
        if exit_date is None:
            stats["skipped_bad_date"] += 1
            continue
        if exit_date < pick_date:
            exit_date = pick_date

        position_pct = normalize_position_pct(
            first_not_none(row.get("suggested_position"), winner.get("suggested_position"))
        )
        if position_pct is None or position_pct <= 0:
            stats["skipped_missing_position"] += 1
            continue

        entry_price = safe_float(first_not_none(row.get("snapshot_price"), row.get("selection_price"), winner.get("price")))
        if entry_price is None or entry_price <= 0:
            stats["skipped_missing_price"] += 1
            continue

        t3_close_return_pct, return_source = extract_t3_close_return_pct(row, winner)
        if t3_close_return_pct is None:
            stats["skipped_missing_return"] += 1
            continue

        pick = Pick(
            id=int(row["id"]),
            pick_date=pick_date,
            exit_date=exit_date,
            code=str(row.get("code") or "").zfill(6),
            name=str(row.get("name") or winner.get("name") or ""),
            strategy_type=str(row.get("strategy_type") or winner.get("strategy_type") or "unknown"),
            tier=str(row.get("tier") or winner.get("selection_tier") or ""),
            position_pct=position_pct,
            entry_price=float(entry_price),
            t3_close_return_pct=float(t3_close_return_pct),
            close_return_source=return_source,
            win_rate=safe_float(row.get("win_rate")),
        )
        stats["loaded_rows"] += 1
        stats["return_sources"][return_source] += 1
        picks.append(pick)

    stats["return_sources"] = dict(stats["return_sources"])
    return picks, stats


def extract_t3_close_return_pct(row: dict[str, Any], winner: dict[str, Any]) -> tuple[Optional[float], str]:
    raw_value = safe_float(winner.get("t3_close_return_pct"))
    if raw_value is not None:
        return raw_value, "raw_json.winner.t3_close_return_pct"

    raw = parse_json(row.get("raw_json"))
    close_signal = raw.get("close_signal") if isinstance(raw.get("close_signal"), dict) else {}
    signal_value = safe_float(close_signal.get("t3_close_return_pct"))
    if signal_value is not None:
        return signal_value, "raw_json.close_signal.t3_close_return_pct"

    table_value = safe_float(row.get("close_return_pct"))
    close_reason = str(row.get("close_reason") or "")
    target_date = optional_iso_date(row.get("target_date"))
    t3_exit_date = optional_iso_date(winner.get("t3_exit_date"))
    if table_value is not None and ("T+3" in close_reason or (t3_exit_date is not None and target_date == t3_exit_date)):
        return table_value, "daily_picks.close_return_pct"

    return None, "missing"


def load_trading_days(db_path: Path, start_date: date, end_date: date, picks: Iterable[Pick]) -> list[date]:
    stock_rows = query_rows(
        db_path,
        """
        SELECT DISTINCT date
        FROM stock_daily
        WHERE date >= ? AND date <= ?
        ORDER BY date ASC
        """,
        (start_date.isoformat(), end_date.isoformat()),
    )
    dates = {parsed for row in stock_rows if (parsed := optional_iso_date(row.get("date"))) is not None}
    for pick in picks:
        if start_date <= pick.pick_date <= end_date:
            dates.add(pick.pick_date)
        if start_date <= pick.exit_date <= end_date:
            dates.add(pick.exit_date)

    if not dates:
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                dates.add(current)
            current += timedelta(days=1)

    return sorted(dates)


def load_price_lows(db_path: Path, codes: set[str], start_date: date, end_date: date) -> dict[tuple[str, date], float]:
    if not codes:
        return {}

    lows: dict[tuple[str, date], float] = {}
    sorted_codes = sorted(codes)
    for code_chunk in chunks(sorted_codes, 500):
        placeholders = ",".join("?" for _ in code_chunk)
        rows = query_rows(
            db_path,
            f"""
            SELECT code, date, low
            FROM stock_daily
            WHERE code IN ({placeholders})
              AND date >= ?
              AND date <= ?
              AND low IS NOT NULL
            """,
            (*code_chunk, start_date.isoformat(), end_date.isoformat()),
        )
        for row in rows:
            trade_date = optional_iso_date(row.get("date"))
            low = safe_float(row.get("low"))
            if trade_date is not None and low is not None and low > 0:
                lows[(str(row.get("code") or "").zfill(6), trade_date)] = low
    return lows


def run_broker_simulation(
    picks: list[Pick],
    trading_days: list[date],
    price_lows: dict[tuple[str, date], float],
    initial_cash: float,
    cost_rate: float,
) -> dict[str, Any]:
    picks_by_date: DefaultDict[date, list[Pick]] = defaultdict(list)
    for pick in picks:
        picks_by_date[pick.pick_date].append(pick)
    for day_picks in picks_by_date.values():
        day_picks.sort(key=lambda item: item.id)

    available_cash = initial_cash
    open_positions: list[Position] = []
    closed_trades: list[ClosedTrade] = []
    buy_ledger: list[dict[str, Any]] = []
    skipped_orders: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    previous_equity = initial_cash

    for current_day in trading_days:
        released_today, open_positions = settle_open_positions(current_day, open_positions, price_lows, cost_rate)
        realized_pnl = 0.0
        realized_cost = 0.0
        for trade in released_today:
            available_cash += trade.proceeds
            realized_pnl += trade.pnl
            realized_cost += trade.cost
            closed_trades.append(trade)

        equity_before_buys = available_cash + sum_locked_principal(open_positions)
        bought_amount = 0.0
        orders = picks_by_date.get(current_day, [])

        for pick in orders:
            if available_cash <= EPSILON:
                skipped_orders.append(
                    {
                        "date": current_day.isoformat(),
                        "id": pick.id,
                        "code": pick.code,
                        "strategy_type": pick.strategy_type,
                        "reason": "available_cash_zero",
                        "wanted_amount": equity_before_buys * pick.position_pct,
                    }
                )
                continue

            wanted_amount = equity_before_buys * pick.position_pct
            buy_amount = min(wanted_amount, available_cash)
            if buy_amount <= EPSILON:
                skipped_orders.append(
                    {
                        "date": current_day.isoformat(),
                        "id": pick.id,
                        "code": pick.code,
                        "strategy_type": pick.strategy_type,
                        "reason": "buy_amount_zero",
                        "wanted_amount": wanted_amount,
                    }
                )
                continue

            available_cash -= buy_amount
            bought_amount += buy_amount
            open_positions.append(
                Position(pick=pick, principal=buy_amount, buy_date=current_day, exit_date=pick.exit_date)
            )
            buy_ledger.append(
                {
                    "date": current_day.isoformat(),
                    "exit_date": pick.exit_date.isoformat(),
                    "id": pick.id,
                    "code": pick.code,
                    "name": pick.name,
                    "strategy_type": pick.strategy_type,
                    "tier": pick.tier,
                    "position_pct": pick.position_pct,
                    "wanted_amount": wanted_amount,
                    "buy_amount": buy_amount,
                    "cash_limited": buy_amount + EPSILON < wanted_amount,
                    "entry_price": pick.entry_price,
                    "t3_close_return_pct": pick.t3_close_return_pct,
                    "return_source": pick.close_return_source,
                }
            )

        locked_principal = sum_locked_principal(open_positions)
        total_equity = available_cash + locked_principal
        equity_curve.append(
            {
                "date": current_day,
                "available_cash": available_cash,
                "locked_capital": locked_principal,
                "total_equity": total_equity,
                "daily_pnl": total_equity - previous_equity,
                "realized_pnl": realized_pnl,
                "realized_cost": realized_cost,
                "bought_amount": bought_amount,
                "released_trades": len(released_today),
                "orders": len(orders),
                "open_positions": len(open_positions),
            }
        )
        previous_equity = total_equity

    return {
        "equity_curve": equity_curve,
        "closed_trades": closed_trades,
        "buy_ledger": buy_ledger,
        "skipped_orders": skipped_orders,
        "open_positions": open_positions,
        "available_cash": available_cash,
        "locked_capital": sum_locked_principal(open_positions),
        "price_low_rows": len(price_lows),
    }


def settle_open_positions(
    current_day: date,
    open_positions: list[Position],
    price_lows: dict[tuple[str, date], float],
    cost_rate: float,
) -> tuple[list[ClosedTrade], list[Position]]:
    closed: list[ClosedTrade] = []
    remaining: list[Position] = []
    for position in open_positions:
        if current_day <= position.buy_date:
            remaining.append(position)
            continue

        low_pct = intraday_low_pct(position.pick, current_day, price_lows)
        if low_pct is not None and low_pct <= STOP_LOSS_TRIGGER_PCT:
            closed.append(close_position(position, current_day, STOP_LOSS_EXIT_PCT, "stop_loss", cost_rate, low_pct))
            continue

        if current_day >= position.exit_date:
            closed.append(
                close_position(
                    position,
                    position.exit_date,
                    position.pick.t3_close_return_pct,
                    "t3_close",
                    cost_rate,
                    low_pct,
                )
            )
            continue

        remaining.append(position)
    return closed, remaining


def intraday_low_pct(
    pick: Pick,
    current_day: date,
    price_lows: dict[tuple[str, date], float],
) -> Optional[float]:
    low = price_lows.get((pick.code, current_day))
    if low is None or pick.entry_price <= 0:
        return None
    return (low / pick.entry_price - 1.0) * 100.0


def close_position(
    position: Position,
    exit_date: date,
    gross_return_pct: float,
    exit_reason: str,
    cost_rate: float,
    intraday_low_pct_value: Optional[float],
) -> ClosedTrade:
    net_return_pct = gross_return_pct - cost_rate * 100.0
    pnl = position.principal * net_return_pct / 100.0
    cost = position.principal * cost_rate
    proceeds = position.principal + pnl
    return ClosedTrade(
        pick=position.pick,
        principal=position.principal,
        proceeds=proceeds,
        pnl=pnl,
        cost=cost,
        gross_return_pct=gross_return_pct,
        net_return_pct=net_return_pct,
        exit_reason=exit_reason,
        intraday_low_pct=intraday_low_pct_value,
        buy_date=position.buy_date,
        exit_date=exit_date,
    )


def calculate_metrics(result: dict[str, Any], initial_cash: float) -> dict[str, Any]:
    curve = result["equity_curve"]
    final_equity = curve[-1]["total_equity"] if curve else initial_cash
    cumulative_return = final_equity / initial_cash - 1.0

    peak = -math.inf
    max_drawdown = 0.0
    max_drawdown_date: Optional[date] = None
    for row in curve:
        equity = float(row["total_equity"])
        if equity > peak:
            peak = equity
        drawdown = equity / peak - 1.0 if peak > 0 else 0.0
        row["drawdown"] = drawdown
        if drawdown < max_drawdown:
            max_drawdown = drawdown
            max_drawdown_date = row["date"]

    daily_pnls = [float(row["daily_pnl"]) for row in curve]
    win_pnls = [value for value in daily_pnls if value > EPSILON]
    loss_pnls = [value for value in daily_pnls if value < -EPSILON]
    flat_days = len(daily_pnls) - len(win_pnls) - len(loss_pnls)

    closed_trades: list[ClosedTrade] = result["closed_trades"]
    trade_wins = [trade for trade in closed_trades if trade.pnl > EPSILON]
    trade_losses = [trade for trade in closed_trades if trade.pnl < -EPSILON]
    stop_loss_trades = [trade for trade in closed_trades if trade.exit_reason == "stop_loss"]
    t3_close_trades = [trade for trade in closed_trades if trade.exit_reason == "t3_close"]
    strategy_stats = build_strategy_stats(closed_trades)
    start_day = curve[0]["date"] if curve else None
    end_day = curve[-1]["date"] if curve else None
    years = max(((end_day - start_day).days / 365.25) if start_day and end_day and end_day > start_day else 0.0, 0.0)
    cagr = (final_equity / initial_cash) ** (1.0 / years) - 1.0 if years > 0 and final_equity > 0 else 0.0

    return {
        "final_equity": final_equity,
        "cumulative_return": cumulative_return,
        "cagr": cagr,
        "max_drawdown": max_drawdown,
        "max_drawdown_date": max_drawdown_date,
        "daily_win_count": len(win_pnls),
        "daily_loss_count": len(loss_pnls),
        "daily_flat_count": flat_days,
        "daily_win_rate": len(win_pnls) / (len(win_pnls) + len(loss_pnls)) if (win_pnls or loss_pnls) else 0.0,
        "daily_profit_loss_ratio": average(win_pnls) / abs(average(loss_pnls)) if win_pnls and loss_pnls else math.inf,
        "daily_profit_factor": sum(win_pnls) / abs(sum(loss_pnls)) if loss_pnls else math.inf,
        "avg_win_day_pnl": average(win_pnls),
        "avg_loss_day_pnl": average(loss_pnls),
        "closed_trade_count": len(closed_trades),
        "stop_loss_count": len(stop_loss_trades),
        "t3_close_count": len(t3_close_trades),
        "trade_win_count": len(trade_wins),
        "trade_loss_count": len(trade_losses),
        "trade_win_rate": len(trade_wins) / len(closed_trades) if closed_trades else 0.0,
        "avg_trade_net_return_pct": average([trade.net_return_pct for trade in closed_trades]),
        "total_realized_pnl": sum(trade.pnl for trade in closed_trades),
        "total_cost": sum(trade.cost for trade in closed_trades),
        "strategy_stats": strategy_stats,
    }


def build_strategy_stats(closed_trades: list[ClosedTrade]) -> list[dict[str, Any]]:
    grouped: DefaultDict[str, list[ClosedTrade]] = defaultdict(list)
    for trade in closed_trades:
        grouped[trade.pick.strategy_type].append(trade)

    rows: list[dict[str, Any]] = []
    for strategy_type, trades in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        wins = [trade for trade in trades if trade.pnl > EPSILON]
        losses = [trade for trade in trades if trade.pnl < -EPSILON]
        rows.append(
            {
                "strategy_type": strategy_type,
                "trades": len(trades),
                "wins": len(wins),
                "losses": len(losses),
                "stop_loss": sum(1 for trade in trades if trade.exit_reason == "stop_loss"),
                "t3_close": sum(1 for trade in trades if trade.exit_reason == "t3_close"),
                "win_rate": len(wins) / len(trades) if trades else 0.0,
                "avg_net_return_pct": average([trade.net_return_pct for trade in trades]),
                "pnl": sum(trade.pnl for trade in trades),
                "invested": sum(trade.principal for trade in trades),
            }
        )
    return rows


def render_report(
    db_path: Path,
    output_path: Path,
    start_date: date,
    requested_end_date: date,
    trading_days: list[date],
    load_stats: dict[str, Any],
    result: dict[str, Any],
    metrics: dict[str, Any],
    initial_cash: float,
    cost_rate: float,
) -> None:
    buy_ledger = result["buy_ledger"]
    skipped_orders = result["skipped_orders"]
    open_positions = result["open_positions"]
    first_trade_day = buy_ledger[0]["date"] if buy_ledger else "-"
    last_trade_day = buy_ledger[-1]["date"] if buy_ledger else "-"
    cash_limited_count = sum(1 for item in buy_ledger if item["cash_limited"])

    print("")
    print("=" * 78)
    print("V4.4 复利资金曲线回测 - Broker Simulation")
    print("=" * 78)
    print(f"SQLite: {db_path}")
    print(f"区间: {start_date.isoformat()} -> {requested_end_date.isoformat()}，实际交易日止于 {trading_days[-1].isoformat()}")
    print(f"交易日数量: {len(trading_days)}")
    print(f"出票记录: 原始 {load_stats['raw_rows']} 条，账务可用 {load_stats['loaded_rows']} 条")
    print(
        "跳过记录: "
        f"日期异常 {load_stats['skipped_bad_date']}，"
        f"价格缺失 {load_stats['skipped_missing_price']}，"
        f"仓位缺失 {load_stats['skipped_missing_position']}，"
        f"T+3收盘收益缺失 {load_stats['skipped_missing_return']}"
    )
    print(f"T+3收盘收益字段来源: {load_stats['return_sources']}")
    print(f"低价止损数据行: {result['price_low_rows']}")
    print(f"硬止损: 低点 <= {STOP_LOSS_TRIGGER_PCT:.1f}% 时按 {STOP_LOSS_EXIT_PCT:.1f}% 毛收益提前出局")
    print(f"交易摩擦成本: 每笔闭环扣除 {cost_rate * 100:.3f}%")
    print("")
    print("核心账务结果")
    print("-" * 78)
    print(f"起始 3W -> 最终 {metrics['final_equity'] / 10000:.4f}W")
    print(f"初始本金: {format_money(initial_cash)}")
    print(f"最终净值: {format_money(metrics['final_equity'])}")
    print(f"累计收益率: {format_pct(metrics['cumulative_return'])}")
    print(f"年化收益率: {format_pct(metrics['cagr'])}")
    print(
        "最大回撤: "
        f"{format_pct(metrics['max_drawdown'])}"
        f" @ {metrics['max_drawdown_date'].isoformat() if metrics['max_drawdown_date'] else '-'}"
    )
    print(f"已实现净损益: {format_money(metrics['total_realized_pnl'])}")
    print(f"总交易摩擦成本: {format_money(metrics['total_cost'])}")
    print(f"期末可用现金: {format_money(result['available_cash'])}")
    print(f"期末锁定本金: {format_money(result['locked_capital'])}，未到期持仓 {len(open_positions)} 笔")
    print("")
    print("交易执行")
    print("-" * 78)
    print(f"首笔买入: {first_trade_day}，末笔买入: {last_trade_day}")
    print(f"成交买入: {len(buy_ledger)} 笔，现金不足被压缩: {cash_limited_count} 笔，现金为零跳过: {len(skipped_orders)} 笔")
    print(
        f"闭环卖出: {metrics['closed_trade_count']} 笔，"
        f"硬止损 {metrics['stop_loss_count']} 笔，T+3收盘 {metrics['t3_close_count']} 笔"
    )
    print(
        "交易胜率: "
        f"{metrics['trade_win_count']}/{metrics['closed_trade_count']} = {format_pct(metrics['trade_win_rate'])}"
    )
    print(f"单笔平均净收益率: {format_pct(metrics['avg_trade_net_return_pct'] / 100.0)}")
    print("")
    print("账户级每日损益")
    print("-" * 78)
    print(
        "日胜率: "
        f"{metrics['daily_win_count']}/{metrics['daily_win_count'] + metrics['daily_loss_count']} "
        f"= {format_pct(metrics['daily_win_rate'])}，平盘日 {metrics['daily_flat_count']}"
    )
    print(f"平均盈利日: {format_money(metrics['avg_win_day_pnl'])}")
    print(f"平均亏损日: {format_money(metrics['avg_loss_day_pnl'])}")
    print(f"日盈亏比: {format_ratio(metrics['daily_profit_loss_ratio'])}")
    print(f"日利润因子: {format_ratio(metrics['daily_profit_factor'])}")
    print("")
    print("分策略闭环表现")
    print("-" * 78)
    print(f"{'策略':<14} {'笔数':>6} {'止损':>6} {'T+3':>6} {'胜率':>10} {'均净收益':>12} {'净损益':>14} {'投入本金':>14}")
    for row in metrics["strategy_stats"]:
        print(
            f"{row['strategy_type']:<14} "
            f"{row['trades']:>6} "
            f"{row['stop_loss']:>6} "
            f"{row['t3_close']:>6} "
            f"{format_pct(row['win_rate']):>10} "
            f"{format_pct(row['avg_net_return_pct'] / 100.0):>12} "
            f"{format_money(row['pnl']):>14} "
            f"{format_money(row['invested']):>14}"
        )
    print("")
    print(f"曲线图已生成: {output_path}")
    print("=" * 78)
    print("")


def plot_equity_curve(equity_curve: list[dict[str, Any]], output_path: Path, metrics: dict[str, Any]) -> None:
    if not equity_curve:
        raise RuntimeError("No equity curve rows to plot.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    dates = [row["date"] for row in equity_curve]
    equity_wan = [row["total_equity"] / 10000.0 for row in equity_curve]
    drawdowns = [row.get("drawdown", 0.0) * 100.0 for row in equity_curve]

    plt.rcParams["font.sans-serif"] = ["PingFang SC", "Heiti TC", "Arial Unicode MS", "SimHei", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    fig, (ax_equity, ax_dd) = plt.subplots(
        2,
        1,
        figsize=(14, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [3, 1]},
    )

    ax_equity.plot(dates, equity_wan, color="#d62728", linewidth=2.0, label="Total Equity")
    ax_equity.fill_between(dates, equity_wan, min(equity_wan), color="#d62728", alpha=0.08)
    ax_equity.set_title("V4.4 复利资金曲线：3万元本金 Broker Simulation", fontsize=15, pad=12)
    ax_equity.set_ylabel("账户净值（万元）")
    ax_equity.grid(True, linestyle="--", linewidth=0.6, alpha=0.35)
    ax_equity.legend(loc="upper left")
    ax_equity.text(
        0.01,
        0.96,
        f"Final: {metrics['final_equity'] / 10000:.4f}W | Return: {metrics['cumulative_return'] * 100:.2f}% | MDD: {metrics['max_drawdown'] * 100:.2f}%",
        transform=ax_equity.transAxes,
        va="top",
        ha="left",
        fontsize=10,
        bbox={"boxstyle": "round,pad=0.35", "facecolor": "white", "edgecolor": "#cccccc", "alpha": 0.85},
    )

    ax_dd.fill_between(dates, drawdowns, 0, color="#1f77b4", alpha=0.28)
    ax_dd.plot(dates, drawdowns, color="#1f77b4", linewidth=1.0)
    ax_dd.set_ylabel("回撤 %")
    ax_dd.set_xlabel("交易日")
    ax_dd.grid(True, linestyle="--", linewidth=0.6, alpha=0.35)
    ax_dd.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=6, maxticks=12))
    ax_dd.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax_dd.xaxis.get_major_locator()))

    fig.tight_layout()
    fig.savefig(output_path, dpi=180)
    plt.close(fig)


def query_rows(db_path: Path, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(sql, params).fetchall()]


def parse_json(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def parse_iso_date(value: str) -> date:
    parsed = optional_iso_date(value)
    if parsed is None:
        raise ValueError(f"Invalid date: {value}")
    return parsed


def optional_iso_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text[:10]).date()
    except ValueError:
        return None


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def normalize_position_pct(value: Any) -> Optional[float]:
    parsed = safe_float(value)
    if parsed is None or parsed <= 0:
        return None
    if parsed > 1.0:
        parsed /= 100.0
    return min(parsed, 1.0)


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def sum_locked_principal(open_positions: Iterable[Position]) -> float:
    return sum(position.principal for position in open_positions)


def chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def average(values: Iterable[float]) -> float:
    items = list(values)
    return sum(items) / len(items) if items else 0.0


def format_money(value: float) -> str:
    return f"{value:,.2f}"


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def format_ratio(value: float) -> str:
    if math.isinf(value):
        return "inf"
    return f"{value:.3f}"


if __name__ == "__main__":
    main()
