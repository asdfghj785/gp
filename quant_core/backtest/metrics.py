from __future__ import annotations

from math import sqrt
from typing import Any

import numpy as np
import pandas as pd


def calculate_metrics(
    equity_curve: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    initial_cash: float,
    periods_per_year: int = 252 * 48,
) -> dict[str, float | int]:
    if not equity_curve:
        return _empty_metrics()
    curve = pd.DataFrame(equity_curve).drop_duplicates(subset=["timestamp"], keep="last")
    curve = curve.sort_values("timestamp")
    equity = pd.to_numeric(curve["equity"], errors="coerce").dropna()
    if equity.empty:
        return _empty_metrics()

    ending = float(equity.iloc[-1])
    total_return = ending / float(initial_cash) - 1
    bar_returns = equity.pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    elapsed_periods = max(1, len(equity) - 1)
    annualized_return = (1 + total_return) ** (periods_per_year / elapsed_periods) - 1 if total_return > -1 else -1.0
    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    max_drawdown = float(drawdown.min()) if not drawdown.empty else 0.0
    sharpe = 0.0
    if len(bar_returns) > 1 and float(bar_returns.std()) > 0:
        sharpe = float(bar_returns.mean() / bar_returns.std() * sqrt(periods_per_year))
    closed_trades = [trade for trade in trades if "pnl" in trade]
    wins = [trade for trade in closed_trades if float(trade.get("pnl", 0)) > 0]
    win_rate = len(wins) / len(closed_trades) if closed_trades else 0.0

    return {
        "total_return_pct": total_return * 100,
        "annualized_return_pct": annualized_return * 100,
        "max_drawdown_pct": max_drawdown * 100,
        "sharpe_ratio": sharpe,
        "win_rate_pct": win_rate * 100,
        "trade_count": len(closed_trades),
        "ending_equity": ending,
    }


def print_tearsheet(metrics: dict[str, float | int]) -> None:
    print("\n========== Event-Driven Backtest Tearsheet ==========")
    print(f"Total Return       : {metrics['total_return_pct']:.2f}%")
    print(f"Annualized Return  : {metrics['annualized_return_pct']:.2f}%")
    print(f"Maximum Drawdown   : {metrics['max_drawdown_pct']:.2f}%")
    print(f"Sharpe Ratio       : {metrics['sharpe_ratio']:.2f}")
    print(f"Win Rate           : {metrics['win_rate_pct']:.2f}%")
    print(f"Trade Count        : {int(metrics['trade_count'])}")
    print(f"Ending Equity      : {metrics['ending_equity']:.2f}")
    print("====================================================\n")


def _empty_metrics() -> dict[str, float | int]:
    return {
        "total_return_pct": 0.0,
        "annualized_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "sharpe_ratio": 0.0,
        "win_rate_pct": 0.0,
        "trade_count": 0,
        "ending_equity": 0.0,
    }

