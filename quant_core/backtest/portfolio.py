from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from .events import FillEvent, MarketEvent, OrderEvent, SignalEvent


LOT_SIZE = 100


@dataclass
class Position:
    symbol: str
    total_quantity: int = 0
    available_quantity: int = 0
    avg_cost: float = 0.0
    last_price: float = 0.0
    frozen_lots: dict[date, int] = field(default_factory=dict)

    @property
    def market_value(self) -> float:
        return self.total_quantity * self.last_price

    @property
    def frozen_quantity(self) -> int:
        return sum(self.frozen_lots.values())


class Portfolio:
    """A-share portfolio manager with T+1 and 100-share lot constraints."""

    def __init__(
        self,
        initial_cash: float = 1_000_000.0,
        max_position_pct: float = 0.95,
        lot_size: int = LOT_SIZE,
    ) -> None:
        self.initial_cash = float(initial_cash)
        self.current_cash = float(initial_cash)
        self.max_position_pct = float(max_position_pct)
        self.lot_size = int(lot_size)
        self.positions: dict[str, Position] = {}
        self.equity_curve: list[dict[str, Any]] = []
        self.trades: list[dict[str, Any]] = []
        self._open_trade_stack: dict[str, list[dict[str, Any]]] = {}
        self.current_date: date | None = None

    def on_market(self, event: MarketEvent) -> None:
        self._roll_trading_day(event.timestamp.date())
        position = self.positions.get(event.symbol)
        if position:
            position.last_price = float(event.close)
        self.mark_to_market(event.timestamp)

    def on_signal(self, event: SignalEvent, market_price: float) -> OrderEvent | None:
        price = float(market_price)
        if price <= 0:
            return None
        if event.direction == "BUY":
            return self._buy_order(event, price)
        if event.direction == "SELL":
            return self._sell_order(event)
        return None

    def on_fill(self, event: FillEvent) -> None:
        if event.quantity <= 0:
            return
        if event.direction == "BUY":
            self._apply_buy(event)
        else:
            self._apply_sell(event)
        self.mark_to_market(event.timestamp)

    def mark_to_market(self, timestamp: datetime) -> None:
        equity = self.current_cash + sum(pos.market_value for pos in self.positions.values())
        self.equity_curve.append(
            {
                "timestamp": timestamp,
                "cash": self.current_cash,
                "market_value": equity - self.current_cash,
                "equity": equity,
            }
        )

    def _buy_order(self, event: SignalEvent, price: float) -> OrderEvent | None:
        equity = self.current_cash + sum(pos.market_value for pos in self.positions.values())
        budget = min(self.current_cash, equity * self.max_position_pct * max(0.0, min(float(event.strength), 1.0)))
        quantity = int(budget // price)
        quantity = self._round_lot(quantity)
        if quantity <= 0:
            return None
        return OrderEvent(
            symbol=event.symbol,
            timestamp=event.timestamp,
            direction="BUY",
            quantity=quantity,
            order_type="MARKET",
            reason=event.reason,
        )

    def _sell_order(self, event: SignalEvent) -> OrderEvent | None:
        position = self.positions.get(event.symbol)
        if not position:
            return None
        quantity = self._round_lot(position.available_quantity)
        if quantity <= 0:
            return None
        return OrderEvent(
            symbol=event.symbol,
            timestamp=event.timestamp,
            direction="SELL",
            quantity=quantity,
            order_type="MARKET",
            reason=event.reason,
        )

    def _apply_buy(self, event: FillEvent) -> None:
        total_cost = event.gross_value + event.commission + event.tax
        if total_cost > self.current_cash + 1e-6:
            return
        position = self.positions.setdefault(event.symbol, Position(symbol=event.symbol))
        old_value = position.avg_cost * position.total_quantity
        new_value = event.fill_price * event.quantity
        position.total_quantity += event.quantity
        position.avg_cost = (old_value + new_value) / position.total_quantity if position.total_quantity else 0.0
        position.last_price = event.fill_price
        buy_date = event.timestamp.date()
        position.frozen_lots[buy_date] = position.frozen_lots.get(buy_date, 0) + event.quantity
        self.current_cash -= total_cost
        self._open_trade_stack.setdefault(event.symbol, []).append(
            {
                "entry_time": event.timestamp,
                "entry_price": event.fill_price,
                "quantity": event.quantity,
                "entry_cost": total_cost,
            }
        )

    def _apply_sell(self, event: FillEvent) -> None:
        position = self.positions.get(event.symbol)
        if not position:
            return
        quantity = min(event.quantity, position.available_quantity, position.total_quantity)
        quantity = self._round_lot(quantity)
        if quantity <= 0:
            return
        sell_gross = event.fill_price * quantity
        sell_cost = event.commission + event.tax
        self.current_cash += sell_gross - sell_cost
        position.total_quantity -= quantity
        position.available_quantity -= quantity
        position.last_price = event.fill_price
        self._close_trade_lots(event, quantity, sell_gross, sell_cost)
        if position.total_quantity <= 0:
            self.positions.pop(event.symbol, None)

    def _close_trade_lots(self, event: FillEvent, quantity: int, sell_gross: float, sell_cost: float) -> None:
        stack = self._open_trade_stack.get(event.symbol, [])
        remaining = quantity
        while stack and remaining > 0:
            lot = stack[0]
            lot_qty = int(lot["quantity"])
            close_qty = min(lot_qty, remaining)
            entry_cost_alloc = float(lot["entry_cost"]) * close_qty / lot_qty
            exit_value_alloc = sell_gross * close_qty / quantity
            exit_cost_alloc = sell_cost * close_qty / quantity
            pnl = exit_value_alloc - exit_cost_alloc - entry_cost_alloc
            self.trades.append(
                {
                    "symbol": event.symbol,
                    "entry_time": lot["entry_time"],
                    "exit_time": event.timestamp,
                    "quantity": close_qty,
                    "entry_price": lot["entry_price"],
                    "exit_price": event.fill_price,
                    "pnl": pnl,
                    "return_pct": pnl / entry_cost_alloc * 100 if entry_cost_alloc else 0.0,
                }
            )
            lot["quantity"] = lot_qty - close_qty
            remaining -= close_qty
            if lot["quantity"] <= 0:
                stack.pop(0)

    def _roll_trading_day(self, current: date) -> None:
        if self.current_date == current:
            return
        self.current_date = current
        for position in self.positions.values():
            releasable = 0
            for buy_date in list(position.frozen_lots):
                if buy_date < current:
                    releasable += position.frozen_lots.pop(buy_date)
            if releasable:
                position.available_quantity += releasable
                position.available_quantity = min(position.available_quantity, position.total_quantity)

    def _round_lot(self, quantity: int) -> int:
        return max(0, int(quantity) // self.lot_size * self.lot_size)

