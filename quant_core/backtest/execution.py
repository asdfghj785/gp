from __future__ import annotations

from queue import Queue
from typing import Any

from .events import FillEvent, MarketEvent, OrderEvent


class SimulatedBroker:
    """Simple A-share broker simulator with slippage, commission and stamp tax."""

    def __init__(
        self,
        events: Queue | None = None,
        commission_rate: float = 0.0003,
        stamp_tax_rate: float = 0.001,
        slippage_rate: float = 0.002,
    ) -> None:
        self.events = events
        self.commission_rate = float(commission_rate)
        self.stamp_tax_rate = float(stamp_tax_rate)
        self.slippage_rate = float(slippage_rate)

    def execute_order(self, order: OrderEvent, market: MarketEvent) -> FillEvent | None:
        if order.quantity <= 0:
            return None
        fill_price = self._fill_price(order, market)
        if fill_price is None or fill_price <= 0:
            return None
        gross = fill_price * order.quantity
        commission = gross * self.commission_rate
        tax = gross * self.stamp_tax_rate if order.direction == "SELL" else 0.0
        slippage = abs(fill_price - market.close) * order.quantity
        net_cash_flow = -(gross + commission + tax) if order.direction == "BUY" else gross - commission - tax
        fill = FillEvent(
            symbol=order.symbol,
            timestamp=market.timestamp,
            direction=order.direction,
            quantity=order.quantity,
            fill_price=round(fill_price, 4),
            commission=round(commission, 4),
            tax=round(tax, 4),
            slippage=round(slippage, 4),
            gross_value=round(gross, 4),
            net_cash_flow=round(net_cash_flow, 4),
            order=order,
        )
        if self.events is not None:
            self.events.put(fill)
        return fill

    def _fill_price(self, order: OrderEvent, market: MarketEvent) -> float | None:
        if order.order_type == "LIMIT":
            if order.limit_price is None:
                return None
            limit = float(order.limit_price)
            if order.direction == "BUY":
                if market.low > limit:
                    return None
                return min(limit, market.open * (1 + self.slippage_rate))
            if market.high < limit:
                return None
            return max(limit, market.open * (1 - self.slippage_rate))
        if order.direction == "BUY":
            return market.close * (1 + self.slippage_rate)
        return market.close * (1 - self.slippage_rate)
