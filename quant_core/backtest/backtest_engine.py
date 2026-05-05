from __future__ import annotations

from collections import deque
from pathlib import Path
from queue import Queue
from typing import Iterable

import pandas as pd

from .events import EventType, MarketEvent, SignalEvent
from .execution import SimulatedBroker
from .metrics import calculate_metrics
from .portfolio import Portfolio


class MovingAverageCrossStrategy:
    """Minimal MA cross strategy used to verify the event-driven engine."""

    def __init__(self, symbol: str, short_window: int = 5, long_window: int = 20) -> None:
        self.symbol = symbol
        self.short_window = int(short_window)
        self.long_window = int(long_window)
        self.prices: deque[float] = deque(maxlen=self.long_window)
        self.in_market = False

    def on_market(self, event: MarketEvent) -> SignalEvent | None:
        if event.symbol != self.symbol:
            return None
        self.prices.append(float(event.close))
        if len(self.prices) < self.long_window:
            return None
        values = list(self.prices)
        short_ma = sum(values[-self.short_window :]) / self.short_window
        long_ma = sum(values) / self.long_window
        if short_ma > long_ma and not self.in_market:
            self.in_market = True
            return SignalEvent(
                symbol=event.symbol,
                timestamp=event.timestamp,
                direction="BUY",
                confidence=0.65,
                strength=0.95,
                reason=f"MA{self.short_window} crossed above MA{self.long_window}",
            )
        if short_ma < long_ma and self.in_market:
            self.in_market = False
            return SignalEvent(
                symbol=event.symbol,
                timestamp=event.timestamp,
                direction="SELL",
                confidence=0.65,
                strength=1.0,
                reason=f"MA{self.short_window} crossed below MA{self.long_window}",
            )
        return None


class EventDrivenBacktestEngine:
    def __init__(
        self,
        data_path: str | Path,
        symbol: str,
        strategy: MovingAverageCrossStrategy,
        portfolio: Portfolio,
        broker: SimulatedBroker | None = None,
    ) -> None:
        self.data_path = Path(data_path)
        self.symbol = symbol
        self.strategy = strategy
        self.events: Queue = Queue()
        self.portfolio = portfolio
        self.broker = broker or SimulatedBroker(self.events)
        self._latest_market: dict[str, MarketEvent] = {}

    def run(self) -> dict[str, float | int]:
        for market_event in self._market_events():
            self.events.put(market_event)
            while True:
                if self.events.empty():
                    break
                event = self.events.get(False)
                self._route_event(event)
        return calculate_metrics(self.portfolio.equity_curve, self.portfolio.trades, self.portfolio.initial_cash)

    def _route_event(self, event) -> None:
        if event.type == EventType.MARKET:
            self._latest_market[event.symbol] = event
            self.portfolio.on_market(event)
            signal = self.strategy.on_market(event)
            if signal:
                self.events.put(signal)
        elif event.type == EventType.SIGNAL:
            market = self._latest_market.get(event.symbol)
            if market is None:
                return
            order = self.portfolio.on_signal(event, market.close)
            if order:
                self.events.put(order)
        elif event.type == EventType.ORDER:
            market = self._latest_market.get(event.symbol)
            if market is not None:
                self.broker.execute_order(event, market)
        elif event.type == EventType.FILL:
            self.portfolio.on_fill(event)

    def _market_events(self) -> Iterable[MarketEvent]:
        df = pd.read_parquet(self.data_path)
        if df.empty:
            return []
        frame = df.copy()
        frame["datetime"] = pd.to_datetime(frame["datetime"], errors="coerce")
        frame = frame.dropna(subset=["datetime", "open", "high", "low", "close"])
        frame = frame.sort_values("datetime")
        for _, row in frame.iterrows():
            yield MarketEvent(
                symbol=str(row.get("symbol") or row.get("code") or self.symbol),
                timestamp=row["datetime"].to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row.get("volume") or 0.0),
                amount=float(row.get("amount") or row.get("money") or 0.0),
                metadata={"source": row.get("source", ""), "period": row.get("period", "5")},
            )

