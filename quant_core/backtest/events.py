from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Literal


class EventType(str, Enum):
    MARKET = "MARKET"
    SIGNAL = "SIGNAL"
    ORDER = "ORDER"
    FILL = "FILL"


Direction = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "LIMIT"]


@dataclass(frozen=True)
class MarketEvent:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    amount: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)
    type: EventType = EventType.MARKET


@dataclass(frozen=True)
class SignalEvent:
    symbol: str
    timestamp: datetime
    direction: Direction
    confidence: float = 1.0
    strength: float = 1.0
    reason: str = ""
    type: EventType = EventType.SIGNAL


@dataclass(frozen=True)
class OrderEvent:
    symbol: str
    timestamp: datetime
    direction: Direction
    quantity: int
    order_type: OrderType = "MARKET"
    limit_price: float | None = None
    reason: str = ""
    type: EventType = EventType.ORDER

    @property
    def is_buy(self) -> bool:
        return self.direction == "BUY"

    @property
    def is_sell(self) -> bool:
        return self.direction == "SELL"


@dataclass(frozen=True)
class FillEvent:
    symbol: str
    timestamp: datetime
    direction: Direction
    quantity: int
    fill_price: float
    commission: float
    tax: float
    slippage: float
    gross_value: float
    net_cash_flow: float
    order: OrderEvent
    type: EventType = EventType.FILL

