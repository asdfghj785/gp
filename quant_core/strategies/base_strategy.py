from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class BaseStrategy(ABC):
    """Common strategy contract for future V3 strategy modules."""

    strategy_type: str

    @abstractmethod
    def filter(self, df: pd.DataFrame) -> pd.Series:
        """Return a boolean mask for the strategy's physical candidate pool."""

    @abstractmethod
    def score(self, df: pd.DataFrame, model: Any | None = None) -> pd.Series:
        """Return model/strategy scores for the filtered candidates."""

    @abstractmethod
    def get_threshold(self) -> float:
        """Return the production threshold for this strategy."""
