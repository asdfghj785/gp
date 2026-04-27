from __future__ import annotations

import pandas as pd

from quant_core.config import BREAKOUT_MIN_SCORE
from quant_core.strategies.base_strategy import BaseStrategy


class BreakoutStrategy(BaseStrategy):
    strategy_type = "尾盘突破"

    def filter(self, df: pd.DataFrame) -> pd.Series:
        return pd.Series(True, index=df.index)

    def score(self, df: pd.DataFrame, model=None) -> pd.Series:
        return pd.to_numeric(df.get("composite_score", 0), errors="coerce").fillna(0)

    def get_threshold(self) -> float:
        return BREAKOUT_MIN_SCORE
