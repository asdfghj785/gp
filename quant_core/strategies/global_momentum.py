from __future__ import annotations

import pandas as pd

from quant_core.config import GLOBAL_MIN_SCORE
from quant_core.strategies.base_strategy import BaseStrategy


class GlobalMomentumStrategy(BaseStrategy):
    strategy_type = "全局动量狙击"

    def filter(self, df: pd.DataFrame) -> pd.Series:
        return df.get("strategy_type", "").eq(self.strategy_type) if "strategy_type" in df else pd.Series(False, index=df.index)

    def score(self, df: pd.DataFrame, model=None) -> pd.Series:
        return pd.to_numeric(df.get("global_probability", 0), errors="coerce").fillna(0)

    def get_threshold(self) -> float:
        return GLOBAL_MIN_SCORE
