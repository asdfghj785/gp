from __future__ import annotations

from quant_core.strategies.base_strategy import BaseStrategy
from quant_core.strategies.breakout import BreakoutStrategy
from quant_core.strategies.global_momentum import GlobalMomentumStrategy
from quant_core.strategies.main_wave import MainWaveStrategy
from quant_core.strategies.reversal import ReversalStrategy


def strategy_registry() -> dict[str, BaseStrategy]:
    strategies: list[BaseStrategy] = [GlobalMomentumStrategy(), MainWaveStrategy(), ReversalStrategy(), BreakoutStrategy()]
    return {strategy.strategy_type: strategy for strategy in strategies}
