"""Event-driven backtesting package.

The legacy top-pick backtest API is re-exported from ``quant_core.engine.backtest``
so existing imports from ``quant_core.backtest`` keep working after this module
was upgraded from a compatibility file into a package.
"""

from quant_core.engine.backtest import *  # noqa: F401,F403

