from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from quant_core.backtest.events import MarketEvent, SignalEvent
from quant_core.backtest.backtest_engine import EventDrivenBacktestEngine, MovingAverageCrossStrategy
from quant_core.backtest.execution import SimulatedBroker
from quant_core.backtest.metrics import print_tearsheet
from quant_core.backtest.portfolio import Portfolio
from quant_core.config import MIN_KLINE_DIR
from quant_core.engine.factor_factory import build_features_for_ticker
from quant_core.engine.model_trainer import (
    META_PATH,
    MODEL_PATH,
    load_model_and_metadata,
    predict_prob,
    train_from_factor_frame,
)


@dataclass
class XGBoostStrategy:
    symbol: str
    data_path: Path
    model_path: Path = MODEL_PATH
    meta_path: Path = META_PATH
    threshold: float = 0.70
    exit_threshold: float = 0.45
    target_horizon: int = 48
    retrain: bool = False
    oos_only: bool = True

    def __post_init__(self) -> None:
        if self.retrain or not self.model_path.exists() or not self.meta_path.exists():
            factor_df = build_features_for_ticker(self.data_path, target_horizon=self.target_horizon)
            train_from_factor_frame(factor_df, model_path=self.model_path, meta_path=self.meta_path)
        self.model, self.metadata = load_model_and_metadata(self.model_path, self.meta_path)
        self.feature_columns = list(self.metadata.get("feature_columns") or [])
        if not self.feature_columns:
            raise ValueError(f"模型元数据缺少 feature_columns: {self.meta_path}")
        self.factor_df = build_features_for_ticker(self.data_path, target_horizon=self.target_horizon)
        if self.oos_only:
            start_idx = int(self.metadata.get("train_rows") or int(len(self.factor_df) * 0.8))
            inference_frame = self.factor_df.iloc[start_idx:].copy()
        else:
            inference_frame = self.factor_df
        probabilities = predict_prob(self.model, self.factor_df, self.feature_columns)
        eligible_times = set(pd.to_datetime(inference_frame["datetime"]))
        self.prob_by_time = {
            pd.Timestamp(ts).to_pydatetime(): float(prob)
            for ts, prob in zip(self.factor_df["datetime"], probabilities)
            if not self.oos_only or pd.Timestamp(ts) in eligible_times
        }
        self.in_market = False
        self.hold_bars = 0

    def on_market(self, event: MarketEvent) -> SignalEvent | None:
        if event.symbol != self.symbol:
            return None
        probability = self.prob_by_time.get(pd.Timestamp(event.timestamp).to_pydatetime())
        if self.in_market:
            self.hold_bars += 1
            if self.hold_bars >= self.target_horizon or (
                probability is not None and probability <= self.exit_threshold
            ):
                self.in_market = False
                return SignalEvent(
                    symbol=event.symbol,
                    timestamp=event.timestamp,
                    direction="SELL",
                    confidence=probability if probability is not None else 0.0,
                    strength=1.0,
                    reason=f"XGB exit prob={probability or 0.0:.2%}, hold_bars={self.hold_bars}",
                )
            return None
        if probability is None:
            return None
        if probability >= self.threshold:
            self.in_market = True
            self.hold_bars = 0
            return SignalEvent(
                symbol=event.symbol,
                timestamp=event.timestamp,
                direction="BUY",
                confidence=probability,
                strength=0.95,
                reason=f"XGB label=1 prob={probability:.2%}",
            )
        return None


def default_data_path(symbol: str) -> Path:
    clean = symbol.lower().replace(".parquet", "")
    candidates = [
        MIN_KLINE_DIR / "5m" / f"{clean}.parquet",
        MIN_KLINE_DIR / "5m" / f"{clean[-6:]}.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path
    raise FileNotFoundError(f"未找到分钟线 Parquet: {candidates}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run event-driven 5m backtest")
    parser.add_argument("--symbol", default="sh600000", help="Symbol/file stem, e.g. sh600000")
    parser.add_argument("--data", default="", help="Explicit Parquet path")
    parser.add_argument("--cash", type=float, default=1_000_000.0)
    parser.add_argument("--strategy", choices=["xgb", "ma"], default="xgb")
    parser.add_argument("--short-window", type=int, default=5)
    parser.add_argument("--long-window", type=int, default=20)
    parser.add_argument("--threshold", type=float, default=0.70)
    parser.add_argument("--exit-threshold", type=float, default=0.45)
    parser.add_argument("--target-horizon", type=int, default=48)
    parser.add_argument("--retrain", action="store_true", help="Retrain the XGBoost model before running")
    parser.add_argument("--allow-insample", action="store_true", help="Allow XGBoost signals inside the training window")
    args = parser.parse_args()

    data_path = Path(args.data) if args.data else default_data_path(args.symbol)
    if args.strategy == "ma":
        strategy = MovingAverageCrossStrategy(args.symbol, args.short_window, args.long_window)
    else:
        strategy = XGBoostStrategy(
            symbol=args.symbol,
            data_path=data_path,
            threshold=args.threshold,
            exit_threshold=args.exit_threshold,
            target_horizon=args.target_horizon,
            retrain=args.retrain,
            oos_only=not args.allow_insample,
        )
    portfolio = Portfolio(initial_cash=args.cash)
    engine = EventDrivenBacktestEngine(data_path, args.symbol, strategy, portfolio)
    engine.broker = SimulatedBroker(events=engine.events)
    metrics = engine.run()

    print(f"Data Path          : {data_path}")
    print(f"Symbol             : {args.symbol}")
    print(f"Strategy           : {args.strategy}")
    print(f"Initial Cash       : {args.cash:.2f}")
    print_tearsheet(metrics)


if __name__ == "__main__":
    main()
