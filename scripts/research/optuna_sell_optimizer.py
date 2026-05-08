from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import time
from pathlib import Path
from typing import Any, Optional

import pandas as pd

try:
    import optuna
except ImportError:  # pragma: no cover - runtime dependency guard
    optuna = None


BASE_DIR = Path("/Users/eudis/ths")
SENTINEL_SCRIPT = BASE_DIR / "scripts" / "backtest" / "simulate_sentinel_5m.py"
DB_PATH = BASE_DIR / "data" / "core_db" / "quant_workstation.sqlite3"
PRE_ADJUSTED_5M_DIR = Path("/Users/eudis/5min/organized_5min_pre_adj")

DEFAULT_START_DATE = "2025-01-02"
DEFAULT_END_DATE = "2026-01-28"
DEFAULT_N_TRIALS = 100

SNIPER_BREAKOUT_STRATEGIES = {"全局动量狙击", "尾盘突破", "尾盘突破-ST特情"}
OPTIMIZED_STRATEGIES = SNIPER_BREAKOUT_STRATEGIES | {"右侧主升浪"}
SKIPPED_STRATEGIES = {"中线超跌反转"}

BASELINE_WIN_RATE_PCT = 67.0
BASELINE_MEAN_YIELD_PCT = 2.11


@dataclass(frozen=True)
class Bar:
    ts: pd.Timestamp
    clock: time
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class ReplayContext:
    record: Any
    bars: tuple[Bar, ...]
    t3_last_index: Optional[int]
    eod_stop_price: float
    eod_stop_reason: str


@dataclass(frozen=True)
class TradeOutcome:
    pick_id: int
    strategy_type: str
    yield_pct: float
    exit_reason: str


@dataclass(frozen=True)
class EvaluationStats:
    mean_yield: float
    win_rate: float
    trade_count: int
    win_count: int
    incomplete_count: int
    reason_counts: dict[str, int]


def load_sentinel_module() -> Any:
    spec = importlib.util.spec_from_file_location("sentinel_5m_backtest", SENTINEL_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载回放脚本：{SENTINEL_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sentinel = load_sentinel_module()


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def load_optimizer_buy_records(db_path: Path, start_date: str, end_date: str) -> list[Any]:
    """Read daily_picks without touching production storage code.

    This intentionally does not apply PAUSED_STRATEGY_TYPES, because the lab run
    must include main-wave samples if they exist while still skipping reversal.
    """
    with connect_db(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id, selection_date, target_date, selected_at, code, name,
                   strategy_type, win_rate, selection_change, snapshot_price,
                   selection_price, snapshot_vol_ratio, suggested_position,
                   tier, open_price, open_premium, open_checked_at,
                   close_date, close_price, close_return_pct, close_reason, raw_json
            FROM daily_picks
            WHERE selection_date >= ?
              AND selection_date <= ?
            ORDER BY selection_date ASC, id ASC
            """,
            (start_date, end_date),
        ).fetchall()

    records: list[Any] = []
    for row in rows:
        raw = sentinel.safe_json(row["raw_json"])
        winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
        strategy_type = str(row["strategy_type"] or winner.get("strategy_type") or "未知策略")
        if strategy_type in SKIPPED_STRATEGIES or strategy_type not in OPTIMIZED_STRATEGIES:
            continue

        cost_price = sentinel.safe_float(row["snapshot_price"]) or sentinel.safe_float(row["selection_price"])
        if cost_price <= 0:
            continue

        suggested_position = row["suggested_position"]
        if suggested_position is None:
            suggested_position = winner.get("suggested_position")

        records.append(
            sentinel.BuyRecord(
                pick_id=int(row["id"]),
                code=sentinel.normalize_code(row["code"]),
                name=str(row["name"] or winner.get("name") or row["code"]),
                buy_date=str(row["selection_date"])[:10],
                selected_at=str(row["selected_at"] or ""),
                cost_price=float(cost_price),
                strategy_type=strategy_type,
                tier=str(row["tier"] or winner.get("selection_tier") or "base"),
                target_date=str(row["target_date"] or "")[:10],
                win_rate=sentinel.safe_float(row["win_rate"]),
                selection_change=sentinel.safe_float(row["selection_change"]),
                snapshot_vol_ratio=sentinel.safe_float(row["snapshot_vol_ratio"]),
                suggested_position=sentinel.safe_optional_float(suggested_position),
                open_price=sentinel.safe_optional_float(row["open_price"]),
                open_premium=sentinel.safe_optional_float(row["open_premium"]),
                open_checked_at=str(row["open_checked_at"] or ""),
                close_date=str(row["close_date"] or "")[:10],
                close_price=sentinel.safe_optional_float(row["close_price"]),
                close_return_pct=sentinel.safe_optional_float(row["close_return_pct"]),
                close_reason=str(row["close_reason"] or ""),
                raw=raw,
            )
        )
    return records


def load_complete_year_contexts(
    records: list[Any],
    db_path: Path,
    minute_root: Path,
    end_date: str,
) -> list[ReplayContext]:
    trading_dates = sentinel.load_trading_dates(db_path)
    sentinel.prime_pre_adjusted_zip_cache(records, minute_root, end_date)

    contexts: list[ReplayContext] = []
    for record in records:
        bars_df = sentinel.load_pre_adjusted_zip_window(record, minute_root, end_date, warn_missing=False)
        if bars_df.empty:
            continue

        t3_date = sentinel.infer_t3_date(record, trading_dates, bars_df)
        if t3_date:
            t3_end = pd.Timestamp(f"{t3_date} 15:00:00")
            bars_df = bars_df.loc[bars_df["datetime"] <= t3_end].copy()
        bars_df = bars_df.sort_values("datetime").reset_index(drop=True)

        t3_last_index = sentinel.t3_close_bar_index(bars_df, t3_date)
        eod_stop_reason, eod_stop_ratio = sentinel.eod_structural_stop_config(record)
        bars = tuple(
            Bar(
                ts=pd.Timestamp(row.datetime),
                clock=pd.Timestamp(row.datetime).time(),
                high=float(row.high),
                low=float(row.low),
                close=float(row.close),
            )
            for row in bars_df.itertuples(index=False)
        )
        contexts.append(
            ReplayContext(
                record=record,
                bars=bars,
                t3_last_index=t3_last_index,
                eod_stop_price=float(record.cost_price) * float(eod_stop_ratio),
                eod_stop_reason=eod_stop_reason,
            )
        )
    return contexts


def simulate_with_params(
    ctx: ReplayContext,
    active_pct: float,
    retracement_pct: float,
    hard_stop_pct: float,
) -> Optional[TradeOutcome]:
    if not ctx.bars:
        return None

    record = ctx.record
    cost_price = float(record.cost_price)
    highest_price = cost_price
    trailing_active = False
    hard_stop_price = cost_price * (1.0 + hard_stop_pct)

    for idx, bar in enumerate(ctx.bars):
        highest_price = max(highest_price, bar.high)

        if bar.low <= hard_stop_price:
            return build_trade_outcome(record, hard_stop_price, "盘中暴雷止损_Optuna")

        if highest_price >= cost_price * active_pct:
            trailing_active = True

        trailing_trigger_price = highest_price * (1.0 - retracement_pct)
        if trailing_active and bar.low <= trailing_trigger_price:
            exit_price = min(trailing_trigger_price, bar.close)
            return build_trade_outcome(record, exit_price, "动态追踪止盈_Optuna")

        if bar.clock in sentinel.EOD_STRUCTURAL_STOP_TIMES and bar.close <= ctx.eod_stop_price:
            return build_trade_outcome(record, bar.close, ctx.eod_stop_reason)

        if ctx.t3_last_index is not None and idx == ctx.t3_last_index:
            return build_trade_outcome(record, bar.close, "T+3强制平仓")

    return None


def build_trade_outcome(record: Any, exit_price: float, exit_reason: str) -> TradeOutcome:
    yield_pct = (float(exit_price) / float(record.cost_price) - 1.0) * 100.0
    return TradeOutcome(
        pick_id=int(record.pick_id),
        strategy_type=str(record.strategy_type),
        yield_pct=float(yield_pct),
        exit_reason=exit_reason,
    )


def evaluate_params(
    contexts: list[ReplayContext],
    active_pct: float,
    retracement_pct: float,
    hard_stop_pct: float,
) -> EvaluationStats:
    outcomes: list[TradeOutcome] = []
    for ctx in contexts:
        outcome = simulate_with_params(ctx, active_pct, retracement_pct, hard_stop_pct)
        if outcome is not None:
            outcomes.append(outcome)

    if not outcomes:
        return EvaluationStats(
            mean_yield=float("-inf"),
            win_rate=0.0,
            trade_count=0,
            win_count=0,
            incomplete_count=len(contexts),
            reason_counts={},
        )

    yields = [item.yield_pct for item in outcomes]
    win_count = sum(1 for value in yields if value > 0)
    reason_counts = Counter(item.exit_reason for item in outcomes)
    return EvaluationStats(
        mean_yield=float(sum(yields) / len(yields)),
        win_rate=float(win_count / len(yields) * 100.0),
        trade_count=len(outcomes),
        win_count=win_count,
        incomplete_count=len(contexts) - len(outcomes),
        reason_counts=dict(reason_counts),
    )


REPLAY_CONTEXTS: list[ReplayContext] = []


def objective(trial: Any) -> float:
    active_pct = trial.suggest_float("active_pct", 1.02, 1.08)
    retracement_pct = trial.suggest_float("retracement_pct", 0.01, 0.04)
    hard_stop_pct = trial.suggest_float("hard_stop_pct", -0.08, -0.03)

    stats = evaluate_params(REPLAY_CONTEXTS, active_pct, retracement_pct, hard_stop_pct)
    trial.set_user_attr("win_rate", stats.win_rate)
    trial.set_user_attr("trade_count", stats.trade_count)
    trial.set_user_attr("incomplete_count", stats.incomplete_count)
    return stats.mean_yield


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optuna sell-parameter optimizer for isolated 5m research.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--db-path", default=str(DB_PATH))
    parser.add_argument("--minute-root", default=str(PRE_ADJUSTED_5M_DIR))
    parser.add_argument("--n-trials", type=int, default=DEFAULT_N_TRIALS)
    parser.add_argument("--output-json", default="", help="Optional isolated research result JSON path.")
    return parser.parse_args()


def main() -> None:
    if optuna is None:
        raise SystemExit("缺少 optuna：请先执行 python3 -m pip install optuna")

    args = parse_args()
    db_path = Path(args.db_path)
    minute_root = Path(args.minute_root)

    records = load_optimizer_buy_records(db_path, args.start_date, args.end_date)
    contexts = load_complete_year_contexts(records, db_path, minute_root, args.end_date)
    if not contexts:
        raise SystemExit("没有可回放的 5m 样本，无法执行 Optuna 寻优。")

    global REPLAY_CONTEXTS
    REPLAY_CONTEXTS = contexts

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="maximize")
    study.optimize(objective, n_trials=int(args.n_trials))

    best_stats = evaluate_params(
        contexts,
        active_pct=float(study.best_params["active_pct"]),
        retracement_pct=float(study.best_params["retracement_pct"]),
        hard_stop_pct=float(study.best_params["hard_stop_pct"]),
    )
    current_rule_stats = evaluate_params(contexts, active_pct=1.04, retracement_pct=0.02, hard_stop_pct=-0.04)

    payload = {
        "source": "scripts/research/optuna_sell_optimizer.py",
        "date_range": {"start": args.start_date, "end": args.end_date},
        "db_path": str(db_path),
        "minute_root": str(minute_root),
        "strategy_scope": sorted(OPTIMIZED_STRATEGIES),
        "skipped_strategy_scope": sorted(SKIPPED_STRATEGIES),
        "records_loaded": len(records),
        "contexts_loaded": len(contexts),
        "n_trials": int(args.n_trials),
        "best_params": study.best_params,
        "best_value_mean_yield_pct": best_stats.mean_yield,
        "best_win_rate_pct": best_stats.win_rate,
        "best_trade_count": best_stats.trade_count,
        "best_win_count": best_stats.win_count,
        "best_incomplete_count": best_stats.incomplete_count,
        "best_reason_counts": best_stats.reason_counts,
        "current_rule_reference": {
            "active_pct": 1.04,
            "retracement_pct": 0.02,
            "hard_stop_pct": -0.04,
            "mean_yield_pct": current_rule_stats.mean_yield,
            "win_rate_pct": current_rule_stats.win_rate,
            "trade_count": current_rule_stats.trade_count,
            "reason_counts": current_rule_stats.reason_counts,
        },
        "baseline_reference": {
            "win_rate_pct": BASELINE_WIN_RATE_PCT,
            "mean_yield_pct": BASELINE_MEAN_YIELD_PCT,
        },
    }

    if args.output_json:
        output_path = Path(args.output_json).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n========== Optuna Sell Optimizer Report ==========")
    print(f"Data Range: {args.start_date} -> {args.end_date}")
    print(f"Records Loaded: {len(records)}")
    print(f"Replay Contexts: {len(contexts)}")
    print(f"Trials: {int(args.n_trials)}")
    print(f"Best Params: {json.dumps(study.best_params, ensure_ascii=False)}")
    print(f"Best Value / Mean Yield: {best_stats.mean_yield:.4f}%")
    print(f"Win Rate: {best_stats.win_rate:.2f}% ({best_stats.win_count}/{best_stats.trade_count})")
    print(f"Incomplete / Unsettled: {best_stats.incomplete_count}")
    print(f"Reason Counts: {json.dumps(best_stats.reason_counts, ensure_ascii=False, sort_keys=True)}")
    print("---------- Current Rule Reference ----------")
    print(
        "active_pct=1.04, retracement_pct=0.02, hard_stop_pct=-0.04 -> "
        f"mean_yield={current_rule_stats.mean_yield:.4f}%, "
        f"win_rate={current_rule_stats.win_rate:.2f}%"
    )
    print("---------- Baseline Gate ----------")
    print(f"Baseline Mean Yield: {BASELINE_MEAN_YIELD_PCT:.2f}%")
    print(f"Baseline Win Rate: {BASELINE_WIN_RATE_PCT:.2f}%")
    print(f"Mean Yield Beats Baseline: {best_stats.mean_yield > BASELINE_MEAN_YIELD_PCT}")
    print(f"Win Rate Beats Baseline: {best_stats.win_rate > BASELINE_WIN_RATE_PCT}")
    print("================================================\n")


if __name__ == "__main__":
    main()
