from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from quant_core.config import DIPBUY_MIN_SCORE, PROFIT_TARGET_PCT
from quant_core.predictor import (
    BREAKOUT_STRATEGY_TYPE,
    DIPBUY_STRATEGY_TYPE,
    _attach_historical_market_modes,
    apply_strategy_sort_score,
)
from quant_core.strategy_lab import prepare_evaluated_candidates


@dataclass(frozen=True)
class SweepResult:
    threshold: float
    trade_count: int
    win_rate: float
    avg_open_premium: float
    max_loss_streak: int


def sweep_thresholds(
    months: int = 12,
    start: float = 60.0,
    end: float = 69.0,
    step: float = 0.5,
    refresh: bool = False,
) -> list[SweepResult]:
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    evaluated = prepared.get("evaluated", pd.DataFrame())
    if evaluated.empty:
        return []

    risk_pool = _apply_risk_filters(evaluated)
    thresholds = list(_threshold_range(start, end, step))
    return [_evaluate_threshold(risk_pool, threshold) for threshold in thresholds]


def format_markdown_table(rows: list[SweepResult]) -> str:
    lines = [
        "| 评分门槛 | 年出手次数 | 胜率 | 平均开盘溢价 | 最大连亏次数 |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row.threshold:.1f} | {row.trade_count} | {row.win_rate:.2f}% | "
            f"{row.avg_open_premium:.2f}% | {row.max_loss_streak} |"
        )
    return "\n".join(lines)


def choose_sweet_spot(rows: list[SweepResult]) -> SweepResult | None:
    qualified = [
        row
        for row in rows
        if row.win_rate >= 60.0 and row.avg_open_premium >= 1.5 and 120 <= row.trade_count <= 150
    ]
    if qualified:
        return max(qualified, key=lambda row: (row.trade_count, row.win_rate, row.avg_open_premium))

    fallback = [row for row in rows if row.win_rate >= 60.0 and row.avg_open_premium >= 1.5]
    if not fallback:
        return None
    return min(fallback, key=lambda row: (abs(row.trade_count - 135), -row.win_rate, -row.avg_open_premium))


def _apply_risk_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    filtered = df.copy()
    if "纯代码" in filtered.columns:
        filtered = filtered[~filtered["纯代码"].astype(str).str.startswith(("68", "689"), na=False)].copy()
    filtered = _attach_historical_market_modes(filtered)
    filtered = filtered[~filtered["market_gate_mode"].isin(["雷暴", "缩量下跌"])].copy()

    if "涨跌幅" in filtered.columns:
        filtered = filtered[_num(filtered, "涨跌幅") < 7].copy()
    if "准涨停未封板标记" in filtered.columns:
        filtered = filtered[_num(filtered, "准涨停未封板标记") < 0.5].copy()
    if "上影线比例" in filtered.columns:
        is_dipbuy = _strategy(filtered).eq(DIPBUY_STRATEGY_TYPE)
        filtered = filtered[is_dipbuy | (_num(filtered, "上影线比例") < 2)].copy()
    if "预期溢价" in filtered.columns:
        filtered = filtered[_num(filtered, "预期溢价") > 0].copy()
    if {"60日高位比例", "量比", "5日量能堆积"}.issubset(filtered.columns):
        high_volume_trap = (
            (_num(filtered, "60日高位比例") >= 97)
            & ((_num(filtered, "量比") > 3) | (_num(filtered, "5日量能堆积") > 3))
        )
        filtered = filtered[~high_volume_trap].copy()
    if "尾盘诱多标记" in filtered.columns:
        filtered = filtered[_num(filtered, "尾盘诱多标记") < 0.5].copy()
    if "近3日断头铡刀标记" in filtered.columns:
        is_dipbuy = _strategy(filtered).eq(DIPBUY_STRATEGY_TYPE)
        filtered = filtered[is_dipbuy | (_num(filtered, "近3日断头铡刀标记") < 0.5)].copy()

    return filtered.replace([np.inf, -np.inf], 0).fillna(0)


def _evaluate_threshold(df: pd.DataFrame, threshold: float) -> SweepResult:
    gated = _apply_variable_score_gate(df, breakout_threshold=threshold)
    picks = _daily_top(gated)
    premiums = _num(picks, "open_premium") if not picks.empty else pd.Series(dtype="float64")
    success = premiums > PROFIT_TARGET_PCT
    return SweepResult(
        threshold=float(threshold),
        trade_count=int(len(picks)),
        win_rate=float(success.mean() * 100) if len(success) else 0.0,
        avg_open_premium=float(premiums.mean()) if len(premiums) else 0.0,
        max_loss_streak=_max_loss_streak(success.tolist()),
    )


def _apply_variable_score_gate(df: pd.DataFrame, breakout_threshold: float) -> pd.DataFrame:
    if df.empty or "综合评分" not in df.columns:
        return df

    filtered = df.copy()
    strategy = _strategy(filtered)
    score = _num(filtered, "综合评分")
    is_dipbuy = strategy.eq(DIPBUY_STRATEGY_TYPE)
    is_breakout = strategy.eq(BREAKOUT_STRATEGY_TYPE) | ~is_dipbuy
    qualified = ((is_breakout) & (score >= breakout_threshold)) | ((is_dipbuy) & (score >= DIPBUY_MIN_SCORE))
    filtered = filtered[qualified].copy()
    if filtered.empty:
        return filtered

    filtered["strategy_type"] = strategy.loc[filtered.index]
    threshold = pd.Series(float(breakout_threshold), index=filtered.index, dtype="float64")
    threshold.loc[filtered["strategy_type"].eq(DIPBUY_STRATEGY_TYPE)] = DIPBUY_MIN_SCORE
    filtered["生产门槛"] = threshold
    return apply_strategy_sort_score(filtered)


def _daily_top(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sort_cols = ["date", "排序评分", "预期溢价", "综合评分"]
    idx = df.sort_values(sort_cols, ascending=[True, False, False, False]).groupby("date")["排序评分"].idxmax()
    return df.loc[idx].sort_values("date").copy()


def _max_loss_streak(success: list[bool]) -> int:
    max_streak = 0
    current = 0
    for item in success:
        if item:
            current = 0
            continue
        current += 1
        max_streak = max(max_streak, current)
    return max_streak


def _threshold_range(start: float, end: float, step: float) -> Iterable[float]:
    current = start
    while current <= end + 1e-9:
        yield round(current, 4)
        current += step


def _strategy(df: pd.DataFrame) -> pd.Series:
    if "strategy_type" not in df.columns:
        return pd.Series(BREAKOUT_STRATEGY_TYPE, index=df.index)
    return df["strategy_type"].fillna(BREAKOUT_STRATEGY_TYPE).astype(str)


def _num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").replace([np.inf, -np.inf], 0).fillna(0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sweep breakout score thresholds using cached evaluated candidates.")
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--start", type=float, default=60.0)
    parser.add_argument("--end", type=float, default=69.0)
    parser.add_argument("--step", type=float, default=0.5)
    parser.add_argument("--refresh", action="store_true", help="Refresh evaluated candidates before sweeping.")
    args = parser.parse_args()

    rows = sweep_thresholds(args.months, args.start, args.end, args.step, refresh=args.refresh)
    if not rows:
        print("没有可用的历史候选缓存或候选数据为空。")
        return

    print(format_markdown_table(rows))
    sweet_spot = choose_sweet_spot(rows)
    if sweet_spot is None:
        print("\n甜蜜点：未找到同时满足胜率>=60%、平均溢价>=1.5%的门槛。")
    else:
        print(
            "\n甜蜜点："
            f"{sweet_spot.threshold:.1f} "
            f"（出手 {sweet_spot.trade_count} 次，胜率 {sweet_spot.win_rate:.2f}%，"
            f"平均开盘溢价 {sweet_spot.avg_open_premium:.2f}%，最大连亏 {sweet_spot.max_loss_streak} 次）"
        )


if __name__ == "__main__":
    main()
