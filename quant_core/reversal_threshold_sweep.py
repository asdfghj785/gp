from __future__ import annotations

import argparse
from typing import Any

import numpy as np
import pandas as pd

from quant_core.engine.predictor import MAIN_WAVE_STRATEGY_TYPE, REVERSAL_STRATEGY_TYPE
from .storage import init_db
from quant_core.strategies.labs.strategy_lab import prepare_evaluated_candidates


SWEEP_PROFILES = {
    "reversal": {
        "title": "雷达 3 号阈值扫频",
        "strategy_type": REVERSAL_STRATEGY_TYPE,
        "strategy_label": "中线超跌反转",
        "start": 5.0,
        "end": 8.0,
        "step": 0.5,
        "target_min_trades": 12,
        "target_max_trades": 24,
        "target_win_rate": 85.0,
        "target_avg_gain": 4.0,
    },
    "main-wave": {
        "title": "雷达 4 号主升浪阈值扫频",
        "strategy_type": MAIN_WAVE_STRATEGY_TYPE,
        "strategy_label": "右侧主升浪",
        "start": 6.5,
        "end": 8.5,
        "step": 0.5,
        "target_min_trades": 10,
        "target_max_trades": 20,
        "target_win_rate": 85.0,
        "target_avg_gain": 6.0,
    },
}


def run_reversal_threshold_sweep(months: int = 12, refresh: bool = False) -> dict[str, Any]:
    return run_strategy_threshold_sweep("reversal", months=months, refresh=refresh)


def run_strategy_threshold_sweep(
    profile_name: str = "reversal",
    months: int = 12,
    refresh: bool = False,
    start: float | None = None,
    end: float | None = None,
    step: float | None = None,
) -> dict[str, Any]:
    init_db()
    profile = SWEEP_PROFILES.get(profile_name, SWEEP_PROFILES["reversal"])
    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    evaluated = prepared["evaluated"]
    if evaluated.empty:
        return {
            "rows": [],
            "model_status": prepared.get("model_status", "empty"),
            "note": "没有可扫频的候选数据",
        }

    strategy_type = profile["strategy_type"]
    strategy_pool = evaluated[evaluated["strategy_type"].eq(strategy_type)].copy()
    strategy_pool = strategy_pool[np.isfinite(pd.to_numeric(strategy_pool.get("t3_max_gain_pct"), errors="coerce"))].copy()
    rows: list[dict[str, Any]] = []
    sweep_start = float(profile["start"] if start is None else start)
    sweep_end = float(profile["end"] if end is None else end)
    sweep_step = float(profile["step"] if step is None else step)
    for threshold in np.arange(sweep_start, sweep_end + 0.001, sweep_step):
        selected = _daily_strategy_top(strategy_pool[strategy_pool["综合评分"] >= threshold])
        gains = pd.to_numeric(selected.get("t3_max_gain_pct"), errors="coerce").dropna()
        wins = gains > 0
        rows.append(
            {
                "threshold": round(float(threshold), 2),
                "trades": int(len(gains)),
                "t3_win_rate": round(float(wins.mean() * 100), 4) if len(gains) else 0.0,
                "avg_t3_max_gain_pct": round(float(gains.mean()), 4) if len(gains) else 0.0,
                "max_losing_streak": _max_losing_streak(wins.tolist()),
            }
        )
    return {
        "rows": rows,
        "candidate_rows": int(len(strategy_pool)),
        "model_status": prepared.get("model_status", "ready"),
        "profile": profile_name,
        "strategy_type": strategy_type,
        "strategy_label": profile["strategy_label"],
        "target_min_trades": int(profile["target_min_trades"]),
        "target_max_trades": int(profile["target_max_trades"]),
        "target_win_rate": float(profile["target_win_rate"]),
        "target_avg_gain": float(profile["target_avg_gain"]),
    }


def _daily_reversal_top(df: pd.DataFrame) -> pd.DataFrame:
    return _daily_strategy_top(df)


def _daily_strategy_top(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "排序评分" not in df.columns:
        df["排序评分"] = (50 + pd.to_numeric(df["综合评分"], errors="coerce").fillna(0).clip(-5, 15) * 5).clip(0, 110)
    sort_cols = ["date"]
    if "策略优先级" in df.columns:
        sort_cols.append("策略优先级")
    sort_cols.extend(["排序评分", "预期溢价", "综合评分"])
    sorted_df = df.sort_values(sort_cols, ascending=[True] + [False] * (len(sort_cols) - 1))
    return sorted_df.drop_duplicates("date", keep="first").sort_values("date").copy()


def _max_losing_streak(wins: list[bool]) -> int:
    current = 0
    max_streak = 0
    for win in wins:
        if win:
            current = 0
            continue
        current += 1
        max_streak = max(max_streak, current)
    return max_streak


def format_markdown(report: dict[str, Any]) -> str:
    profile = SWEEP_PROFILES.get(str(report.get("profile") or "reversal"), SWEEP_PROFILES["reversal"])
    min_trades = int(report.get("target_min_trades", profile["target_min_trades"]))
    max_trades = int(report.get("target_max_trades", profile["target_max_trades"]))
    target_win_rate = float(report.get("target_win_rate", profile["target_win_rate"]))
    target_avg_gain = float(report.get("target_avg_gain", profile["target_avg_gain"]))
    lines = [
        f"# {profile['title']}",
        "",
        f"- 扫频策略：{report.get('strategy_label', profile['strategy_label'])}",
        f"- 候选行数：{report.get('candidate_rows', 0)}",
        f"- 模型状态：{report.get('model_status', '-')}",
        "",
        "| 评分门槛 | 年出手次数 | T+3 胜率 | T+3 平均最大涨幅 | 最大连亏次数 |",
        "|---:|---:|---:|---:|---:|",
    ]
    for row in report.get("rows", []):
        lines.append(
            f"| {row['threshold']:.1f} | {row['trades']} | "
            f"{row['t3_win_rate']:.2f}% | {row['avg_t3_max_gain_pct']:.2f}% | {row['max_losing_streak']} |"
    )
    rows = report.get("rows", [])
    sweet = [
        row for row in rows
        if min_trades <= row["trades"] <= max_trades
        and row["t3_win_rate"] >= target_win_rate
        and row["avg_t3_max_gain_pct"] >= target_avg_gain
    ]
    lines.extend(["", "## 结论", ""])
    if sweet:
        best = max(sweet, key=lambda row: (row["avg_t3_max_gain_pct"], row["t3_win_rate"], row["trades"]))
        lines.append(
            f"- 存在甜蜜点：门槛 {best['threshold']:.1f}，出手 {best['trades']} 次，"
            f"胜率 {best['t3_win_rate']:.2f}%，平均涨幅 {best['avg_t3_max_gain_pct']:.2f}%，"
            f"最大连亏 {best['max_losing_streak']} 次。"
        )
    else:
        lines.append(
            f"- 当前区间内没有同时满足出手 {min_trades}-{max_trades} 次、"
            f"T+3 平均涨幅 >={target_avg_gain:.1f}% 且胜率 >={target_win_rate:.1f}% 的门槛。"
        )
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="波段策略阈值扫频")
    parser.add_argument("--strategy", choices=sorted(SWEEP_PROFILES), default="reversal")
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--start", type=float, default=None)
    parser.add_argument("--end", type=float, default=None)
    parser.add_argument("--step", type=float, default=None)
    args = parser.parse_args()
    print(
        format_markdown(
            run_strategy_threshold_sweep(
                args.strategy,
                months=args.months,
                refresh=args.refresh,
                start=args.start,
                end=args.end,
                step=args.step,
            )
        )
    )
