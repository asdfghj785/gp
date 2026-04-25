from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd

from quant_core.config import BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE, PROFIT_TARGET_PCT
from quant_core.daily_pick import list_daily_pick_results
from quant_core.predictor import apply_production_filters
from quant_core.storage import clear_daily_picks, save_daily_pick, update_daily_pick_open
from quant_core.strategy_lab import prepare_evaluated_candidates


def rebuild_historical_picks(months: int = 12) -> dict[str, Any]:
    print(f"开始重建 daily_picks：清空旧模拟数据，按最新双轨生产模型回放过去 {months} 个月。", flush=True)
    deleted = clear_daily_picks()
    print(f"已清空 daily_picks 旧记录：{deleted} 条。", flush=True)

    prepared = prepare_evaluated_candidates(months, refresh=True)
    evaluated = prepared["evaluated"]
    if evaluated.empty:
        print("最新模型没有生成可评估候选池，回放结束。", flush=True)
        return _summary(months, deleted, inserted=0, updated=0, skipped=0, model_status=prepared.get("model_status", "empty"))

    trading_dates = sorted(evaluated["date"].dropna().astype(str).unique().tolist())
    production_pool = apply_production_filters(evaluated)
    if not production_pool.empty:
        production_pool = production_pool.sort_values(["date", "排序评分", "预期溢价", "综合评分"], ascending=[True, False, False, False])
        top_index = production_pool.groupby("date")["排序评分"].idxmax()
        daily_top = production_pool.loc[top_index].set_index("date", drop=False).sort_index()
    else:
        daily_top = pd.DataFrame()

    total = len(trading_dates)
    inserted = 0
    updated = 0
    skipped = 0

    if total == 0:
        print("过去 12 个月没有有效交易日，回放结束。", flush=True)
        return _summary(months, deleted, inserted, updated, skipped, model_status=prepared.get("model_status", "no_trading_dates"))

    for index, selection_date in enumerate(trading_dates, start=1):
        if daily_top.empty or selection_date not in daily_top.index:
            skipped += 1
            print(
                f"进度 {index}/{total} 天 {selection_date}：无生产级标的"
                f"（突破>={BREAKOUT_MIN_SCORE:.2f}，低吸>={DIPBUY_MIN_SCORE:.2f}），空仓。",
                flush=True,
            )
            continue

        pick_row = daily_top.loc[selection_date]
        if isinstance(pick_row, pd.DataFrame):
            pick_row = pick_row.iloc[0]
        code = str(pick_row["纯代码"])
        score = float(pick_row.get("综合评分") or 0)
        strategy_type = str(pick_row.get("strategy_type") or "尾盘突破")
        threshold = float(pick_row.get("生产门槛") or (DIPBUY_MIN_SCORE if strategy_type == "首阴低吸" else BREAKOUT_MIN_SCORE))
        if score < threshold:
            skipped += 1
            print(f"进度 {index}/{total} 天 {selection_date}：{code} 综合评分 {score:.2f} < {threshold:.2f}，跳过。", flush=True)
            continue

        pick = _pick_from_candidate_row(pick_row, prepared)
        row_id = save_daily_pick(pick)
        if row_id:
            inserted += 1
        next_open = pick_row.get("next_open")
        if pd.notna(next_open):
            checked_at = f"{pick_row.get('next_date') or selection_date}T09:30:00"
            if update_daily_pick_open(selection_date, float(next_open), checked_at):
                updated += 1
        premium = pick_row.get("open_premium")
        premium_text = "-" if pd.isna(premium) else f"{float(premium):.2f}%"
        print(
            f"进度 {index}/{total} 天 {selection_date}：锁定 {code} {pick_row.get('名称', '')} "
            f"[{strategy_type}] 评分 {score:.2f}，排序 {float(pick_row.get('排序评分') or score):.2f}，次日开盘溢价 {premium_text}。",
            flush=True,
        )

    result = _summary(months, deleted, inserted, updated, skipped, model_status=prepared.get("model_status", "ready"))
    print(
        "重建完成："
        f"出手 {result['trade_count']} 次，"
        f"成功 {result['win_count']} 次，"
        f"胜率 {result['win_rate']:.2f}%，"
        f"策略分布 {result['strategy_counts']}。",
        flush=True,
    )
    return result


def _pick_from_candidate_row(row: pd.Series, prepared: dict[str, Any]) -> dict[str, Any]:
    selection_date = str(row["date"])
    target_date = str(row.get("next_date") or selection_date)
    winner = {
        "code": str(row["纯代码"]),
        "name": str(row.get("名称") or ""),
        "strategy_type": str(row.get("strategy_type") or "尾盘突破"),
        "price": float(row.get("最新价") or 0),
        "change": float(row.get("涨跌幅") or 0),
        "win_rate": float(row.get("AI胜率") or 0),
        "expected_premium": float(row.get("预期溢价") or 0),
        "risk_score": float(row.get("风险评分") or 0),
        "liquidity_score": float(row.get("流动性评分") or 0),
        "composite_score": float(row.get("综合评分") or 0),
        "sort_score": float(row.get("排序评分", row.get("综合评分") or 0) or 0),
        "score_threshold": float(row.get("生产门槛") or 0),
        "sentiment_bonus": float(row.get("情绪补偿分") or 0),
        "market_gate_mode": str(row.get("market_gate_mode") or ""),
    }
    return {
        "selection_date": selection_date,
        "target_date": target_date,
        "selected_at": f"{selection_date}T14:50:00",
        "code": winner["code"],
        "name": winner["name"],
        "strategy_type": winner["strategy_type"],
        "win_rate": winner["win_rate"],
        "selection_price": winner["price"],
        "selection_change": winner["change"],
        "model_status": str(prepared.get("model_status", "")),
        "status": "pending_open",
        "raw": {
            "source": "historical_production_replay",
            "winner": winner,
            "scan_created_at": f"{selection_date}T14:50:00",
            "strategy": "historical replay using current dual-track production filters",
            "profit_target_pct": PROFIT_TARGET_PCT,
        },
    }


def _summary(months: int, deleted: int, inserted: int, updated: int, skipped: int, model_status: str) -> dict[str, Any]:
    stored = list_daily_pick_results(limit=10000).get("rows", [])
    evaluated = [row for row in stored if row.get("success") is not None]
    wins = [row for row in evaluated if row.get("success")]
    strategy_counts = Counter(str(row.get("strategy_type") or "尾盘突破") for row in stored)
    premiums = [float(row["open_premium"]) for row in evaluated if row.get("open_premium") is not None]
    return {
        "months": months,
        "deleted_rows": deleted,
        "inserted_rows": inserted,
        "updated_open_rows": updated,
        "skipped_rows": skipped,
        "trade_count": len(stored),
        "evaluated_count": len(evaluated),
        "win_count": len(wins),
        "loss_count": len(evaluated) - len(wins),
        "win_rate": round(len(wins) / len(evaluated) * 100, 4) if evaluated else 0.0,
        "avg_open_premium": round(float(pd.Series(premiums).mean()), 4) if premiums else 0.0,
        "strategy_counts": dict(strategy_counts),
        "model_status": model_status,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


if __name__ == "__main__":
    rebuild_historical_picks(months=12)
