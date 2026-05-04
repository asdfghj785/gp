from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd

from quant_core.config import PROFIT_TARGET_PCT
from quant_core.data_pipeline.trading_calendar import next_trading_day, nth_trading_day
from quant_core.daily_pick import list_daily_pick_results
from quant_core.engine.predictor import SWING_STRATEGY_TYPES, prepare_historical_playback_candidates, scan_market
from quant_core.storage import (
    clear_daily_picks,
    mark_daily_pick_closed,
    save_daily_pick,
    update_daily_pick_open,
    update_daily_pick_t3_gain,
)


DEFAULT_START_DATE = "2024-03-01"


def rebuild_historical_picks(start_date: str = DEFAULT_START_DATE, end_date: str | None = None) -> dict[str, Any]:
    print(
        f"开始 V4.4 Historical Playback：清空 daily_picks 后从 {start_date} 回放到最近有效交易日。",
        flush=True,
    )
    deleted = clear_daily_picks()
    print(f"已清空 daily_picks 旧记录：{deleted} 条。", flush=True)

    prepared = prepare_historical_playback_candidates(start_date=start_date, end_date=end_date)
    candidates = prepared.get("candidates", pd.DataFrame())
    trading_dates = list(prepared.get("trading_dates") or [])
    if candidates.empty or not trading_dates:
        print(f"历史候选池为空：{prepared.get('model_status')}", flush=True)
        return _summary(start_date, prepared.get("end_date"), deleted, inserted=0, updated=0, skipped=0, model_status=str(prepared.get("model_status") or "empty"))

    print(
        "历史候选池就绪："
        f"{prepared.get('start_date')} -> {prepared.get('end_date')}，"
        f"{len(trading_dates)} 个交易日，{len(candidates)} 行候选，"
        f"model_status={prepared.get('model_status')}",
        flush=True,
    )

    inserted = 0
    updated = 0
    skipped = 0
    total = len(trading_dates)

    for index, selection_date in enumerate(trading_dates, start=1):
        scan = scan_market(
            limit=0,
            persist_snapshot=False,
            cache_prediction=False,
            target_date=selection_date,
            historical_candidates=candidates,
        )
        rows = list(scan.get("rows") or [])
        if not rows:
            skipped += 1
            gate = scan.get("market_gate") or {}
            reasons = "；".join(str(item) for item in gate.get("reasons", []) if item) or scan.get("model_status", "无合格标的")
            print(f"进度 {index}/{total} 天 {selection_date}：空仓，mode={gate.get('mode', '-')}，reason={reasons}", flush=True)
            continue

        day_inserted = 0
        day_updated = 0
        messages: list[str] = []
        for winner in rows:
            strategy_type = str(winner.get("strategy_type") or "尾盘突破")
            missing_settlement = strategy_type in SWING_STRATEGY_TYPES and _swing_settlement_return(winner) is None
            missing_open = strategy_type not in SWING_STRATEGY_TYPES and winner.get("next_open") is None

            pick = _pick_from_scan_winner(winner, scan)
            row_id = save_daily_pick(pick)
            if row_id:
                inserted += 1
                day_inserted += 1

            next_open = winner.get("next_open")
            if next_open is not None:
                close_position = strategy_type not in SWING_STRATEGY_TYPES
                updated_pick = update_daily_pick_open(
                    selection_date,
                    float(next_open),
                    f"{winner.get('next_date') or selection_date}T09:30:00",
                    close_position=close_position,
                    strategy_type=strategy_type,
                    code=str(winner.get("code") or ""),
                    pick_id=row_id or None,
                )
                if updated_pick:
                    updated += 1
                    day_updated += 1

            if strategy_type in SWING_STRATEGY_TYPES:
                t3_gain = winner.get("t3_max_gain_pct")
                t3_exit_date = winner.get("t3_exit_date") or winner.get("next_date") or selection_date
                if t3_gain is not None:
                    updated_pick = update_daily_pick_t3_gain(
                        selection_date,
                        float(t3_gain),
                        f"{t3_exit_date}T14:45:00",
                        strategy_type=strategy_type,
                        code=str(winner.get("code") or ""),
                        pick_id=row_id or None,
                    )
                    if updated_pick:
                        updated += 1
                        day_updated += 1
                t3_close = winner.get("t3_close")
                close_return = winner.get("t3_close_return_pct")
                if t3_close is not None and close_return is not None:
                    close_reason = "历史T+3收盘闭环"
                    closed_pick = mark_daily_pick_closed(
                        selection_date,
                        float(t3_close),
                        float(close_return),
                        close_reason,
                        checked_at=f"{t3_exit_date}T15:00:00",
                        close_signal={
                            "action": close_reason,
                            "level": "time",
                            "instruction": "Historical Playback 统一使用 T+3 当天 15:00 收盘价闭环；T+3 最大浮盈只作潜力参考，不参与盈亏统计。",
                            "t3_max_gain_pct": winner.get("t3_max_gain_pct"),
                            "t3_close_return_pct": winner.get("t3_close_return_pct"),
                            "t3_settlement_return_pct": winner.get("t3_close_return_pct"),
                            "pushed_at": f"{t3_exit_date}T15:00:00",
                            "push_status": "historical_no_push",
                        },
                        strategy_type=strategy_type,
                        code=str(winner.get("code") or ""),
                        pick_id=row_id or None,
                    )
                    if closed_pick:
                        updated += 1
                        day_updated += 1

            if missing_settlement:
                messages.append(f"{winner.get('code')}[{strategy_type}] T+3 未闭环，已保留为未结算推荐")
            elif missing_open:
                messages.append(f"{winner.get('code')}[{strategy_type}] T+1 未闭环，已保留为未结算推荐")
            messages.append(_winner_log_line(winner))

        if day_inserted == 0:
            skipped += 1
        print(
            f"进度 {index}/{total} 天 {selection_date}：锁定 {day_inserted} 只，回填 {day_updated} 次；"
            + "；".join(messages),
            flush=True,
        )

    result = _summary(
        start_date,
        prepared.get("end_date"),
        deleted,
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        model_status=str(prepared.get("model_status") or "ready"),
    )
    print(
        "V4.4 Historical Playback 完成："
        f"出手 {result['trade_count']} 次，"
        f"已评估 {result['evaluated_count']} 次，"
        f"胜率 {result['win_rate']:.2f}%，"
        f"策略分布 {result['strategy_counts']}。",
        flush=True,
    )
    return result


def _pick_from_scan_winner(winner: dict[str, Any], scan: dict[str, Any]) -> dict[str, Any]:
    selection_date = str(scan.get("prediction_date") or winner.get("date"))
    strategy_type = str(winner.get("strategy_type") or "尾盘突破")
    target_date = _target_date_for_replay(selection_date, strategy_type, winner)
    return {
        "selection_date": selection_date,
        "target_date": target_date,
        "selected_at": f"{selection_date}T14:50:00",
        "code": str(winner["code"]).zfill(6),
        "name": str(winner.get("name") or ""),
        "strategy_type": strategy_type,
        "win_rate": float(winner.get("win_rate") or 0),
        "selection_price": float(winner.get("price") or 0),
        "selection_change": float(winner.get("change") or 0),
        "snapshot_time": "14:50:00",
        "snapshot_price": float(winner.get("price") or 0),
        "snapshot_vol_ratio": float(winner.get("volume_ratio") or 0),
        "is_shadow_test": False,
        "t3_max_gain_pct": float(winner["t3_max_gain_pct"]) if strategy_type in SWING_STRATEGY_TYPES and winner.get("t3_max_gain_pct") is not None else None,
        "suggested_position": winner.get("suggested_position"),
        "tier": winner.get("selection_tier") or "base",
        "model_status": str(scan.get("model_status") or ""),
        "status": "pending_open",
        "raw": {
            "source": "historical_production_replay",
            "winner": winner,
            "scan_id": scan.get("id"),
            "scan_created_at": scan.get("created_at"),
            "strategy": scan.get("strategy"),
            "market_gate": scan.get("market_gate"),
            "profit_target_pct": PROFIT_TARGET_PCT,
        },
    }


def _target_date_for_replay(selection_date: str, strategy_type: str, winner: dict[str, Any]) -> str:
    if strategy_type in SWING_STRATEGY_TYPES:
        explicit = winner.get("t3_exit_date") or winner.get("next_date")
        if explicit:
            return str(explicit)[:10]
        return nth_trading_day(pd.Timestamp(selection_date).date(), 3).isoformat()
    explicit = winner.get("next_date")
    if explicit:
        return str(explicit)[:10]
    return next_trading_day(pd.Timestamp(selection_date).date()).isoformat()


def _winner_log_line(winner: dict[str, Any]) -> str:
    tier = str(winner.get("selection_tier") or "base")
    position = winner.get("suggested_position")
    position_text = "-" if position is None else f"{float(position) * 100:.0f}%"
    score = winner.get("selection_score", winner.get("composite_score"))
    floor = winner.get("dynamic_floor", winner.get("score_floor"))
    open_premium = winner.get("open_premium")
    t3_gain = winner.get("t3_max_gain_pct")
    close_return = _swing_settlement_return(winner)
    result_text = f"T+1开盘 {float(open_premium):.2f}%" if open_premium is not None else "T+1开盘 -"
    if winner.get("strategy_type") in SWING_STRATEGY_TYPES:
        result_text = (
            f"T+3最大浮盈 {float(t3_gain):.2f}% / 结算 {float(close_return):.2f}%"
            if t3_gain is not None and close_return is not None
            else "T+3未闭环"
        )
    probe = "，dynamic_floor敢死队仓位=5%" if tier == "dynamic_floor" else ""
    return (
        f"{winner.get('code')} {winner.get('name')}[{winner.get('strategy_type')}] "
        f"tier={tier}{probe} score={float(score or 0):.4f} floor={float(floor or 0):.4f} "
        f"position={position_text} {result_text}"
    )


def _swing_settlement_return(winner: dict[str, Any]) -> float | None:
    value = winner.get("t3_settlement_return_pct")
    if value is None:
        value = winner.get("t3_close_return_pct")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if pd.notna(parsed) else None


def _summary(
    start_date: str,
    end_date: str | None,
    deleted: int,
    inserted: int,
    updated: int,
    skipped: int,
    model_status: str,
) -> dict[str, Any]:
    stored = list_daily_pick_results(limit=10000, shadow_only=False).get("rows", [])
    evaluated = [row for row in stored if row.get("success") is not None]
    wins = [row for row in evaluated if row.get("success")]
    strategy_counts = Counter(str(row.get("strategy_type") or "尾盘突破") for row in stored)
    dynamic_floor_count = sum(1 for row in stored if row.get("selection_tier") == "dynamic_floor")
    premiums = [float(row["open_premium"]) for row in evaluated if row.get("open_premium") is not None]
    swing_rows = [row for row in stored if row.get("strategy_type") in SWING_STRATEGY_TYPES and row.get("close_return_pct") is not None]
    swing_returns = [float(row["close_return_pct"]) for row in swing_rows]
    return {
        "start_date": start_date,
        "end_date": end_date,
        "deleted_rows": deleted,
        "inserted_rows": inserted,
        "updated_rows": updated,
        "skipped_rows": skipped,
        "trade_count": len(stored),
        "evaluated_count": len(evaluated),
        "win_count": len(wins),
        "loss_count": len(evaluated) - len(wins),
        "win_rate": round(len(wins) / len(evaluated) * 100, 4) if evaluated else 0.0,
        "dynamic_floor_count": dynamic_floor_count,
        "avg_open_premium": round(float(pd.Series(premiums).mean()), 4) if premiums else 0.0,
        "swing_trade_count": len(swing_rows),
        "swing_t3_win_count": int((pd.Series(swing_returns) > 0).sum()) if swing_returns else 0,
        "swing_t3_win_rate": round(float((pd.Series(swing_returns) > 0).mean() * 100), 4) if swing_returns else 0.0,
        "swing_avg_t3_close_return_pct": round(float(pd.Series(swing_returns).mean()), 4) if swing_returns else 0.0,
        "strategy_counts": dict(strategy_counts),
        "model_status": model_status,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }


if __name__ == "__main__":
    rebuild_historical_picks()
