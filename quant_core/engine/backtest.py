from __future__ import annotations

import contextlib
import io
from datetime import datetime
from functools import lru_cache
from typing import Any

import pandas as pd

from quant_core.config import BREAKOUT_MIN_SCORE, GLOBAL_MIN_SCORE, PRODUCTION_TOTAL_PICK_LIMIT, ST_BREAKOUT_MIN_SCORE
from quant_core.data_pipeline.market import fetch_sina_snapshot
from quant_core.engine.predictor import (
    BREAKOUT_STRATEGY_TYPE,
    GLOBAL_MOMENTUM_STRATEGY_TYPE,
    PRODUCTION_OUTPUT_STRATEGIES,
    PROFIT_TARGET_PCT,
    apply_production_filters,
    filter_paused_strategies,
    prepare_historical_playback_candidates,
    scan_market,
)
from quant_core.storage import connect, init_db


SWING_STRATEGY_TYPES = {GLOBAL_MOMENTUM_STRATEGY_TYPE}


def top_pick_open_backtest(months: int = 2, refresh: bool = False) -> dict[str, Any]:
    init_db()
    latest_date = _latest_trade_date()
    if not latest_date:
        return _empty_result("stock_daily 无可用交易日")

    end_date = str(latest_date)
    start_date = (pd.Timestamp(end_date) - pd.DateOffset(months=max(1, int(months)))).strftime("%Y-%m-%d")
    prepared = prepare_historical_playback_candidates(start_date=start_date, end_date=end_date)
    candidates = prepared.get("candidates", pd.DataFrame())
    trading_dates = list(prepared.get("trading_dates") or [])
    if candidates.empty or not trading_dates:
        result = _empty_result(str(prepared.get("model_status") or "历史候选池为空"))
        result["summary"]["months"] = months
        result["summary"]["start_date"] = prepared.get("start_date")
        result["summary"]["end_date"] = prepared.get("end_date")
        return result

    active_strategies = set(PRODUCTION_OUTPUT_STRATEGIES)
    candidate_strategy_counts = (
        candidates["strategy_type"]
        .fillna(BREAKOUT_STRATEGY_TYPE)
        .loc[lambda series: series.isin(active_strategies)]
        .value_counts()
        .to_dict()
        if "strategy_type" in candidates.columns
        else {}
    )
    results: list[dict[str, Any]] = []
    for trade_date in trading_dates:
        payload = scan_market(
            limit=PRODUCTION_TOTAL_PICK_LIMIT,
            persist_snapshot=False,
            cache_prediction=False,
            async_persist=False,
            target_date=trade_date,
            historical_candidates=candidates,
        )
        for row in payload.get("rows") or []:
            if str(row.get("strategy_type") or "") not in active_strategies:
                continue
            results.append(_backtest_row_from_api(row))

    evaluated = [row for row in results if row["success"] is not None]
    wins = [row for row in evaluated if row["success"]]
    premiums = [float(row["open_premium"]) for row in evaluated if row["open_premium"] is not None]
    strategy_counts = pd.Series([row.get("strategy_type", BREAKOUT_STRATEGY_TYPE) for row in results]).value_counts().to_dict()
    strategy_performance = _strategy_performance_rows(results)
    summary = {
        "months": months,
        "start_date": prepared["start_date"],
        "end_date": prepared["end_date"],
        "total_days": int(pd.Series([row["date"] for row in results]).nunique()) if results else 0,
        "trade_count": len(results),
        "evaluated_days": len(evaluated),
        "pending_days": len(results) - len(evaluated),
        "win_count": len(wins),
        "loss_count": len(evaluated) - len(wins),
        "win_rate": round(len(wins) / len(evaluated) * 100, 4) if evaluated else 0.0,
        "strategy_counts": {str(key): int(value) for key, value in strategy_counts.items()},
        "candidate_strategy_counts": {str(key): int(value) for key, value in candidate_strategy_counts.items()},
        "strategy_performance": strategy_performance,
        "avg_open_premium": round(float(pd.Series(premiums).mean()), 4) if premiums else 0.0,
        "reversal_trade_count": 0,
        "reversal_t3_win_rate": 0.0,
        "reversal_avg_t3_close_return_pct": 0.0,
        "main_wave_trade_count": 0,
        "main_wave_t3_win_rate": 0.0,
        "main_wave_avg_t3_close_return_pct": 0.0,
        "median_open_premium": round(float(pd.Series(premiums).median()), 4) if premiums else 0.0,
        "best_open_premium": round(max(premiums), 4) if premiums else 0.0,
        "worst_open_premium": round(min(premiums), 4) if premiums else 0.0,
        "model_status": prepared["model_status"],
        "repaired_pre_close_count": prepared["repaired_pre_close_count"],
        "repaired_volume_ratio_count": prepared["repaired_volume_ratio_count"],
        "rule": f"生产策略复盘：历史回放逐日调用 scan_market(target_date)，与 14:50 实时推送共用评分、过滤、动态底线、Half-Kelly 仓位和分策略 Top1 选择链路；当前启用全局动量狙击、尾盘突破与尾盘突破-ST特情。普通突破综合评分>={BREAKOUT_MIN_SCORE:.1f}，ST特情综合评分>={ST_BREAKOUT_MIN_SCORE:.1f}，全局狙击概率>={GLOBAL_MIN_SCORE:.2f}；雷暴或大盘下跌且缩量时空仓；历史日线 15:00 收盘行作为 14:50 观察代理。",
        "trading_day_filter": "weekday<5 且全市场有效样本>=1000 且成交额>0。",
        "rank_rule": "全局动量狙击按 T+3 波段收益口径结算；尾盘突破按 T+1 开盘溢价口径结算；同一策略内按当前生产 selection_score 和动态底线择优。",
        "active_strategy_types": list(PRODUCTION_OUTPUT_STRATEGIES),
    }
    strategy_rows = results[::-1][: max(0, min(len(results), 80))]
    return {"created_at": datetime.now().isoformat(timespec="seconds"), "summary": summary, "rows": results[::-1], "strategy_rows": strategy_rows}


def _backtest_row_from_api(row: dict[str, Any]) -> dict[str, Any]:
    strategy_type = str(row.get("strategy_type") or BREAKOUT_STRATEGY_TYPE)
    premium = _optional_float(row.get("open_premium"))
    t3_settlement_price = _optional_float(row.get("t3_settlement_price"))
    t3_settlement_return = _optional_float(row.get("t3_settlement_return_pct"))
    close_price = t3_settlement_price if strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE else _optional_float(row.get("next_open"))
    close_return = t3_settlement_return if strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE else premium
    if strategy_type == GLOBAL_MOMENTUM_STRATEGY_TYPE:
        success = (t3_settlement_return > 0) if t3_settlement_return is not None else None
    else:
        success = (premium > PROFIT_TARGET_PCT) if premium is not None else None
    return {
        "date": str(row.get("date") or ""),
        "code": str(row.get("code") or ""),
        "name": str(row.get("name") or ""),
        "name_source": str(row.get("name_source") or "scan_market"),
        "strategy_type": strategy_type,
        "win_rate": _rounded(row.get("win_rate")),
        "close": _rounded(row.get("price")),
        "change": _rounded(row.get("change")),
        "turnover": _rounded(row.get("turnover")),
        "expected_premium": _rounded(row.get("expected_premium")),
        "risk_score": _rounded(row.get("risk_score")),
        "liquidity_score": _rounded(row.get("liquidity_score")),
        "composite_score": _rounded(row.get("composite_score")),
        "sort_score": _rounded(row.get("sort_score")),
        "score_threshold": _rounded(row.get("score_threshold")),
        "selection_score": _rounded(row.get("selection_score"), digits=6),
        "selection_tier": str(row.get("selection_tier") or ""),
        "dynamic_floor": _rounded(row.get("dynamic_floor"), digits=6),
        "risk_warning": str(row.get("risk_warning") or ""),
        "suggested_position": _rounded(row.get("suggested_position")),
        "sentiment_bonus": _rounded(row.get("sentiment_bonus")),
        "market_gate_mode": str(row.get("market_gate_mode") or ""),
        "next_date": str(row.get("next_date")) if row.get("next_date") else None,
        "t3_exit_date": str(row.get("t3_exit_date")) if row.get("t3_exit_date") else None,
        "next_open": _rounded(row.get("next_open")),
        "open_premium": _rounded(premium),
        "t3_max_gain_pct": _rounded(row.get("t3_max_gain_pct")),
        "t3_close": _rounded(row.get("t3_close")),
        "t3_close_return_pct": _rounded(row.get("t3_close_return_pct")),
        "t3_settlement_price": _rounded(t3_settlement_price),
        "t3_settlement_return_pct": _rounded(t3_settlement_return),
        "close_price": _rounded(close_price),
        "close_return_pct": _rounded(close_return),
        "success": success,
    }


def _backtest_row(pick: pd.Series, current_close: float, next_open: float | None, premium: float | None) -> dict[str, Any]:
    code = str(pick["纯代码"])
    strategy_type = str(pick.get("strategy_type", "尾盘突破"))
    t3_gain = float(pick["t3_max_gain_pct"]) if pd.notna(pick.get("t3_max_gain_pct")) else None
    t3_close = _optional_float(pick.get("t3_close"))
    t3_close_return = _optional_float(pick.get("t3_close_return_pct"))
    t3_settlement_price = _optional_float(pick.get("t3_settlement_price"))
    t3_settlement_return = _swing_settlement_return(pick)
    if t3_settlement_price is None:
        t3_settlement_price = t3_close
    success = (t3_settlement_return > 0) if strategy_type in SWING_STRATEGY_TYPES and t3_settlement_return is not None else (premium > PROFIT_TARGET_PCT if premium is not None else None)
    close_price = t3_settlement_price if strategy_type in SWING_STRATEGY_TYPES else next_open
    close_return = t3_settlement_return if strategy_type in SWING_STRATEGY_TYPES else premium
    return {
        "date": str(pick["date"]),
        "code": code,
        "name": _display_name(code, pick.get("名称")),
        "name_source": str(pick.get("name_source", "unknown")),
        "strategy_type": strategy_type,
        "win_rate": round(float(pick["AI胜率"]), 4),
        "close": round(current_close, 4),
        "change": round(float(pick["涨跌幅"]), 4),
        "turnover": round(float(pick["换手率"]), 4),
        "expected_premium": round(float(pick.get("预期溢价", 0)), 4),
        "risk_score": round(float(pick.get("风险评分", 0)), 4),
        "liquidity_score": round(float(pick.get("流动性评分", 0)), 4),
        "composite_score": round(float(pick.get("综合评分", pick["AI胜率"])), 4),
        "sort_score": round(float(pick.get("排序评分", pick.get("综合评分", pick["AI胜率"]))), 4),
        "score_threshold": round(float(pick.get("生产门槛", BREAKOUT_MIN_SCORE)), 4),
        "sentiment_bonus": round(float(pick.get("情绪补偿分", 0)), 4),
        "market_gate_mode": str(pick.get("market_gate_mode", "")),
        "next_date": str(pick.get("next_date")) if pd.notna(pick.get("next_date")) else None,
        "t3_exit_date": str(pick.get("t3_exit_date")) if pd.notna(pick.get("t3_exit_date")) else None,
        "next_open": round(next_open, 4) if next_open is not None else None,
        "open_premium": round(premium, 4) if premium is not None else None,
        "t3_max_gain_pct": round(t3_gain, 4) if t3_gain is not None else None,
        "t3_close": round(t3_close, 4) if t3_close is not None else None,
        "t3_close_return_pct": round(t3_close_return, 4) if t3_close_return is not None else None,
        "t3_settlement_price": round(t3_settlement_price, 4) if t3_settlement_price is not None else None,
        "t3_settlement_return_pct": round(t3_settlement_return, 4) if t3_settlement_return is not None else None,
        "close_price": round(close_price, 4) if close_price is not None else None,
        "close_return_pct": round(close_return, 4) if close_return is not None else None,
        "success": success,
    }


def _strategy_performance_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = list(PRODUCTION_OUTPUT_STRATEGIES)
    rows: list[dict[str, Any]] = []
    for strategy_type in order:
        items = [row for row in results if row.get("strategy_type", "尾盘突破") == strategy_type]
        evaluated = [row for row in items if row.get("success") is not None]
        wins = [row for row in evaluated if row.get("success")]
        if strategy_type in SWING_STRATEGY_TYPES:
            returns = [float(row["t3_settlement_return_pct"]) for row in evaluated if row.get("t3_settlement_return_pct") is not None]
            metric_label = "T+3平均结算收益"
            metric_value = round(float(pd.Series(returns).mean()), 4) if returns else 0.0
        else:
            premiums = [float(row["open_premium"]) for row in evaluated if row.get("open_premium") is not None]
            metric_label = "T+1平均开盘溢价"
            metric_value = round(float(pd.Series(premiums).mean()), 4) if premiums else 0.0
        rows.append(
            {
                "strategy_type": strategy_type,
                "trades": len(items),
                "evaluated": len(evaluated),
                "wins": len(wins),
                "losses": len(evaluated) - len(wins),
                "win_rate": round(len(wins) / len(evaluated) * 100, 4) if evaluated else 0.0,
                "metric_label": metric_label,
                "metric_value": metric_value,
            }
        )
    return rows


def _rounded(value: Any, digits: int = 4) -> float | None:
    parsed = _optional_float(value)
    return round(parsed, digits) if parsed is not None else None


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if pd.notna(parsed) else None


def _swing_settlement_return(row: pd.Series | dict[str, Any]) -> float | None:
    value = _optional_float(row.get("t3_settlement_return_pct"))
    if value is not None:
        return value
    return _optional_float(row.get("t3_close_return_pct"))


def _strategy_pick_rows(df: pd.DataFrame, months: int = 2) -> list[dict[str, Any]]:
    if df.empty or "strategy_type" not in df.columns:
        return []
    candidates = df.copy()
    candidates["strategy_type"] = candidates["strategy_type"].fillna("尾盘突破")
    candidates = filter_paused_strategies(candidates)
    if candidates.empty:
        return []
    candidates["_date_sort"] = pd.to_datetime(candidates["date"], errors="coerce")
    latest_date = candidates["_date_sort"].max()
    if pd.notna(latest_date):
        start_date = latest_date - pd.DateOffset(months=max(1, int(months)))
        candidates = candidates[candidates["_date_sort"] >= start_date].copy()
    qualified_indices = set(apply_production_filters(candidates).index)
    picks = []
    for _, group in candidates.groupby(["date", "strategy_type"], sort=False):
        qualified = group[group.index.isin(qualified_indices)]
        source = qualified if not qualified.empty else group
        sort_cols = ["排序评分", "预期溢价", "综合评分"] if "排序评分" in source.columns else ["预期溢价", "综合评分"]
        pick = source.sort_values(sort_cols, ascending=[False] * len(sort_cols)).iloc[0].copy()
        pick["production_qualified"] = bool(pick.name in qualified_indices)
        picks.append(pick)
    if not picks:
        return []
    picks_df = pd.DataFrame(picks).sort_values(["date", "strategy_type"], ascending=[False, True])
    rows: list[dict[str, Any]] = []
    for _, pick in picks_df.iterrows():
        next_open = float(pick["next_open"]) if pd.notna(pick.get("next_open")) else None
        premium = float(pick["open_premium"]) if pd.notna(pick.get("open_premium")) else None
        row = _backtest_row(pick, float(pick["最新价"]), next_open, premium)
        row["production_qualified"] = bool(pick.get("production_qualified", False))
        rows.append(row)
    return rows


def _latest_trade_date() -> str | None:
    with connect() as conn:
        row = conn.execute("SELECT MAX(date) AS latest_date FROM stock_daily").fetchone()
    return row["latest_date"] if row and row["latest_date"] else None


def _load_daily_rows(start_date: str) -> pd.DataFrame:
    with connect() as conn:
        return pd.read_sql_query(
            """
            SELECT code, name, date, open, high, low, close, pre_close, change_pct,
                   volume, amount, turnover, volume_ratio
            FROM stock_daily
            WHERE date >= ?
            ORDER BY date ASC, code ASC
            """,
            conn,
            params=(start_date,),
        )


def _repair_missing_pre_close(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df.sort_values(["code", "date"], inplace=True)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["pre_close"] = pd.to_numeric(df["pre_close"], errors="coerce")
    previous_close = df.groupby("code", sort=False)["close"].shift(1)
    missing = (df["pre_close"].isna() | (df["pre_close"] <= 0)) & previous_close.notna() & (previous_close > 0)
    repaired_count = int(missing.sum())
    if repaired_count:
        df.loc[missing, "pre_close"] = previous_close.loc[missing]
    return repaired_count


def _repair_missing_volume_ratio(df: pd.DataFrame, window: int = 5) -> int:
    if df.empty:
        return 0
    df.sort_values(["code", "date"], inplace=True)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["volume_ratio"] = pd.to_numeric(df["volume_ratio"], errors="coerce")
    avg_volume = (
        df.groupby("code", sort=False)["volume"]
        .transform(lambda values: values.shift(1).rolling(window=window, min_periods=3).mean())
    )
    missing = (df["volume_ratio"].isna() | (df["volume_ratio"] <= 0)) & avg_volume.notna() & (avg_volume > 0) & (df["volume"] > 0)
    repaired_count = int(missing.sum())
    if repaired_count:
        df.loc[missing, "volume_ratio"] = df.loc[missing, "volume"] / avg_volume.loc[missing]
    return repaired_count


def _fill_missing_names(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["code"] = out["code"].astype(str).str.extract(r"(\d{6})")[0].fillna("")
    out["name"] = out["name"].fillna("").astype(str).str.strip()
    out["name_source"] = "daily"
    missing = out["name"] == ""
    out.loc[missing, "name_source"] = "missing"
    if not missing.any():
        return out

    name_map = _latest_db_name_map()
    _apply_name_map(out, name_map, "db_latest")

    missing_codes = _missing_name_codes(out)
    if missing_codes:
        akshare_names = _akshare_name_map()
        _apply_name_map(out, {code: akshare_names[code] for code in missing_codes if code in akshare_names}, "akshare")

    missing_codes = _missing_name_codes(out)
    if missing_codes:
        _apply_name_map(out, _sina_name_map(missing_codes), "sina_snapshot")

    out.loc[out["name"].fillna("").astype(str).str.strip() == "", "name"] = "名称缺失"
    return out


def _apply_name_map(df: pd.DataFrame, name_map: dict[str, str], source: str) -> None:
    if not name_map:
        return
    missing = df["name"].fillna("").astype(str).str.strip() == ""
    mapped = df.loc[missing, "code"].map(name_map).fillna("").astype(str).str.strip()
    has_name = mapped != ""
    if not has_name.any():
        return
    target_index = mapped[has_name].index
    df.loc[target_index, "name"] = mapped.loc[target_index]
    df.loc[target_index, "name_source"] = source


def _missing_name_codes(df: pd.DataFrame) -> set[str]:
    missing = df["name"].fillna("").astype(str).str.strip() == ""
    return set(df.loc[missing, "code"].dropna().astype(str))


@lru_cache(maxsize=1)
def _latest_db_name_map() -> dict[str, str]:
    query = """
        SELECT code, name, date
        FROM stock_daily
        WHERE name IS NOT NULL AND TRIM(name) <> ''
        ORDER BY date DESC
    """
    with connect() as conn:
        rows = conn.execute(query).fetchall()

    names: dict[str, str] = {}
    for row in rows:
        code = str(row["code"] or "")
        name = str(row["name"] or "").strip()
        if code and name and code not in names:
            names[code] = name
    return names


@lru_cache(maxsize=1)
def _akshare_name_map() -> dict[str, str]:
    try:
        import akshare as ak

        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            stock_names = ak.stock_info_a_code_name()
    except Exception:
        return {}

    if stock_names.empty or "code" not in stock_names.columns or "name" not in stock_names.columns:
        return {}
    codes = stock_names["code"].astype(str).str.extract(r"(\d{6})")[0].fillna("")
    names = stock_names["name"].fillna("").astype(str).str.strip()
    return {
        code: name
        for code, name in zip(codes, names)
        if code and name
    }


def _sina_name_map(codes: set[str]) -> dict[str, str]:
    if not codes:
        return {}
    try:
        snapshot = fetch_sina_snapshot(timeout=5)
    except Exception:
        return {}
    if snapshot.empty or "code" not in snapshot.columns or "name" not in snapshot.columns:
        return {}
    subset = snapshot[snapshot["code"].astype(str).isin(codes)].copy()
    return {
        str(row["code"]): str(row["name"]).strip()
        for _, row in subset.iterrows()
        if str(row.get("name", "")).strip()
    }


def _display_name(code: str, value: object) -> str:
    name = str(value or "").strip()
    return name if name else f"{code} 名称缺失"


def _valid_trading_dates(df: pd.DataFrame) -> list[str]:
    daily = (
        df.groupby("date", as_index=False)
        .agg(row_count=("code", "nunique"), amount_sum=("amount", "sum"))
        .sort_values("date")
    )
    daily["weekday"] = pd.to_datetime(daily["date"], errors="coerce").dt.weekday
    valid = daily[
        (daily["weekday"] < 5)
        & (daily["row_count"] >= 1000)
        & (daily["amount_sum"].fillna(0) > 0)
    ].copy()
    return valid["date"].astype(str).tolist()


def _row_for_code_on_date(group: pd.DataFrame | None, target_date: str | None) -> pd.Series | None:
    if group is None or group.empty or target_date is None:
        return None
    matched = group[group["date"] == target_date]
    if matched.empty:
        return None
    row = matched.iloc[0]
    if float(row.get("open") or 0) <= 0:
        return None
    return row


def _empty_result(reason: str) -> dict[str, Any]:
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "summary": {
            "months": 2,
            "start_date": None,
            "end_date": None,
            "total_days": 0,
            "evaluated_days": 0,
            "pending_days": 0,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0.0,
            "strategy_counts": {},
            "candidate_strategy_counts": {},
            "strategy_performance": [],
            "avg_open_premium": 0.0,
            "median_open_premium": 0.0,
            "best_open_premium": 0.0,
            "worst_open_premium": 0.0,
            "model_status": reason,
            "rule": "无可用数据",
        },
        "rows": [],
        "strategy_rows": [],
    }


if __name__ == "__main__":
    import json

    result = top_pick_open_backtest(months=12, refresh=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
