from __future__ import annotations

import contextlib
import io
from datetime import datetime
from functools import lru_cache
from typing import Any

import pandas as pd

from quant_core.config import BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE, MAIN_WAVE_MIN_SCORE, REVERSAL_MIN_SCORE
from quant_core.data_pipeline.market import fetch_sina_snapshot
from quant_core.engine.predictor import PROFIT_TARGET_PCT, apply_production_filters, build_features, score_candidates, select_strategy_top_picks
from quant_core.storage import connect, init_db


SWING_STRATEGY_TYPES = {"中线超跌反转", "右侧主升浪", "全局动量狙击"}


def top_pick_open_backtest(months: int = 2, refresh: bool = False) -> dict[str, Any]:
    init_db()
    from quant_core.strategies.labs.strategy_lab import prepare_evaluated_candidates

    prepared = prepare_evaluated_candidates(months, refresh=refresh)
    feature_df = prepared["evaluated"]
    if feature_df.empty:
        return _empty_result("过滤后没有候选股票")
    strategy_rows = _strategy_pick_rows(feature_df, months=min(2, months))
    candidate_strategy_counts = feature_df["strategy_type"].fillna("尾盘突破").value_counts().to_dict() if "strategy_type" in feature_df.columns else {}
    feature_df = apply_production_filters(feature_df)
    if feature_df.empty:
        result = _empty_result("生产过滤后没有候选股票")
        result["strategy_rows"] = strategy_rows
        result["summary"]["candidate_strategy_counts"] = {str(key): int(value) for key, value in candidate_strategy_counts.items()}
        return result

    pick_frames = []
    for _, day_pool in feature_df.groupby("date", sort=True):
        pick_frames.append(select_strategy_top_picks(day_pool, limit_per_strategy=1))
    picks = pd.concat(pick_frames, ignore_index=True) if pick_frames else pd.DataFrame()
    if not picks.empty:
        picks = picks.sort_values(
            ["date", "策略优先级", "排序评分", "预期溢价", "综合评分"],
            ascending=[True, False, False, False, False],
        ).copy()

    results: list[dict[str, Any]] = []
    for _, pick in picks.iterrows():
        strategy_type = str(pick.get("strategy_type", "尾盘突破"))
        if strategy_type in SWING_STRATEGY_TYPES and pd.isna(pick.get("t3_max_gain_pct")):
            continue
        code = str(pick["纯代码"])
        current_date = str(pick["date"])
        current_close = float(pick["最新价"])
        next_open = float(pick["next_open"]) if pd.notna(pick.get("next_open")) else None
        premium = float(pick["open_premium"]) if pd.notna(pick.get("open_premium")) else None
        results.append(_backtest_row(pick, current_close, next_open, premium))

    evaluated = [row for row in results if row["success"] is not None]
    wins = [row for row in evaluated if row["success"]]
    premiums = [float(row["open_premium"]) for row in evaluated if row["open_premium"] is not None]
    reversal_rows = [row for row in results if row.get("strategy_type") == "中线超跌反转" and row.get("t3_max_gain_pct") is not None]
    reversal_gains = [float(row["t3_max_gain_pct"]) for row in reversal_rows]
    main_wave_rows = [row for row in results if row.get("strategy_type") == "右侧主升浪" and row.get("t3_max_gain_pct") is not None]
    main_wave_gains = [float(row["t3_max_gain_pct"]) for row in main_wave_rows]
    strategy_counts = pd.Series([row.get("strategy_type", "尾盘突破") for row in results]).value_counts().to_dict()
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
        "reversal_trade_count": len(reversal_rows),
        "reversal_t3_win_rate": round(float((pd.Series(reversal_gains) > 0).mean() * 100), 4) if reversal_gains else 0.0,
        "reversal_avg_t3_max_gain_pct": round(float(pd.Series(reversal_gains).mean()), 4) if reversal_gains else 0.0,
        "main_wave_trade_count": len(main_wave_rows),
        "main_wave_t3_win_rate": round(float((pd.Series(main_wave_gains) > 0).mean() * 100), 4) if main_wave_gains else 0.0,
        "main_wave_avg_t3_max_gain_pct": round(float(pd.Series(main_wave_gains).mean()), 4) if main_wave_gains else 0.0,
        "median_open_premium": round(float(pd.Series(premiums).median()), 4) if premiums else 0.0,
        "best_open_premium": round(max(premiums), 4) if premiums else 0.0,
        "worst_open_premium": round(min(premiums), 4) if premiums else 0.0,
        "model_status": prepared["model_status"],
        "repaired_pre_close_count": prepared["repaired_pre_close_count"],
        "repaired_volume_ratio_count": prepared["repaired_volume_ratio_count"],
        "rule": f"生产策略复盘：排除周末、节假日、非完整交易日、创业板、北交所、科创板、ST/退市；大盘风控采用晴天/震荡/阴天/雷暴分级，尾盘突破综合评分>={BREAKOUT_MIN_SCORE:.1f}，首阴低吸综合评分>={DIPBUY_MIN_SCORE:.1f}，中线超跌反转预期T+3最大涨幅>={REVERSAL_MIN_SCORE:.1f}%，右侧主升浪预期T+3最大涨幅>={MAIN_WAVE_MIN_SCORE:.1f}%；雷暴或大盘下跌且缩量时空仓；过滤高位爆量、尾盘诱多，突破额外过滤涨幅>=7%、上影>=2%、近3日断头铡刀。每个交易日按策略分组独立选 Top1，短线策略按次日开盘卖出，波段策略统计T+3最大区间涨幅。",
        "trading_day_filter": "weekday<5 且全市场有效样本>=1000 且成交额>0。",
        "rank_rule": "XGBRegressor 分策略预测收益；全局日线模型已收编为全局动量狙击，四大核心军团各自独立出票；同一策略内按排序评分、预期收益和综合评分择优。",
    }
    return {"created_at": datetime.now().isoformat(timespec="seconds"), "summary": summary, "rows": results[::-1], "strategy_rows": strategy_rows}


def _backtest_row(pick: pd.Series, current_close: float, next_open: float | None, premium: float | None) -> dict[str, Any]:
    code = str(pick["纯代码"])
    strategy_type = str(pick.get("strategy_type", "尾盘突破"))
    t3_gain = float(pick["t3_max_gain_pct"]) if pd.notna(pick.get("t3_max_gain_pct")) else None
    success = (t3_gain > 0) if strategy_type in SWING_STRATEGY_TYPES and t3_gain is not None else (premium > PROFIT_TARGET_PCT if premium is not None else None)
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
        "success": success,
    }


def _strategy_performance_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = ["全局动量狙击", "右侧主升浪", "中线超跌反转", "尾盘突破"]
    rows: list[dict[str, Any]] = []
    for strategy_type in order:
        items = [row for row in results if row.get("strategy_type", "尾盘突破") == strategy_type]
        evaluated = [row for row in items if row.get("success") is not None]
        wins = [row for row in evaluated if row.get("success")]
        if strategy_type in SWING_STRATEGY_TYPES:
            gains = [float(row["t3_max_gain_pct"]) for row in evaluated if row.get("t3_max_gain_pct") is not None]
            metric_label = "T+3平均最大涨幅"
            metric_value = round(float(pd.Series(gains).mean()), 4) if gains else 0.0
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


def _strategy_pick_rows(df: pd.DataFrame, months: int = 2) -> list[dict[str, Any]]:
    if df.empty or "strategy_type" not in df.columns:
        return []
    candidates = df.copy()
    candidates["strategy_type"] = candidates["strategy_type"].fillna("尾盘突破")
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
