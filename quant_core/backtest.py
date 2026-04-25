from __future__ import annotations

import contextlib
import io
from datetime import datetime
from functools import lru_cache
from typing import Any

import pandas as pd

from .config import MIN_COMPOSITE_SCORE
from .market import fetch_sina_snapshot
from .predictor import PROFIT_TARGET_PCT, apply_production_filters, build_features, score_candidates
from .storage import connect, init_db


def top_pick_open_backtest(months: int = 2) -> dict[str, Any]:
    init_db()
    latest_date = _latest_trade_date()
    if latest_date is None:
        return _empty_result("数据库没有可复盘的日线数据")

    start_date = (pd.Timestamp(latest_date) - pd.DateOffset(months=months)).strftime("%Y-%m-%d")
    load_start_date = (pd.Timestamp(start_date) - pd.DateOffset(days=90)).strftime("%Y-%m-%d")
    df = _load_daily_rows(load_start_date)
    if df.empty:
        return _empty_result("近两个月没有可复盘的日线数据")
    df = _fill_missing_names(df)
    repaired_pre_close_count = _repair_missing_pre_close(df)
    repaired_volume_ratio_count = _repair_missing_volume_ratio(df)
    period_df = df[df["date"] >= start_date].copy()
    trading_dates = _valid_trading_dates(period_df)
    if not trading_dates:
        return _empty_result("近两个月没有有效交易日数据")

    latest_date = trading_dates[-1]
    all_trading_dates = _valid_trading_dates(df)
    df = df[df["date"].isin(all_trading_dates)].copy()
    feature_df = build_features(df)
    feature_df = feature_df[feature_df["date"].isin(trading_dates)].copy()
    if feature_df.empty:
        return _empty_result("过滤后没有候选股票")

    feature_df, model_status = score_candidates(feature_df)
    feature_df = apply_production_filters(feature_df)

    feature_df = feature_df.sort_values(["date", "预期溢价", "综合评分"], ascending=[True, False, False])
    idx = feature_df.groupby("date")["预期溢价"].idxmax()
    picks = feature_df.loc[idx].sort_values("date").copy()
    next_trade_date = {
        trading_dates[index]: trading_dates[index + 1]
        for index in range(len(trading_dates) - 1)
    }

    all_rows = df.sort_values(["code", "date"])
    by_code = {
        code: group.reset_index(drop=True)
        for code, group in all_rows.groupby("code", sort=False)
    }

    results: list[dict[str, Any]] = []
    for _, pick in picks.iterrows():
        code = str(pick["纯代码"])
        current_date = str(pick["date"])
        current_close = float(pick["最新价"])
        target_next_date = next_trade_date.get(current_date)
        next_row = _row_for_code_on_date(by_code.get(code), target_next_date)
        item = {
            "date": current_date,
            "code": code,
            "name": _display_name(code, pick.get("名称")),
            "name_source": str(pick.get("name_source", "unknown")),
            "win_rate": round(float(pick["AI胜率"]), 4),
            "close": round(current_close, 4),
            "change": round(float(pick["涨跌幅"]), 4),
            "turnover": round(float(pick["换手率"]), 4),
            "expected_premium": round(float(pick.get("预期溢价", 0)), 4),
            "risk_score": round(float(pick.get("风险评分", 0)), 4),
            "liquidity_score": round(float(pick.get("流动性评分", 0)), 4),
            "composite_score": round(float(pick.get("综合评分", pick["AI胜率"])), 4),
            "next_date": None,
            "next_open": None,
            "open_premium": None,
            "success": None,
        }
        if next_row is not None and current_close > 0:
            next_open = float(next_row["open"])
            premium = (next_open / current_close - 1) * 100
            item.update(
                {
                    "next_date": str(next_row["date"]),
                    "next_open": round(next_open, 4),
                    "open_premium": round(premium, 4),
                    "success": premium > PROFIT_TARGET_PCT,
                }
            )
        results.append(item)

    evaluated = [row for row in results if row["success"] is not None]
    wins = [row for row in evaluated if row["success"]]
    premiums = [float(row["open_premium"]) for row in evaluated if row["open_premium"] is not None]
    summary = {
        "months": months,
        "start_date": start_date,
        "end_date": latest_date,
        "total_days": len(results),
        "evaluated_days": len(evaluated),
        "pending_days": len(results) - len(evaluated),
        "win_count": len(wins),
        "loss_count": len(evaluated) - len(wins),
        "win_rate": round(len(wins) / len(evaluated) * 100, 4) if evaluated else 0.0,
        "avg_open_premium": round(float(pd.Series(premiums).mean()), 4) if premiums else 0.0,
        "median_open_premium": round(float(pd.Series(premiums).median()), 4) if premiums else 0.0,
        "best_open_premium": round(max(premiums), 4) if premiums else 0.0,
        "worst_open_premium": round(min(premiums), 4) if premiums else 0.0,
        "model_status": model_status,
        "repaired_pre_close_count": repaired_pre_close_count,
        "repaired_volume_ratio_count": repaired_volume_ratio_count,
        "rule": f"生产策略复盘：排除周末、节假日、非完整交易日、创业板、北交所、科创板、ST/退市；大盘风控采用晴天/阴天/雷暴分级，综合评分>={MIN_COMPOSITE_SCORE:.1f}，雷暴或大盘下跌且缩量时空仓；过滤涨幅>=7%、上影>=2%、预期溢价<=0、高位爆量、尾盘诱多、近3日断头铡刀。停盘前最后一个交易日按回归模型预期溢价选第一名，停盘后第一个交易日开盘卖出；扣除滑点费率后的有效成功阈值为开盘溢价>{PROFIT_TARGET_PCT:.2f}%。",
        "trading_day_filter": "weekday<5 且全市场有效样本>=1000 且成交额>0。",
        "rank_rule": f"XGBRegressor 直接预测次日开盘预期溢价；综合评分=预期溢价60%+风险20%+流动性10%+收益信号10%，最终排序优先看预期溢价。",
    }
    return {"created_at": datetime.now().isoformat(timespec="seconds"), "summary": summary, "rows": results[::-1]}


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
            "avg_open_premium": 0.0,
            "median_open_premium": 0.0,
            "best_open_premium": 0.0,
            "worst_open_premium": 0.0,
            "model_status": reason,
            "rule": "无可用数据",
        },
        "rows": [],
    }
