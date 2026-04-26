from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_core.config import DATA_DIR


SCAN_COLUMNS = ["date", "open", "high", "low", "close", "volume", "pre_close", "change_pct", "pctChg", "code", "symbol", "name"]
EXCLUDED_PREFIXES = ("68", "4", "8", "92")


@dataclass
class BottomReversalHit:
    code: str
    name: str
    date: str
    close: float
    body_pct: float
    drawdown_60d: float
    close_ma60_bias: float
    min_volume_5d_ratio_to_60d: float
    volume_ratio_to_10d: float
    next_open: float
    next_open_premium: float
    max_gain_t3: float


def analyze_bottom_reversal(data_dir: Path = DATA_DIR, months: int = 6) -> dict[str, Any]:
    files = sorted(data_dir.glob("*_daily.parquet"))
    if not files:
        raise RuntimeError(f"没有找到 Parquet 日线文件: {data_dir}")

    latest_date = _latest_valid_date(files)
    if latest_date is None:
        raise RuntimeError("无法从 Parquet 日线库识别有效交易日期")

    start_date = latest_date - pd.DateOffset(months=months)
    load_start_date = start_date - pd.DateOffset(days=120)

    hits: list[BottomReversalHit] = []
    scanned_stocks = 0
    candidate_stock_days = 0
    eligible_stock_days = 0

    for path in files:
        code = path.name.split("_", 1)[0]
        if code.startswith(EXCLUDED_PREFIXES):
            continue
        df = _load_one_stock(path, load_start_date)
        if df.empty:
            continue
        latest_name = _latest_name(df)
        if _is_excluded_name(latest_name):
            continue
        scanned_stocks += 1
        in_window = df[(df["date"] >= start_date) & (df["date"] <= latest_date)]
        candidate_stock_days += int(len(in_window))
        eligible_stock_days += max(int(len(in_window) - 3), 0)
        hits.extend(_scan_stock(df, start_date, latest_date, default_code=code, default_name=latest_name))

    hit_df = pd.DataFrame([hit.__dict__ for hit in hits])
    trading_days = _trading_day_count(files, start_date, latest_date)
    next_open_premiums = pd.to_numeric(hit_df.get("next_open_premium", pd.Series(dtype="float64")), errors="coerce").dropna()
    max_gain_t3 = pd.to_numeric(hit_df.get("max_gain_t3", pd.Series(dtype="float64")), errors="coerce").dropna()

    return {
        "months": months,
        "data_dir": str(data_dir),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": latest_date.strftime("%Y-%m-%d"),
        "scanned_stocks": scanned_stocks,
        "candidate_stock_days": candidate_stock_days,
        "eligible_stock_days": eligible_stock_days,
        "trading_days": trading_days,
        "hit_count": int(len(hit_df)),
        "avg_daily_triggers": round(float(len(hit_df) / trading_days), 4) if trading_days else 0.0,
        "next_open_win_rate": round(float((next_open_premiums > 0).mean() * 100), 4) if len(next_open_premiums) else 0.0,
        "avg_next_open_premium": round(float(next_open_premiums.mean()), 4) if len(next_open_premiums) else 0.0,
        "median_next_open_premium": round(float(next_open_premiums.median()), 4) if len(next_open_premiums) else 0.0,
        "avg_max_gain_t3": round(float(max_gain_t3.mean()), 4) if len(max_gain_t3) else 0.0,
        "median_max_gain_t3": round(float(max_gain_t3.median()), 4) if len(max_gain_t3) else 0.0,
        "t3_positive_rate": round(float((max_gain_t3 > 0).mean() * 100), 4) if len(max_gain_t3) else 0.0,
        "best_max_gain_t3": round(float(max_gain_t3.max()), 4) if len(max_gain_t3) else 0.0,
        "worst_next_open_premium": round(float(next_open_premiums.min()), 4) if len(next_open_premiums) else 0.0,
        "hit_examples": hit_df.sort_values("date", ascending=False).head(10).to_dict(orient="records") if not hit_df.empty else [],
    }


def format_markdown_report(result: dict[str, Any]) -> str:
    lines = [
        "# 中线超跌反转战法历史探伤报告",
        "",
        "## 形态条件",
        "",
        "- 中线超跌：T-1 收盘价低于 60 日均线，且过去 60 个交易日区间最大跌幅 >= 20%。",
        "- 地量吸筹：T-5 到 T-1 至少一天成交量低于过去 60 日平均成交量的 40%。",
        "- 右侧反转：T 日实体涨幅 >= 5%，收盘价同时站上 5 日和 10 日均线，成交量 >= 过去 10 日均量的 2 倍。",
        "",
        f"- 扫描区间：{result['start_date']} 至 {result['end_date']}（过去 {result['months']} 个月有效交易日）",
        f"- 扫描股票数：{result['scanned_stocks']}",
        f"- 扫描样本总数：{result['candidate_stock_days']} 个股票日",
        f"- 可评估样本数：{result['eligible_stock_days']} 个股票日（要求存在 T+3 数据）",
        f"- 符合中线超跌反转次数：{result['hit_count']}",
        f"- 日均触发频次：{result['avg_daily_triggers']:.2f} 次/交易日",
        f"- T+1 自然胜率：{result['next_open_win_rate']:.2f}%",
        f"- T+1 平均开盘溢价：{result['avg_next_open_premium']:.2f}%",
        f"- T+1 中位开盘溢价：{result['median_next_open_premium']:.2f}%",
        f"- T+3 平均区间最大涨幅：{result['avg_max_gain_t3']:.2f}%",
        f"- T+3 中位区间最大涨幅：{result['median_max_gain_t3']:.2f}%",
        f"- T+3 区间最大涨幅为正比例：{result['t3_positive_rate']:.2f}%",
        f"- T+3 最强最大涨幅：{result['best_max_gain_t3']:.2f}%",
        f"- T+1 最差开盘溢价：{result['worst_next_open_premium']:.2f}%",
    ]
    examples = result.get("hit_examples") or []
    if examples:
        lines.extend(
            [
                "",
                "## 最近触发样例",
                "",
                "| 日期 | 代码 | 名称 | 60日跌幅 | 地量/60日均量 | T日量/10日均量 | T+1开盘溢价 | T+3最大涨幅 |",
                "|---|---:|---|---:|---:|---:|---:|---:|",
            ]
        )
        for item in examples[:10]:
            lines.append(
                f"| {item.get('date')} | {item.get('code')} | {item.get('name')} | "
                f"{float(item.get('drawdown_60d') or 0):.2f}% | "
                f"{float(item.get('min_volume_5d_ratio_to_60d') or 0):.2f} | "
                f"{float(item.get('volume_ratio_to_10d') or 0):.2f} | "
                f"{float(item.get('next_open_premium') or 0):.2f}% | "
                f"{float(item.get('max_gain_t3') or 0):.2f}% |"
            )
    return "\n".join(lines)


def _scan_stock(df: pd.DataFrame, start_date: pd.Timestamp, latest_date: pd.Timestamp, default_code: str, default_name: str) -> list[BottomReversalHit]:
    hits: list[BottomReversalHit] = []
    if len(df) < 65:
        return hits

    close = df["close"]
    volume = df["volume"]
    ma5 = close.rolling(5, min_periods=5).mean()
    ma10 = close.rolling(10, min_periods=10).mean()
    ma60 = close.rolling(60, min_periods=60).mean()
    vol_ma10 = volume.rolling(10, min_periods=10).mean()
    vol_ma60 = volume.rolling(60, min_periods=60).mean()
    high_60 = close.rolling(60, min_periods=60).max()
    low_60 = close.rolling(60, min_periods=60).min()
    drawdown_60d = (low_60 / high_60 - 1) * 100
    body_pct = (df["close"] / df["open"] - 1) * 100

    for idx in range(60, len(df) - 3):
        current_date = df.at[idx, "date"]
        if current_date < start_date or current_date > latest_date:
            continue
        prev_idx = idx - 1
        if float(df.at[prev_idx, "close"]) >= float(ma60.iloc[prev_idx]):
            continue
        if float(drawdown_60d.iloc[prev_idx]) > -20.0:
            continue
        recent_5 = df.iloc[idx - 5:idx]
        base_vol_60 = float(vol_ma60.iloc[prev_idx])
        if base_vol_60 <= 0:
            continue
        min_volume_ratio = float(recent_5["volume"].min() / base_vol_60)
        if min_volume_ratio >= 0.4:
            continue
        if float(body_pct.iloc[idx]) < 5.0:
            continue
        if float(df.at[idx, "close"]) <= float(ma5.iloc[idx]) or float(df.at[idx, "close"]) <= float(ma10.iloc[idx]):
            continue
        vol_base_10 = float(vol_ma10.iloc[prev_idx])
        if vol_base_10 <= 0:
            continue
        volume_ratio = float(df.at[idx, "volume"] / vol_base_10)
        if volume_ratio < 2.0:
            continue
        next_open = float(df.at[idx + 1, "open"])
        current_close = float(df.at[idx, "close"])
        future_high = float(df.iloc[idx + 1:idx + 4]["high"].max())
        if next_open <= 0 or current_close <= 0 or future_high <= 0:
            continue
        hits.append(
            BottomReversalHit(
                code=str(df.at[idx, "code"] or default_code).zfill(6),
                name=str(df.at[idx, "name"] or default_name),
                date=current_date.strftime("%Y-%m-%d"),
                close=round(current_close, 4),
                body_pct=round(float(body_pct.iloc[idx]), 4),
                drawdown_60d=round(float(drawdown_60d.iloc[prev_idx]), 4),
                close_ma60_bias=round((float(df.at[prev_idx, "close"]) / float(ma60.iloc[prev_idx]) - 1) * 100, 4),
                min_volume_5d_ratio_to_60d=round(min_volume_ratio, 4),
                volume_ratio_to_10d=round(volume_ratio, 4),
                next_open=round(next_open, 4),
                next_open_premium=round((next_open / current_close - 1) * 100, 4),
                max_gain_t3=round((future_high / current_close - 1) * 100, 4),
            )
        )
    return hits


def _latest_valid_date(files: list[Path]) -> pd.Timestamp | None:
    latest: pd.Timestamp | None = None
    for path in files:
        try:
            dates = pd.read_parquet(path, columns=["date"])["date"]
        except Exception:
            continue
        parsed = _parse_dates(dates)
        parsed = parsed[parsed.dt.weekday < 5].dropna()
        if parsed.empty:
            continue
        current = parsed.max()
        if latest is None or current > latest:
            latest = current
    return latest


def _trading_day_count(files: list[Path], start_date: pd.Timestamp, latest_date: pd.Timestamp) -> int:
    seen: set[str] = set()
    for path in files[: min(len(files), 800)]:
        try:
            dates = pd.read_parquet(path, columns=["date"])["date"]
        except Exception:
            continue
        parsed = _parse_dates(dates)
        parsed = parsed[(parsed.dt.weekday < 5) & (parsed >= start_date) & (parsed <= latest_date)].dropna()
        seen.update(parsed.dt.strftime("%Y-%m-%d").tolist())
    return len(seen)


def _load_one_stock(path: Path, load_start_date: pd.Timestamp) -> pd.DataFrame:
    try:
        columns = pd.read_parquet(path).columns
        selected = [col for col in SCAN_COLUMNS if col in columns]
        df = pd.read_parquet(path, columns=selected)
    except Exception:
        return pd.DataFrame()
    if df.empty or not {"date", "open", "high", "low", "close", "volume"}.issubset(df.columns):
        return pd.DataFrame()

    out = df.copy()
    out["date"] = _parse_dates(out["date"])
    out = out[out["date"].notna() & (out["date"].dt.weekday < 5)].copy()
    out = out[out["date"] >= load_start_date].copy()
    if out.empty:
        return out
    for col in ["open", "high", "low", "close", "volume", "pre_close", "change_pct", "pctChg"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "code" not in out.columns:
        out["code"] = path.name.split("_", 1)[0]
    out["code"] = out["code"].fillna(path.name.split("_", 1)[0]).astype(str).str.extract(r"(\d{6})")[0].fillna(path.name.split("_", 1)[0])
    if "name" not in out.columns:
        out["name"] = ""
    out["name"] = out["name"].fillna("")
    out = out.dropna(subset=["open", "high", "low", "close", "volume"]).copy()
    out = out[(out["open"] > 0) & (out["high"] > 0) & (out["low"] > 0) & (out["close"] > 0) & (out["volume"] > 0)].copy()
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return out


def _parse_dates(values: pd.Series) -> pd.Series:
    text = values.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    yyyymmdd = text.str.fullmatch(r"\d{8}", na=False)
    parsed.loc[yyyymmdd] = pd.to_datetime(text.loc[yyyymmdd], format="%Y%m%d", errors="coerce")
    parsed.loc[~yyyymmdd] = pd.to_datetime(text.loc[~yyyymmdd], errors="coerce")
    return parsed


def _latest_name(df: pd.DataFrame) -> str:
    names = df["name"].astype(str).str.strip()
    names = names[names != ""]
    return str(names.iloc[-1]) if not names.empty else ""


def _is_excluded_name(name: str) -> bool:
    upper = str(name).upper()
    return "ST" in upper or "退" in str(name)


def main() -> None:
    parser = argparse.ArgumentParser(description="中线超跌反转历史探伤 EDA")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    result = analyze_bottom_reversal(data_dir=args.data_dir, months=args.months)
    print(format_markdown_report(result))


if __name__ == "__main__":
    main()
