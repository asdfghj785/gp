from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_core.config import DATA_DIR


SCAN_COLUMNS = ["date", "open", "high", "low", "close", "volume", "pre_close", "change_pct", "pctChg", "code", "symbol", "name"]
EXCLUDED_PREFIXES = ("30", "68", "4", "8", "92")


@dataclass
class MainWaveHit:
    code: str
    name: str
    date: str
    close: float
    ma20: float
    ma60: float
    pullback_from_60d_high: float
    contraction_amplitude_5d: float
    prev_volume_ratio_to_5d: float
    breakout_close: float
    body_pct: float
    volume_ratio_to_5d: float
    next_open: float
    next_open_premium: float
    max_gain_t3: float


def analyze_main_wave(data_dir: Path = DATA_DIR, months: int = 6) -> dict[str, Any]:
    files = sorted(data_dir.glob("*_daily.parquet"))
    if not files:
        raise RuntimeError(f"没有找到 Parquet 日线文件: {data_dir}")

    latest_date = _latest_valid_date(files)
    if latest_date is None:
        raise RuntimeError("无法从 Parquet 日线库识别有效交易日期")

    start_date = latest_date - pd.DateOffset(months=months)
    load_start_date = start_date - pd.DateOffset(days=140)

    hits: list[MainWaveHit] = []
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
    avg_next = float(next_open_premiums.mean()) if len(next_open_premiums) else 0.0
    avg_t3 = float(max_gain_t3.mean()) if len(max_gain_t3) else 0.0
    profit_focus = "T+3 波段发酵" if avg_t3 > avg_next + 0.5 else "T+1 极速隔夜" if avg_next > avg_t3 + 0.5 else "T+1 与 T+3 接近"

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
        "avg_next_open_premium": round(avg_next, 4),
        "median_next_open_premium": round(float(next_open_premiums.median()), 4) if len(next_open_premiums) else 0.0,
        "best_next_open_premium": round(float(next_open_premiums.max()), 4) if len(next_open_premiums) else 0.0,
        "worst_next_open_premium": round(float(next_open_premiums.min()), 4) if len(next_open_premiums) else 0.0,
        "t3_positive_rate": round(float((max_gain_t3 > 0).mean() * 100), 4) if len(max_gain_t3) else 0.0,
        "avg_max_gain_t3": round(avg_t3, 4),
        "median_max_gain_t3": round(float(max_gain_t3.median()), 4) if len(max_gain_t3) else 0.0,
        "best_max_gain_t3": round(float(max_gain_t3.max()), 4) if len(max_gain_t3) else 0.0,
        "worst_max_gain_t3": round(float(max_gain_t3.min()), 4) if len(max_gain_t3) else 0.0,
        "profit_focus": profit_focus,
        "trigger_health": "健康" if 10 <= (len(hit_df) / trading_days if trading_days else 0) <= 30 else "偏少" if (len(hit_df) / trading_days if trading_days else 0) < 10 else "偏多",
        "hit_examples": hit_df.sort_values("date", ascending=False).head(10).to_dict(orient="records") if not hit_df.empty else [],
    }


def format_markdown_report(result: dict[str, Any]) -> str:
    lines = [
        "# 右侧主升浪（顺势接力）历史探伤报告",
        "",
        "## 形态条件",
        "",
        "- 大级别趋势：T-1 的 20 日均线 > 60 日均线，且 T-1 收盘价距离 60 日高点回撤不超过 -15%。",
        "- 缩量蓄势：T-5 到 T-1 区间最大振幅 <= 12%，且 T-1 成交量低于过去 5 日平均成交量。",
        "- 右侧起爆：T 日收盘价突破 T-5 到 T-1 最高收盘价，实体涨幅 >= 3.5%，成交量 >= 过去 5 日均量的 1.15 倍。",
        "",
        f"- 扫描区间：{result['start_date']} 至 {result['end_date']}（过去 {result['months']} 个月有效交易日）",
        f"- 扫描股票数：{result['scanned_stocks']}",
        f"- 扫描样本总数：{result['candidate_stock_days']} 个股票日",
        f"- 可评估样本数：{result['eligible_stock_days']} 个股票日（要求存在 T+3 数据）",
        f"- 符合右侧主升浪次数：{result['hit_count']}",
        f"- 日均触发频次：{result['avg_daily_triggers']:.2f} 次/交易日（{result['trigger_health']}，目标 10-30 次）",
        "",
        "## 收益演算",
        "",
        f"- T+1 自然胜率：{result['next_open_win_rate']:.2f}%",
        f"- T+1 平均开盘溢价：{result['avg_next_open_premium']:.2f}%",
        f"- T+1 中位开盘溢价：{result['median_next_open_premium']:.2f}%",
        f"- T+1 最好/最差开盘溢价：{result['best_next_open_premium']:.2f}% / {result['worst_next_open_premium']:.2f}%",
        f"- T+3 最大涨幅为正比例：{result['t3_positive_rate']:.2f}%",
        f"- T+3 平均区间最大涨幅：{result['avg_max_gain_t3']:.2f}%",
        f"- T+3 中位区间最大涨幅：{result['median_max_gain_t3']:.2f}%",
        f"- T+3 最好/最差区间最大涨幅：{result['best_max_gain_t3']:.2f}% / {result['worst_max_gain_t3']:.2f}%",
        f"- 初步利润重心判断：{result['profit_focus']}",
    ]
    examples = result.get("hit_examples") or []
    if examples:
        lines.extend(
            [
                "",
                "## 最近触发样例",
                "",
                "| 日期 | 代码 | 名称 | 60日高点回撤 | 5日振幅 | T日实体 | T日量/5日均量 | T+1开盘溢价 | T+3最大涨幅 |",
                "|---|---:|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for item in examples[:10]:
            lines.append(
                f"| {item.get('date')} | {item.get('code')} | {item.get('name')} | "
                f"{float(item.get('pullback_from_60d_high') or 0):.2f}% | "
                f"{float(item.get('contraction_amplitude_5d') or 0):.2f}% | "
                f"{float(item.get('body_pct') or 0):.2f}% | "
                f"{float(item.get('volume_ratio_to_5d') or 0):.2f} | "
                f"{float(item.get('next_open_premium') or 0):.2f}% | "
                f"{float(item.get('max_gain_t3') or 0):.2f}% |"
            )
    return "\n".join(lines)


def _scan_stock(df: pd.DataFrame, start_date: pd.Timestamp, latest_date: pd.Timestamp, default_code: str, default_name: str) -> list[MainWaveHit]:
    hits: list[MainWaveHit] = []
    if len(df) < 65:
        return hits

    close = df["close"]
    volume = df["volume"]
    ma20 = close.rolling(20, min_periods=20).mean()
    ma60 = close.rolling(60, min_periods=60).mean()
    high_60 = close.rolling(60, min_periods=60).max()
    body_pct = (df["close"] / df["open"] - 1) * 100
    volume_ma5 = volume.rolling(5, min_periods=5).mean()

    for idx in range(60, len(df) - 3):
        current_date = df.at[idx, "date"]
        if current_date < start_date or current_date > latest_date:
            continue

        prev_idx = idx - 1
        prev_ma20 = float(ma20.iloc[prev_idx])
        prev_ma60 = float(ma60.iloc[prev_idx])
        if not np.isfinite(prev_ma20) or not np.isfinite(prev_ma60) or prev_ma20 <= prev_ma60:
            continue

        prev_close = float(df.at[prev_idx, "close"])
        prev_high_60 = float(high_60.iloc[prev_idx])
        if prev_high_60 <= 0:
            continue
        pullback = (prev_close / prev_high_60 - 1) * 100
        if pullback < -15.0:
            continue

        platform = df.iloc[idx - 5:idx]
        if len(platform) < 5:
            continue
        platform_high = float(platform["high"].max())
        platform_low = float(platform["low"].min())
        if platform_low <= 0:
            continue
        contraction_amplitude = (platform_high / platform_low - 1) * 100
        if contraction_amplitude > 15.0:
            continue

        prev_volume_base = float(volume_ma5.iloc[prev_idx])
        if prev_volume_base <= 0:
            continue
        prev_volume_ratio = float(df.at[prev_idx, "volume"] / prev_volume_base)
        if prev_volume_ratio >= 1.0:
            continue

        current_close = float(df.at[idx, "close"])
        platform_max_close = float(platform["close"].max())
        if current_close <= platform_max_close:
            continue

        current_body_pct = float(body_pct.iloc[idx])
        if current_body_pct < 3.5:
            continue

        volume_base = float(volume_ma5.iloc[prev_idx])
        current_volume_ratio = float(df.at[idx, "volume"] / volume_base) if volume_base > 0 else 0.0
        if current_volume_ratio < 1.15:
            continue

        next_open = float(df.at[idx + 1, "open"])
        future_high = float(df.iloc[idx + 1:idx + 4]["high"].max())
        if next_open <= 0 or current_close <= 0 or future_high <= 0:
            continue

        hits.append(
            MainWaveHit(
                code=str(df.at[idx, "code"] or default_code).zfill(6),
                name=str(df.at[idx, "name"] or default_name),
                date=current_date.strftime("%Y-%m-%d"),
                close=round(current_close, 4),
                ma20=round(prev_ma20, 4),
                ma60=round(prev_ma60, 4),
                pullback_from_60d_high=round(pullback, 4),
                contraction_amplitude_5d=round(contraction_amplitude, 4),
                prev_volume_ratio_to_5d=round(prev_volume_ratio, 4),
                breakout_close=round(platform_max_close, 4),
                body_pct=round(current_body_pct, 4),
                volume_ratio_to_5d=round(current_volume_ratio, 4),
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
    parser = argparse.ArgumentParser(description="右侧主升浪（顺势接力）历史探伤 EDA")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    result = analyze_main_wave(data_dir=args.data_dir, months=args.months)
    print(format_markdown_report(result))


if __name__ == "__main__":
    main()
