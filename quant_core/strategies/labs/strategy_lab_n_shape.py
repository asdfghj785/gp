from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from quant_core.config import DATA_DIR


SCAN_COLUMNS = ["date", "open", "high", "low", "close", "volume", "pre_close", "change_pct", "pctChg", "code", "symbol", "name"]
EXCLUDED_PREFIXES = ("68", "4", "8", "92")
SINA_INDEX_KLINE_URL = (
    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "CN_MarketData.getKLineData?symbol=sh000001&scale=240&ma=no&datalen=260"
)
BASELINE_REFERENCE = {
    "hit_count": 2993,
    "avg_daily_triggers": 24.33,
    "next_open_win_rate": 35.38,
    "avg_next_open_premium": -0.28,
}


@dataclass
class NShapeHit:
    code: str
    name: str
    date: str
    anchor_date: str
    anchor_open: float
    anchor_body_pct: float
    wash_avg_volume_ratio: float
    wash_min_volume_ratio: float
    market_change_pct: float
    relative_strength_pct: float
    close: float
    prev_high: float
    volume_ratio_vs_prev: float
    next_open: float
    next_open_premium: float


def analyze_n_shape(data_dir: Path = DATA_DIR, months: int = 6) -> dict[str, Any]:
    files = sorted(data_dir.glob("*_daily.parquet"))
    if not files:
        raise RuntimeError(f"没有找到 Parquet 日线文件: {data_dir}")

    latest_date = _latest_valid_date(files)
    if latest_date is None:
        raise RuntimeError("无法从 Parquet 日线库识别有效交易日期")

    start_date = latest_date - pd.DateOffset(months=months)
    load_start_date = start_date - pd.DateOffset(days=45)
    market_changes, market_source = _load_market_changes(files, start_date, latest_date)

    hits: list[NShapeHit] = []
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
        stock_hits = _scan_stock(df, start_date, latest_date, market_changes, default_code=code, default_name=latest_name)
        eligible_stock_days += max(int(len(in_window) - 1), 0)
        hits.extend(stock_hits)

    hit_df = pd.DataFrame([hit.__dict__ for hit in hits])
    trading_days = _trading_day_count(files, start_date, latest_date)
    premiums = pd.to_numeric(hit_df.get("next_open_premium", pd.Series(dtype="float64")), errors="coerce").dropna()

    return {
        "months": months,
        "data_dir": str(data_dir),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": latest_date.strftime("%Y-%m-%d"),
        "scanned_stocks": scanned_stocks,
        "candidate_stock_days": candidate_stock_days,
        "eligible_stock_days": eligible_stock_days,
        "trading_days": trading_days,
        "market_source": market_source,
        "hit_count": int(len(hit_df)),
        "avg_daily_triggers": round(float(len(hit_df) / trading_days), 4) if trading_days else 0.0,
        "next_open_win_rate": round(float((premiums > 0).mean() * 100), 4) if len(premiums) else 0.0,
        "avg_next_open_premium": round(float(premiums.mean()), 4) if len(premiums) else 0.0,
        "median_next_open_premium": round(float(premiums.median()), 4) if len(premiums) else 0.0,
        "best_next_open_premium": round(float(premiums.max()), 4) if len(premiums) else 0.0,
        "worst_next_open_premium": round(float(premiums.min()), 4) if len(premiums) else 0.0,
        "hit_examples": hit_df.sort_values("date", ascending=False).head(10).to_dict(orient="records") if not hit_df.empty else [],
    }


def format_markdown_report(result: dict[str, Any]) -> str:
    lines = [
        "# N字反包（主力蓄水池）3.0 探伤报告",
        "",
        "## 3.0 条件",
        "",
        "- 建仓锚定：只认涨停板级别爆发，实体涨跌幅或总涨跌幅 >= 9.5%。",
        "- 极限洗盘：不破锚定开盘价，且洗盘期至少一天成交量低于锚定量 50%；删除均线贴线硬约束。",
        "- 右侧点火：T 日收盘反包 T-1 最高价，实体涨幅 >= 2%，成交量为 T-1 的 0.8 到 1.4 倍，剔除爆量滞涨。",
        "- 相对强度：T 日个股涨跌幅必须强于大盘涨跌幅 1.5 个点以上。",
        "",
        f"- 扫描区间：{result['start_date']} 至 {result['end_date']}（过去 {result['months']} 个月有效交易日）",
        f"- 大盘基准：{result['market_source']}",
        f"- 扫描股票数：{result['scanned_stocks']}",
        f"- 扫描样本总数：{result['candidate_stock_days']} 个股票日",
        f"- 可评估样本数：{result['eligible_stock_days']} 个股票日（要求存在 T+1 开盘）",
        f"- 符合 N 字反包形态次数：{result['hit_count']}",
        f"- 日均触发频次：{result['avg_daily_triggers']:.2f} 次/交易日",
        f"- T+1 自然胜率：{result['next_open_win_rate']:.2f}%",
        f"- T+1 平均开盘溢价：{result['avg_next_open_premium']:.2f}%",
        f"- T+1 中位开盘溢价：{result['median_next_open_premium']:.2f}%",
        f"- 最好/最差 T+1 开盘溢价：{result['best_next_open_premium']:.2f}% / {result['worst_next_open_premium']:.2f}%",
        "",
        "## 与初始探伤对比",
        "",
        "| 指标 | 初始形态 | 3.0 | 变化 |",
        "|---|---:|---:|---:|",
        f"| 触发次数 | {BASELINE_REFERENCE['hit_count']} | {result['hit_count']} | {result['hit_count'] - BASELINE_REFERENCE['hit_count']} |",
        f"| 日均触发 | {BASELINE_REFERENCE['avg_daily_triggers']:.2f} | {result['avg_daily_triggers']:.2f} | {result['avg_daily_triggers'] - BASELINE_REFERENCE['avg_daily_triggers']:.2f} |",
        f"| T+1胜率 | {BASELINE_REFERENCE['next_open_win_rate']:.2f}% | {result['next_open_win_rate']:.2f}% | {result['next_open_win_rate'] - BASELINE_REFERENCE['next_open_win_rate']:.2f}% |",
        f"| 平均开盘溢价 | {BASELINE_REFERENCE['avg_next_open_premium']:.2f}% | {result['avg_next_open_premium']:.2f}% | {result['avg_next_open_premium'] - BASELINE_REFERENCE['avg_next_open_premium']:.2f}% |",
    ]
    examples = result.get("hit_examples") or []
    if examples:
        lines.extend(
            [
                "",
                "## 最近触发样例",
                "",
                "| 日期 | 代码 | 名称 | 锚定日 | 洗盘地量/锚定量 | 量比T/T-1 | 强于大盘 | T+1开盘溢价 |",
                "|---|---:|---|---|---:|---:|---:|---:|",
            ]
        )
        for item in examples[:10]:
            lines.append(
                f"| {item.get('date')} | {item.get('code')} | {item.get('name')} | "
                f"{item.get('anchor_date')} | {float(item.get('wash_min_volume_ratio') or 0):.2f} | "
                f"{float(item.get('volume_ratio_vs_prev') or 0):.2f} | "
                f"{float(item.get('relative_strength_pct') or 0):.2f}% | {float(item.get('next_open_premium') or 0):.2f}% |"
            )
    return "\n".join(lines)


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


def _scan_stock(
    df: pd.DataFrame,
    start_date: pd.Timestamp,
    latest_date: pd.Timestamp,
    market_changes: dict[str, float],
    default_code: str,
    default_name: str,
) -> list[NShapeHit]:
    hits: list[NShapeHit] = []
    if len(df) < 10:
        return hits
    body_pct = (df["close"] / df["open"] - 1) * 100
    change_pct = df["change_pct"].fillna(df["pctChg"]).fillna((df["close"] / df["pre_close"] - 1) * 100)
    big_bull = (body_pct >= 9.5) | (change_pct >= 9.5)

    for idx in range(8, len(df) - 1):
        current_date = df.at[idx, "date"]
        if current_date < start_date or current_date > latest_date:
            continue
        anchor_candidates = [anchor_idx for anchor_idx in range(idx - 4, idx - 9, -1) if anchor_idx >= 0 and bool(big_bull.iloc[anchor_idx])]
        if not anchor_candidates:
            continue
        anchor_idx = anchor_candidates[0]
        wash = df.iloc[anchor_idx + 1:idx]
        if wash.empty:
            continue
        anchor_open = float(df.at[anchor_idx, "open"])
        anchor_volume = float(df.at[anchor_idx, "volume"])
        if float(wash["low"].min()) < anchor_open:
            continue
        wash_min_volume_ratio = float(wash["volume"].min() / anchor_volume) if anchor_volume else 0.0
        if wash_min_volume_ratio >= 0.5:
            continue
        t_body_pct = float(body_pct.iloc[idx])
        if t_body_pct < 2.0:
            continue
        current_change = float(change_pct.iloc[idx]) if pd.notna(change_pct.iloc[idx]) else float((df.at[idx, "close"] / df.at[idx, "pre_close"] - 1) * 100)
        market_change = float(market_changes.get(current_date.strftime("%Y-%m-%d"), 0.0))
        relative_strength = current_change - market_change
        if relative_strength <= 1.5:
            continue
        if float(df.at[idx, "close"]) <= float(df.at[idx - 1, "high"]):
            continue
        volume_ratio = float(df.at[idx, "volume"] / df.at[idx - 1, "volume"]) if float(df.at[idx - 1, "volume"]) else 0.0
        if not (0.8 <= volume_ratio <= 1.4):
            continue
        next_open = float(df.at[idx + 1, "open"])
        close = float(df.at[idx, "close"])
        if next_open <= 0 or close <= 0:
            continue
        hits.append(
            NShapeHit(
                code=str(df.at[idx, "code"] or default_code).zfill(6),
                name=str(df.at[idx, "name"] or default_name),
                date=current_date.strftime("%Y-%m-%d"),
                anchor_date=df.at[anchor_idx, "date"].strftime("%Y-%m-%d"),
                anchor_open=round(anchor_open, 4),
                anchor_body_pct=round(float(body_pct.iloc[anchor_idx]), 4),
                wash_avg_volume_ratio=round(float(wash["volume"].mean() / anchor_volume), 4) if anchor_volume else 0.0,
                wash_min_volume_ratio=round(wash_min_volume_ratio, 4),
                market_change_pct=round(market_change, 4),
                relative_strength_pct=round(relative_strength, 4),
                close=round(close, 4),
                prev_high=round(float(df.at[idx - 1, "high"]), 4),
                volume_ratio_vs_prev=round(volume_ratio, 4),
                next_open=round(next_open, 4),
                next_open_premium=round((next_open / close - 1) * 100, 4),
            )
        )
    return hits


def _load_market_changes(files: list[Path], start_date: pd.Timestamp, latest_date: pd.Timestamp) -> tuple[dict[str, float], str]:
    sina_changes = _fetch_sina_index_changes(start_date, latest_date)
    if len(sina_changes) >= 40:
        return sina_changes, "上证指数 sh000001（日线，新浪历史K线）"
    proxy = _build_local_market_proxy(files, start_date, latest_date)
    return proxy, "本地全市场等权涨跌代理（上证指数接口不可用时回退）"


def _fetch_sina_index_changes(start_date: pd.Timestamp, latest_date: pd.Timestamp) -> dict[str, float]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "https://finance.sina.com.cn/",
    }
    try:
        response = requests.get(SINA_INDEX_KLINE_URL, headers=headers, timeout=8)
        response.raise_for_status()
        rows = _loads_sina_js(response.text)
    except Exception:
        return {}
    changes: dict[str, float] = {}
    prev_close: float | None = None
    for row in sorted(rows, key=lambda item: str(item.get("day", ""))):
        day = pd.to_datetime(row.get("day"), errors="coerce")
        close = _safe_float(row.get("close"))
        if pd.isna(day) or close <= 0:
            continue
        if prev_close and start_date <= day <= latest_date:
            changes[day.strftime("%Y-%m-%d")] = (close / prev_close - 1) * 100
        prev_close = close
    return changes


def _loads_sina_js(text: str) -> list[dict[str, Any]]:
    if not text or text in {"null", "[]"}:
        return []
    valid_json_text = re.sub(r"([{,])([a-zA-Z0-9_]+):", r'\1"\2":', text)
    return json.loads(valid_json_text)


def _build_local_market_proxy(files: list[Path], start_date: pd.Timestamp, latest_date: pd.Timestamp) -> dict[str, float]:
    daily_changes: dict[str, list[float]] = {}
    load_start_date = start_date - pd.DateOffset(days=5)
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
        change_pct = df["change_pct"].fillna(df["pctChg"]).fillna((df["close"] / df["pre_close"] - 1) * 100)
        for day, change in zip(df["date"], change_pct):
            if pd.isna(day) or day < start_date or day > latest_date or pd.isna(change):
                continue
            daily_changes.setdefault(day.strftime("%Y-%m-%d"), []).append(float(change))
    return {day: float(np.mean(values)) for day, values in daily_changes.items() if values}


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


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
    parser = argparse.ArgumentParser(description="N字反包历史探伤 EDA")
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    result = analyze_n_shape(data_dir=args.data_dir, months=args.months)
    print(format_markdown_report(result))


if __name__ == "__main__":
    main()
