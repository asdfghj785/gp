from __future__ import annotations

import argparse
import os
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import requests

from quant_core.config import DATA_DIR


warnings.filterwarnings(
    "ignore",
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated.*",
    category=FutureWarning,
)
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""
os.environ["all_proxy"] = ""
os.environ["ALL_PROXY"] = ""

_REQUEST_PATCHED = False
TENCENT_DAY_KLINE_URL = "http://ifzq.gtimg.cn/appstock/app/kline/kline"


def _disable_requests_system_proxy() -> None:
    """Force third-party data clients to bypass broken local system proxies."""
    global _REQUEST_PATCHED
    if _REQUEST_PATCHED:
        return
    original_request = requests.sessions.Session.request

    def request_without_proxy(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
        self.trust_env = False
        kwargs.setdefault("proxies", {"http": None, "https": None})
        return original_request(self, method, url, **kwargs)

    request_without_proxy._quant_no_proxy = True  # type: ignore[attr-defined]
    requests.sessions.Session.request = request_without_proxy
    _REQUEST_PATCHED = True


@dataclass(frozen=True)
class HealResult:
    path: str
    code: str
    before_max_date: str | None
    after_max_date: str | None
    appended_rows: int
    status: str
    error: str = ""


def heal_all(
    data_dir: str | Path = DATA_DIR,
    *,
    target_date: date | None = None,
    max_workers: int = 12,
    limit: int = 0,
) -> dict[str, Any]:
    """Heal stale daily parquet files up to the previous A-share trading day."""
    directory = Path(data_dir)
    files = sorted(directory.glob("*_daily.parquet"))
    if limit > 0:
        files = files[:limit]
    if not files:
        raise FileNotFoundError(f"未找到日线 parquet 文件：{directory}")

    target = target_date or previous_trading_day()
    results: list[HealResult] = []
    workers = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(heal_one_file, path, target): path
            for path in files
        }
        for future in as_completed(futures):
            path = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    HealResult(
                        path=str(path),
                        code=_code_from_path(path),
                        before_max_date=None,
                        after_max_date=None,
                        appended_rows=0,
                        status="error",
                        error=str(exc),
                    )
                )

    healed = [item for item in results if item.status == "healed"]
    stale = [item for item in results if item.status == "stale_no_data"]
    errors = [item for item in results if item.status == "error"]
    skipped = [item for item in results if item.status == "fresh"]
    return {
        "data_dir": str(directory),
        "target_date": target.isoformat(),
        "file_count": len(files),
        "healed_count": len(healed),
        "fresh_count": len(skipped),
        "stale_no_data_count": len(stale),
        "error_count": len(errors),
        "appended_rows": int(sum(item.appended_rows for item in healed)),
        "results": results,
        "errors": errors[:50],
        "stale_no_data": stale[:50],
    }


def heal_one_file(path: str | Path, target_date: date) -> HealResult:
    parquet_path = Path(path)
    code = _code_from_path(parquet_path)

    try:
        history = pd.read_parquet(parquet_path)
    except Exception as exc:
        rebuilt = rebuild_file_from_ashare(code, target_date)
        if rebuilt.empty:
            return HealResult(str(parquet_path), code, None, None, 0, "error", f"坏文件且 Ashare 重建为空: {exc}")
        rebuilt = _normalize_dates(rebuilt)
        rebuilt = _recalculate_indicators(rebuilt, code, _latest_name(rebuilt, code))
        after_date = pd.Timestamp(rebuilt["_date"].max()).date()
        _write_parquet_atomic(rebuilt.drop(columns=["_date"], errors="ignore"), parquet_path)
        return HealResult(str(parquet_path), code, None, after_date.isoformat(), len(rebuilt), "healed")

    if history.empty:
        return HealResult(str(parquet_path), code, None, None, 0, "error", "空 parquet")

    history = _normalize_dates(history)
    name = _latest_name(history, code)
    before_max = history["_date"].max()
    if pd.isna(before_max):
        return HealResult(str(parquet_path), code, None, None, 0, "error", "date 列无有效日期")

    before_date = pd.Timestamp(before_max).date()
    if before_date >= target_date:
        return HealResult(str(parquet_path), code, before_date.isoformat(), before_date.isoformat(), 0, "fresh")

    start_date = before_date + timedelta(days=1)
    missing = fetch_missing_daily(code, start_date, target_date, name)
    if missing.empty:
        missing = _build_flat_suspend_rows(history, code, name, start_date, target_date)
    if missing.empty:
        return HealResult(str(parquet_path), code, before_date.isoformat(), before_date.isoformat(), 0, "stale_no_data")

    before_len = len(history)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        merged = pd.concat([history.drop(columns=["_date"], errors="ignore"), missing], ignore_index=True, sort=False)
    merged = _normalize_dates(merged)
    merged = (
        merged.dropna(subset=["_date"])
        .sort_values("_date")
        .drop_duplicates(subset=["_date"], keep="last")
        .reset_index(drop=True)
    )
    merged = _recalculate_indicators(merged, code, name)
    after_date = pd.Timestamp(merged["_date"].max()).date()
    merged = merged.drop(columns=["_date"], errors="ignore")
    _write_parquet_atomic(merged, parquet_path)
    return HealResult(
        path=str(parquet_path),
        code=code,
        before_max_date=before_date.isoformat(),
        after_max_date=after_date.isoformat(),
        appended_rows=max(0, len(merged) - before_len),
        status="healed",
    )


def rebuild_file_from_ashare(code: str, target_date: date, lookback_days: int = 760) -> pd.DataFrame:
    start_date = target_date - timedelta(days=lookback_days)
    return fetch_missing_daily(code, start_date, target_date, None)


def fetch_missing_daily(code: str, start_date: date, end_date: date, name: str | None = None) -> pd.DataFrame:
    """Fetch missing daily rows through the Ashare/Tencent day K-line endpoint."""
    _disable_requests_system_proxy()
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            return _fetch_ashare_daily(code, start_date, end_date, name)
        except Exception as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"Ashare 拉取失败 {code} {start_date:%Y%m%d}-{end_date:%Y%m%d}: {last_error}")


def previous_trading_day(today: date | None = None) -> date:
    """Return the previous A-share trading day from the Ashare/Tencent index daily K line."""
    current = today or datetime.now().date()
    try:
        index_rows = _fetch_ashare_daily("000001", current - timedelta(days=45), current, "上证指数", market_prefix="sh")
        trade_dates = pd.to_datetime(index_rows["date"], errors="coerce").dt.date.dropna()
        candidates = [item for item in trade_dates if item < current]
        if candidates:
            return max(candidates)
    except Exception:
        pass

    candidate = current - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


@lru_cache(maxsize=16)
def _trading_days_between(start_text: str, end_text: str) -> tuple[date, ...]:
    start_date = date.fromisoformat(start_text)
    end_date = date.fromisoformat(end_text)
    index_rows = _fetch_ashare_daily("000001", start_date - timedelta(days=10), end_date, "上证指数", market_prefix="sh")
    if index_rows.empty:
        return tuple()
    days = pd.to_datetime(index_rows["date"].astype(str), format="%Y%m%d", errors="coerce").dt.date.dropna()
    return tuple(day for day in days if start_date <= day <= end_date)


def _build_flat_suspend_rows(
    history: pd.DataFrame,
    code: str,
    name: str,
    start_date: date,
    target_date: date,
) -> pd.DataFrame:
    days = _trading_days_between(start_date.isoformat(), target_date.isoformat())
    if not days:
        return pd.DataFrame()
    last = history.sort_values("_date").iloc[-1].copy()
    close = _safe_float(last.get("close"))
    if close <= 0:
        return pd.DataFrame()
    rows: list[pd.Series] = []
    for day in days:
        row = last.copy()
        row["date"] = day.strftime("%Y%m%d")
        row["symbol"] = code
        row["code"] = code
        row["name"] = name or code
        row["open"] = close
        row["high"] = close
        row["low"] = close
        row["close"] = close
        row["volume"] = 0.0
        row["amount"] = 0.0
        row["turn"] = 0.0
        row["turnover"] = 0.0
        row["volume_ratio"] = 0.0
        rows.append(row.drop(labels=["_date"], errors="ignore"))
    return pd.DataFrame(rows)


def _fetch_ashare_daily(
    code: str,
    start_date: date,
    end_date: date,
    name: str | None = None,
    *,
    market_prefix: str | None = None,
) -> pd.DataFrame:
    symbol = _ashare_symbol(code, market_prefix=market_prefix)
    count = max(60, (end_date - start_date).days + 15)
    end_text = end_date.isoformat()
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        TENCENT_DAY_KLINE_URL,
        params={"param": f"{symbol},day,,{end_text},{count}"},
        timeout=10,
        proxies={},
    )
    response.raise_for_status()
    payload = response.json()
    data = (payload.get("data") or {}).get(symbol) or {}
    rows = data.get("day") or []
    qt = data.get("qt") or {}
    quote_name = ""
    if isinstance(qt, dict) and isinstance(qt.get(symbol), list) and len(qt[symbol]) > 1:
        quote_name = str(qt[symbol][1] or "")
    parsed: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < 6:
            continue
        row_date = pd.to_datetime(row[0], errors="coerce")
        if pd.isna(row_date):
            continue
        row_day = row_date.date()
        if row_day < start_date or row_day > end_date:
            continue
        open_price = _safe_float(row[1])
        close = _safe_float(row[2])
        high = _safe_float(row[3])
        low = _safe_float(row[4])
        volume = _safe_float(row[5]) * 100.0
        amount = ((open_price + high + low + close) / 4.0) * volume
        parsed.append(
            {
                "symbol": code,
                "date": row_day.strftime("%Y%m%d"),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
                "turn": pd.NA,
                "pctChg": pd.NA,
                "code": code,
                "name": name or quote_name or code,
                "pricechange": pd.NA,
                "change_pct": pd.NA,
                "buy": None,
                "sell": None,
                "pre_close": pd.NA,
                "ticktime": None,
                "per": pd.NA,
                "pb": pd.NA,
                "mktcap": pd.NA,
                "nmc": pd.NA,
                "turnover": pd.NA,
                "volume_ratio": pd.NA,
            }
        )
    return pd.DataFrame(parsed)


def _recalculate_indicators(df: pd.DataFrame, code: str, name: str | None = None) -> pd.DataFrame:
    out = df.sort_values("_date").reset_index(drop=True).copy()
    for col in ["open", "high", "low", "close", "volume", "amount", "turn", "pctChg", "change_pct", "turnover"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "symbol" not in out.columns:
        out["symbol"] = code
    if "code" not in out.columns:
        out["code"] = code
    out["symbol"] = out["symbol"].fillna(code).astype(str)
    out["code"] = out["code"].fillna(code).astype(str)
    if name:
        out["name"] = out.get("name", name).fillna(name)
    elif "name" not in out.columns:
        out["name"] = code

    out["MA5"] = out["close"].rolling(window=5).mean()
    out["MA10"] = out["close"].rolling(window=10).mean()
    out["MA20"] = out["close"].rolling(window=20).mean()
    typical_price = (out["open"] + out["high"] + out["low"] + out["close"]) / 4.0
    out["amount"] = pd.to_numeric(out.get("amount"), errors="coerce")
    out.loc[out["amount"].isna() | (out["amount"] <= 0), "amount"] = typical_price * out["volume"]
    prev_close = out["close"].shift(1)
    out["pre_close"] = prev_close
    out["pricechange"] = out["close"] - prev_close
    out["pctChg"] = (out["pricechange"] / prev_close.replace(0, pd.NA)) * 100
    out["change_pct"] = out["pctChg"]
    if "turnover" in out.columns:
        out["turnover"] = pd.to_numeric(out["turnover"], errors="coerce")
    else:
        out["turnover"] = pd.NA
    out["turn"] = pd.to_numeric(out.get("turn"), errors="coerce")
    out.loc[out["turn"].isna(), "turn"] = out["turnover"]
    vol_ma5 = out["volume"].shift(1).rolling(window=5).mean()
    out["量比"] = out["volume"] / vol_ma5.replace(0, pd.NA)
    ema12 = out["close"].ewm(span=12, adjust=False).mean()
    ema26 = out["close"].ewm(span=26, adjust=False).mean()
    out["MACD_DIF"] = ema12 - ema26
    out["MACD_DEA"] = out["MACD_DIF"].ewm(span=9, adjust=False).mean()
    out["MACD_hist"] = (out["MACD_DIF"] - out["MACD_DEA"]) * 2
    out["真实涨幅点数"] = out["close"].pct_change() * 100

    for col in _preferred_columns():
        if col not in out.columns:
            out[col] = pd.NA
    return out[_preferred_columns() + ["_date"]]


def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    if "date" not in df.columns:
        raise ValueError("日线文件缺少 date 列")
    out = df.copy()
    date_text = out["date"].astype(str).str.replace(r"\D", "", regex=True).str[:8]
    out["_date"] = pd.to_datetime(date_text, format="%Y%m%d", errors="coerce")
    return out


def _write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    df.to_parquet(tmp, engine="pyarrow", index=False)
    tmp.replace(path)


def _code_from_path(path: Path) -> str:
    digits = "".join(ch for ch in path.stem.replace("_daily", "") if ch.isdigit())
    if len(digits) < 6:
        raise ValueError(f"无法从文件名解析股票代码：{path}")
    return digits[-6:]


def _latest_name(df: pd.DataFrame, code: str) -> str:
    if "name" not in df.columns:
        return code
    names = df["name"].dropna().astype(str)
    names = names[(names != "") & (names != "None") & (names != "nan")]
    return names.iloc[-1] if not names.empty else code


def _ashare_symbol(code: str, *, market_prefix: str | None = None) -> str:
    clean = str(code).zfill(6)[-6:]
    if market_prefix:
        return f"{market_prefix}{clean}"
    return f"sh{clean}" if clean.startswith(("5", "6", "9")) else f"sz{clean}"


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _preferred_columns() -> list[str]:
    return [
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "turn",
        "pctChg",
        "MA5",
        "MA10",
        "MA20",
        "量比",
        "MACD_DIF",
        "MACD_DEA",
        "MACD_hist",
        "真实涨幅点数",
        "code",
        "name",
        "pricechange",
        "change_pct",
        "buy",
        "sell",
        "pre_close",
        "ticktime",
        "per",
        "pb",
        "mktcap",
        "nmc",
        "turnover",
        "volume_ratio",
    ]


def main(argv: Sequence[str] | None = None) -> dict[str, Any]:
    parser = argparse.ArgumentParser(description="A 股日线冷数据自愈引擎")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="日线 parquet 目录")
    parser.add_argument("--target-date", help="补齐到指定日期 YYYY-MM-DD；默认上一交易日")
    parser.add_argument("--workers", type=int, default=12, help="并发股票数")
    parser.add_argument("--limit", type=int, default=0, help="仅处理前 N 个文件，0 表示全量")
    args = parser.parse_args(argv)

    target = pd.Timestamp(args.target_date).date() if args.target_date else None
    summary = heal_all(args.data_dir, target_date=target, max_workers=args.workers, limit=args.limit)
    print(
        "[data-healer] "
        f"target={summary['target_date']} files={summary['file_count']} "
        f"healed={summary['healed_count']} fresh={summary['fresh_count']} "
        f"stale_no_data={summary['stale_no_data_count']} errors={summary['error_count']} "
        f"appended_rows={summary['appended_rows']}"
    )
    for item in summary["errors"][:10]:
        print(f"[data-healer][ERROR] {item.code} {item.error}")
    for item in summary["stale_no_data"][:10]:
        print(f"[data-healer][NO_DATA] {item.code} max={item.before_max_date}")
    return summary


if __name__ == "__main__":
    main()
