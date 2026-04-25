from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import pandas as pd
import requests


SINA_HS_A_URL = (
    "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "Market_Center.getHQNodeData?page={page}&num=100&sort=symbol&asc=1"
    "&node=hs_a&symbol=&_s_r_a=page"
)
SINA_INDEX_URL = "https://hq.sinajs.cn/list=sh000001,sh000852"
SINA_INDEX_KLINE_URL = (
    "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
    "CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen=30"
)


def _loads_sina_js(text: str) -> list[dict[str, Any]]:
    if not text or text in {"null", "[]"}:
        return []
    valid_json_text = re.sub(r"([{,])([a-zA-Z0-9_]+):", r'\1"\2":', text)
    return json.loads(valid_json_text)


def fetch_sina_snapshot(max_pages: int | None = None, timeout: int = 8) -> pd.DataFrame:
    """Fetch the A-share market snapshot from Sina's public endpoint."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "http://vip.stock.finance.sina.com.cn/",
    }
    rows: list[dict[str, Any]] = []
    page_limit = max_pages or 90
    page = 1
    batch_size = 16

    while page <= page_limit:
        pages = list(range(page, min(page + batch_size, page_limit + 1)))
        page_results: dict[int, list[dict[str, Any]]] = {}
        with ThreadPoolExecutor(max_workers=min(batch_size, len(pages))) as executor:
            futures = {
                executor.submit(_fetch_sina_page, page_no, headers, timeout): page_no
                for page_no in pages
            }
            for future in as_completed(futures):
                page_no = futures[future]
                page_results[page_no] = future.result()

        stop = False
        for page_no in pages:
            data = page_results.get(page_no, [])
            if not data:
                stop = True
                break
            rows.extend(data)
            if len(data) < 100:
                stop = True
                break
        if stop:
            break
        page += batch_size

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    rename_map = {
        "symbol": "raw_code",
        "code": "raw_code",
        "name": "name",
        "trade": "close",
        "changepercent": "change_pct",
        "turnoverratio": "turnover",
        "high": "high",
        "low": "low",
        "open": "open",
        "settlement": "pre_close",
        "volume": "volume",
        "amount": "amount",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    df = _coalesce_duplicate_columns(df)
    if "raw_code" not in df.columns:
        df["raw_code"] = ""
    df["code"] = df["raw_code"].astype(str).str.extract(r"(\d{6})")[0]
    df["date"] = datetime.now().strftime("%Y-%m-%d")
    if "volume_ratio" not in df.columns:
        df["volume_ratio"] = 0.0
    df["volume_ratio"] = pd.to_numeric(df["volume_ratio"], errors="coerce").fillna(0.0)

    numeric_cols = ["open", "high", "low", "close", "pre_close", "change_pct", "turnover", "volume", "amount"]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[(df["code"].notna()) & (df["close"] > 0)].copy()
    return df


def fetch_market_indices(timeout: int = 5) -> dict[str, dict[str, float | str]]:
    """Fetch broad market index changes used by the live risk gate."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Referer": "https://finance.sina.com.cn/",
    }
    try:
        response = requests.get(SINA_INDEX_URL, headers=headers, timeout=timeout)
        response.raise_for_status()
    except Exception:
        return {}

    result: dict[str, dict[str, float | str]] = {}
    for match in re.finditer(r'var hq_str_(sh\d{6})="([^"]*)"', response.text):
        code = match.group(1)
        fields = match.group(2).split(",")
        if len(fields) < 4:
            continue
        name = fields[0]
        try:
            open_price = float(fields[1])
            pre_close = float(fields[2])
            close = float(fields[3])
        except ValueError:
            continue
        change_pct = (close / pre_close - 1) * 100 if pre_close > 0 else 0.0
        result[code] = {
            "code": code,
            "name": name,
            "open": open_price,
            "pre_close": pre_close,
            "close": close,
            "change_pct": round(change_pct, 4),
        }
    _attach_index_trends(result, headers, timeout)
    return result


def _attach_index_trends(indices: dict[str, dict[str, float | str]], headers: dict[str, str], timeout: int) -> None:
    for code, item in indices.items():
        try:
            response = requests.get(SINA_INDEX_KLINE_URL.format(symbol=code), headers=headers, timeout=timeout)
            response.raise_for_status()
            rows = _loads_sina_js(response.text)
        except Exception:
            continue
        closes = [float(row.get("close", 0) or 0) for row in rows if float(row.get("close", 0) or 0) > 0]
        if len(closes) < 20:
            continue
        close = float(item.get("close", closes[-1]) or closes[-1])
        ma10 = sum(closes[-10:]) / 10
        ma20 = sum(closes[-20:]) / 20
        item["ma10"] = round(ma10, 4)
        item["ma20"] = round(ma20, 4)
        item["above_ma10"] = close >= ma10
        item["above_ma20"] = close >= ma20


def _fetch_sina_page(page: int, headers: dict[str, str], timeout: int) -> list[dict[str, Any]]:
    try:
        response = requests.get(SINA_HS_A_URL.format(page=page), headers=headers, timeout=timeout)
        response.raise_for_status()
        return _loads_sina_js(response.text)
    except Exception:
        return []


def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.duplicated().any():
        return df
    merged = {}
    for col in dict.fromkeys(df.columns):
        subset = df.loc[:, df.columns == col]
        merged[col] = subset.iloc[:, 0] if subset.shape[1] == 1 else subset.bfill(axis=1).iloc[:, 0]
    return pd.DataFrame(merged)
