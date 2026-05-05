from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

import pandas as pd
import requests


TENCENT_MKLINE_URL = "http://ifzq.gtimg.cn/appstock/app/kline/mkline"
TENCENT_REALTIME_URL = "http://qt.gtimg.cn/q={symbol}"


def get_tencent_m5(code: str, count: int = 48) -> pd.DataFrame:
    """Fetch latest Tencent 5-minute K lines for one A-share."""
    symbol = tencent_symbol(code)
    response = _request_get(
        TENCENT_MKLINE_URL,
        params={"param": f"{symbol},m5,,{max(1, int(count))}"},
        timeout=8,
    )
    payload = response.json()
    rows = (((payload.get("data") or {}).get(symbol) or {}).get("m5") or [])
    if not rows:
        return _empty_m5_frame()

    normalized: list[dict[str, Any]] = []
    for row in rows:
        if len(row) < 6:
            continue
        normalized.append(
            {
                "datetime": pd.to_datetime(row[0], errors="coerce"),
                "open": _safe_float(row[1]),
                "close": _safe_float(row[2]),
                "high": _safe_float(row[3]),
                "low": _safe_float(row[4]),
                "volume": _safe_float(row[5]),
            }
        )
    df = pd.DataFrame(normalized)
    if df.empty:
        return _empty_m5_frame()
    df = df.dropna(subset=["datetime"])
    for col in ["open", "close", "high", "low", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df.sort_values("datetime").reset_index(drop=True)


def get_tencent_realtime(code: str) -> dict[str, Any]:
    """Fetch Tencent realtime quote.

    Field 3 is current price. During 09:15-09:25 call auction it maps to the
    virtual matching price. Field 4 is previous close and field 5 is open.
    """
    symbol = tencent_symbol(code)
    response = _request_get(TENCENT_REALTIME_URL.format(symbol=symbol), timeout=5)
    text = response.content.decode("gbk", errors="ignore").strip()
    match = re.search(r'v_[a-z]{2}\d{6}="([^"]*)"', text)
    if not match:
        raise RuntimeError(f"腾讯实时行情未返回 {code} 的有效数据：{text[:120]}")
    fields = match.group(1).split("~")
    if len(fields) < 6:
        raise RuntimeError(f"腾讯实时行情返回 {code} 字段不足：{fields}")

    clean = normalize_stock_code(code)
    price = _safe_float(_field(fields, 3))
    pre_close = _safe_float(_field(fields, 4))
    open_price = _safe_float(_field(fields, 5))
    high = _safe_float(_field(fields, 33))
    low = _safe_float(_field(fields, 34))
    volume = _safe_float(_field(fields, 6))
    amount = _safe_float(_field(fields, 37))
    quote_date = str(_field(fields, 30) or "")
    quote_time = _format_quote_time(quote_date)

    if price <= 0 and open_price > 0:
        price = open_price
    if price <= 0:
        raise RuntimeError(f"腾讯实时行情未返回 {clean} 的有效当前价/竞价虚拟匹配价")

    return {
        "code": clean,
        "symbol": symbol,
        "name": str(_field(fields, 1) or ""),
        "price": price,
        "pre_close": pre_close,
        "open": open_price,
        "current_price": price,
        "auction_price": price,
        "high": high,
        "low": low,
        "volume": volume,
        "amount": amount,
        "date": quote_time[:10] if quote_time else datetime.now().strftime("%Y-%m-%d"),
        "time": quote_time[11:] if len(quote_time) > 10 else datetime.now().strftime("%H:%M:%S"),
        "source": "tencent.qt",
    }


def tencent_symbol(code: str) -> str:
    clean = normalize_stock_code(code)
    return f"sh{clean}" if clean.startswith(("5", "6", "9")) else f"sz{clean}"


def normalize_stock_code(code: str) -> str:
    digits = "".join(ch for ch in str(code).strip() if ch.isdigit())
    if len(digits) < 6:
        raise ValueError(f"非法股票代码：{code}")
    return digits[-6:]


def _request_get(url: str, params: dict[str, Any] | None = None, timeout: int = 8) -> requests.Response:
    session = requests.Session()
    session.trust_env = False
    response = session.get(url, params=params, timeout=timeout, proxies={})
    response.raise_for_status()
    response.encoding = response.apparent_encoding or response.encoding
    return response


def _empty_m5_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["datetime", "open", "close", "high", "low", "volume"])


def _field(fields: list[str], index: int) -> str:
    return fields[index] if len(fields) > index else ""


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_quote_time(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) >= 14:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]} {digits[8:10]}:{digits[10:12]}:{digits[12:14]}"
    return ""
