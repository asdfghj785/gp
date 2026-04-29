from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any

import pandas as pd
import requests


TENCENT_DAY_KLINE_URL = "http://ifzq.gtimg.cn/appstock/app/kline/kline"


def is_trading_day(day: date | None = None) -> bool:
    target = day or datetime.now().date()
    return target in trading_days_on_or_before(target, lookback_days=90)


def latest_trading_day_on_or_before(day: date | None = None) -> date | None:
    target = day or datetime.now().date()
    days = trading_days_on_or_before(target, lookback_days=90)
    return max(days) if days else None


@lru_cache(maxsize=64)
def _cached_trading_days(end_text: str, lookback_days: int) -> tuple[date, ...]:
    end_day = date.fromisoformat(end_text)
    start_day = end_day - timedelta(days=max(30, int(lookback_days)))
    count = max(80, int(lookback_days) + 30)
    session = requests.Session()
    session.trust_env = False
    response = session.get(
        TENCENT_DAY_KLINE_URL,
        params={"param": f"sh000001,day,,{end_day.isoformat()},{count}"},
        timeout=8,
        proxies={},
    )
    response.raise_for_status()
    payload: dict[str, Any] = response.json()
    rows = (((payload.get("data") or {}).get("sh000001") or {}).get("day") or [])
    parsed: list[date] = []
    for row in rows:
        if not row:
            continue
        day_value = pd.to_datetime(row[0], errors="coerce")
        if pd.isna(day_value):
            continue
        item = day_value.date()
        if start_day <= item <= end_day:
            parsed.append(item)
    return tuple(sorted(set(parsed)))


def trading_days_on_or_before(day: date, lookback_days: int = 90) -> set[date]:
    return set(_cached_trading_days(day.isoformat(), int(lookback_days)))
