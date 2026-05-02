from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any

import pandas as pd
import requests

from quant_core.config import BASE_DIR


TENCENT_DAY_KLINE_URL = "http://ifzq.gtimg.cn/appstock/app/kline/kline"
CALENDAR_CACHE_PATH = BASE_DIR / "data" / "cache" / "trading_calendar_sina.json"
CALENDAR_CACHE_MAX_AGE_DAYS = 30


def is_trading_day(day: date | None = None) -> bool:
    target = day or datetime.now().date()
    return target in trading_days_between(target, target)


def latest_trading_day_on_or_before(day: date | None = None) -> date | None:
    target = day or datetime.now().date()
    days = trading_days_between(target - timedelta(days=370), target)
    return max(days) if days else None


def next_trading_day(day: date | None = None, n: int = 1) -> date:
    current = day or datetime.now().date()
    target_index = max(1, int(n))
    days = [item for item in all_known_trading_days() if item > current]
    if len(days) >= target_index:
        return days[target_index - 1]
    return _next_weekday_fallback(current, target_index)


def nth_trading_day(day: date, n: int) -> date:
    return next_trading_day(day, n=max(1, int(n)))


def trading_day_count_after(start: date, end: date) -> int:
    if end <= start:
        return 0
    return len(trading_days_between(start + timedelta(days=1), end))


def trading_days_between(start: date, end: date) -> set[date]:
    if end < start:
        return set()
    known_days = all_known_trading_days()
    days = {item for item in known_days if start <= item <= end}
    if known_days and start >= known_days[0] and end <= known_days[-1]:
        return days
    if days:
        return days
    if end <= datetime.now().date():
        return trading_days_on_or_before(end, lookback_days=max(90, (end - start).days + 30))
    return _weekday_days_between(start, end)


def all_known_trading_days() -> tuple[date, ...]:
    cached = _load_cached_sina_calendar()
    if cached and _calendar_cache_is_fresh() and _calendar_cache_covers_future(cached):
        return cached

    try:
        fetched = _fetch_sina_trading_calendar()
    except Exception:
        if cached:
            return cached
        return tuple()

    _save_sina_calendar(fetched)
    return fetched


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
    start = day - timedelta(days=max(30, int(lookback_days)))
    known_days = all_known_trading_days()
    calendar_days = {item for item in known_days if start <= item <= day}
    if known_days and start >= known_days[0] and day <= known_days[-1]:
        return calendar_days
    if calendar_days:
        return calendar_days
    return set(_cached_trading_days(day.isoformat(), int(lookback_days)))


def _fetch_sina_trading_calendar() -> tuple[date, ...]:
    import akshare as ak

    df = ak.tool_trade_date_hist_sina()
    if df.empty or "trade_date" not in df.columns:
        raise RuntimeError("Sina/AkShare 交易日历为空")
    parsed = pd.to_datetime(df["trade_date"], errors="coerce").dropna().dt.date
    days = tuple(sorted(set(parsed.tolist())))
    if not days:
        raise RuntimeError("Sina/AkShare 交易日历没有有效日期")
    return days


def _load_cached_sina_calendar() -> tuple[date, ...]:
    if not CALENDAR_CACHE_PATH.exists():
        return tuple()
    try:
        payload = json.loads(CALENDAR_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return tuple()
    days: list[date] = []
    for value in payload.get("trading_days", []):
        try:
            days.append(date.fromisoformat(str(value)[:10]))
        except ValueError:
            continue
    return tuple(sorted(set(days)))


def _save_sina_calendar(days: tuple[date, ...]) -> None:
    CALENDAR_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source": "akshare.tool_trade_date_hist_sina",
        "fetched_at": datetime.now().isoformat(timespec="seconds"),
        "start": days[0].isoformat() if days else "",
        "end": days[-1].isoformat() if days else "",
        "trading_days": [item.isoformat() for item in days],
    }
    CALENDAR_CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _calendar_cache_is_fresh() -> bool:
    if not CALENDAR_CACHE_PATH.exists():
        return False
    mtime = datetime.fromtimestamp(CALENDAR_CACHE_PATH.stat().st_mtime).date()
    return (datetime.now().date() - mtime).days <= CALENDAR_CACHE_MAX_AGE_DAYS


def _calendar_cache_covers_future(days: tuple[date, ...]) -> bool:
    return bool(days) and days[-1] >= datetime.now().date() + timedelta(days=30)


def _next_weekday_fallback(day: date, n: int) -> date:
    target = day
    count = 0
    while count < n:
        target += timedelta(days=1)
        if target.weekday() < 5:
            count += 1
    return target


def _weekday_days_between(start: date, end: date) -> set[date]:
    days: set[date] = set()
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.add(cursor)
        cursor += timedelta(days=1)
    return days
