from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd

from quant_core.config import BASE_DIR, DATA_DIR
from quant_core.data_pipeline.sector_engine import _import_ashare_get_price


CONCEPT_KLINE_DIR = BASE_DIR / "data" / "concept_kline"
CONCEPT_INDEX_PATH = CONCEPT_KLINE_DIR / "concept_index.parquet"
CONCEPT_STOCK_MAP_PATH = BASE_DIR / "data" / "concept_stock_map.json"
CONCEPT_CATALOG_PATH = BASE_DIR / "data" / "concept_catalog.json"
CONCEPT_CONSTITUENTS_PATH = BASE_DIR / "data" / "concept_constituents.json"
CONCEPT_FACTOR_COLUMNS = [
    "concept_pct_chg_1",
    "concept_pct_chg_3",
    "rs_stock_vs_concept",
    "rs_ema_5",
    "concept_volatility_5",
]
_STOCK_DAILY_CACHE: dict[str, pd.DataFrame] = {}


def sync_concept_daily(
    *,
    start_date: str = "20230101",
    end_date: Optional[str] = None,
    limit: int = 0,
) -> dict[str, Any]:
    CONCEPT_KLINE_DIR.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now().isoformat(timespec="seconds")
    end = _yyyymmdd(end_date or date.today())
    catalog = _load_concept_catalog()
    constituents = _load_concept_constituents()
    if limit and limit > 0:
        catalog = catalog[: int(limit)]
    summary: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": started_at,
        "status": "running",
        "source": "sina.concept_constituents + Ashare/tencent-or-local-synthetic",
        "concept_count": len(catalog),
        "updated": 0,
        "skipped": 0,
        "failed": 0,
        "errors": [],
    }
    index_rows: list[dict[str, Any]] = []
    for item in catalog:
        concept_code = str(item.get("concept_code") or "").strip()
        concept_name = str(item.get("concept_name") or "").strip()
        if not concept_code or not concept_name:
            continue
        members = constituents.get(concept_code, [])
        path = concept_cache_path(concept_code, concept_name)
        try:
            frame = _fetch_concept_daily(concept_code, concept_name, members, start_date, end)
            if frame.empty:
                summary["skipped"] += 1
                continue
            frame.to_parquet(path, index=False)
            summary["updated"] += 1
            index_rows.append(
                {
                    "concept_code": concept_code,
                    "concept_name": concept_name,
                    "path": str(path),
                    "member_count": int(len(members)),
                    "start": str(pd.to_datetime(frame["datetime"]).min().date()),
                    "end": str(pd.to_datetime(frame["datetime"]).max().date()),
                    "updated_at": datetime.now().isoformat(timespec="seconds"),
                }
            )
        except Exception as exc:
            summary["failed"] += 1
            _append_error(summary, f"{concept_code} {concept_name}: {exc}")
    if index_rows:
        pd.DataFrame(index_rows).to_parquet(CONCEPT_INDEX_PATH, index=False)
    summary["status"] = "success" if summary["failed"] == 0 else "partial"
    summary["finished_at"] = datetime.now().isoformat(timespec="seconds")
    summary_path = CONCEPT_KLINE_DIR / f"concept_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        summary["summary_path"] = str(summary_path)
    except Exception:
        pass
    return summary


def get_stock_concept_map(refresh: bool = False) -> dict[str, str]:
    if refresh:
        sync_concept_daily(limit=0)
    if not CONCEPT_STOCK_MAP_PATH.exists():
        return {}
    try:
        payload = json.loads(CONCEPT_STOCK_MAP_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, str] = {}
    for code, concepts in payload.items():
        stock_code = normalize_stock_code(code)
        if not stock_code:
            continue
        if isinstance(concepts, list) and concepts:
            out[stock_code] = str(concepts[0])
        elif isinstance(concepts, str):
            out[stock_code] = concepts
    return out


@lru_cache(maxsize=2048)
def load_concept_daily(concept_code: str) -> pd.DataFrame:
    index = _load_concept_index()
    if not index.empty:
        match = index[index["concept_code"].astype(str) == str(concept_code)]
        if not match.empty:
            path = Path(str(match.iloc[0].get("path") or ""))
            if path.exists():
                try:
                    return normalize_concept_daily_frame(pd.read_parquet(path), concept_code=concept_code)
                except Exception:
                    return pd.DataFrame()
    return pd.DataFrame()


def concept_relative_factor_frame(
    stock_frame: pd.DataFrame,
    *,
    code: Optional[str] = None,
    concept_map: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    zero = pd.DataFrame(0.0, index=stock_frame.index, columns=CONCEPT_FACTOR_COLUMNS)
    if stock_frame.empty or "datetime" not in stock_frame.columns or "close" not in stock_frame.columns:
        return zero
    stock_code = normalize_stock_code(code or _first_non_empty(stock_frame.get("code")) or _first_non_empty(stock_frame.get("symbol")))
    if not stock_code:
        return zero
    mapping = concept_map if concept_map is not None else get_stock_concept_map(refresh=False)
    concept_code = mapping.get(stock_code, "")
    if not concept_code:
        return zero
    concept = load_concept_daily(concept_code)
    if concept.empty:
        return zero

    stock_dates = pd.to_datetime(stock_frame["datetime"], errors="coerce").dt.normalize()
    stock_close = pd.to_numeric(stock_frame["close"], errors="coerce")
    stock_ret = stock_close.pct_change(fill_method=None)

    concept = concept.copy()
    concept["datetime"] = pd.to_datetime(concept["datetime"], errors="coerce").dt.normalize()
    concept = concept.dropna(subset=["datetime"]).drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime")
    concept_close = pd.to_numeric(concept["close"], errors="coerce")
    concept_ret = _normalize_pct_series(concept["pct_chg"]) if "pct_chg" in concept.columns else concept_close.pct_change(fill_method=None)
    concept["concept_pct_chg_1"] = concept_ret
    concept["concept_pct_chg_3"] = (1 + concept_ret).rolling(3, min_periods=1).apply(np.prod, raw=True) - 1
    concept["concept_volatility_5"] = _true_range_pct(concept).rolling(5, min_periods=1).mean()

    aligned = pd.DataFrame({"datetime": stock_dates}, index=stock_frame.index).merge(
        concept[["datetime", "concept_pct_chg_1", "concept_pct_chg_3", "concept_volatility_5"]],
        on="datetime",
        how="left",
    )
    out = pd.DataFrame(index=stock_frame.index)
    out["concept_pct_chg_1"] = pd.to_numeric(aligned["concept_pct_chg_1"], errors="coerce")
    out["concept_pct_chg_3"] = pd.to_numeric(aligned["concept_pct_chg_3"], errors="coerce")
    out["rs_stock_vs_concept"] = stock_ret.reset_index(drop=True) - out["concept_pct_chg_1"].reset_index(drop=True)
    out["rs_ema_5"] = out["rs_stock_vs_concept"].ewm(span=5, adjust=False, min_periods=1).mean()
    out["concept_volatility_5"] = pd.to_numeric(aligned["concept_volatility_5"], errors="coerce")
    out.index = stock_frame.index
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)[CONCEPT_FACTOR_COLUMNS]


def concept_cache_path(concept_code: str, concept_name: str = "") -> Path:
    raw = f"{concept_code}_{concept_name}"
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()[:10]
    slug = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]+", "_", raw).strip("_")[:48] or "concept"
    return CONCEPT_KLINE_DIR / f"{slug}_{digest}.parquet"


def normalize_concept_daily_frame(df: pd.DataFrame, *, concept_code: str = "", concept_name: str = "") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["datetime", "date", "concept_code", "concept_name", "open", "high", "low", "close", "volume", "amount", "pct_chg"])
    out = pd.DataFrame(index=df.index)
    date_col = _col(df, ["datetime", "date", "time", "日期", "day"])
    if pd.to_datetime(date_col, errors="coerce").notna().any():
        out["datetime"] = pd.to_datetime(date_col, errors="coerce")
    else:
        out["datetime"] = pd.to_datetime(df.index, errors="coerce")
    out["date"] = out["datetime"].dt.date.astype(str)
    out["concept_code"] = concept_code or str(_first_non_empty(df.get("concept_code")) or "")
    out["concept_name"] = concept_name or str(_first_non_empty(df.get("concept_name")) or "")
    out["open"] = _numeric_col(df, ["open", "开盘"])
    out["high"] = _numeric_col(df, ["high", "最高"])
    out["low"] = _numeric_col(df, ["low", "最低"])
    out["close"] = _numeric_col(df, ["close", "收盘"])
    out["volume"] = _numeric_col(df, ["volume", "vol", "成交量"])
    out["amount"] = _numeric_col(df, ["amount", "money", "成交额"])
    pct = _normalize_pct_series(_numeric_col(df, ["pct_chg", "pctChg", "change_pct", "涨跌幅"]))
    out["pct_chg"] = pct if pct.notna().any() else out["close"].pct_change(fill_method=None)
    out = out.dropna(subset=["datetime", "open", "high", "low", "close"])
    return out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").reset_index(drop=True)


def _fetch_concept_daily(
    concept_code: str,
    concept_name: str,
    members: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    direct = _fetch_concept_daily_direct(concept_code, concept_name, start_date, end_date)
    if not direct.empty:
        return direct
    return _build_synthetic_concept_daily(concept_code, concept_name, members, start_date, end_date)


def _fetch_concept_daily_direct(concept_code: str, concept_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    try:
        get_price: Callable[..., pd.DataFrame] = _import_ashare_get_price()
        start_ts = pd.Timestamp(_yyyymmdd(start_date))
        end_ts = pd.Timestamp(_yyyymmdd(end_date))
        count = max(80, int((end_ts - start_ts).days) + 20)
        raw = get_price(concept_code, frequency="1d", count=count, end_date=end_ts.strftime("%Y-%m-%d"))
        frame = normalize_concept_daily_frame(raw, concept_code=concept_code, concept_name=concept_name)
        if frame.empty:
            return frame
        mask = (frame["datetime"] >= start_ts) & (frame["datetime"] <= end_ts + pd.Timedelta(days=1))
        return frame.loc[mask].copy()
    except Exception:
        return pd.DataFrame()


def _build_synthetic_concept_daily(
    concept_code: str,
    concept_name: str,
    members: list[dict[str, Any]],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(_yyyymmdd(start_date))
    end_ts = pd.Timestamp(_yyyymmdd(end_date))
    frames = []
    for item in members:
        code = normalize_stock_code(item.get("stock_code") or item.get("code"))
        if not code:
            continue
        stock = _load_local_stock_daily(code)
        if stock.empty:
            continue
        stock = stock[(stock["datetime"] >= start_ts) & (stock["datetime"] <= end_ts)].copy()
        if stock.empty:
            continue
        base = pd.to_numeric(stock["close"], errors="coerce").dropna()
        if base.empty or float(base.iloc[0]) <= 0:
            continue
        first_close = float(base.iloc[0])
        stock["open_norm"] = pd.to_numeric(stock["open"], errors="coerce") / first_close * 1000.0
        stock["high_norm"] = pd.to_numeric(stock["high"], errors="coerce") / first_close * 1000.0
        stock["low_norm"] = pd.to_numeric(stock["low"], errors="coerce") / first_close * 1000.0
        stock["close_norm"] = pd.to_numeric(stock["close"], errors="coerce") / first_close * 1000.0
        frames.append(stock[["datetime", "open_norm", "high_norm", "low_norm", "close_norm", "volume", "amount"]])
    if not frames:
        return pd.DataFrame()
    panel = pd.concat(frames, ignore_index=True).dropna(subset=["datetime", "close_norm"])
    grouped = panel.groupby("datetime", as_index=False).agg(
        open=("open_norm", "mean"),
        high=("high_norm", "mean"),
        low=("low_norm", "mean"),
        close=("close_norm", "mean"),
        volume=("volume", "sum"),
        amount=("amount", "sum"),
    )
    grouped["concept_code"] = concept_code
    grouped["concept_name"] = concept_name
    grouped["pct_chg"] = grouped["close"].pct_change(fill_method=None)
    return normalize_concept_daily_frame(grouped, concept_code=concept_code, concept_name=concept_name)


def _load_local_stock_daily(code: str) -> pd.DataFrame:
    code = normalize_stock_code(code)
    if not code:
        return pd.DataFrame()
    cached = _STOCK_DAILY_CACHE.get(code)
    if cached is not None:
        return cached.copy()
    path = DATA_DIR / f"{code}_daily.parquet"
    if not path.exists():
        _STOCK_DAILY_CACHE[code] = pd.DataFrame()
        return pd.DataFrame()
    try:
        df = pd.read_parquet(path, columns=["date", "open", "high", "low", "close", "volume", "amount"])
    except Exception:
        try:
            df = pd.read_parquet(path)
        except Exception:
            _STOCK_DAILY_CACHE[code] = pd.DataFrame()
            return pd.DataFrame()
    if "datetime" not in df.columns:
        df["datetime"] = pd.to_datetime(df["date"].astype(str), errors="coerce")
    else:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")
    if "amount" not in df.columns:
        df["amount"] = 0.0
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    out = df.dropna(subset=["datetime", "open", "high", "low", "close"]).sort_values("datetime").reset_index(drop=True)
    _STOCK_DAILY_CACHE[code] = out
    return out.copy()


def _load_concept_catalog() -> list[dict[str, Any]]:
    if not CONCEPT_CATALOG_PATH.exists():
        return []
    try:
        payload = json.loads(CONCEPT_CATALOG_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, list) else []
    except Exception:
        return []


def _load_concept_constituents() -> dict[str, list[dict[str, Any]]]:
    if not CONCEPT_CONSTITUENTS_PATH.exists():
        return {}
    try:
        payload = json.loads(CONCEPT_CONSTITUENTS_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _load_concept_index() -> pd.DataFrame:
    if not CONCEPT_INDEX_PATH.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(CONCEPT_INDEX_PATH)
    except Exception:
        return pd.DataFrame()


def normalize_stock_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else ""


def _true_range_pct(frame: pd.DataFrame) -> pd.Series:
    high = pd.to_numeric(frame["high"], errors="coerce")
    low = pd.to_numeric(frame["low"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr / close.replace(0, np.nan)


def _normalize_pct_series(value: Any) -> pd.Series:
    series = pd.to_numeric(value, errors="coerce") if not isinstance(value, pd.Series) else pd.to_numeric(value, errors="coerce")
    finite = series.replace([np.inf, -np.inf], np.nan).dropna()
    if not finite.empty and finite.abs().quantile(0.95) > 1.5:
        series = series / 100.0
    return series


def _col(df: pd.DataFrame, names: list[str]) -> Any:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series([np.nan] * len(df), index=df.index)


def _numeric_col(df: pd.DataFrame, names: list[str]) -> pd.Series:
    return pd.to_numeric(_col(df, names), errors="coerce")


def _first_non_empty(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, pd.Series):
        for item in value.dropna().astype(str):
            if item:
                return item
        return ""
    return str(value or "").strip()


def _yyyymmdd(value: Any) -> str:
    if isinstance(value, str):
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) >= 8:
            return digits[:8]
        return pd.to_datetime(value).strftime("%Y%m%d")
    return pd.Timestamp(value).strftime("%Y%m%d")


def _append_error(summary: dict[str, Any], message: str) -> None:
    errors = summary.setdefault("errors", [])
    if isinstance(errors, list) and len(errors) < 20:
        errors.append(message)
