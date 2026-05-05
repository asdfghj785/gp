from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import requests
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.ai_agent.agent_gateway import run_1446_ai_interview
from quant_core.cache_utils import CACHE_DIR, read_json_cache
from quant_core.config import MODELS_DIR
from quant_core.data_pipeline.concept_engine import CONCEPT_CATALOG_PATH, CONCEPT_INDEX_PATH, get_stock_concept_map
from quant_core.data_pipeline.market import fetch_realtime_quote, fetch_sina_snapshot
from quant_core.data_pipeline.sector_engine import get_stock_sector_map
from quant_core.data_pipeline.trading_calendar import is_trading_day
from quant_core.engine.daily_factor_factory import THEME_FACTOR_COLUMNS, generate_daily_factors
from quant_core.engine.daily_model_trainer import discover_daily_data_dir, list_daily_files
from quant_core.engine.model_evaluator import load_daily_model
from quant_core.storage import (
    get_v3_sniper_lock,
    list_v3_sniper_locks,
    save_v3_sniper_lock,
    v3_sniper_followup_rows,
    v3_sniper_future_closes,
)


router = APIRouter(prefix="/api/v3", tags=["v3-sniper"])
v4_router = APIRouter(prefix="/api/v4", tags=["v4-sniper"])

GLOBAL_MODEL_PATH = MODELS_DIR / "xgboost_daily_swing_global_v1.json"
GLOBAL_META_PATH = MODELS_DIR / "xgboost_daily_swing_global_v1.meta.json"
_SCAN_CACHE: dict[str, Any] = {"key": "", "created_at": 0.0, "payload": None}
_TOP_K = 5
_LOCK_HOUR = 14
_LOCK_MINUTE = 50
_LOCK_LABEL = f"{_LOCK_HOUR:02d}:{_LOCK_MINUTE:02d}"
_EXCLUDED_BOARD_PREFIXES = ("300", "301", "688", "689", "4", "8", "92")
_WORKER_MODEL: Any = None
_WORKER_FEATURE_COLS: list[str] = []
_STOCK_CONCEPT_MAP_CACHE: Optional[dict[str, str]] = None
_STOCK_SECTOR_MAP_CACHE: Optional[dict[str, str]] = None
_CONCEPT_NAME_MAP_CACHE: Optional[dict[str, str]] = None


class AnalyzeStockRequest(BaseModel):
    code: str
    name: Optional[str] = ""
    candidate: Optional[dict[str, Any]] = None


@router.get("/sniper/scan_today")
async def scan_today(
    threshold: float = Query(0.85, ge=0.0, le=1.0, description="Legacy param; Top-K mode ignores hard threshold."),
    limit: int = Query(default=0, ge=0, le=10000),
    max_workers: int = Query(default=8, ge=1, le=16),
    cache_seconds: int = Query(default=120, ge=0, le=900),
) -> dict[str, Any]:
    """Run or read the 14:50 live global XGBoost inference pass.

    limit=0 means full local daily universe. After 14:50 on a current A-share
    trading session, the first successful full scan is persisted and all later
    calls return the immutable daily Top 5 lock.
    """
    try:
        return await asyncio.to_thread(_scan_today_sync, threshold, limit, max_workers, cache_seconds)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"scan_today failed: {exc}") from exc


@router.get("/sniper/history")
async def sniper_history(limit: int = Query(default=20, ge=1, le=120)) -> dict[str, Any]:
    """Return V4 Theme Alpha sniper locks plus synced 全局动量狙击 backtest settlement rows."""
    return await asyncio.to_thread(_sniper_history_sync, limit)


@v4_router.get("/sniper/scan_today")
async def scan_today_v4(
    threshold: float = Query(0.85, ge=0.0, le=1.0, description="Legacy param; Top-K mode ignores hard threshold."),
    limit: int = Query(default=0, ge=0, le=10000),
    max_workers: int = Query(default=8, ge=1, le=16),
    cache_seconds: int = Query(default=120, ge=0, le=900),
) -> dict[str, Any]:
    return await scan_today(threshold=threshold, limit=limit, max_workers=max_workers, cache_seconds=cache_seconds)


@v4_router.get("/sniper/history")
async def sniper_history_v4(limit: int = Query(default=20, ge=1, le=120)) -> dict[str, Any]:
    return await sniper_history(limit=limit)


@router.post("/agent/analyze_stock")
async def analyze_stock(payload: AnalyzeStockRequest) -> dict[str, Any]:
    code = "".join(ch for ch in str(payload.code) if ch.isdigit())[-6:]
    if len(code) != 6:
        raise HTTPException(status_code=422, detail=f"非法股票代码：{payload.code}")
    name = payload.name or code
    candidate = dict(payload.candidate or {})
    candidate.setdefault("code", code)
    candidate.setdefault("name", name)
    result = await asyncio.to_thread(run_1446_ai_interview, [code], [name], [candidate])
    return {
        "code": code,
        "name": name,
        "created_at": result.get("created_at"),
        "status": result.get("status"),
        "model": result.get("model"),
        "markdown": result.get("markdown", ""),
        "items": result.get("items", []),
        "news": result.get("news", {}),
    }


def _scan_today_sync(threshold: float, limit: int, max_workers: int, cache_seconds: int) -> dict[str, Any]:
    start_ts = time.time()
    prediction_date = datetime.now().date().isoformat()
    locked = get_v3_sniper_lock(prediction_date)
    if locked:
        return _payload_from_lock(locked, start_ts)

    can_attempt_lock = _can_attempt_daily_lock(limit)
    cache_key = f"{prediction_date}:{limit}:{max_workers}:top{_TOP_K}:live-v4-theme"
    now_ts = datetime.now().timestamp()
    if (
        not can_attempt_lock
        and cache_seconds > 0
        and _SCAN_CACHE.get("key") == cache_key
        and _SCAN_CACHE.get("payload") is not None
        and now_ts - float(_SCAN_CACHE.get("created_at") or 0) <= cache_seconds
    ):
        payload = dict(_SCAN_CACHE["payload"])
        payload["cache"] = {"hit": True, "ttl_seconds": cache_seconds}
        payload["served_elapsed_seconds"] = round(time.time() - start_ts, 3)
        return payload

    if not GLOBAL_MODEL_PATH.exists():
        raise FileNotFoundError(f"全局 XGBoost 模型不存在：{GLOBAL_MODEL_PATH}")
    meta = _load_model_meta()
    feature_cols = list(meta.get("feature_columns") or [])
    if not feature_cols:
        raise RuntimeError(f"模型元数据缺少 feature_columns：{GLOBAL_META_PATH}")

    data_dir = discover_daily_data_dir()
    files = list_daily_files(data_dir, limit=limit)
    if not files:
        raise FileNotFoundError(f"未找到日线文件：{data_dir}")

    codes = [_code_from_path(path) for path in files]
    quote_pool, quote_meta = _fetch_live_quote_pool(codes)
    quote_meta["current_quote_count"] = _current_session_quote_count(quote_pool, prediction_date)
    tasks, filter_stats = _build_candidate_tasks(files, quote_pool)
    scored_rows, errors = _score_candidate_tasks(tasks, feature_cols, max_workers=max_workers)
    evaluated_count = len(scored_rows)
    rows = [
        _ensure_theme_contract(row)
        for row in sorted(scored_rows, key=lambda item: float(item.get("probability") or 0.0), reverse=True)[:_TOP_K]
    ]
    if not rows:
        raise RuntimeError(f"没有生成任何有效最新因子。errors={errors[:5]} filter_stats={filter_stats}")

    elapsed_seconds = round(time.time() - start_ts, 3)
    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "prediction_date": prediction_date,
        "live_data": {
            "enabled": True,
            "source": quote_meta.get("source") or "tencent.qt",
            "fetch_mode": quote_meta.get("mode") or "batch",
            "mode": "local_daily_tail_plus_live_quote",
        },
        "threshold": None,
        "legacy_threshold_param": threshold,
        "selection_mode": "top_k",
        "top_k": _TOP_K,
        "locked": False,
        "lock_cutoff": _LOCK_LABEL,
        "universe_filter": "mainboard_only_exclude_chinext_star_bj_st",
        "elapsed_seconds": elapsed_seconds,
        "universe_count": len(files),
        "quote_count": len(quote_pool),
        "prefiltered_count": len(tasks),
        "evaluated_count": evaluated_count,
        "signal_count": len(rows),
        "rows": rows,
        "errors": errors[:12],
        "filter_stats": filter_stats,
        "model": {
            "path": str(GLOBAL_MODEL_PATH),
            "split_date": meta.get("split_date"),
            "metrics": meta.get("metrics", {}),
            "high_confidence_precision": "85.78%",
        },
        "cache": {"hit": False, "ttl_seconds": cache_seconds},
    }
    should_lock, lock_reason = _should_lock_payload(limit, quote_pool, quote_meta, prediction_date)
    payload["lock_status"] = lock_reason
    if should_lock:
        payload["locked_at"] = datetime.now().isoformat(timespec="seconds")
        locked = save_v3_sniper_lock(payload, created_by="v4_theme_alpha_1450")
        payload = _payload_from_lock(locked, start_ts)
        payload["cache"] = {"hit": False, "type": "persistent_lock", "inserted": bool(locked.get("inserted"))}
        if locked.get("inserted"):
            payload["pushplus"] = _send_v3_sniper_pushplus(payload)
        return payload

    _SCAN_CACHE.update({"key": cache_key, "created_at": now_ts, "payload": payload})
    return payload


def lock_today_sniper_snapshot(limit: int = 0, max_workers: int = 8) -> dict[str, Any]:
    """CLI-friendly entry point for the 14:50 LaunchAgent."""
    return _scan_today_sync(threshold=0.85, limit=limit, max_workers=max_workers, cache_seconds=0)


def _sniper_history_sync(limit: int) -> dict[str, Any]:
    locks = list_v3_sniper_locks(limit=limit)
    rows: list[dict[str, Any]] = []
    backtest_by_date = _global_momentum_backtest_history(limit=limit)
    for lock in locks:
        payload = dict(lock.get("payload") or {})
        signals = _filter_display_rows(list(payload.get("rows") or []))[: int(payload.get("top_k") or _TOP_K)]
        stocks = [_history_stock_row(signal, str(lock["selection_date"])) for signal in signals]
        _merge_backtest_stocks(stocks, backtest_by_date.pop(str(lock["selection_date"]), []))
        rows.append(
            {
                "id": lock.get("id"),
                "selection_date": lock.get("selection_date"),
                "locked_at": lock.get("locked_at"),
                "top_k": lock.get("top_k") or payload.get("top_k") or _TOP_K,
                "signal_count": len(signals),
                "elapsed_seconds": payload.get("elapsed_seconds"),
                "live_source": (payload.get("live_data") or {}).get("source"),
                "stocks": stocks,
            }
        )
    for selection_date in sorted(backtest_by_date.keys(), reverse=True):
        stocks = backtest_by_date[selection_date]
        if not stocks:
            continue
        rows.append(
            {
                "id": f"backtest-global-{selection_date}",
                "selection_date": selection_date,
                "locked_at": f"{selection_date}T15:00:00",
                "top_k": len(stocks),
                "signal_count": len(stocks),
                "elapsed_seconds": None,
                "live_source": "top_pick_backtest_m12",
                "history_source": "top_pick_backtest_m12",
                "stocks": stocks,
            }
        )
        if len(rows) >= limit:
            break
    rows = sorted(rows, key=lambda item: str(item.get("selection_date") or ""), reverse=True)[:limit]
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(rows),
        "backtest_sync": {
            "enabled": True,
            "source": "data/strategy_cache/top_pick_backtest_m12.json",
            "strategy_type": "全局动量狙击",
        },
        "rows": rows,
    }


def _merge_backtest_stocks(stocks: list[dict[str, Any]], backtest_stocks: list[dict[str, Any]]) -> None:
    if not backtest_stocks:
        return
    by_code = {str(item.get("code") or ""): item for item in stocks}
    for backtest in backtest_stocks:
        code = str(backtest.get("code") or "")
        if code in by_code:
            target = by_code[code]
            target.update(
                {
                    "backtest_sync": True,
                    "t3_max_gain_pct": backtest.get("t3_max_gain_pct"),
                    "t3_close": backtest.get("t3_close"),
                    "t3_close_return_pct": backtest.get("t3_close_return_pct"),
                    "t3_settlement_price": backtest.get("t3_settlement_price"),
                    "t3_settlement_return_pct": backtest.get("t3_settlement_return_pct"),
                    "close_price": backtest.get("close_price"),
                    "close_return_pct": backtest.get("close_return_pct"),
                    "success": backtest.get("success"),
                }
            )
            if len(target.get("t_days") or []) >= 3 and backtest.get("t3_close") is not None:
                target["t_days"][2].update(
                    {
                        "status": "closed",
                        "date": backtest.get("t3_exit_date") or target["t_days"][2].get("date"),
                        "close": backtest.get("t3_close"),
                        "return_pct": backtest.get("t3_close_return_pct"),
                        "source": "top_pick_backtest_m12",
                    }
                )
        else:
            stocks.append(backtest)


def _global_momentum_backtest_history(limit: int = 120) -> dict[str, list[dict[str, Any]]]:
    payload = _read_top_pick_backtest_payload()
    if not payload:
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in payload.get("rows") or []:
        if str(row.get("strategy_type") or "") != "全局动量狙击":
            continue
        selection_date = str(row.get("selection_date") or row.get("date") or "")[:10]
        if not selection_date:
            continue
        grouped.setdefault(selection_date, []).append(_backtest_history_stock_row(dict(row), selection_date))
        if len(grouped) >= limit:
            break
    return grouped


def _read_top_pick_backtest_payload() -> dict[str, Any] | None:
    cached = read_json_cache("top_pick_backtest", 12)
    if cached:
        return cached
    path = CACHE_DIR / "top_pick_backtest_m12.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _backtest_history_stock_row(row: dict[str, Any], selection_date: str) -> dict[str, Any]:
    base_close = _safe_float(_first_present(row, "snapshot_price", "selection_price", "close"), None)
    signal = {
        "code": row.get("code"),
        "name": row.get("name"),
        "close": base_close,
        "theme_name": _first_present(row, "theme_name", "core_theme"),
        "theme_source": row.get("theme_source") or "",
        "theme_pct_chg_3": _first_present(row, "theme_pct_chg_3", "theme_momentum_3d", "theme_momentum"),
        "probability_pct": _first_present(row, "global_probability_pct", "probability_pct", "composite_score"),
        "pct_chg": _first_present(row, "pct_chg", "change"),
        "live_quote_time": row.get("snapshot_time") or "15:00",
        "strategy_type": "全局动量狙击",
    }
    stock = _history_stock_row(signal, selection_date)
    t3_close = _safe_float(_first_present(row, "t3_close", "t3_settlement_price", "close_price"), None)
    t3_return = _safe_float(_first_present(row, "t3_close_return_pct", "t3_settlement_return_pct", "close_return_pct"), None)
    t3_exit_date = row.get("t3_exit_date")
    if len(stock.get("t_days") or []) >= 3 and t3_close is not None and t3_return is not None:
        stock["t_days"][2].update(
            {
                "status": "closed",
                "date": t3_exit_date or stock["t_days"][2].get("date"),
                "close": round(float(t3_close), 4),
                "return_pct": round(float(t3_return), 4),
                "source": "top_pick_backtest_m12",
                "checked_at": f"{t3_exit_date or selection_date}T15:00:00",
            }
        )
    stock.update(
        {
            "history_source": "top_pick_backtest_m12",
            "backtest_sync": True,
            "locked_price": base_close,
            "locked_quote_time": row.get("snapshot_time") or "15:00",
            "strategy_type": "全局动量狙击",
            "t3_exit_date": t3_exit_date,
            "t3_max_gain_pct": _safe_float(row.get("t3_max_gain_pct"), None),
            "t3_close": t3_close,
            "t3_close_return_pct": t3_return,
            "t3_settlement_price": _safe_float(_first_present(row, "t3_settlement_price", "t3_close", "close_price"), None),
            "t3_settlement_return_pct": _safe_float(
                _first_present(row, "t3_settlement_return_pct", "t3_close_return_pct", "close_return_pct"),
                None,
            ),
            "close_price": _safe_float(_first_present(row, "close_price", "t3_settlement_price", "t3_close"), None),
            "close_return_pct": _safe_float(_first_present(row, "close_return_pct", "t3_settlement_return_pct", "t3_close_return_pct"), None),
            "success": row.get("success"),
        }
    )
    return stock


def _first_present(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None and value != "":
            return value
    return None


def _history_stock_row(signal: dict[str, Any], selection_date: str) -> dict[str, Any]:
    signal = _ensure_theme_contract(dict(signal))
    code = str(signal.get("code") or "").zfill(6)[-6:]
    base_close = _safe_float(signal.get("close"), None)
    closes = _future_close_rows(code, selection_date, limit=3)
    t_days: list[dict[str, Any]] = []
    for idx in range(3):
        label = f"T+{idx + 1}"
        if idx >= len(closes):
            t_days.append({"label": label, "status": "pending", "date": None, "close": None, "return_pct": None, "change_pct": None})
            continue
        row = closes[idx]
        close = _safe_float(row.get("close"), None)
        return_pct = _safe_float(row.get("return_pct"), None)
        if return_pct is None and base_close and close:
            return_pct = round((float(close) / float(base_close) - 1) * 100, 2)
        t_days.append(
            {
                "label": label,
                "status": "closed",
                "date": row.get("date"),
                "close": round(float(close), 4) if close is not None else None,
                "return_pct": return_pct,
                "change_pct": _safe_float(row.get("change_pct"), None),
                "source": row.get("source") or "stock_daily",
                "checked_at": row.get("checked_at"),
            }
        )
    return {
        "code": code,
        "name": signal.get("name") or code,
        "theme_name": signal.get("theme_name") or "-",
        "theme_source": signal.get("theme_source") or "",
        "theme_pct_chg_3": signal.get("theme_pct_chg_3"),
        "probability_pct": signal.get("probability_pct"),
        "pct_chg": signal.get("pct_chg"),
        "locked_price": base_close,
        "locked_quote_time": signal.get("live_quote_time") or "",
        "strategy_type": signal.get("strategy_type") or "全局日线XGB",
        "t_days": t_days,
    }


def _future_close_rows(code: str, selection_date: str, limit: int = 3) -> list[dict[str, Any]]:
    db_rows = v3_sniper_followup_rows(code, selection_date, limit=limit)
    if len(db_rows) < limit:
        existing_dates = {str(row.get("date")) for row in db_rows if row.get("date")}
        for row in v3_sniper_future_closes(code, selection_date, limit=limit):
            if str(row.get("date")) not in existing_dates:
                db_rows.append(row)
                existing_dates.add(str(row.get("date")))
            if len(db_rows) >= limit:
                break
    by_date = {str(row.get("date")): dict(row) for row in db_rows if row.get("date")}
    if len(by_date) < limit:
        for row in _future_close_rows_from_parquet(code, selection_date, limit=limit):
            by_date.setdefault(str(row.get("date")), row)
    return [by_date[key] for key in sorted(by_date)[:limit]]


def _future_close_rows_from_parquet(code: str, selection_date: str, limit: int = 3) -> list[dict[str, Any]]:
    try:
        path = discover_daily_data_dir() / f"{str(code).zfill(6)[-6:]}_daily.parquet"
        if not path.exists():
            return []
        raw = pd.read_parquet(path)
    except Exception:
        return []
    if raw.empty or "date" not in raw.columns:
        return []
    frame = raw.copy()
    frame["_date"] = pd.to_datetime(frame["date"].astype(str), format="%Y%m%d", errors="coerce")
    missing_mask = frame["_date"].isna()
    if missing_mask.any():
        frame.loc[missing_mask, "_date"] = pd.to_datetime(frame.loc[missing_mask, "date"].astype(str), errors="coerce")
    frame = frame.dropna(subset=["_date"]).sort_values("_date")
    selected = pd.Timestamp(selection_date).date()
    frame = frame[(frame["_date"].dt.weekday < 5) & (frame["_date"].dt.date > selected)].head(limit)
    rows: list[dict[str, Any]] = []
    for _, item in frame.iterrows():
        close = _safe_float(item.get("close"), None)
        if close is None:
            continue
        rows.append(
            {
                "code": str(code).zfill(6)[-6:],
                "name": str(item.get("name") or ""),
                "date": item["_date"].date().isoformat(),
                "close": float(close),
                "change_pct": _safe_float(item.get("pctChg"), None),
            }
        )
    return rows


def _payload_from_lock(lock: dict[str, Any], start_ts: float) -> dict[str, Any]:
    payload = dict(lock.get("payload") or {})
    rows = [_ensure_theme_contract(row) for row in _filter_display_rows(list(payload.get("rows") or []))]
    payload["rows"] = rows
    payload["signal_count"] = len(rows)
    payload.setdefault("universe_filter", "mainboard_only_exclude_chinext_star_bj_st")
    payload["lock_cutoff"] = _LOCK_LABEL
    payload["locked"] = True
    payload["locked_at"] = payload.get("locked_at") or lock.get("locked_at")
    payload["lock_id"] = lock.get("id")
    payload["lock_status"] = "locked"
    payload["cache"] = {"hit": True, "type": "persistent_lock"}
    payload["served_elapsed_seconds"] = round(time.time() - start_ts, 3)
    return payload


def _can_attempt_daily_lock(limit: int) -> bool:
    now = datetime.now()
    return int(limit) == 0 and is_trading_day(now.date()) and (now.hour, now.minute) >= (_LOCK_HOUR, _LOCK_MINUTE)


def _should_lock_payload(
    limit: int,
    quote_pool: dict[str, dict[str, Any]],
    quote_meta: dict[str, Any],
    prediction_date: str,
) -> tuple[bool, str]:
    now = datetime.now()
    if int(limit) != 0:
        return False, "preview_only_limit"
    if not is_trading_day(now.date()):
        return False, "not_trading_day"
    if (now.hour, now.minute) < (_LOCK_HOUR, _LOCK_MINUTE):
        return False, "waiting_for_1450"
    if str(quote_meta.get("source") or "") != "tencent.qt":
        return False, f"live_source_not_locked:{quote_meta.get('source') or 'unknown'}"
    current_quotes = _current_session_quote_count(quote_pool, prediction_date)
    required = max(100, int(max(1, len(quote_pool)) * 0.5))
    if current_quotes < required:
        return False, f"quote_date_not_current:{current_quotes}/{required}"
    return True, "ready_to_lock"


def _current_session_quote_count(quote_pool: dict[str, dict[str, Any]], prediction_date: str) -> int:
    return sum(1 for quote in quote_pool.values() if str(quote.get("date") or "")[:10] == prediction_date)


def _fetch_live_quote_pool(codes: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    unique_codes = sorted({str(code).zfill(6)[-6:] for code in codes if str(code).strip()})
    quote_pool = _fetch_tencent_quote_pool(unique_codes)
    min_usable = max(20, int(len(unique_codes) * 0.5))
    if len(quote_pool) >= min_usable:
        return quote_pool, {"source": "tencent.qt", "mode": "tencent_batch", "requested": len(unique_codes)}

    fallback = _fetch_sina_quote_pool(unique_codes)
    if fallback:
        return fallback, {"source": "sina.snapshot", "mode": "sina_snapshot", "requested": len(unique_codes)}

    return quote_pool, {"source": "tencent.qt", "mode": "tencent_batch_partial", "requested": len(unique_codes)}


def _fetch_tencent_quote_pool(codes: list[str], chunk_size: int = 420) -> dict[str, dict[str, Any]]:
    chunks = [codes[idx : idx + chunk_size] for idx in range(0, len(codes), chunk_size)]
    if not chunks:
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=min(12, len(chunks))) as executor:
        futures = {executor.submit(_fetch_tencent_quote_chunk, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            try:
                rows.update(future.result())
            except Exception:
                continue
    return rows


def _fetch_tencent_quote_chunk(codes: list[str]) -> dict[str, dict[str, Any]]:
    symbols = ",".join(_tencent_symbol(code) for code in codes)
    if not symbols:
        return {}
    session = requests.Session()
    session.trust_env = False
    response = session.get(f"http://qt.gtimg.cn/q={symbols}", timeout=6, proxies={})
    response.raise_for_status()
    text = response.content.decode("gbk", errors="ignore")
    rows: dict[str, dict[str, Any]] = {}
    for match in re.finditer(r'v_([a-z]{2}\d{6})="([^"]*)"', text):
        fields = match.group(2).split("~")
        if len(fields) < 6:
            continue
        code = str(_field(fields, 2) or match.group(1)[-6:]).zfill(6)[-6:]
        price = _safe_float(_field(fields, 3), 0.0) or 0.0
        open_price = _safe_float(_field(fields, 5), 0.0) or 0.0
        pre_close = _safe_float(_field(fields, 4), 0.0) or 0.0
        if price <= 0 and open_price > 0:
            price = open_price
        rows[code] = {
            "code": code,
            "symbol": match.group(1),
            "name": str(_field(fields, 1) or "").strip(),
            "price": price,
            "current_price": price,
            "auction_price": price,
            "pre_close": pre_close,
            "open": open_price,
            "high": _safe_float(_field(fields, 33), 0.0) or 0.0,
            "low": _safe_float(_field(fields, 34), 0.0) or 0.0,
            "volume": _safe_float(_field(fields, 6), 0.0) or 0.0,
            "amount": _safe_float(_field(fields, 37), 0.0) or 0.0,
            "change_pct": _safe_float(_field(fields, 32), None),
            "date": _format_tencent_quote_time(str(_field(fields, 30) or ""))[:10],
            "time": _format_tencent_quote_time(str(_field(fields, 30) or ""))[11:],
            "source": "tencent.qt",
        }
    return rows


def _fetch_sina_quote_pool(codes: list[str]) -> dict[str, dict[str, Any]]:
    try:
        snapshot = fetch_sina_snapshot(timeout=6)
    except Exception:
        return {}
    if snapshot.empty or "code" not in snapshot.columns:
        return {}
    wanted = set(codes)
    rows: dict[str, dict[str, Any]] = {}
    for _, item in snapshot.iterrows():
        code = "".join(ch for ch in str(item.get("code") or "") if ch.isdigit())[-6:]
        if code not in wanted:
            continue
        price = _safe_float(item.get("close"), 0.0) or 0.0
        rows[code] = {
            "code": code,
            "name": str(item.get("name") or "").strip(),
            "price": price,
            "current_price": price,
            "auction_price": price,
            "pre_close": _safe_float(item.get("pre_close"), 0.0) or 0.0,
            "open": _safe_float(item.get("open"), 0.0) or 0.0,
            "high": _safe_float(item.get("high"), 0.0) or 0.0,
            "low": _safe_float(item.get("low"), 0.0) or 0.0,
            "volume": _safe_float(item.get("volume"), 0.0) or 0.0,
            "amount": _safe_float(item.get("amount"), 0.0) or 0.0,
            "change_pct": _safe_float(item.get("change_pct"), None),
            "date": str(item.get("date") or datetime.now().date().isoformat()),
            "time": datetime.now().strftime("%H:%M:%S"),
            "source": "sina.snapshot",
        }
    return rows


def _build_candidate_tasks(files: list[Path], quote_pool: dict[str, dict[str, Any]]) -> tuple[list[tuple[str, dict[str, Any]]], dict[str, int]]:
    tasks: list[tuple[str, dict[str, Any]]] = []
    stats = {
        "excluded_board": 0,
        "missing_quote": 0,
        "st": 0,
        "suspended": 0,
        "limit_down": 0,
        "accepted": 0,
    }
    for path in files:
        code = _code_from_path(path)
        if _is_excluded_board(code):
            stats["excluded_board"] += 1
            continue
        quote = quote_pool.get(code)
        if not quote:
            stats["missing_quote"] += 1
            continue
        name = str(quote.get("name") or "").strip()
        current = _safe_float(quote.get("current_price", quote.get("price")), 0.0) or 0.0
        pre_close = _safe_float(quote.get("pre_close"), 0.0) or 0.0
        volume = _safe_float(quote.get("volume"), 0.0) or 0.0
        if _is_st_stock(name):
            stats["st"] += 1
            continue
        if current <= 0 or pre_close <= 0 or volume <= 0:
            stats["suspended"] += 1
            continue
        if _is_limit_down(code, name, current, pre_close, quote.get("change_pct")):
            stats["limit_down"] += 1
            continue
        tasks.append((str(path), quote))
        stats["accepted"] += 1
    return tasks, stats


def _filter_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if not _is_excluded_board(str(row.get("code") or ""))
        and not _is_st_stock(str(row.get("name") or ""))
    ]


def _score_candidate_tasks(
    tasks: list[tuple[str, dict[str, Any]]],
    feature_cols: list[str],
    max_workers: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    if not tasks:
        return rows, errors
    worker_count = min(max(1, int(max_workers)), len(tasks))
    with ProcessPoolExecutor(
        max_workers=worker_count,
        initializer=_init_scan_worker,
        initargs=(str(GLOBAL_MODEL_PATH), feature_cols),
    ) as executor:
        futures = {executor.submit(_score_candidate_worker, task): task for task in tasks}
        for future in as_completed(futures):
            path_str, quote = futures[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                code = quote.get("code") or Path(path_str).name
                errors.append(f"{code}: {exc}")
    return rows, errors


def _init_scan_worker(model_path: str, feature_cols: list[str]) -> None:
    global _WORKER_MODEL, _WORKER_FEATURE_COLS
    _WORKER_FEATURE_COLS = list(feature_cols)
    _WORKER_MODEL = load_daily_model(Path(model_path))
    try:
        _WORKER_MODEL.set_params(n_jobs=1)
    except Exception:
        pass


def _score_candidate_worker(task: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    path_str, quote = task
    if _WORKER_MODEL is None:
        _init_scan_worker(str(GLOBAL_MODEL_PATH), _WORKER_FEATURE_COLS or _load_model_meta().get("feature_columns") or [])
    path = Path(path_str)
    raw = pd.read_parquet(path)
    stitched = _stitch_live_daily_row(raw, path, quote=quote)
    factors = generate_daily_factors(stitched)
    if factors.empty:
        raise ValueError("因子表为空")
    row = factors.iloc[-1].copy()
    row["source_path"] = str(path)
    row["file_symbol"] = path.stem.replace("_daily", "")[-6:]
    aligned = _align_features(pd.DataFrame([row]), _WORKER_FEATURE_COLS)
    probability = float(_WORKER_MODEL.predict_proba(aligned)[:, 1][0])
    row["probability"] = probability
    return _format_signal_row(row)


def _load_latest_factor_rows(files: list[Path], max_workers: int) -> tuple[pd.DataFrame, list[str]]:
    rows: list[pd.Series] = []
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_latest_factor_row, path): path for path in files}
        for future in as_completed(futures):
            path = futures[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                errors.append(f"{path.name}: {exc}")
    if not rows:
        return pd.DataFrame(), errors
    return pd.DataFrame(rows).reset_index(drop=True), errors


def _latest_factor_row(path: Path) -> pd.Series:
    raw = pd.read_parquet(path)
    stitched = _stitch_live_daily_row(raw, path)
    factors = generate_daily_factors(stitched)
    if factors.empty:
        raise ValueError("因子表为空")
    row = factors.iloc[-1].copy()
    row["source_path"] = str(path)
    row["file_symbol"] = path.stem.replace("_daily", "")[-6:]
    return row


def _stitch_live_daily_row(
    raw: pd.DataFrame,
    path: Path,
    quote: Optional[dict[str, Any]] = None,
    history_days: int = 80,
) -> pd.DataFrame:
    if raw.empty:
        raise ValueError("本地日线为空")
    code = _code_from_frame_or_path(raw, path)
    quote = quote or fetch_realtime_quote(code)

    history = raw.copy()
    if "date" in history.columns:
        history["_dt"] = pd.to_datetime(history["date"].astype(str), errors="coerce")
    elif "datetime" in history.columns:
        history["_dt"] = pd.to_datetime(history["datetime"], errors="coerce")
    else:
        raise ValueError("本地日线缺少 date/datetime")
    history = history.dropna(subset=["_dt"]).sort_values("_dt").tail(history_days)
    if history.empty:
        raise ValueError("本地日线清洗后为空")

    last = history.iloc[-1].copy()
    today = datetime.now().date()
    pre_close = _safe_float(quote.get("pre_close"), _safe_float(last.get("close"), 0.0)) or 0.0
    current = _safe_float(quote.get("current_price"), _safe_float(quote.get("price"), 0.0)) or 0.0
    open_price = _safe_float(quote.get("open"), 0.0) or current or pre_close
    high = _safe_float(quote.get("high"), 0.0) or max(open_price, current, pre_close)
    low = _safe_float(quote.get("low"), 0.0) or min(open_price, current, pre_close)
    volume = _safe_float(quote.get("volume"), 0.0) or 0.0
    amount = _safe_float(quote.get("amount"), 0.0) or 0.0
    pct_chg = (current / pre_close - 1) * 100 if pre_close > 0 and current > 0 else _safe_float(last.get("pctChg"), 0.0)

    live = last.copy()
    live["date"] = today.strftime("%Y%m%d")
    live["datetime"] = pd.Timestamp(today)
    live["open"] = open_price
    live["high"] = max(high, open_price, current)
    live["low"] = min(low, open_price, current)
    live["close"] = current or open_price or pre_close
    live["volume"] = volume
    live["amount"] = amount
    live["pre_close"] = pre_close
    live["pctChg"] = pct_chg
    live["code"] = code
    live["symbol"] = code
    live["name"] = quote.get("name") or last.get("name") or code
    live["live_source"] = quote.get("source", "tencent.qt")
    live["live_quote_time"] = f"{quote.get('date', today.isoformat())} {quote.get('time', '')}".strip()

    history = history.drop(columns=["_dt"], errors="ignore")
    history_dates = pd.to_datetime(history.get("date", history.get("datetime")).astype(str), errors="coerce").dt.date
    history = history[history_dates != today].copy()
    return pd.concat([history, pd.DataFrame([live.drop(labels=["_dt"], errors="ignore")])], ignore_index=True)


def _format_signal_row(row: pd.Series) -> dict[str, Any]:
    code = str(row.get("code") or row.get("file_symbol") or "")[-6:]
    name = str(row.get("name") or "").strip()
    if not name or name == "None":
        name = code
    pct_chg = _safe_float(row.get("pctChg"))
    if pct_chg is None:
        pct_chg = (_safe_float(row.get("close"), 0.0) / max(_safe_float(row.get("open"), 0.0), 1e-9) - 1) * 100
    pressure = _safe_float(row.get("close_location_value"), 0.0)
    probability = _safe_float(row.get("probability"), 0.0)
    theme_name, theme_source = _theme_name_for_code(code)
    theme_pct_chg_3 = _safe_float(row.get("theme_pct_chg_3"), None)
    return {
        "code": code,
        "name": name,
        "date": pd.Timestamp(row.get("datetime")).date().isoformat(),
        "live_quote_time": str(row.get("live_quote_time") or ""),
        "live_source": str(row.get("live_source") or ""),
        "probability": round(float(probability), 6),
        "probability_pct": round(float(probability) * 100, 2),
        "pct_chg": round(float(pct_chg), 2),
        "pressure_factor": round(float(pressure), 4),
        "theme_name": theme_name,
        "theme_source": theme_source,
        "theme_pct_chg_3": round(float(theme_pct_chg_3), 6) if theme_pct_chg_3 is not None else None,
        "close": round(_safe_float(row.get("close"), 0.0), 4),
        "strategy_type": "全局日线XGB",
        "signal": "极品高置信" if probability >= 0.9 else "高置信狙击",
        "is_elite": bool(probability >= 0.9),
    }


def _align_features(frame: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for col in feature_cols:
        if col not in out.columns:
            out[col] = np.nan if col in THEME_FACTOR_COLUMNS else 0.0
    aligned = out[feature_cols].apply(pd.to_numeric, errors="coerce")
    aligned = aligned.replace([np.inf, -np.inf], np.nan)
    fillable_cols = [col for col in aligned.columns if col not in THEME_FACTOR_COLUMNS]
    if fillable_cols:
        aligned[fillable_cols] = aligned[fillable_cols].ffill().fillna(0.0)
    return aligned.astype("float32", copy=False)


def _ensure_theme_contract(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    code = _normalize_code(out.get("code") or out.get("file_symbol"))
    if code:
        theme_name, theme_source = _theme_name_for_code(code)
        out["theme_name"] = out.get("theme_name") or theme_name
        out["theme_source"] = out.get("theme_source") or theme_source
    else:
        out.setdefault("theme_name", "-")
        out.setdefault("theme_source", "")
    pct3 = _safe_float(out.get("theme_pct_chg_3"), None)
    if pct3 is None and code:
        pct3 = _latest_theme_pct_chg_3(code)
    out["theme_pct_chg_3"] = round(float(pct3), 6) if pct3 is not None else None
    return out


def ensure_theme_contract(row: dict[str, Any]) -> dict[str, Any]:
    return _ensure_theme_contract(row)


def _theme_name_for_code(code: str) -> tuple[str, str]:
    clean = _normalize_code(code)
    concept_code = _stock_concept_map().get(clean, "")
    if concept_code:
        return _concept_name_map().get(concept_code, concept_code), "concept"
    sector_name = _stock_sector_map().get(clean, "")
    if sector_name:
        return sector_name, "sector"
    return "-", ""


def _stock_concept_map() -> dict[str, str]:
    global _STOCK_CONCEPT_MAP_CACHE
    if _STOCK_CONCEPT_MAP_CACHE is None:
        _STOCK_CONCEPT_MAP_CACHE = get_stock_concept_map(refresh=False)
    return _STOCK_CONCEPT_MAP_CACHE


def _stock_sector_map() -> dict[str, str]:
    global _STOCK_SECTOR_MAP_CACHE
    if _STOCK_SECTOR_MAP_CACHE is None:
        _STOCK_SECTOR_MAP_CACHE = get_stock_sector_map(refresh=False)
    return _STOCK_SECTOR_MAP_CACHE


def _concept_name_map() -> dict[str, str]:
    global _CONCEPT_NAME_MAP_CACHE
    if _CONCEPT_NAME_MAP_CACHE is not None:
        return _CONCEPT_NAME_MAP_CACHE
    mapping: dict[str, str] = {}
    try:
        if CONCEPT_INDEX_PATH.exists():
            index = pd.read_parquet(CONCEPT_INDEX_PATH, columns=["concept_code", "concept_name"])
            mapping.update(
                {
                    str(row.get("concept_code") or ""): str(row.get("concept_name") or "")
                    for row in index.to_dict("records")
                    if row.get("concept_code") and row.get("concept_name")
                }
            )
        if CONCEPT_CATALOG_PATH.exists():
            catalog = json.loads(CONCEPT_CATALOG_PATH.read_text(encoding="utf-8"))
            for item in catalog if isinstance(catalog, list) else []:
                code = str(item.get("concept_code") or "")
                name = str(item.get("concept_name") or "")
                if code and name:
                    mapping.setdefault(code, name)
    except Exception:
        mapping = {}
    _CONCEPT_NAME_MAP_CACHE = mapping
    return mapping


def _latest_theme_pct_chg_3(code: str) -> Optional[float]:
    clean = _normalize_code(code)
    if not clean:
        return None
    try:
        data_dir = discover_daily_data_dir()
        candidates = [data_dir / f"{clean}_daily.parquet"]
        candidates.extend(sorted(data_dir.glob(f"*{clean}*_daily.parquet")))
        path = next((item for item in candidates if item.exists()), None)
        if path is None:
            return None
        raw = pd.read_parquet(path).tail(120)
        factors = generate_daily_factors(raw)
        if factors.empty:
            return None
        return _safe_float(factors.iloc[-1].get("theme_pct_chg_3"), None)
    except Exception:
        return None


def _normalize_code(value: Any) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    return digits[-6:] if len(digits) >= 6 else ""


def _load_model_meta() -> dict[str, Any]:
    if not GLOBAL_META_PATH.exists():
        return {}
    try:
        return json.loads(GLOBAL_META_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _code_from_frame_or_path(raw: pd.DataFrame, path: Path) -> str:
    candidates = []
    for col in ["code", "symbol"]:
        if col in raw.columns and not raw[col].dropna().empty:
            candidates.append(str(raw[col].dropna().iloc[-1]))
    candidates.append(path.stem.replace("_daily", ""))
    for value in candidates:
        digits = "".join(ch for ch in str(value) if ch.isdigit())
        if len(digits) >= 6:
            return digits[-6:]
    raise ValueError(f"无法识别股票代码：{path}")


def _code_from_path(path: Path) -> str:
    digits = "".join(ch for ch in path.stem.replace("_daily", "") if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    raise ValueError(f"无法从文件名识别股票代码：{path}")


def _tencent_symbol(code: str) -> str:
    clean = str(code).zfill(6)[-6:]
    if clean.startswith(("5", "6", "9")):
        return f"sh{clean}"
    if clean.startswith(("4", "8")):
        return f"bj{clean}"
    return f"sz{clean}"


def _field(fields: list[str], index: int) -> str:
    return fields[index] if len(fields) > index else ""


def _format_tencent_quote_time(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) >= 14:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]} {digits[8:10]}:{digits[10:12]}:{digits[12:14]}"
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _is_st_stock(name: str) -> bool:
    upper = str(name or "").upper()
    return "ST" in upper or "退" in upper


def _is_excluded_board(code: str) -> bool:
    digits = "".join(ch for ch in str(code or "") if ch.isdigit())
    if len(digits) < 6:
        return True
    clean = digits[-6:]
    if clean == "000000":
        return True
    return clean.startswith(_EXCLUDED_BOARD_PREFIXES) or not clean.startswith(("00", "60"))


def _send_v3_sniper_pushplus(payload: dict[str, Any]) -> dict[str, Any]:
    rows = _filter_display_rows(list(payload.get("rows") or []))[: int(payload.get("top_k") or _TOP_K)]
    if not rows:
        return {"status": "skipped_empty", "reason": "V4 锁榜后主板过滤无可推送标的"}
    try:
        from quant_core.execution.pushplus_tasks import send_pushplus

        title = f"{_LOCK_LABEL} V4全局动量狙击 Top {len(rows)}"
        lines = "\n".join(_v3_push_line(index, row) for index, row in enumerate(rows, start=1))
        live = payload.get("live_data") or {}
        stats = payload.get("filter_stats") or {}
        content = f"""## {_LOCK_LABEL} V4 Theme Alpha 全局动量狙击

预测日期: {payload.get('prediction_date') or '-'}
锁定时间: {payload.get('locked_at') or '-'}
行情源: {live.get('source') or '-'}
扫描耗时: {_fmt_number(payload.get('elapsed_seconds'))} 秒

{lines}

过滤规则: 已排除创业板、科创板、北交所、ST/退市；仅保留沪深主板候选。
候选统计: 全量 {payload.get('universe_count', '-')} / 板块过滤 {stats.get('excluded_board', 0)} / ST过滤 {stats.get('st', 0)} / 入模 {payload.get('evaluated_count', '-')}
"""
        result = send_pushplus(title, content)
        print(json.dumps({"task": "v3_sniper_pushplus", "status": result.get("status"), "count": len(rows)}, ensure_ascii=False))
        return result
    except Exception as exc:
        print(f"[V3 Sniper][PushPlus][ERROR] {exc}")
        return {"status": "failed", "error": str(exc)}


def _v3_push_line(index: int, row: dict[str, Any]) -> str:
    return (
        f"{index}. {row.get('name') or '-'}({row.get('code') or '-'}) "
        f"概率 {_fmt_number(row.get('probability_pct'))}% / "
        f"{_LOCK_LABEL}价 {_fmt_number(row.get('close'))} / "
        f"实时涨幅 {_fmt_signed_pct(row.get('pct_chg'))} / "
        f"压差 {float(_safe_float(row.get('pressure_factor'), 0.0) or 0.0):.4f} / "
        f"行情 {row.get('live_quote_time') or '-'}"
    )


def _fmt_number(value: Any) -> str:
    num = _safe_float(value, None)
    if num is None:
        return "-"
    return f"{float(num):.2f}"


def _fmt_signed_pct(value: Any) -> str:
    num = _safe_float(value, None)
    if num is None:
        return "-"
    prefix = "+" if float(num) > 0 else ""
    return f"{prefix}{float(num):.2f}%"


def _is_limit_down(
    code: str,
    name: str,
    current: float,
    pre_close: float,
    change_pct_value: Any,
) -> bool:
    change_pct = _safe_float(change_pct_value, None)
    limit_pct = _limit_down_pct(code, name)
    if change_pct is not None and change_pct <= limit_pct:
        return True
    if pre_close <= 0 or current <= 0:
        return False
    limit_price = pre_close * (1 + limit_pct / 100)
    return current <= limit_price * 1.002


def _limit_down_pct(code: str, name: str) -> float:
    if _is_st_stock(name):
        return -4.8
    clean = str(code).zfill(6)[-6:]
    if clean.startswith(("300", "301", "688", "689")):
        return -19.5
    if clean.startswith(("4", "8")):
        return -29.0
    return -9.8


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(num) or not np.isfinite(num):
        return default
    return num
