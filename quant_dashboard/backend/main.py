from __future__ import annotations

import ast
import json
import math
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pydantic import BaseModel

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from quant_core.config import (
    MAIN_WAVE_MIN_SCORE,
    MIN_KLINE_DIR,
    OLLAMA_API,
    OLLAMA_MODEL,
    PAUSED_STRATEGY_TYPES,
    REVERSAL_MIN_SCORE,
    check_push_config,
)
from quant_core.data_pipeline.fetch_minute_data import save_stock_min_data
from quant_core.engine.backtest import top_pick_open_backtest
from quant_core.engine.intraday_exit import run_intraday_exit_backtest
from quant_core.explainability import explain_models, explain_pick
from quant_core.cache_utils import read_json_cache, write_json_cache
from quant_core.daily_pick import list_daily_pick_results, update_pending_open_results
from quant_core.failure_analysis import analyze_prediction_failures
from quant_core.data_pipeline.market_sync import latest_sync, run_market_close_sync, sync_history
from quant_core.engine.predictor import attach_pick_theme_fields, scan_market
from quant_core.execution.mac_sniper import aim_and_fire, read_trade_panel_snapshot
from quant_core.execution.position_sizer import (
    InsufficientFundsError,
    build_broker_confirmed_trade_record,
    calculate_order,
    set_available_cash,
    shadow_account_summary,
    sync_shadow_account_from_broker,
)
from quant_core.sniper_status import get_sniper_status, read_sniper_status, set_sniper_status
from quant_core.storage import (
    database_overview,
    import_parquet_files,
    latest_prediction_snapshot,
    list_validation_reports,
    recent_daily_rows,
)
from quant_core.strategies.labs.strategy_lab import run_strategy_lab
from quant_core.up_reason_analysis import analyze_next_day_up_reasons
from quant_core.validation import validate_one_code, validate_repository
from routers.v3_dashboard import router as v3_dashboard_router
from routers.v3_sniper import ensure_theme_contract, router as v3_sniper_router, v4_router as v4_sniper_router


app = FastAPI(title="离岸量化工作站 API", version="4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173", "http://127.0.0.1:5174", "http://localhost:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(v3_dashboard_router)
app.include_router(v3_sniper_router)
app.include_router(v4_sniper_router)


class AnalyzeRequest(BaseModel):
    code: str
    name: str


class ExplainPickRequest(BaseModel):
    code: str
    date: Optional[str] = None
    selection_date: Optional[str] = None
    strategy_type: Optional[str] = None
    source: Optional[str] = None
    months: int = 12
    row: Optional[dict[str, Any]] = None


class SniperToggleRequest(BaseModel):
    enabled: bool


class SniperTestFireRequest(BaseModel):
    code: str
    action: str = "buy"


class ShadowCashRequest(BaseModel):
    available_cash: float


class ShadowTestOrderRequest(BaseModel):
    code: str
    name: str = ""
    current_price: float
    position_pct: float
    execute: bool = False
    available_cash: Optional[float] = None


_scheduler_started = False
_last_pick_date = ""
_last_open_update_date = ""
IGNORE_BROKER_CASH_FOR_TEST_ORDER = False
SENTINEL_5M_DEFAULT_START_DATE = "2025-10-01"
SENTINEL_5M_DEFAULT_END_DATE = "2026-01-28"
SENTINEL_5M_SCRIPT = BASE_DIR / "scripts" / "backtest" / "simulate_sentinel_5m.py"
SENTINEL_5M_CACHE_DIR = BASE_DIR / "data" / "strategy_cache"
SENTINEL_5M_LATEST_CACHE = SENTINEL_5M_CACHE_DIR / "sentinel_5m_backtest_latest.json"
SWING_STRATEGY_TYPES = {"中线超跌反转", "右侧主升浪", "全局动量狙击"}
LEDGER_STRATEGY_PRIORITY = {
    "全局动量狙击": 4,
    "右侧主升浪": 3,
    "中线超跌反转": 2,
    "尾盘突破": 1,
    "首阴低吸": 0,
}


@app.on_event("startup")
def start_daily_pick_scheduler() -> None:
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True
    thread = threading.Thread(target=_daily_pick_scheduler_loop, daemon=True)
    thread.start()


@app.get("/health")
def health() -> dict[str, Any]:
    pushplus = check_push_config(print_warning=False)
    return {
        "ok": bool(pushplus.get("ok")),
        "service": "quant_dashboard",
        "pushplus": pushplus,
    }


@app.get("/api/sniper/status")
def sniper_status() -> dict[str, Any]:
    return read_sniper_status()


@app.post("/api/sniper/toggle")
def sniper_toggle(payload: SniperToggleRequest) -> dict[str, Any]:
    return set_sniper_status(payload.enabled)


@app.post("/api/sniper/test_fire")
def sniper_test_fire(payload: SniperTestFireRequest) -> dict[str, Any]:
    if not get_sniper_status():
        raise HTTPException(status_code=403, detail="请先解锁物理外挂")

    clean_code = _normalize_stock_code(payload.code)
    action = (payload.action or "buy").strip().lower()
    if action not in {"buy", "sell"}:
        raise HTTPException(status_code=422, detail=f"非法试射动作：{payload.action}")

    result = aim_and_fire(clean_code)
    if result.get("status") != "fired":
        raise HTTPException(status_code=502, detail=result)
    return {"status": "sent", "code": clean_code, "action": action, "mac_sniper": result}


@app.get("/api/shadow-account")
def shadow_account() -> dict[str, Any]:
    try:
        return shadow_account_summary()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/shadow-account/cash")
def shadow_account_cash(payload: ShadowCashRequest) -> dict[str, Any]:
    try:
        return set_available_cash(payload.available_cash)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/shadow-account/sync-broker")
def shadow_account_sync_broker() -> dict[str, Any]:
    try:
        broker_snapshot = read_trade_panel_snapshot()
        shadow_account_data = sync_shadow_account_from_broker(broker_snapshot)
        warning = shadow_account_data.get("broker_sync_warning")
        return {
            "status": "partial" if warning else "synced",
            "warning": warning,
            "broker_snapshot": broker_snapshot,
            "shadow_account": shadow_account_data,
        }
    except subprocess.CalledProcessError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "message": "同花顺交易窗口读取失败",
                "stdout": (exc.stdout or "").strip(),
                "stderr": (exc.stderr or "").strip(),
            },
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/api/shadow-account/test_order")
def shadow_account_test_order(payload: ShadowTestOrderRequest) -> dict[str, Any]:
    clean_code = _normalize_stock_code(payload.code)
    if payload.execute:
        if not get_sniper_status():
            raise HTTPException(status_code=403, detail="请先解锁物理外挂")

        try:
            broker_snapshot = read_trade_panel_snapshot()
            order_form = broker_snapshot.get("order_form") or {}
            broker_price = _broker_snapshot_price(broker_snapshot, clean_code) or payload.current_price
            broker_name = _broker_snapshot_name(broker_snapshot, clean_code) or order_form.get("name") or payload.name
            if not IGNORE_BROKER_CASH_FOR_TEST_ORDER:
                sync_shadow_account_from_broker(broker_snapshot)
            sizing = calculate_order(
                clean_code,
                broker_price,
                payload.position_pct,
                available_cash_override=payload.available_cash if IGNORE_BROKER_CASH_FOR_TEST_ORDER else None,
            )
        except InsufficientFundsError as exc:
            raise HTTPException(
                status_code=409,
                detail=f"按资金池金额重算后资金不足：{exc}",
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except subprocess.CalledProcessError as exc:
            raise HTTPException(
                status_code=502,
                detail={
                    "message": "同花顺资金/价格同步失败，未触发下单",
                    "stdout": (exc.stdout or "").strip(),
                    "stderr": (exc.stderr or "").strip(),
                },
            ) from exc

        result = aim_and_fire(clean_code, shares=int(sizing["shares"]), limit_price=broker_price)
        if result.get("status") != "broker_confirmed":
            if _is_handled_broker_alert(result):
                alert = result.get("broker_alert") or {}
                return {
                    "status": "broker_alert",
                    "success_kind": "broker_alert_recorded",
                    "message": f"券商弹窗已记录并自动确认：{alert.get('message') or '无弹窗文本'}",
                    "order": sizing,
                    "mac_sniper": result,
                    "shadow_account": shadow_account_summary(),
                    "trade_record": None,
                }
            if _is_off_hours_submitted_unfilled(result):
                return {
                    "status": "submitted_unverified",
                    "success_kind": "off_hours_submitted_unfilled",
                    "message": "休市试射链路已完整填单并提交；持仓未增加，本地成交流水未写入。",
                    "order": sizing,
                    "mac_sniper": result,
                    "shadow_account": shadow_account_summary(),
                    "trade_record": None,
                }
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "买入提交后未在同花顺持仓表确认成交，本地记录未写入",
                    "order": sizing,
                    "shadow_account": shadow_account_summary(),
                    "broker_cash_guard": "ignored_for_test_order" if IGNORE_BROKER_CASH_FOR_TEST_ORDER else "enabled",
                    "mac_sniper": result,
                },
            )

        try:
            trade_record = build_broker_confirmed_trade_record(
                clean_code,
                broker_name,
                broker_price,
                int(sizing["shares"]),
                payload.position_pct,
                "dashboard.shadow_account.test_order",
                mac_sniper_result=result,
                metadata={
                    "name": broker_name,
                    "source": "dashboard.shadow_account.test_order",
                    "execute": True,
                    "broker_synced_before_order": True,
                },
            )
            synced = sync_shadow_account_from_broker(result.get("after_snapshot") or read_trade_panel_snapshot(), trade_record=trade_record)
        except InsufficientFundsError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        return {
            "status": "sent",
            "order": sizing,
            "mac_sniper": result,
            "shadow_account": synced,
            "trade_record": trade_record,
        }

    try:
            sizing = calculate_order(
                clean_code,
                payload.current_price,
                payload.position_pct,
                available_cash_override=payload.available_cash,
            )
    except InsufficientFundsError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if not payload.execute:
        return {
            "status": "preview",
            "order": sizing,
            "shadow_account": shadow_account_summary(),
        }


def _broker_snapshot_price(snapshot: dict[str, Any], code: str) -> Optional[float]:
    order_form = snapshot.get("order_form") or {}
    order_code = _normalize_stock_code(order_form.get("code") or "") if order_form.get("code") else ""
    for key in ("current_price", "limit_price"):
        price = _positive_float(order_form.get(key))
        if price and (not order_code or order_code == code):
            return price
    for position in snapshot.get("positions") or []:
        if _normalize_stock_code(position.get("code") or "") != code:
            continue
        for key in ("market_price", "cost_price"):
            price = _positive_float(position.get(key))
            if price:
                return price
    return None


def _broker_snapshot_name(snapshot: dict[str, Any], code: str) -> str:
    for position in snapshot.get("positions") or []:
        if _normalize_stock_code(position.get("code") or "") == code:
            return str(position.get("name") or "").strip()
    return ""


def _positive_float(value: Any) -> Optional[float]:
    try:
        number = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _is_off_hours_submitted_unfilled(result: dict[str, Any]) -> bool:
    if result.get("status") != "submitted_unverified":
        return False
    if result.get("stderr"):
        return False
    verification = result.get("broker_verification") or {}
    return verification.get("reason") == "position_quantity_not_increased"


def _is_handled_broker_alert(result: dict[str, Any]) -> bool:
    if result.get("status") != "broker_alert":
        return False
    alert = result.get("broker_alert") or {}
    return bool(alert.get("present") and alert.get("dismissed"))


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "ok": True,
        "service": "离岸量化工作站 API",
        "frontend": "http://127.0.0.1:5173",
        "docs": "http://127.0.0.1:8000/docs",
        "endpoints": {
            "health": "/health",
            "overview": "/api/overview",
            "radar_cached": "/api/radar/cache",
            "radar_scan": "/api/radar/scan?limit=10",
            "v4_sniper_scan": "/api/v4/sniper/scan_today?limit=3&cache_seconds=0",
            "validate": "/api/data/validate?sample=200",
            "history": "/api/data/history/600519?limit=120",
            "daily_picks": "/api/daily-picks",
            "market_sync_latest": "/api/data/market-sync/latest",
            "top_pick_backtest": "/api/backtest/top-pick-open?months=2",
            "strategy_lab": "/api/strategy/lab?months=2",
            "failure_analysis": "/api/strategy/failure-analysis?months=12",
            "up_reason_analysis": "/api/strategy/up-reason-analysis?months=12",
        },
    }


@app.get("/api/overview")
def overview() -> dict[str, Any]:
    return database_overview()


@app.get("/api/ollama/status")
def ollama_status() -> dict[str, Any]:
    base_url = OLLAMA_API.rsplit("/api/", 1)[0]
    try:
        session = requests.Session()
        session.trust_env = False
        version = session.get(f"{base_url}/api/version", timeout=3, proxies={"http": None, "https": None})
        version.raise_for_status()
        tags = session.get(f"{base_url}/api/tags", timeout=5, proxies={"http": None, "https": None})
        tags.raise_for_status()
        models = [item.get("name") for item in tags.json().get("models", [])]
        return {
            "ok": True,
            "api": OLLAMA_API,
            "model": OLLAMA_MODEL,
            "version": version.json().get("version"),
            "model_available": OLLAMA_MODEL in models,
            "models": models,
        }
    except Exception as exc:
        return {"ok": False, "api": OLLAMA_API, "model": OLLAMA_MODEL, "error": str(exc)}


@app.post("/api/data/sync")
def sync_data(limit: Optional[int] = Query(default=None, ge=1, le=10000), code: Optional[List[str]] = Query(default=None)) -> dict[str, Any]:
    return import_parquet_files(codes=code, limit=limit)


@app.get("/api/data/market-sync/latest")
def market_sync_latest() -> dict[str, Any]:
    return {"latest": latest_sync()}


@app.get("/api/data/minute-fetch/status")
def minute_fetch_status() -> dict[str, Any]:
    return {
        "created_at": _now_text(),
        "jq": _jq_minute_fetch_status(),
        "ashare": _ashare_minute_fetch_status(),
    }


@app.get("/api/data/market-sync/history")
def market_sync_history(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    return sync_history(limit=limit)


@app.post("/api/data/market-sync/run")
def market_sync_run() -> dict[str, Any]:
    try:
        return run_market_close_sync()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/data/validate")
def validate_data(
    sample: int = Query(default=200, ge=0, le=10000),
    source_check: bool = Query(default=False),
    code: Optional[str] = Query(default=None, pattern=r"^\d{6}$"),
) -> dict[str, Any]:
    if code:
        return validate_one_code(code, source_check=source_check)
    return validate_repository(sample=None if sample == 0 else sample, source_check=source_check)


@app.get("/api/data/reports")
def reports(limit: int = Query(default=20, ge=1, le=100)) -> list[dict[str, Any]]:
    return list_validation_reports(limit=limit)


@app.get("/api/data/history/{code}")
def history(code: str, limit: int = Query(default=120, ge=1, le=1000)) -> dict[str, Any]:
    clean_code = _normalize_stock_code(code)
    rows = recent_daily_rows(clean_code, limit=limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"数据库中没有 {clean_code} 的历史记录，请先运行数据同步")
    return {"code": clean_code, "count": len(rows), "latest_date": rows[-1].get("date") if rows else None, "rows": rows}


@app.get("/api/data/history_min/{code}")
def history_min(
    code: str,
    period: str = Query(default="5", pattern=r"^(1|5|15|30|60)$"),
    limit: int = Query(default=5000, ge=1, le=50000),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    clean_code = _normalize_stock_code(code)
    refresh_result = _refresh_minute_kline(clean_code, period) if refresh else None
    paths = _minute_kline_paths(clean_code, period)
    frames: list[pd.DataFrame] = []
    read_errors: list[str] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            frame = pd.read_parquet(path)
            frame["_local_path"] = str(path)
            frames.append(frame)
        except Exception as exc:
            read_errors.append(f"{path}: {exc}")
    if not frames:
        suffix = f"；刷新结果：{refresh_result}" if refresh_result else ""
        raise HTTPException(status_code=404, detail=f"没有找到 {clean_code} 的 {period} 分钟线文件{suffix}")

    df = pd.concat(frames, ignore_index=True, sort=False)

    required = ["datetime", "open", "high", "low", "close", "volume"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"分钟线文件缺少字段：{missing}")

    out = df.copy()
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    if "amount" not in out.columns and "money" in out.columns:
        out["amount"] = out["money"]
    if "money" not in out.columns and "amount" in out.columns:
        out["money"] = out["amount"]
    for col in ["open", "high", "low", "close", "volume", "money", "amount"]:
        if col not in out.columns:
            out[col] = None
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["datetime", "open", "high", "low", "close"])
    out = out.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last").tail(limit)
    rows = []
    for item in out[["datetime", "open", "high", "low", "close", "volume", "money", "amount"]].to_dict(orient="records"):
        rows.append(
            {
                "datetime": item["datetime"].strftime("%Y-%m-%d %H:%M:%S"),
                "open": _safe_number(item.get("open")),
                "high": _safe_number(item.get("high")),
                "low": _safe_number(item.get("low")),
                "close": _safe_number(item.get("close")),
                "volume": _safe_number(item.get("volume")),
                "money": _safe_number(item.get("money")),
                "amount": _safe_number(item.get("amount")),
            }
        )
    existing_paths = [str(path) for path in paths if path.exists()]
    source_counts = {}
    if "source" in df.columns:
        source_counts = {str(key): int(value) for key, value in df["source"].fillna("unknown").value_counts().to_dict().items()}
    return {
        "code": clean_code,
        "period": f"{period}m",
        "path": existing_paths[0] if existing_paths else "",
        "paths": existing_paths,
        "count": len(rows),
        "latest_datetime": rows[-1]["datetime"] if rows else None,
        "refresh": refresh_result,
        "read_errors": read_errors,
        "source_counts": source_counts,
        "rows": rows,
    }


@app.get("/api/radar/cache")
def radar_cache() -> dict[str, Any]:
    cached = latest_prediction_snapshot()
    if cached is None:
        return {"id": None, "created_at": "", "model_status": "no_cache", "rows": []}
    cached["rows"] = cached.get("rows", [])[:12]
    return _attach_theme_contract(cached)


@app.get("/api/radar/scan")
def radar_scan(limit: int = Query(default=12, ge=1, le=50)) -> dict[str, Any]:
    try:
        return _attach_theme_contract(scan_market(limit=limit, persist_snapshot=True, cache_prediction=True, async_persist=True))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/daily-picks")
def daily_picks(
    limit: int = Query(default=5000, ge=1, le=10000),
    shadow_only: bool = Query(default=False),
    view: str = Query(default="strategy_top1"),
) -> dict[str, Any]:
    result = list_daily_pick_results(limit=limit, shadow_only=shadow_only)
    source_rows = result.get("rows", [])
    result["source_raw_count"] = len(source_rows)
    result["rows"] = _filter_paused_strategy_rows(source_rows)
    result["rows"], exit_policy_meta = _attach_unified_exit_policy(result.get("rows", []))
    result["exit_policy"] = exit_policy_meta
    result["paused_strategy_types"] = list(PAUSED_STRATEGY_TYPES)
    result["paused_strategy_count"] = len(source_rows) - len(result.get("rows", []))
    if view in {"actionable", "strategy_top1"}:
        result["raw_count"] = len(result.get("rows", []))
        result["rows"] = _apply_current_strategy_rules(result.get("rows", []))
        result["current_rule_count"] = len(result.get("rows", []))
        result["rows"] = _collapse_actionable_daily_picks(result.get("rows", []), mode="strategy")
        result["view"] = "actionable_strategy_top1"
    elif view == "daily_top1":
        result["raw_count"] = len(result.get("rows", []))
        result["rows"] = _apply_current_strategy_rules(result.get("rows", []))
        result["current_rule_count"] = len(result.get("rows", []))
        result["rows"] = _collapse_actionable_daily_picks(result.get("rows", []), mode="day")
        result["view"] = "actionable_daily_top1"
    elif view == "all":
        result["raw_count"] = len(result.get("rows", []))
        result["view"] = "all_candidates"
    else:
        raise HTTPException(status_code=422, detail="view 只能是 daily_top1、strategy_top1、actionable 或 all")
    result = _attach_daily_pick_theme_contract(result)
    for row in result.get("rows", []):
        row.setdefault("t3_max_gain_pct", None)
    return result


@app.get("/api/explain/models")
def explain_model_cards() -> dict[str, Any]:
    try:
        return explain_models()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/explain/pick")
def explain_pick_api(payload: ExplainPickRequest) -> dict[str, Any]:
    try:
        return explain_pick(payload.dict())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _apply_current_strategy_rules(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        strategy = str(row.get("strategy_type") or "")
        if strategy in PAUSED_STRATEGY_TYPES:
            continue
        if strategy == "右侧主升浪" and _pick_score(row) < MAIN_WAVE_MIN_SCORE:
            continue
        if strategy == "中线超跌反转" and _pick_score(row) < REVERSAL_MIN_SCORE:
            continue
        out.append(row)
    return out


def _filter_paused_strategy_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not PAUSED_STRATEGY_TYPES:
        return list(rows)
    paused = set(PAUSED_STRATEGY_TYPES)
    return [row for row in rows if str(row.get("strategy_type") or "") not in paused]


def _attach_daily_pick_theme_contract(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    rows = []
    for row in out.get("rows") or []:
        item = dict(row)
        raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
        winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
        theme_name = str(
            item.get("theme_name")
            or item.get("core_theme")
            or winner.get("theme_name")
            or winner.get("core_theme")
            or "-"
        ).strip() or "-"
        theme_pct = item.get("theme_pct_chg_3")
        if theme_pct is None:
            theme_pct = item.get("theme_momentum_3d")
        if theme_pct is None:
            theme_pct = winner.get("theme_pct_chg_3")
        if theme_pct is None:
            theme_pct = winner.get("theme_momentum_3d")
        try:
            theme_pct = float(theme_pct)
        except (TypeError, ValueError):
            theme_pct = 0.0
        item["theme_name"] = theme_name
        item["core_theme"] = "" if theme_name == "-" else theme_name
        item["theme_pct_chg_3"] = theme_pct
        item["theme_momentum_3d"] = theme_pct
        item["theme_momentum"] = theme_pct
        rows.append(item)
    out["rows"] = rows
    return out


def _attach_unified_exit_policy(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sentinel_rows, source_paths, cache_error = _load_sentinel_5m_rows_for_ledger()
    by_pick_id: dict[int, dict[str, Any]] = {}
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in sentinel_rows:
        if not isinstance(row, dict):
            continue
        pick_id = _optional_int(row.get("pick_id"))
        if pick_id is not None:
            by_pick_id[pick_id] = row
        key = _exit_policy_match_key(row)
        if key:
            by_key[key] = row

    out: list[dict[str, Any]] = []
    matched = 0
    for row in rows:
        item = dict(row)
        pick_id = _optional_int(item.get("id"))
        sentinel = by_pick_id.get(pick_id) if pick_id is not None else None
        if sentinel is None:
            key = _exit_policy_match_key(item)
            sentinel = by_key.get(key) if key else None
        if sentinel is not None:
            item = _merge_sentinel_exit_policy(item, sentinel)
            matched += 1
        else:
            item = _attach_daily_pick_exit_policy(item)
        out.append(item)

    meta = {
        "mode": "v5_6_sentinel_5m_overlay",
        "source": ", ".join(str(path) for path in source_paths),
        "source_count": len(source_paths),
        "cache_available": bool(sentinel_rows),
        "cache_error": cache_error,
        "matched_count": matched,
        "unmatched_count": len(rows) - matched,
        "row_count": len(rows),
        "rule": "daily_picks 出票事实为底座；若 sentinel_5m 回测缓存覆盖同一 pick，则卖出策略、结算价和收益统一以后者为准。",
    }
    return out, meta


def _load_sentinel_5m_rows_for_ledger() -> tuple[list[dict[str, Any]], list[Path], str]:
    cache_paths = sorted(
        SENTINEL_5M_CACHE_DIR.glob("sentinel_5m_backtest_*.json"),
        key=lambda path: (path.name == SENTINEL_5M_LATEST_CACHE.name, path.stat().st_mtime),
    )
    if not cache_paths:
        return [], [], "sentinel_5m_backtest cache not found"

    rows_by_pick_id: dict[int, dict[str, Any]] = {}
    rows_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    used_paths: list[Path] = []
    errors: list[str] = []
    for cache_path in cache_paths:
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception as exc:
            errors.append(f"{cache_path.name}: {exc}")
            continue
        if not isinstance(payload, dict):
            errors.append(f"{cache_path.name}: payload is not an object")
            continue
        payload = _filter_paused_strategy_payload(payload)
        rows = payload.get("rows")
        if not isinstance(rows, list):
            errors.append(f"{cache_path.name}: rows missing")
            continue
        used_paths.append(cache_path)
        for row in rows:
            if not isinstance(row, dict):
                continue
            pick_id = _optional_int(row.get("pick_id"))
            if pick_id is not None:
                rows_by_pick_id[pick_id] = row
                continue
            key = _exit_policy_match_key(row)
            if key:
                rows_by_key[key] = row

    merged = list(rows_by_pick_id.values())
    known_keys = {_exit_policy_match_key(row) for row in merged}
    merged.extend(row for key, row in rows_by_key.items() if key not in known_keys)
    return merged, used_paths, "；".join(errors)


def _merge_sentinel_exit_policy(row: dict[str, Any], sentinel: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    raw = dict(item.get("raw") or {})
    winner = dict(raw.get("winner") or {})
    sentinel_raw = dict(sentinel.get("raw") or {})
    sentinel_winner = dict(sentinel_raw.get("winner") or {})
    coverage_status = str(sentinel.get("coverage_status") or "")
    sentinel_sell_strategy = sentinel.get("sell_strategy")
    sentinel_exit_policy = sentinel.get("exit_policy")
    if coverage_status == "missing_5m":
        sentinel_sell_strategy = "5m数据缺失：等待真实账本闭环"
        sentinel_exit_policy = sentinel_sell_strategy
    elif coverage_status == "open_or_incomplete":
        sentinel_sell_strategy = "5m数据未覆盖到结算点：等待真实账本闭环"
        sentinel_exit_policy = sentinel_sell_strategy
    sentinel_payload = {
        "source": "sentinel_5m_backtest",
        "pick_id": sentinel.get("pick_id"),
        "coverage_status": sentinel.get("coverage_status"),
        "exit_category": sentinel.get("exit_category"),
        "exit_reason": sentinel.get("exit_reason"),
        "close_reason": sentinel.get("close_reason"),
        "sell_strategy": sentinel_sell_strategy,
        "exit_policy": sentinel_exit_policy,
        "close_time": sentinel.get("close_time"),
        "close_price": sentinel.get("close_price"),
        "close_return_pct": sentinel.get("close_return_pct"),
        "highest_price": sentinel.get("highest_price"),
        "highest_gain_pct": sentinel.get("highest_gain_pct"),
        "bars_replayed": sentinel.get("bars_replayed"),
        "warning": sentinel.get("warning"),
    }
    raw["sentinel_5m"] = sentinel_payload
    raw["exit_policy_source"] = "sentinel_5m_backtest"

    for key in (
        "close_reason",
        "exit_category",
        "coverage_status",
        "warning",
        "bars_replayed",
    ):
        if sentinel.get(key) not in (None, ""):
            item[key] = sentinel.get(key)
            winner[key] = sentinel.get(key)
    if sentinel_sell_strategy not in (None, ""):
        item["sell_strategy"] = sentinel_sell_strategy
        winner["sell_strategy"] = sentinel_sell_strategy
    if sentinel_exit_policy not in (None, ""):
        item["exit_policy"] = sentinel_exit_policy
        winner["exit_policy"] = sentinel_exit_policy
    for key in (
        "close_price",
        "close_return_pct",
        "t3_settlement_price",
        "t3_settlement_return_pct",
        "t3_max_gain_pct",
        "highest_price",
        "highest_gain_pct",
    ):
        if sentinel.get(key) is not None:
            item[key] = sentinel.get(key)
            winner[key] = sentinel.get(key)
    close_date = sentinel.get("close_date") or str(sentinel.get("close_time") or "")[:10]
    if close_date:
        item["close_date"] = close_date
        winner["close_date"] = close_date
    if sentinel.get("close_time"):
        item["close_time"] = sentinel.get("close_time")
        winner["close_time"] = sentinel.get("close_time")

    item["is_closed"] = bool(sentinel.get("is_closed")) if sentinel.get("is_closed") is not None else item.get("is_closed")
    if sentinel.get("status"):
        item["status"] = sentinel.get("status")
    item["exit_policy_source"] = "sentinel_5m_backtest"
    item["evaluation_source"] = "sentinel_5m_backtest"
    item["ledger_exit_policy"] = item.get("sell_strategy") or item.get("exit_policy") or item.get("close_reason")
    winner["sell_strategy"] = item.get("sell_strategy")
    winner["exit_policy"] = item.get("exit_policy")
    raw["winner"] = winner
    item["raw"] = raw
    return item


def _attach_daily_pick_exit_policy(row: dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    raw = dict(item.get("raw") or {})
    winner = dict(raw.get("winner") or {})
    policy = (
        item.get("sell_strategy")
        or item.get("exit_policy")
        or winner.get("sell_strategy")
        or winner.get("exit_policy")
        or item.get("close_reason")
    )
    if not policy:
        strategy = str(item.get("strategy_type") or winner.get("strategy_type") or "")
        if item.get("is_closed"):
            policy = "真实账本闭环结算"
        elif strategy in SWING_STRATEGY_TYPES:
            policy = "真实账本T+3观察中"
        else:
            policy = "真实账本T+1待闭环"
    item["sell_strategy"] = policy
    item["exit_policy"] = policy
    item["ledger_exit_policy"] = policy
    item["exit_policy_source"] = "daily_picks"
    item["evaluation_source"] = "daily_picks"
    winner.setdefault("sell_strategy", policy)
    winner.setdefault("exit_policy", policy)
    raw["winner"] = winner
    raw["exit_policy_source"] = "daily_picks"
    item["raw"] = raw
    return item


def _exit_policy_match_key(row: dict[str, Any]) -> Optional[tuple[str, str, str]]:
    selection_date = str(row.get("selection_date") or row.get("date") or "")[:10]
    code = _normalize_code_key(row.get("code"))
    strategy = str(row.get("strategy_type") or "").strip()
    if not selection_date or not code or not strategy:
        return None
    return selection_date, code, strategy


def _normalize_code_key(value: Any) -> str:
    text = str(value or "").strip().upper()
    if "." in text:
        text = text.split(".", 1)[0]
    for prefix in ("SH", "SZ", "BJ"):
        if text.startswith(prefix):
            text = text[len(prefix):]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits[-6:].zfill(6) if digits else text


def _optional_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _pick_score(row: dict[str, Any]) -> float:
    for key in ("sort_score", "selection_score", "composite_score", "expected_t3_max_gain_pct", "expected_premium"):
        value = row.get(key)
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if pd.notna(parsed):
            return parsed
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    for key in ("sort_score", "selection_score", "composite_score", "expected_t3_max_gain_pct", "expected_premium"):
        try:
            parsed = float(winner.get(key))
        except (TypeError, ValueError):
            continue
        if pd.notna(parsed):
            return parsed
    return float("-inf")


def _pick_expected_score(row: dict[str, Any]) -> float:
    for key in ("expected_t3_max_gain_pct", "expected_premium"):
        value = row.get(key)
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if pd.notna(parsed):
            return parsed
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    for key in ("expected_t3_max_gain_pct", "expected_premium"):
        try:
            parsed = float(winner.get(key))
        except (TypeError, ValueError):
            continue
        if pd.notna(parsed):
            return parsed
    return float("-inf")


def _ledger_top1_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    winner = raw.get("winner") if isinstance(raw.get("winner"), dict) else {}
    strategy = str(row.get("strategy_type") or winner.get("strategy_type") or "")
    try:
        pick_id = float(row.get("id") or row.get("pick_id") or 0)
    except (TypeError, ValueError):
        pick_id = 0.0
    return (
        float(LEDGER_STRATEGY_PRIORITY.get(strategy, 0)),
        _pick_score(row),
        _pick_expected_score(row),
        -pick_id,
    )


def _attach_theme_contract(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    rows = []
    for row in out.get("rows") or []:
        try:
            rows.append(attach_pick_theme_fields(dict(row)))
        except Exception:
            fallback = dict(row)
            try:
                fallback = ensure_theme_contract(fallback)
            except Exception:
                fallback.setdefault("theme_name", "-")
                fallback.setdefault("theme_source", "")
                fallback.setdefault("theme_pct_chg_3", None)
            fallback.setdefault("core_theme", "" if fallback.get("theme_name") == "-" else fallback.get("theme_name", ""))
            fallback.setdefault("theme_momentum_3d", fallback.get("theme_pct_chg_3") or 0.0)
            fallback.setdefault("theme_momentum", fallback.get("theme_momentum_3d"))
            rows.append(fallback)
    out["rows"] = rows
    return out


def _collapse_actionable_daily_picks(rows: list[dict[str, Any]], mode: str = "strategy") -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for row in rows:
        date_key = str(row.get("selection_date") or row.get("date") or "")
        if date_key:
            counts[date_key] = counts.get(date_key, 0) + 1

    best_by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        date_key = str(row.get("selection_date") or row.get("date") or "")
        strategy_key = str(row.get("strategy_type") or "")
        key = date_key if mode == "day" else f"{date_key}::{strategy_key}"
        if not date_key:
            continue
        current = best_by_key.get(key)
        if current is None or _ledger_top1_sort_key(row) > _ledger_top1_sort_key(current):
            best_by_key[key] = row

    out: list[dict[str, Any]] = []
    for row in best_by_key.values():
        item = dict(row)
        item_date = str(item.get("selection_date") or item.get("date") or "")
        item["ledger_view"] = "actionable_daily_top1" if mode == "day" else "actionable_strategy_top1"
        item["raw_pick_count_for_day"] = counts.get(item_date, 1)
        out.append(item)
    return sorted(
        out,
        key=lambda row: (str(row.get("selection_date") or row.get("date") or ""), _ledger_top1_sort_key(row)),
        reverse=True,
    )


@app.get("/api/backtest/top-pick-open")
def top_pick_backtest(months: int = Query(default=2, ge=1, le=12), refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return _attach_theme_contract(_cached_strategy_response(
            "top_pick_backtest",
            months,
            refresh,
            lambda: top_pick_open_backtest(months=months, refresh=refresh),
        ))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/backtest/intraday-exit")
def intraday_exit_backtest(months: int = Query(default=12, ge=1, le=12), refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return _cached_strategy_response(
            "intraday_exit_backtest",
            months,
            refresh,
            lambda: run_intraday_exit_backtest(months=months, refresh=refresh, retrain=refresh),
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/backtest/sentinel-5m")
def sentinel_5m_backtest(
    start_date: str = Query(default=SENTINEL_5M_DEFAULT_START_DATE),
    end_date: str = Query(default=SENTINEL_5M_DEFAULT_END_DATE),
    refresh: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return _attach_theme_contract(_load_sentinel_5m_backtest(start_date, end_date, refresh=refresh))
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/strategy/lab")
def strategy_lab(months: int = Query(default=2, ge=1, le=12), refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return _cached_strategy_response(
            "strategy_lab",
            months,
            refresh,
            lambda: run_strategy_lab(months=months, refresh=refresh),
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/strategy/failure-analysis")
def failure_analysis(months: int = Query(default=12, ge=2, le=24), refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return _cached_strategy_response(
            "failure_analysis",
            months,
            refresh,
            lambda: analyze_prediction_failures(months=months, refresh=refresh),
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/strategy/up-reason-analysis")
def up_reason_analysis(months: int = Query(default=12, ge=2, le=24), refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return _cached_strategy_response(
            "up_reason_analysis",
            months,
            refresh,
            lambda: analyze_next_day_up_reasons(months=months, refresh=refresh),
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/daily-picks/save-now")
def daily_pick_save_now(force: bool = Query(default=True)) -> dict[str, Any]:
    raise HTTPException(status_code=409, detail="14:50 推送标的自动锁定，前端不允许手动保存或修改")


@app.post("/api/daily-picks/update-open")
def daily_pick_update_open(force: bool = Query(default=True)) -> dict[str, Any]:
    try:
        return update_pending_open_results(force=force)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/radar/analyze")
def radar_analyze(payload: AnalyzeRequest) -> dict[str, Any]:
    raw_data = fetch_stock_micro_data(payload.code)
    analysis = analyze_with_ollama(payload.code, payload.name, raw_data)
    return {"code": payload.code, "name": payload.name, "analysis": analysis, "raw_data": raw_data}


def fetch_stock_micro_data(stock_code: str) -> dict[str, list[str]]:
    prefix = "sh" if stock_code.startswith("6") else "sz"
    return {
        "announcements": _fetch_guba_titles(f"http://guba.eastmoney.com/list,{prefix}{stock_code},2,f.html", 6),
        "news": _fetch_guba_titles(f"http://guba.eastmoney.com/list,{prefix}{stock_code},1,f.html", 8),
        "retail": [
            title
            for title in _fetch_guba_titles(f"http://guba.eastmoney.com/list,{prefix}{stock_code}.html", 18)
            if "资讯" not in title and "公告" not in title
        ][:10],
    }


def _normalize_stock_code(code: str) -> str:
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    if len(digits) < 6:
        raise HTTPException(status_code=422, detail=f"非法股票代码：{code}")
    return digits[-6:]


def _minute_kline_path(code: str, period: str) -> Path:
    paths = _minute_kline_paths(code, period)
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _minute_kline_paths(code: str, period: str) -> list[Path]:
    plain = MIN_KLINE_DIR / f"{period}m" / f"{code}.parquet"
    prefix = "sh" if code.startswith(("5", "6", "9")) else "sz"
    prefixed = MIN_KLINE_DIR / f"{period}m" / f"{prefix}{code}.parquet"
    paths = [plain, prefixed]
    unique: list[Path] = []
    for path in paths:
        if path not in unique:
            unique.append(path)
    return unique


def _refresh_minute_kline(code: str, period: str) -> dict[str, Any]:
    if period != "5":
        return {"status": "skipped", "reason": "腾讯热数据刷新当前只支持 5 分钟周期"}
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=10)
    try:
        result = save_stock_min_data(
            code,
            period=period,
            start_date=start_dt.strftime("%Y-%m-%d 09:30:00"),
            end_date=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            merge_existing=True,
        )
        result["status"] = "saved" if int(result.get("written_rows") or 0) > 0 else result.get("status", "empty")
        return result
    except Exception as exc:
        return {"status": "failed", "error": str(exc)}


def _safe_number(value: Any) -> Optional[float]:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(num):
        return None
    return num


def _fetch_guba_titles(url: str, limit: int) -> list[str]:
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")
        titles: list[str] = []
        for article in soup.find_all("div", class_="title"):
            link = article.find("a")
            title = link.get("title") if link else None
            if title:
                titles.append(str(title).strip())
            if len(titles) >= limit:
                break
        return titles or ["暂无最新数据"]
    except Exception as exc:
        return [f"抓取失败: {exc}"]


def analyze_with_ollama(code: str, name: str, raw_data: dict[str, list[str]]) -> dict[str, Any]:
    prompt = f"""
你是一个严谨的 A 股短线风控分析师。请只基于给定材料分析 {name}({code}) 的尾盘买入风险。

材料：
官方公告：{raw_data.get("announcements", [])}
行业资讯：{raw_data.get("news", [])}
散户讨论：{raw_data.get("retail", [])}

必须输出严格 JSON，不要输出 Markdown，不要输出解释性前后缀。JSON 结构：
{{
  "verdict": "绿灯：允许观察买入 或 红灯：强制否决",
  "sentiment": "利好/中性/利空/分歧",
  "logic": "一句话说明核心推理",
  "evidence": [
    {{"source": "公告/资讯/散户/系统", "quote": "原始证据标题"}},
    {{"source": "公告/资讯/散户/系统", "quote": "原始证据标题"}},
    {{"source": "公告/资讯/散户/系统", "quote": "原始证据标题"}}
  ]
}}
"""
    try:
        session = requests.Session()
        session.trust_env = False
        response = session.post(
            OLLAMA_API,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
            timeout=45,
            proxies={"http": None, "https": None},
        )
        response.raise_for_status()
        text = response.json().get("response", "{}")
        parsed = json.loads(text)
        return _normalize_analysis(parsed)
    except Exception as exc:
        return {
            "verdict": "红灯：AI 风控不可用",
            "sentiment": "未知",
            "logic": f"Ollama 或 JSON 解析失败，不能把黑盒结果用于交易判断：{exc}",
            "evidence": [{"source": "系统", "quote": "模型未返回可信 JSON，已自动降级为风控否决"}],
        }


def _normalize_analysis(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        value = {}
    evidence = value.get("evidence")
    if not isinstance(evidence, list):
        evidence = []
    normalized_evidence = []
    for item in evidence[:5]:
        if isinstance(item, dict):
            normalized_evidence.append(
                {"source": str(item.get("source", "未知"))[:20], "quote": str(item.get("quote", ""))[:240]}
            )
        else:
            normalized_evidence.append({"source": "未知", "quote": str(item)[:240]})
    if not normalized_evidence:
        normalized_evidence = [{"source": "系统", "quote": "没有可用证据"}]
    return {
        "verdict": str(value.get("verdict", "红灯：证据不足"))[:80],
        "sentiment": str(value.get("sentiment", "未知"))[:40],
        "logic": str(value.get("logic", "模型未给出逻辑"))[:500],
        "evidence": normalized_evidence,
    }


def _jq_minute_fetch_status() -> dict[str, Any]:
    data_dir = MIN_KLINE_DIR / "5m"
    summary_path = _latest_full_jq_summary_file(data_dir) or _latest_file(data_dir, "jq_summary_*.json")
    summary = _read_json_object(summary_path)
    progress_path = data_dir / "jq_cold_5m_progress.json"
    progress = _read_json_object(progress_path)
    progress_stats = _jq_progress_stats(progress)
    quota = _latest_jq_quota()
    forecast = _jq_completion_forecast(progress, summary, progress_stats)
    last_fetch_at = _mtime_text(summary_path) or progress_stats.get("latest_updated_at") or ""
    stopped_by_quota = bool(summary.get("stopped_by_quota"))
    failed = _safe_int(summary.get("failed"))
    success = _safe_int(summary.get("success"))
    status = "missing"
    status_label = "未发现运行记录"
    if summary:
        if stopped_by_quota:
            status = "quota_exhausted"
            status_label = "今日额度已用完"
        elif failed:
            status = "partial"
            status_label = "部分失败"
        else:
            status = "success"
            status_label = "运行完成"
    return {
        "name": "聚宽冷数据 5m",
        "source": "jqdatasdk.get_price",
        "status": status,
        "status_label": status_label,
        "run_date": last_fetch_at[:10] if last_fetch_at else "",
        "last_fetch_at": last_fetch_at,
        "period": summary.get("period") or "5m",
        "range": f"{summary.get('start_date') or '-'} / {summary.get('end_date') or '-'}",
        "universe": _safe_int(summary.get("universe")),
        "success": success,
        "failed": failed,
        "skipped": _safe_int(summary.get("skipped")),
        "stopped_by_quota": stopped_by_quota,
        "quota_spare": quota.get("spare"),
        "quota_total": quota.get("total"),
        "progress_codes": progress_stats.get("codes"),
        "progress_segments": progress_stats.get("segments"),
        "progress_segments_raw": progress_stats.get("raw_segments"),
        "progress_segments_duplicate": progress_stats.get("duplicate_segments"),
        "progress_segments_per_code": forecast.get("segments_per_code"),
        "progress_total_segments": forecast.get("total_segments"),
        "progress_equivalent_codes": forecast.get("equivalent_codes"),
        "progress_pct": forecast.get("progress_pct"),
        "eta_rate_codes_per_day": forecast.get("rate_codes_per_day"),
        "eta_remaining_codes": forecast.get("remaining_codes"),
        "eta_days": forecast.get("eta_days"),
        "eta_date": forecast.get("eta_date"),
        "eta_basis": forecast.get("basis"),
        "summary_file": summary_path.name if summary_path else "",
        "progress_file": progress_path.name if progress_path.exists() else "",
    }


def _ashare_minute_fetch_status() -> dict[str, Any]:
    data_dir = MIN_KLINE_DIR / "5m"
    summary_path = _latest_file(data_dir, "ashare_summary_*.json")
    summary = _read_json_object(summary_path)
    if not summary:
        summary = _latest_ashare_log_summary()
    log_path = BASE_DIR / "logs" / "daily_ashare_archiver.log"
    last_fetch_at = str(summary.get("finished_at") or "") or _mtime_text(summary_path) or _mtime_text(log_path) or ""
    universe = _safe_int(summary.get("universe"))
    success = _safe_int(summary.get("success"))
    failed = _safe_int(summary.get("failed"))
    status = "missing"
    status_label = "未发现运行记录"
    if summary:
        if failed:
            status = "partial"
            status_label = "部分失败"
        elif universe and success >= universe:
            status = "success"
            status_label = "运行完成"
        else:
            status = "partial"
            status_label = "未覆盖全量"
    return {
        "name": "Ashare/Tencent 热数据 5m",
        "source": summary.get("source") or "tencent.m5",
        "status": status,
        "status_label": status_label,
        "run_date": last_fetch_at[:10] if last_fetch_at else "",
        "last_fetch_at": last_fetch_at,
        "period": summary.get("period") or "5m",
        "universe": universe,
        "success": success,
        "failed": failed,
        "count": _safe_int(summary.get("count")),
        "summary_file": summary_path.name if summary_path else "",
        "log_file": log_path.name if log_path.exists() else "",
    }


def _latest_file(directory: Path, pattern: str) -> Optional[Path]:
    matches = [path for path in directory.glob(pattern) if path.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def _latest_full_jq_summary_file(directory: Path, min_universe: int = 1000) -> Optional[Path]:
    matches: list[Path] = []
    for path in directory.glob("jq_summary_*.json"):
        if not path.is_file():
            continue
        summary = _read_json_object(path)
        if _safe_int(summary.get("universe")) >= min_universe:
            matches.append(path)
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def _read_json_object(path: Optional[Path]) -> dict[str, Any]:
    if not path or not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _mtime_text(path: Optional[Path]) -> str:
    if not path or not path.exists():
        return ""
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except Exception:
        return ""


def _jq_progress_stats(progress: dict[str, Any]) -> dict[str, Any]:
    codes = 0
    raw_segments = 0
    unique_segments: set[tuple[str, str]] = set()
    latest = ""
    for code, code_state in progress.items():
        if not isinstance(code_state, dict):
            continue
        codes += 1
        clean_code = str(code)
        for key, item in code_state.items():
            if not isinstance(item, dict):
                continue
            if item.get("status") in {"saved", "empty"}:
                raw_segments += 1
                end_text = str(key).split("|", 1)[1] if isinstance(key, str) and "|" in key else str(key)
                unique_segments.add((clean_code, end_text))
            updated_at = str(item.get("updated_at") or "")
            if updated_at > latest:
                latest = updated_at
    segments = len(unique_segments)
    return {
        "codes": codes,
        "segments": segments,
        "raw_segments": raw_segments,
        "duplicate_segments": max(0, raw_segments - segments),
        "latest_updated_at": latest,
    }


def _jq_completion_forecast(
    progress: dict[str, Any],
    summary: dict[str, Any],
    progress_stats: dict[str, Any],
) -> dict[str, Any]:
    universe = _safe_int(summary.get("universe")) or 0
    segments_per_code = _jq_segments_per_code(summary)
    completed_segments = _safe_int(progress_stats.get("segments"))
    if universe <= 0 or segments_per_code <= 0:
        return {}

    total_segments = universe * segments_per_code
    equivalent_codes = completed_segments / segments_per_code
    remaining_codes = max(0.0, universe - equivalent_codes)
    progress_pct = (completed_segments / total_segments) * 100 if total_segments > 0 else 0.0
    rate = _jq_recent_equivalent_rate(progress, segments_per_code)
    eta_days = None
    eta_date = ""
    if rate > 0 and remaining_codes > 0:
        eta_days = int(math.ceil(remaining_codes / rate))
        eta_date = (datetime.now().date() + timedelta(days=eta_days)).isoformat()
    elif remaining_codes <= 0:
        eta_days = 0
        eta_date = datetime.now().date().isoformat()

    return {
        "segments_per_code": segments_per_code,
        "total_segments": total_segments,
        "equivalent_codes": round(equivalent_codes, 1),
        "remaining_codes": round(remaining_codes, 1),
        "progress_pct": round(progress_pct, 2),
        "rate_codes_per_day": round(rate, 1) if rate > 0 else None,
        "eta_days": eta_days,
        "eta_date": eta_date,
        "basis": "按断点最近3个有效日净新增的等价股票数中位数估算",
    }


def _jq_segments_per_code(summary: dict[str, Any]) -> int:
    start = str(summary.get("start_date") or "")
    end = str(summary.get("end_date") or "")
    if not start or not end:
        return 0
    try:
        start_dt = datetime.fromisoformat(start[:19])
        end_dt = datetime.fromisoformat(end[:19])
    except ValueError:
        return 0
    if start_dt > end_dt:
        return 0
    count = 0
    year = start_dt.year
    month = start_dt.month
    while (year, month) <= (end_dt.year, end_dt.month):
        count += 1
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return count


def _jq_recent_equivalent_rate(progress: dict[str, Any], segments_per_code: int) -> float:
    if segments_per_code <= 0:
        return 0.0
    per_day_segments: dict[str, set[tuple[str, str]]] = {}
    for code, code_state in progress.items():
        if not isinstance(code_state, dict):
            continue
        clean_code = str(code)
        for key, item in code_state.items():
            if not isinstance(item, dict) or item.get("status") not in {"saved", "empty"}:
                continue
            updated_at = str(item.get("updated_at") or "")
            if len(updated_at) < 10:
                continue
            end_text = str(key).split("|", 1)[1] if isinstance(key, str) and "|" in key else str(key)
            per_day_segments.setdefault(updated_at[:10], set()).add((clean_code, end_text))
    if not per_day_segments:
        return 0.0

    cumulative = 0
    points: list[tuple[str, float]] = []
    for day in sorted(per_day_segments):
        cumulative += len(per_day_segments[day])
        points.append((day, cumulative / segments_per_code))
    deltas = [
        round(points[idx][1] - points[idx - 1][1], 4)
        for idx in range(1, len(points))
        if points[idx][1] > points[idx - 1][1]
    ]
    recent = deltas[-3:]
    if not recent:
        return points[-1][1] if len(points) == 1 else 0.0
    ordered = sorted(recent)
    return ordered[len(ordered) // 2]


def _latest_jq_quota() -> dict[str, Optional[int]]:
    log_path = BASE_DIR / "logs" / "jq_cold_5m.log"
    line = _latest_log_line(log_path, "spare=")
    match = re.search(r"spare=(\d+)\s+total=(\d+)", line or "")
    if not match:
        return {"spare": None, "total": None}
    return {"spare": int(match.group(1)), "total": int(match.group(2))}


def _latest_ashare_log_summary() -> dict[str, Any]:
    log_path = BASE_DIR / "logs" / "daily_ashare_archiver.log"
    text = _read_log_tail(log_path)
    for line in reversed(text.splitlines()):
        clean = line.strip()
        if not clean.startswith("{") or "universe" not in clean:
            continue
        try:
            value = ast.literal_eval(clean)
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return {}


def _latest_log_line(path: Path, needle: str) -> str:
    text = _read_log_tail(path)
    for line in reversed(text.splitlines()):
        if needle in line:
            return line.strip()
    return ""


def _read_log_tail(path: Path, max_bytes: int = 262144) -> str:
    if not path.exists():
        return ""
    try:
        with path.open("rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - max_bytes))
            return fh.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _now_text() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _cached_strategy_response(namespace: str, months: int, refresh: bool, factory) -> dict[str, Any]:
    if not refresh:
        cached = read_json_cache(namespace, months)
        if cached is not None:
            return _filter_paused_strategy_payload(cached)
    payload = factory()
    payload["cache"] = {"hit": False, "namespace": namespace}
    write_json_cache(namespace, months, payload)
    return _filter_paused_strategy_payload(payload)


def _load_sentinel_5m_backtest(start_date: str, end_date: str, refresh: bool = False) -> dict[str, Any]:
    start = _normalize_iso_date(start_date, "start_date")
    end = _normalize_iso_date(end_date, "end_date")
    cache_path = _sentinel_5m_cache_path(start, end)
    refreshed = False
    if refresh or not cache_path.exists():
        _refresh_sentinel_5m_cache(start, end, cache_path)
        refreshed = True
    if not cache_path.exists():
        raise RuntimeError(f"5m 回放缓存不存在：{cache_path}")
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"5m 回放缓存格式非法：{cache_path}")
    payload["cache"] = {
        "hit": not refreshed,
        "namespace": "sentinel_5m_backtest",
        "path": str(cache_path),
        "created_at": payload.get("created_at"),
    }
    return _filter_paused_strategy_payload(payload)


def _filter_paused_strategy_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not PAUSED_STRATEGY_TYPES:
        return payload
    paused = set(PAUSED_STRATEGY_TYPES)
    out = dict(payload)
    filtered_out = 0
    filtered_rows: Optional[List[dict[str, Any]]] = None
    for key in ("rows", "strategy_rows", "daily_picks"):
        value = out.get(key)
        if not isinstance(value, list):
            continue
        kept = [row for row in value if not isinstance(row, dict) or str(row.get("strategy_type") or "") not in paused]
        filtered_out += len(value) - len(kept)
        out[key] = kept
        if key == "rows":
            filtered_rows = [row for row in kept if isinstance(row, dict)]
    summary = out.get("summary")
    if isinstance(summary, dict):
        summary = dict(summary)
        for key in ("strategy_counts", "candidate_strategy_counts"):
            counts = summary.get(key)
            if isinstance(counts, dict):
                summary[key] = {str(k): v for k, v in counts.items() if str(k) not in paused}
        performance = summary.get("strategy_performance")
        if isinstance(performance, list):
            summary["strategy_performance"] = [
                row for row in performance
                if not isinstance(row, dict) or str(row.get("strategy_type") or "") not in paused
            ]
        if filtered_rows is not None:
            _refresh_summary_from_rows(summary, filtered_rows)
        summary["paused_strategy_types"] = list(PAUSED_STRATEGY_TYPES)
        summary["paused_strategy_count"] = filtered_out
        out["summary"] = summary
    out["paused_strategy_types"] = list(PAUSED_STRATEGY_TYPES)
    out["paused_strategy_count"] = filtered_out
    return out


def _refresh_summary_from_rows(summary: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    yields = [_row_yield_pct(row) for row in rows]
    evaluated = [value for value in yields if value is not None and math.isfinite(value)]
    wins = [value for value in evaluated if value > 0]
    summary["total_count"] = len(rows)
    summary["trade_count"] = len(rows)
    summary["evaluated_count"] = len(evaluated)
    summary["incomplete_count"] = len(rows) - len(evaluated)
    summary["win_count"] = len(wins)
    summary["loss_count"] = len(evaluated) - len(wins)
    summary["win_rate"] = round(len(wins) / len(evaluated) * 100.0, 4) if evaluated else 0.0
    summary["mean_yield"] = round(sum(evaluated) / len(evaluated), 4) if evaluated else 0.0
    summary["avg_open_premium"] = summary["mean_yield"]
    if evaluated:
        sorted_values = sorted(evaluated)
        mid = len(sorted_values) // 2
        median = sorted_values[mid] if len(sorted_values) % 2 else (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
        summary["median_yield"] = round(median, 4)
    else:
        summary["median_yield"] = 0.0
    strategy_counts: dict[str, int] = {}
    for row in rows:
        strategy = str(row.get("strategy_type") or "")
        if not strategy:
            continue
        strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
    summary["strategy_counts"] = strategy_counts
    if any("coverage_status" in row for row in rows):
        summary["any_5m_count"] = sum(1 for row in rows if _safe_int(row.get("bars_replayed")) > 0)
        summary["covered_count"] = sum(1 for row in rows if row.get("coverage_status") == "covered")
        summary["daily_t3_fallback_count"] = sum(1 for row in rows if row.get("coverage_status") == "daily_t3_fallback")


def _row_yield_pct(row: dict[str, Any]) -> Optional[float]:
    for key in ("close_return_pct", "yield_pct", "t3_settlement_return_pct", "t3_close_return_pct", "open_premium"):
        value = row.get(key)
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(parsed):
            return parsed
    return None


def _refresh_sentinel_5m_cache(start_date: str, end_date: str, cache_path: Path) -> None:
    if not SENTINEL_5M_SCRIPT.exists():
        raise RuntimeError(f"5m 回放脚本不存在：{SENTINEL_5M_SCRIPT}")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        [
            sys.executable,
            str(SENTINEL_5M_SCRIPT),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--output-json",
            str(cache_path),
        ],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=900,
    )
    if completed.returncode != 0:
        stdout_tail = (completed.stdout or "")[-4000:]
        stderr_tail = (completed.stderr or "")[-4000:]
        raise RuntimeError(
            "5m 回放缓存刷新失败："
            f"returncode={completed.returncode}\nstdout_tail={stdout_tail}\nstderr_tail={stderr_tail}"
        )


def _sentinel_5m_cache_path(start_date: str, end_date: str) -> Path:
    start_key = start_date.replace("-", "")
    end_key = end_date.replace("-", "")
    return SENTINEL_5M_CACHE_DIR / f"sentinel_5m_backtest_{start_key}_{end_key}.json"


def _normalize_iso_date(value: str, field_name: str) -> str:
    text = str(value or "").strip()[:10]
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        raise HTTPException(status_code=422, detail=f"{field_name} 必须是 YYYY-MM-DD")
    return text


def _daily_pick_scheduler_loop() -> None:
    global _last_pick_date, _last_open_update_date
    while True:
        now = datetime.now()
        today = now.date().isoformat()
        is_weekday = now.weekday() < 5

        if is_weekday and now.hour == 9 and now.minute >= 30 and _last_open_update_date != today:
            try:
                update_pending_open_results(force=False)
                _last_open_update_date = today
            except Exception as exc:
                print(f"[daily-pick] 开盘价更新失败: {exc}")

        time.sleep(60)
