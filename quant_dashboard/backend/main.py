from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import OLLAMA_API, OLLAMA_MODEL
from quant_core.backtest import top_pick_open_backtest
from quant_core.cache_utils import read_json_cache, write_json_cache
from quant_core.daily_pick import list_daily_pick_results, update_pending_open_results
from quant_core.failure_analysis import analyze_prediction_failures
from quant_core.market_sync import latest_sync, run_market_close_sync, sync_history
from quant_core.predictor import scan_market
from quant_core.storage import (
    database_overview,
    import_parquet_files,
    latest_prediction_snapshot,
    list_validation_reports,
    recent_daily_rows,
)
from quant_core.strategy_lab import run_strategy_lab
from quant_core.up_reason_analysis import analyze_next_day_up_reasons
from quant_core.validation import validate_one_code, validate_repository


app = FastAPI(title="离岸量化工作站 API", version="2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AnalyzeRequest(BaseModel):
    code: str
    name: str


_scheduler_started = False
_last_pick_date = ""
_last_open_update_date = ""


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
    return {"ok": True, "service": "quant_dashboard"}


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
        version = requests.get(f"{base_url}/api/version", timeout=3)
        version.raise_for_status()
        tags = requests.get(f"{base_url}/api/tags", timeout=5)
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
    rows = recent_daily_rows(code, limit=limit)
    if not rows:
        raise HTTPException(status_code=404, detail=f"数据库中没有 {code} 的历史记录，请先运行数据同步")
    return {"code": code, "rows": rows}


@app.get("/api/radar/cache")
def radar_cache() -> dict[str, Any]:
    cached = latest_prediction_snapshot()
    if cached is None:
        return {"id": None, "created_at": "", "model_status": "no_cache", "rows": []}
    cached["rows"] = cached.get("rows", [])[:10]
    return cached


@app.get("/api/radar/scan")
def radar_scan(limit: int = Query(default=10, ge=1, le=50)) -> dict[str, Any]:
    try:
        return scan_market(limit=limit, persist_snapshot=True, cache_prediction=True, async_persist=True)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/daily-picks")
def daily_picks(limit: int = Query(default=10, ge=1, le=50)) -> dict[str, Any]:
    return list_daily_pick_results(limit=limit)


@app.get("/api/backtest/top-pick-open")
def top_pick_backtest(months: int = Query(default=2, ge=1, le=12), refresh: bool = Query(default=False)) -> dict[str, Any]:
    try:
        return _cached_strategy_response(
            "top_pick_backtest",
            months,
            refresh,
            lambda: top_pick_open_backtest(months=months, refresh=refresh),
        )
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
        response = requests.post(
            OLLAMA_API,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "format": "json"},
            timeout=45,
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


def _cached_strategy_response(namespace: str, months: int, refresh: bool, factory) -> dict[str, Any]:
    if not refresh:
        cached = read_json_cache(namespace, months)
        if cached is not None:
            return cached
    payload = factory()
    payload["cache"] = {"hit": False, "namespace": namespace}
    write_json_cache(namespace, months, payload)
    return payload


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
