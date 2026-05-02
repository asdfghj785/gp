from __future__ import annotations

import ast
import json
import re
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

from quant_core.config import MIN_KLINE_DIR, OLLAMA_API, OLLAMA_MODEL, check_push_config
from quant_core.data_pipeline.fetch_minute_data import save_stock_min_data
from quant_core.engine.backtest import top_pick_open_backtest
from quant_core.cache_utils import read_json_cache, write_json_cache
from quant_core.daily_pick import list_daily_pick_results, update_pending_open_results
from quant_core.failure_analysis import analyze_prediction_failures
from quant_core.data_pipeline.market_sync import latest_sync, run_market_close_sync, sync_history
from quant_core.engine.predictor import scan_market
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
    pushplus = check_push_config(print_warning=False)
    return {
        "ok": bool(pushplus.get("ok")),
        "service": "quant_dashboard",
        "pushplus": pushplus,
    }


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
def daily_picks(limit: int = Query(default=500, ge=1, le=1000)) -> dict[str, Any]:
    result = list_daily_pick_results(limit=limit)
    for row in result.get("rows", []):
        row.setdefault("t3_max_gain_pct", None)
    return result


def _attach_theme_contract(payload: dict[str, Any]) -> dict[str, Any]:
    out = dict(payload)
    rows = []
    for row in out.get("rows") or []:
        try:
            rows.append(ensure_theme_contract(dict(row)))
        except Exception:
            fallback = dict(row)
            fallback.setdefault("theme_name", "-")
            fallback.setdefault("theme_source", "")
            fallback.setdefault("theme_pct_chg_3", None)
            rows.append(fallback)
    out["rows"] = rows
    return out


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
    segments = 0
    latest = ""
    for code_state in progress.values():
        if not isinstance(code_state, dict):
            continue
        codes += 1
        for item in code_state.values():
            if not isinstance(item, dict):
                continue
            if item.get("status") in {"saved", "empty"}:
                segments += 1
            updated_at = str(item.get("updated_at") or "")
            if updated_at > latest:
                latest = updated_at
    return {"codes": codes, "segments": segments, "latest_updated_at": latest}


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
