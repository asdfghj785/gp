from __future__ import annotations

import asyncio
import json
import sys
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
from quant_core.ai_agent.llm_engine import ollama_config
from quant_core.config import DATA_DIR, MODELS_DIR
from quant_core.engine.daily_factor_factory import THEME_FACTOR_COLUMNS, generate_daily_factors
from quant_core.engine.daily_model_trainer import discover_daily_data_dir, list_daily_files
from quant_core.engine.model_evaluator import load_daily_model


router = APIRouter(prefix="/api/v3", tags=["v3-dashboard"])

GLOBAL_MODEL_PATH = MODELS_DIR / "xgboost_daily_swing_global_v1.json"
GLOBAL_META_PATH = MODELS_DIR / "xgboost_daily_swing_global_v1.meta.json"


class AgentAnalyzeRequest(BaseModel):
    code: str
    name: Optional[str] = ""
    candidate: Optional[dict[str, Any]] = None


@router.get("/system/status")
async def system_status() -> dict[str, Any]:
    return await asyncio.to_thread(_system_status_sync)


@router.get("/sniper/signals")
async def sniper_signals(
    limit: int = Query(default=50, ge=1, le=500),
    threshold: float = Query(default=0.8, ge=0.0, le=1.0),
) -> dict[str, Any]:
    try:
        return await asyncio.to_thread(_sniper_signals_sync, limit, threshold)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"V3 狙击雷达推理失败：{exc}") from exc


@router.post("/agent/analyze")
async def agent_analyze(payload: AgentAnalyzeRequest) -> dict[str, Any]:
    code = "".join(ch for ch in str(payload.code) if ch.isdigit())[-6:]
    if len(code) != 6:
        raise HTTPException(status_code=422, detail=f"非法股票代码：{payload.code}")
    name = payload.name or code
    candidate = dict(payload.candidate or {})
    candidate.setdefault("code", code)
    candidate.setdefault("name", name)
    result = await asyncio.to_thread(
        run_1446_ai_interview,
        [code],
        [name],
        [candidate],
    )
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


def _system_status_sync() -> dict[str, Any]:
    meta = _load_model_meta()
    latest = _latest_daily_data_date()
    ollama = _ollama_status()
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "prediction_date": datetime.now().date().isoformat(),
        "xgboost": {
            "ready": GLOBAL_MODEL_PATH.exists() and GLOBAL_META_PATH.exists(),
            "model_path": str(GLOBAL_MODEL_PATH),
            "meta_path": str(GLOBAL_META_PATH),
            "metrics": meta.get("metrics", {}),
            "split_date": meta.get("split_date"),
            "feature_count": len(meta.get("feature_columns", [])),
            "high_confidence_precision": "85.78%",
        },
        "ollama": ollama,
        "data_pool": {
            "path": str(DATA_DIR),
            "latest_date": latest,
            "cold_latest_date": latest,
            "prediction_date": datetime.now().date().isoformat(),
        },
    }


def _sniper_signals_sync(limit: int, threshold: float) -> dict[str, Any]:
    if not GLOBAL_MODEL_PATH.exists():
        raise FileNotFoundError(f"全局 XGBoost 模型不存在：{GLOBAL_MODEL_PATH}")
    meta = _load_model_meta()
    feature_cols = list(meta.get("feature_columns") or [])
    if not feature_cols:
        raise RuntimeError(f"模型元数据缺少 feature_columns：{GLOBAL_META_PATH}")

    model = load_daily_model(GLOBAL_MODEL_PATH)
    data_dir = discover_daily_data_dir()
    files = list_daily_files(data_dir, limit=limit)
    rows: list[dict[str, Any]] = []
    errors: list[str] = []

    for path in files:
        try:
            item = _predict_latest_file(path, model, feature_cols)
            if item["probability"] >= threshold:
                rows.append(item)
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    rows.sort(key=lambda item: item["probability"], reverse=True)
    return {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "threshold": threshold,
        "source_limit": limit,
        "signal_count": len(rows),
        "rows": rows,
        "errors": errors[:8],
        "model": {
            "path": str(GLOBAL_MODEL_PATH),
            "split_date": meta.get("split_date"),
            "metrics": meta.get("metrics", {}),
        },
    }


def _predict_latest_file(path: Path, model: Any, feature_cols: list[str]) -> dict[str, Any]:
    raw = pd.read_parquet(path)
    factors = generate_daily_factors(raw)
    if factors.empty:
        raise ValueError("因子表为空")
    latest = factors.tail(1).copy()
    aligned = _align_features(latest, feature_cols)
    probability = float(model.predict_proba(aligned)[:, 1][0])
    row = latest.iloc[-1]
    code = str(row.get("code") or _symbol_from_path(path))[-6:]
    name = str(row.get("name") or "").strip()
    if not name or name == "None":
        name = code
    pct_chg = _safe_float(row.get("pctChg"))
    if pct_chg is None:
        pct_chg = (_safe_float(row.get("close"), 0.0) / max(_safe_float(row.get("open"), 0.0), 1e-9) - 1) * 100
    pressure = _safe_float(row.get("close_location_value"), 0.0)
    return {
        "code": code,
        "name": name,
        "date": pd.Timestamp(row.get("datetime")).date().isoformat(),
        "probability": round(probability, 6),
        "probability_pct": round(probability * 100, 2),
        "pct_chg": round(float(pct_chg), 2),
        "pressure_factor": round(float(pressure), 4),
        "close": round(_safe_float(row.get("close"), 0.0), 4),
        "strategy_type": "全局日线XGB",
        "signal": "高置信狙击" if probability >= 0.8 else "观察",
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


def _load_model_meta() -> dict[str, Any]:
    if not GLOBAL_META_PATH.exists():
        return {}
    try:
        return json.loads(GLOBAL_META_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _latest_daily_data_date() -> str:
    try:
        files = list_daily_files(discover_daily_data_dir(), limit=40)
        latest: pd.Timestamp | None = None
        for path in files:
            df = pd.read_parquet(path, columns=["date"]) if path.suffix == ".parquet" else pd.read_csv(path, usecols=["date"])
            if df.empty:
                continue
            value = pd.to_datetime(df["date"].astype(str), errors="coerce").max()
            if pd.isna(value):
                continue
            latest = value if latest is None or value > latest else latest
        return latest.date().isoformat() if latest is not None else ""
    except Exception:
        return ""


def _ollama_status() -> dict[str, Any]:
    cfg = ollama_config()
    api_base = cfg.base_url[:-3] if cfg.base_url.endswith("/v1") else cfg.base_url
    try:
        session = requests.Session()
        session.trust_env = False
        response = session.get(f"{api_base}/api/tags", timeout=2.5, proxies={"http": None, "https": None})
        response.raise_for_status()
        models = [item.get("name") for item in response.json().get("models", [])]
        return {
            "ready": True,
            "base_url": cfg.base_url,
            "model": cfg.model,
            "model_available": cfg.model in models,
        }
    except Exception as exc:
        return {
            "ready": False,
            "base_url": cfg.base_url,
            "model": cfg.model,
            "error": str(exc),
        }


def _symbol_from_path(path: Path) -> str:
    return path.stem.replace("_daily", "")[-6:]


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return default
    if pd.isna(num) or not np.isfinite(num):
        return default
    return num
