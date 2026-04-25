from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .config import BASE_DIR, BREAKOUT_MIN_SCORE, DIPBUY_MIN_SCORE, DIPBUY_PREMIUM_MODEL_PATH, PREMIUM_MODEL_PATH
from .storage import database_overview


CACHE_VERSION = "strategy-cache-v7-strategy-thresholds"
CACHE_DIR = BASE_DIR / "data" / "strategy_cache"


def strategy_cache_signature(months: int) -> dict[str, Any]:
    overview = database_overview()
    return {
        "version": CACHE_VERSION,
        "months": int(months),
        "max_date": overview.get("max_date"),
        "rows_count": overview.get("rows_count"),
        "breakout_min_score": BREAKOUT_MIN_SCORE,
        "dipbuy_min_score": DIPBUY_MIN_SCORE,
        "premium_model_mtime": _mtime(PREMIUM_MODEL_PATH),
        "dipbuy_model_mtime": _mtime(DIPBUY_PREMIUM_MODEL_PATH),
    }


def read_json_cache(namespace: str, months: int) -> dict[str, Any] | None:
    paths = _cache_paths(namespace, months, suffix=".json")
    meta = _read_meta(paths["meta"])
    if not meta or meta.get("signature") != strategy_cache_signature(months):
        return None
    if not paths["data"].exists():
        return None
    try:
        payload = json.loads(paths["data"].read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(payload, dict):
        payload["cache"] = {"hit": True, "created_at": meta.get("created_at"), "namespace": namespace}
    return payload


def write_json_cache(namespace: str, months: int, payload: dict[str, Any]) -> None:
    paths = _cache_paths(namespace, months, suffix=".json")
    paths["data"].parent.mkdir(parents=True, exist_ok=True)
    payload_to_store = dict(payload)
    payload_to_store.pop("cache", None)
    paths["data"].write_text(json.dumps(payload_to_store, ensure_ascii=False), encoding="utf-8")
    _write_meta(paths["meta"], namespace, months)


def read_dataframe_cache(namespace: str, months: int) -> tuple[pd.DataFrame, dict[str, Any]] | None:
    paths = _cache_paths(namespace, months, suffix=".parquet")
    meta = _read_meta(paths["meta"])
    if not meta or meta.get("signature") != strategy_cache_signature(months):
        return None
    if not paths["data"].exists():
        return None
    try:
        df = pd.read_parquet(paths["data"])
    except Exception:
        return None
    return df, meta.get("extra", {})


def write_dataframe_cache(namespace: str, months: int, df: pd.DataFrame, extra: dict[str, Any]) -> None:
    paths = _cache_paths(namespace, months, suffix=".parquet")
    paths["data"].parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(paths["data"], engine="pyarrow")
    _write_meta(paths["meta"], namespace, months, extra=extra)


def _cache_paths(namespace: str, months: int, suffix: str) -> dict[str, Path]:
    safe_namespace = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in namespace)
    stem = f"{safe_namespace}_m{int(months)}"
    return {
        "data": CACHE_DIR / f"{stem}{suffix}",
        "meta": CACHE_DIR / f"{stem}.meta.json",
    }


def _read_meta(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_meta(path: Path, namespace: str, months: int, extra: dict[str, Any] | None = None) -> None:
    payload = {
        "namespace": namespace,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "signature": strategy_cache_signature(months),
        "extra": extra or {},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _mtime(path: Path) -> float | None:
    try:
        return round(path.stat().st_mtime, 3)
    except FileNotFoundError:
        return None
