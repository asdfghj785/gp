from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(os.getenv("QUANT_BASE_DIR", "/Users/eudis/ths"))
DATA_DIR = Path(os.getenv("QUANT_DATA_DIR", str(BASE_DIR / "data" / "all_kline")))
CORE_DB_DIR = Path(os.getenv("QUANT_CORE_DB_DIR", str(BASE_DIR / "data" / "core_db")))
SQLITE_PATH = Path(os.getenv("QUANT_SQLITE_PATH", str(CORE_DB_DIR / "quant_workstation.sqlite3")))
MODEL_PATH = Path(os.getenv("QUANT_MODEL_PATH", str(BASE_DIR / "overnight_xgboost.json")))
PREMIUM_MODEL_PATH = Path(os.getenv("QUANT_PREMIUM_MODEL_PATH", str(BASE_DIR / "overnight_premium_xgboost.json")))
LATEST_TOP50_PATH = Path(os.getenv("QUANT_LATEST_TOP50", str(BASE_DIR / "latest_top_50.json")))
INTRADAY_SNAPSHOT_PATH = Path(os.getenv("QUANT_INTRADAY_SNAPSHOT_PATH", str(BASE_DIR / "data" / "intraday" / "price_1430.json")))
OLLAMA_API = os.getenv("OLLAMA_API", "http://127.0.0.1:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN", "").strip()
PROFIT_TARGET_PCT = float(os.getenv("QUANT_PROFIT_TARGET_PCT", "1.00"))
BREAKOUT_HIGH_TARGET_PCT = float(os.getenv("QUANT_BREAKOUT_HIGH_TARGET_PCT", "2.00"))
MIN_COMPOSITE_SCORE = float(os.getenv("QUANT_MIN_COMPOSITE_SCORE", "69.00"))
LATE_PULL_TRAP_THRESHOLD_PCT = float(os.getenv("QUANT_LATE_PULL_TRAP_THRESHOLD_PCT", "4.00"))


def ensure_dirs() -> None:
    CORE_DB_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
