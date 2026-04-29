from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


def _load_local_env() -> None:
    env_path = Path(os.getenv("QUANT_BASE_DIR", "/Users/eudis/ths")) / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_local_env()


BASE_DIR = Path(os.getenv("QUANT_BASE_DIR", "/Users/eudis/ths"))
DATA_DIR = Path(os.getenv("QUANT_DATA_DIR", str(BASE_DIR / "data" / "all_kline")))
MIN_KLINE_DIR = Path(os.getenv("QUANT_MIN_KLINE_DIR", str(BASE_DIR / "data" / "min_kline")))
CORE_DB_DIR = Path(os.getenv("QUANT_CORE_DB_DIR", str(BASE_DIR / "data" / "core_db")))
SQLITE_PATH = Path(os.getenv("QUANT_SQLITE_PATH", str(CORE_DB_DIR / "quant_workstation.sqlite3")))
MODELS_DIR = Path(os.getenv("QUANT_MODELS_DIR", str(BASE_DIR / "models")))
MODEL_PATH = Path(os.getenv("QUANT_MODEL_PATH", str(MODELS_DIR / "overnight_xgboost.json")))
PREMIUM_MODEL_PATH = Path(os.getenv("QUANT_PREMIUM_MODEL_PATH", str(MODELS_DIR / "overnight_premium_xgboost.json")))
DIPBUY_PREMIUM_MODEL_PATH = Path(os.getenv("QUANT_DIPBUY_PREMIUM_MODEL_PATH", str(MODELS_DIR / "dipbuy_premium_xgboost.json")))
REVERSAL_MODEL_PATH = Path(os.getenv("QUANT_REVERSAL_MODEL_PATH", str(MODELS_DIR / "reversal_t3_xgboost.json")))
MAIN_WAVE_MODEL_PATH = Path(os.getenv("QUANT_MAIN_WAVE_MODEL_PATH", str(MODELS_DIR / "main_wave_t3_xgboost.json")))
LATEST_TOP50_PATH = Path(os.getenv("QUANT_LATEST_TOP50", str(MODELS_DIR / "latest_top_50.json")))
INTRADAY_SNAPSHOT_PATH = Path(os.getenv("QUANT_INTRADAY_SNAPSHOT_PATH", str(BASE_DIR / "data" / "intraday" / "price_1430.json")))
OLLAMA_API = os.getenv("OLLAMA_API", "http://127.0.0.1:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
PUSHPLUS_TOKEN = os.getenv("PUSHPLUS_TOKEN", "").strip()
PROFIT_TARGET_PCT = float(os.getenv("QUANT_PROFIT_TARGET_PCT", "1.00"))
BREAKOUT_HIGH_TARGET_PCT = float(os.getenv("QUANT_BREAKOUT_HIGH_TARGET_PCT", "2.00"))
BREAKOUT_MIN_SCORE = float(os.getenv("QUANT_BREAKOUT_MIN_SCORE", os.getenv("QUANT_MIN_COMPOSITE_SCORE", "65.50")))
DIPBUY_MIN_SCORE = float(os.getenv("QUANT_DIPBUY_MIN_SCORE", "99.00"))
REVERSAL_MIN_SCORE = float(os.getenv("QUANT_REVERSAL_MIN_SCORE", "3.00"))
MAIN_WAVE_MIN_SCORE = float(os.getenv("QUANT_MAIN_WAVE_MIN_SCORE", "3.00"))
MIN_COMPOSITE_SCORE = BREAKOUT_MIN_SCORE
LATE_PULL_TRAP_THRESHOLD_PCT = float(os.getenv("QUANT_LATE_PULL_TRAP_THRESHOLD_PCT", "4.00"))


def check_push_config(print_warning: bool = True) -> dict[str, Any]:
    token = (os.getenv("PUSHPLUS_TOKEN") or PUSHPLUS_TOKEN or "").strip()
    if not token:
        status = {
            "ok": False,
            "status": "critical",
            "reason": "PUSHPLUS_TOKEN 未配置，所有微信推送都会跳过",
            "token_length": 0,
        }
    elif not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", token):
        status = {
            "ok": False,
            "status": "critical",
            "reason": "PUSHPLUS_TOKEN 格式非法，请检查是否包含空格或错误字符",
            "token_length": len(token),
        }
    else:
        status = {
            "ok": True,
            "status": "ok",
            "reason": "PushPlus token 已配置",
            "token_length": len(token),
        }

    if print_warning and not status["ok"]:
        print(f"\033[91m[CRITICAL] PushPlus 配置异常：{status['reason']}\033[0m")
    return status


def ensure_dirs() -> None:
    CORE_DB_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    MIN_KLINE_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
