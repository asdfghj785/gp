from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Union

from quant_core.config import BASE_DIR


SNIPER_STATUS_PATH = BASE_DIR / "data" / "sniper_status.json"
DEFAULT_SNIPER_STATUS = {"enabled": False}


def get_sniper_status() -> bool:
    return bool(read_sniper_status().get("enabled"))


def read_sniper_status() -> dict[str, Any]:
    if not SNIPER_STATUS_PATH.exists():
        write_sniper_status(DEFAULT_SNIPER_STATUS)
        return dict(DEFAULT_SNIPER_STATUS)
    try:
        payload = json.loads(SNIPER_STATUS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        write_sniper_status(DEFAULT_SNIPER_STATUS)
        return dict(DEFAULT_SNIPER_STATUS)
    if not isinstance(payload, dict):
        write_sniper_status(DEFAULT_SNIPER_STATUS)
        return dict(DEFAULT_SNIPER_STATUS)
    return {"enabled": bool(payload.get("enabled"))}


def set_sniper_status(status: Union[bool, dict[str, Any]]) -> dict[str, Any]:
    enabled = bool(status.get("enabled")) if isinstance(status, dict) else bool(status)
    payload = {"enabled": enabled}
    write_sniper_status(payload)
    return payload


def write_sniper_status(payload: dict[str, Any]) -> None:
    SNIPER_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = SNIPER_STATUS_PATH.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(SNIPER_STATUS_PATH)
