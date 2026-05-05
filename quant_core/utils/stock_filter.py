from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from quant_core.config import DATA_DIR, SQLITE_PATH


MAINBOARD_PREFIXES = ("00", "60")
EXCLUDED_PREFIXES = ("300", "301", "688", "4", "8")
RISK_NAME_KEYWORDS = ("ST", "*ST", "退")


def get_core_universe() -> list[str]:
    """Return pure main-board A-share codes after risk-name filtering."""
    name_map = _latest_name_map_from_db()
    codes = set(name_map) | _codes_from_daily_parquets(DATA_DIR)
    clean_codes: list[str] = []
    for code in sorted(codes):
        if not _is_core_mainboard_code(code):
            continue
        name = name_map.get(code, "")
        if _is_risky_name(name):
            continue
        clean_codes.append(code)
    return clean_codes


def _latest_name_map_from_db() -> dict[str, str]:
    if not SQLITE_PATH.exists():
        return {}
    try:
        with sqlite3.connect(SQLITE_PATH) as conn:
            rows = conn.execute(
                """
                SELECT code, name
                FROM (
                    SELECT code, name,
                           ROW_NUMBER() OVER (PARTITION BY code ORDER BY date DESC) AS rn
                    FROM stock_daily
                    WHERE code IS NOT NULL AND code != ''
                )
                WHERE rn = 1
                """
            ).fetchall()
    except Exception:
        return {}
    out: dict[str, str] = {}
    for code, name in rows:
        clean = _normalize_code(code)
        if clean:
            out[clean] = str(name or "").strip()
    return out


def _codes_from_daily_parquets(data_dir: Path) -> set[str]:
    if not data_dir.exists():
        return set()
    codes = set()
    for path in data_dir.glob("*_daily.parquet"):
        clean = _normalize_code(path.name.split("_", 1)[0])
        if clean:
            codes.add(clean)
    return codes


def _is_core_mainboard_code(code: str) -> bool:
    clean = _normalize_code(code)
    if not clean:
        return False
    if clean.startswith(EXCLUDED_PREFIXES):
        return False
    return clean.startswith(MAINBOARD_PREFIXES)


def _is_risky_name(name: str) -> bool:
    text = str(name or "").strip().upper()
    return any(keyword in text for keyword in RISK_NAME_KEYWORDS)


def _normalize_code(value: object) -> str:
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if len(digits) < 6:
        return ""
    return digits[-6:]


def describe_universe(codes: Iterable[str]) -> dict[str, int]:
    code_list = list(codes)
    return {
        "total": len(code_list),
        "sz00": sum(1 for code in code_list if code.startswith("00")),
        "sh60": sum(1 for code in code_list if code.startswith("60")),
    }


if __name__ == "__main__":
    universe = get_core_universe()
    print(describe_universe(universe))
    print(universe[:20])
