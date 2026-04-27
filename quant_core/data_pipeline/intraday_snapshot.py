from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_core.config import INTRADAY_SNAPSHOT_PATH, LATE_PULL_TRAP_THRESHOLD_PCT
from .market import fetch_sina_snapshot


def save_price_snapshot(path: Path = INTRADAY_SNAPSHOT_PATH) -> dict[str, Any]:
    snapshot = fetch_sina_snapshot()
    if snapshot.empty:
        raise RuntimeError("新浪行情源返回空数据，无法保存14:30快照")

    rows = []
    for _, row in snapshot.iterrows():
        code = str(row.get("code", "")).strip()
        price = float(row.get("close", 0) or 0)
        if len(code) == 6 and price > 0:
            rows.append({"code": code, "name": str(row.get("name", "")), "price": round(price, 4)})

    payload = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "date": datetime.now().strftime("%Y-%m-%d"),
        "source": "sina_snapshot",
        "count": len(rows),
        "rows": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return {key: payload[key] for key in ("created_at", "date", "source", "count")}


def load_price_snapshot(path: Path = INTRADAY_SNAPSHOT_PATH) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    rows = payload.get("rows") or []
    prices = {
        str(item.get("code", "")).strip(): float(item.get("price", 0) or 0)
        for item in rows
        if float(item.get("price", 0) or 0) > 0
    }
    payload["prices"] = prices
    payload["count"] = len(prices)
    return payload


def attach_late_pull_trap(df: pd.DataFrame, path: Path = INTRADAY_SNAPSHOT_PATH) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = df.copy()
    out["尾盘快照价"] = 0.0
    out["尾盘拉升幅度"] = 0.0
    if "尾盘诱多标记" not in out.columns:
        out["尾盘诱多标记"] = 0.0
    if out.empty:
        return out, _meta("empty_candidates")

    payload = load_price_snapshot(path)
    current_date = str(out["date"].iloc[0]) if "date" in out.columns and not out["date"].empty else datetime.now().strftime("%Y-%m-%d")
    if not payload:
        return out, _meta("missing_snapshot", current_date=current_date)
    if str(payload.get("date")) != current_date:
        return out, _meta("stale_snapshot", current_date=current_date, snapshot_date=payload.get("date"), snapshot_count=payload.get("count", 0))

    code_col = "纯代码" if "纯代码" in out.columns else "code"
    prices = payload.get("prices") or {}
    snapshot_price = out[code_col].astype(str).map(prices)
    current_price = pd.to_numeric(out.get("最新价", out.get("close", 0)), errors="coerce")
    valid = snapshot_price.notna() & (snapshot_price > 0) & current_price.notna() & (current_price > 0)
    late_pull_pct = pd.Series(0.0, index=out.index)
    late_pull_pct.loc[valid] = (current_price.loc[valid] / snapshot_price.loc[valid] - 1) * 100

    trap = late_pull_pct >= LATE_PULL_TRAP_THRESHOLD_PCT
    out["尾盘快照价"] = snapshot_price.replace([np.inf, -np.inf], 0).fillna(0)
    out["尾盘拉升幅度"] = late_pull_pct.replace([np.inf, -np.inf], 0).fillna(0)
    out["尾盘诱多标记"] = (
        pd.to_numeric(out["尾盘诱多标记"], errors="coerce").fillna(0).clip(0, 1)
        .where(~trap, 1.0)
    )
    return out, _meta(
        "ready",
        current_date=current_date,
        snapshot_date=payload.get("date"),
        snapshot_at=payload.get("created_at"),
        snapshot_count=payload.get("count", 0),
        matched_count=int(valid.sum()),
        trapped_count=int(trap.sum()),
        threshold_pct=LATE_PULL_TRAP_THRESHOLD_PCT,
    )


def _meta(status: str, **kwargs: Any) -> dict[str, Any]:
    return {"status": status, **kwargs}
