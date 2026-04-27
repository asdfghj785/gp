from __future__ import annotations

from datetime import datetime
from typing import Any

from .market import fetch_sina_snapshot
from quant_core.storage import (
    count_existing_daily_keys,
    latest_market_sync_run,
    list_market_sync_runs,
    normalize_daily_frame,
    save_market_sync_run,
    upsert_daily_rows,
)


def run_market_close_sync() -> dict[str, Any]:
    started_at = datetime.now().isoformat(timespec="seconds")
    report: dict[str, Any] = {
        "started_at": started_at,
        "finished_at": started_at,
        "sync_date": None,
        "status": "running",
        "source": "sina_hs_a",
        "fetched_rows": 0,
        "valid_rows": 0,
        "inserted_rows": 0,
        "updated_rows": 0,
        "error": None,
        "summary": {},
    }

    try:
        raw = fetch_sina_snapshot()
        report["fetched_rows"] = int(len(raw))
        normalized = normalize_daily_frame(raw, source="sina_close_sync")
        normalized = normalized[(normalized["close"] > 0) & (normalized["amount"].fillna(0) > 0)].copy()
        report["valid_rows"] = int(len(normalized))
        if normalized.empty:
            raise RuntimeError("行情源返回空数据或无有效成交数据")

        dates = sorted(normalized["date"].dropna().unique().tolist())
        report["sync_date"] = dates[-1] if dates else None
        keys = list(normalized[["code", "date"]].itertuples(index=False, name=None))
        existing_rows = count_existing_daily_keys(keys)
        upsert_daily_rows(normalized, source="sina_close_sync")
        report["inserted_rows"] = max(0, int(len(keys) - existing_rows))
        report["updated_rows"] = int(existing_rows)
        report["status"] = "success"
        report["summary"] = {
            "dates": dates,
            "stock_count": int(normalized["code"].nunique()),
            "latest_date": report["sync_date"],
            "note": "按 code/date 主键幂等入库；重复运行会覆盖同日最新行情。",
        }
    except Exception as exc:
        report["status"] = "fail"
        report["error"] = str(exc)
        report["summary"] = {"note": "盘后同步失败", "error": str(exc)}
    finally:
        report["finished_at"] = datetime.now().isoformat(timespec="seconds")
        report["id"] = save_market_sync_run(report)

    if report["status"] == "fail":
        raise RuntimeError(report["error"])
    return report


def latest_sync() -> dict[str, Any] | None:
    return latest_market_sync_run()


def sync_history(limit: int = 20) -> dict[str, Any]:
    return {"rows": list_market_sync_runs(limit=limit)}
