from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Sequence

import pandas as pd
from jqdatasdk import get_price, get_query_count
from tqdm import tqdm

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import MIN_KLINE_DIR
from quant_core.data_pipeline.fetch_minute_data import init_jq, normalize_code
from quant_core.utils.stock_filter import get_core_universe


START_DATE = "2025-01-21 09:30:00"
END_DATE = "2026-01-23 15:00:00"
JQ_ROLLING_START_LOOKBACK_DAYS = 465
QUOTA_SPARE_FUSE = 20_000
MIN_COMPLETE_ROWS = 10_000
FIELDS = ["open", "close", "high", "low", "volume", "money"]


def batch_fetch_jq_history(limit: int | None = None, force: bool = False, one: bool = False) -> dict[str, object]:
    init_jq()
    universe = get_core_universe()
    if one:
        universe = universe[:1]
    elif limit:
        universe = universe[: max(0, int(limit))]

    output_dir = MIN_KLINE_DIR / "5m"
    output_dir.mkdir(parents=True, exist_ok=True)
    effective_start = _jq_rolling_start_date()
    if effective_start != START_DATE:
        print(f"[系统提示] 聚宽滚动窗口限制：起点从 {START_DATE} 调整为 {effective_start}")
    success = 0
    skipped = 0
    errors: list[dict[str, str]] = []
    pbar = tqdm(universe, desc="jq cold 5m", unit="stock")
    for code in pbar:
        path = _plain_minute_path(code)
        if not force and _is_complete_file(path):
            skipped += 1
            pbar.set_postfix(code=code, status="skip", spare=_quota_spare_text())
            continue

        try:
            df = _fetch_one_jq(code)
            if df.empty:
                errors.append({"code": code, "error": "empty"})
                pbar.set_postfix(code=code, status="empty", spare=_quota_spare_text())
            else:
                df.to_parquet(path, engine="pyarrow", index=False)
                success += 1
                pbar.set_postfix(code=code, rows=len(df), spare=_quota_spare_text())
        except Exception as exc:
            errors.append({"code": code, "error": str(exc)})
            pbar.set_postfix(code=code, status="error", spare=_quota_spare_text())

        spare = _quota_spare()
        if spare is not None and spare < QUOTA_SPARE_FUSE:
            print("[系统提示] 今日聚宽额度已耗尽，安全熔断，进度已保存")
            sys.exit(0)

    return {
        "universe": len(universe),
        "success": success,
        "skipped": skipped,
        "failed": len(errors),
        "errors": errors[:50],
        "range": f"{effective_start} -> {END_DATE}",
    }


def _fetch_one_jq(code: str) -> pd.DataFrame:
    jq_code = normalize_code(code)
    raw = get_price(
        jq_code,
        start_date=_jq_rolling_start_date(),
        end_date=END_DATE,
        frequency="5m",
        fields=FIELDS,
        fq="pre",
        panel=False,
    )
    return _normalize_jq_frame(raw, code, jq_code)


def _jq_rolling_start_date(today: date | None = None) -> str:
    requested = datetime.fromisoformat(START_DATE)
    rolling_floor = datetime.combine(
        (today or date.today()) - timedelta(days=JQ_ROLLING_START_LOOKBACK_DAYS),
        requested.time() or dt_time(9, 30),
    )
    return max(requested, rolling_floor).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_jq_frame(df: pd.DataFrame, code: str, jq_code: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=_columns())
    out = df.copy()
    if isinstance(out.index, pd.DatetimeIndex):
        out = out.reset_index().rename(columns={out.reset_index().columns[0]: "datetime"})
    elif "time" in out.columns:
        out = out.rename(columns={"time": "datetime"})
    elif "date" in out.columns:
        out = out.rename(columns={"date": "datetime"})
    if "datetime" not in out.columns:
        raise ValueError(f"聚宽返回缺少 datetime 列：{list(df.columns)}")
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.rename(columns={"money": "amount"})
    if "amount" not in out.columns:
        out["amount"] = 0.0
    for col in ["open", "close", "high", "low", "volume", "amount"]:
        out[col] = pd.to_numeric(out.get(col, 0), errors="coerce").fillna(0.0)
    out["code"] = code
    out["jq_code"] = jq_code
    out["symbol"] = _symbol(code)
    out["period"] = "5"
    out["source"] = "jqdatasdk.get_price"
    out["ingested_at"] = datetime.now().isoformat(timespec="seconds")
    out = out.dropna(subset=["datetime"]).sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    return out[_columns()].reset_index(drop=True)


def _columns() -> list[str]:
    return ["datetime", "open", "close", "high", "low", "volume", "amount", "code", "jq_code", "symbol", "period", "source", "ingested_at"]


def _plain_minute_path(code: str) -> Path:
    return MIN_KLINE_DIR / "5m" / f"{code}.parquet"


def _is_complete_file(path: Path) -> bool:
    if not path.exists() or path.stat().st_size <= 0:
        return False
    try:
        return len(pd.read_parquet(path, columns=["datetime"])) > MIN_COMPLETE_ROWS
    except Exception:
        return False


def _quota_spare() -> int | None:
    try:
        raw = get_query_count()
    except Exception:
        return None
    if isinstance(raw, dict):
        value = raw.get("spare") or raw.get("remaining") or raw.get("left")
    else:
        value = getattr(raw, "spare", None) or getattr(raw, "remaining", None)
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _quota_spare_text() -> str:
    spare = _quota_spare()
    return "-" if spare is None else str(spare)


def _symbol(code: str) -> str:
    return f"sh{code}" if code.startswith("60") else f"sz{code}"


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="聚宽历史冷数据采集车间：核心主板 2025-01-21 至 2026-01-23 5m 前复权")
    parser.add_argument("--limit", type=int, help="只抓前 N 只，用于测试")
    parser.add_argument("--one", action="store_true", help="只抓核心票池第一只，用于验收")
    parser.add_argument("--force", action="store_true", help="忽略断点续传，强制覆盖")
    args = parser.parse_args(argv)
    print(batch_fetch_jq_history(limit=args.limit, force=args.force, one=args.one))


if __name__ == "__main__":
    main()
