from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
from tqdm import tqdm

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import MIN_KLINE_DIR
from quant_core.data_pipeline.tencent_engine import get_tencent_m5, normalize_stock_code, tencent_symbol
from quant_core.utils.stock_filter import get_core_universe


def daily_ashare_archive(limit: Optional[int] = None, sleep_seconds: float = 0.1, count: int = 100) -> dict[str, object]:
    """Archive daily hot 5m samples via Ashare-style Tencent API."""
    started_at = datetime.now().isoformat(timespec="seconds")
    universe = get_core_universe()
    if limit:
        universe = universe[: max(0, int(limit))]
    success = 0
    errors: list[dict[str, str]] = []
    for code in tqdm(universe, desc="ashare/tencent hot 5m", unit="stock"):
        try:
            rows = _fetch_today_rows(code, count=count)
            written = _upsert_rows(code, rows)
            success += 1
            tqdm.write(f"[hot-archive] {code} rows={len(rows)} total={written}")
        except Exception as exc:
            errors.append({"code": code, "error": str(exc)})
            tqdm.write(f"[hot-archive][ERROR] {code}: {exc}")
        time.sleep(max(0.0, float(sleep_seconds)))
    finished_at = datetime.now().isoformat(timespec="seconds")
    summary = {
        "source": "tencent.m5",
        "period": "5m",
        "started_at": started_at,
        "finished_at": finished_at,
        "universe": len(universe),
        "success": success,
        "failed": len(errors),
        "count": count,
        "errors": errors[:50],
    }
    summary_path = MIN_KLINE_DIR / "5m" / f"ashare_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    summary["summary_path"] = str(summary_path)
    return summary


def _fetch_today_rows(code: str, count: int = 100) -> pd.DataFrame:
    df = get_tencent_m5(code, count=count)
    if df.empty:
        return _empty_frame()
    out = df.copy()
    out["amount"] = 0.0
    out["code"] = normalize_stock_code(code)
    out["jq_code"] = _jq_code(code)
    out["symbol"] = tencent_symbol(code)
    out["period"] = "5"
    out["source"] = "tencent.m5"
    out["ingested_at"] = datetime.now().isoformat(timespec="seconds")
    for col in ["open", "close", "high", "low", "volume", "amount"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)
    out["datetime"] = pd.to_datetime(out["datetime"], errors="coerce")
    out = out.dropna(subset=["datetime"])
    return out[_columns()].sort_values("datetime").reset_index(drop=True)


def _upsert_rows(code: str, hot: pd.DataFrame) -> int:
    path = _plain_minute_path(code)
    path.parent.mkdir(parents=True, exist_ok=True)
    frames = []
    if path.exists():
        frames.append(pd.read_parquet(path))
    if not hot.empty:
        frames.append(hot)
    if not frames:
        return 0
    merged = pd.concat(frames, ignore_index=True)
    merged = _normalize_merged_frame(merged, code)
    merged = merged.drop_duplicates(subset=["datetime"], keep="last").sort_values("datetime").reset_index(drop=True)
    merged.to_parquet(path, engine="pyarrow", index=False)
    return len(merged)


def _normalize_merged_frame(df: pd.DataFrame, code: str) -> pd.DataFrame:
    merged = df.copy()
    merged["datetime"] = pd.to_datetime(merged["datetime"], errors="coerce")
    merged = merged.dropna(subset=["datetime"])
    if "amount" not in merged.columns:
        merged["amount"] = 0.0
    for col in ["open", "close", "high", "low", "volume", "amount"]:
        merged[col] = pd.to_numeric(merged.get(col, 0), errors="coerce").fillna(0.0)
    clean = normalize_stock_code(code)
    merged["code"] = clean
    merged["jq_code"] = merged.get("jq_code", _jq_code(clean))
    merged["jq_code"] = merged["jq_code"].fillna(_jq_code(clean)).astype(str)
    merged["symbol"] = merged.get("symbol", tencent_symbol(clean))
    merged["symbol"] = merged["symbol"].fillna(tencent_symbol(clean)).astype(str)
    merged["period"] = merged.get("period", "5")
    merged["period"] = merged["period"].fillna("5").astype(str)
    merged["source"] = merged.get("source", "tencent.m5")
    merged["source"] = merged["source"].fillna("tencent.m5").astype(str)
    merged["ingested_at"] = merged.get("ingested_at", datetime.now().isoformat(timespec="seconds"))
    merged["ingested_at"] = merged["ingested_at"].fillna(datetime.now().isoformat(timespec="seconds")).astype(str)
    return merged[_columns()]


def _plain_minute_path(code: str) -> Path:
    clean = normalize_stock_code(code)
    plain = MIN_KLINE_DIR / "5m" / f"{clean}.parquet"
    prefixed = MIN_KLINE_DIR / "5m" / f"{tencent_symbol(clean)}.parquet"
    if plain.exists():
        return plain
    if prefixed.exists():
        return prefixed
    return plain


def _columns() -> list[str]:
    return ["datetime", "open", "close", "high", "low", "volume", "amount", "code", "jq_code", "symbol", "period", "source", "ingested_at"]


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=_columns())


def _jq_code(code: str) -> str:
    clean = normalize_stock_code(code)
    return f"{clean}.XSHG" if clean.startswith("60") else f"{clean}.XSHE"


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="每日热数据标本归档器（Ashare/Tencent 5m）")
    parser.add_argument("--limit", type=int, help="只归档前 N 只，用于测试")
    parser.add_argument("--count", type=int, default=100, help="每只股票拉取最近 N 根 5 分钟线，默认 100")
    parser.add_argument("--sleep", type=float, default=0.1, help="每只股票之间休眠秒数")
    args = parser.parse_args(argv)
    print(daily_ashare_archive(limit=args.limit, sleep_seconds=args.sleep, count=args.count))


if __name__ == "__main__":
    main()
