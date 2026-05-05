from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Sequence

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.config import MIN_KLINE_DIR
from quant_core.data_pipeline.fetch_minute_data import (
    minute_parquet_path,
    normalize_stock_code,
    write_minute_parquet,
)
from quant_core.data_pipeline.tencent_engine import get_tencent_m5


DEFAULT_CODES = ("002709", "600865")


def load_codes(codes: Sequence[str] | None = None, code_file: str | Path | None = None) -> list[str]:
    items: list[str] = []
    if codes:
        items.extend(codes)
    if code_file:
        text = Path(code_file).read_text(encoding="utf-8")
        items.extend(item for line in text.splitlines() for item in line.replace(",", " ").split())
    if not items:
        items = list(DEFAULT_CODES)
    return sorted({normalize_stock_code(item) for item in items if str(item).strip()})


def fast_fetch_today_m5(
    codes: Sequence[str] | None = None,
    code_file: str | Path | None = None,
    count: int = 48,
    sleep_seconds: float = 0.1,
) -> dict[str, object]:
    code_list = load_codes(codes, code_file)
    results: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []
    for idx, code in enumerate(code_list, start=1):
        try:
            df = get_tencent_m5(code, count=count)
            if not df.empty:
                df["money"] = 0.0
                df["source"] = "tencent.m5"
            path = minute_parquet_path(code, period="5", output_root=MIN_KLINE_DIR)
            written = write_minute_parquet(df, path, code=code, period="5", merge_existing=True) if not df.empty else 0
            item = {"code": code, "rows": len(df), "written_rows": written, "path": str(path), "status": "saved" if written else "empty"}
            print(f"[tencent-m5] {idx}/{len(code_list)} {code} rows={len(df)} written={written} path={path}")
            results.append(item)
        except Exception as exc:
            item = {"code": code, "status": "error", "error": str(exc)}
            print(f"[tencent-m5][ERROR] {idx}/{len(code_list)} {code}: {exc}")
            errors.append(item)
        if idx < len(code_list):
            time.sleep(max(0.0, float(sleep_seconds)))
    return {"total": len(code_list), "success": len(results), "failed": len(errors), "results": results, "errors": errors}


def main() -> None:
    parser = argparse.ArgumentParser(description="腾讯极速 5 分钟线热数据拉取器")
    parser.add_argument("--code", action="append", dest="codes", help="股票代码，可重复传入")
    parser.add_argument("--code-file", help="股票代码文件，支持空格/逗号/换行分隔")
    parser.add_argument("--count", type=int, default=48, help="拉取最近 N 根 5 分钟 K 线，默认 48")
    parser.add_argument("--sleep", type=float, default=0.1, help="每只股票之间的短暂休眠秒数，默认 0.1")
    args = parser.parse_args()
    print(fast_fetch_today_m5(codes=args.codes, code_file=args.code_file, count=args.count, sleep_seconds=args.sleep))


if __name__ == "__main__":
    main()
