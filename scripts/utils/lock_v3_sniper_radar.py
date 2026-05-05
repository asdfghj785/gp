from __future__ import annotations

import json
import sys
import argparse
from pathlib import Path


BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_dashboard.backend.routers.v3_sniper import lock_today_sniper_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Lock and push the V3 14:50 sniper radar snapshot.")
    parser.add_argument("--limit", type=int, default=0, help="0 means full universe; non-zero runs a preview and never locks.")
    parser.add_argument("--max-workers", type=int, default=8)
    args = parser.parse_args()

    result = lock_today_sniper_snapshot(limit=args.limit, max_workers=args.max_workers)
    summary = {
        "prediction_date": result.get("prediction_date"),
        "locked": bool(result.get("locked")),
        "locked_at": result.get("locked_at"),
        "lock_status": result.get("lock_status"),
        "signal_count": result.get("signal_count"),
        "elapsed_seconds": result.get("elapsed_seconds"),
        "cache": result.get("cache"),
        "pushplus": result.get("pushplus"),
    }
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()
