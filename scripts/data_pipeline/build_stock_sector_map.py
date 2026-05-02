from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

BASE_DIR = Path("/Users/eudis/ths")
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from quant_core.data_pipeline.sector_engine import build_stock_sector_map_from_local_universe


def main() -> None:
    parser = argparse.ArgumentParser(description="Build offline stock-to-sector mapping from local daily K-line files.")
    parser.add_argument("--data-dir", default=None, help="Local daily K-line directory. Defaults to quant_core.config.DATA_DIR.")
    parser.add_argument("--default-sector", default="全指工业", help="Fallback sector name for unmatched stocks.")
    args = parser.parse_args()
    summary = build_stock_sector_map_from_local_universe(
        data_dir=args.data_dir,
        default_sector=args.default_sector,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
