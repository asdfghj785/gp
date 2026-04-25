from __future__ import annotations

import argparse

from quant_core.market_sync import latest_sync, run_market_close_sync, sync_history


def main() -> None:
    parser = argparse.ArgumentParser(description="盘后全市场行情入库同步")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="执行一次盘后同步")
    sub.add_parser("latest", help="查看最近一次同步结果")
    history = sub.add_parser("history", help="查看同步历史")
    history.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    if args.command == "run":
        print(run_market_close_sync())
    elif args.command == "latest":
        print(latest_sync())
    elif args.command == "history":
        print(sync_history(limit=args.limit))


if __name__ == "__main__":
    main()
