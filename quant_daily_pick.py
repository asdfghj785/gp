from __future__ import annotations

import argparse

from quant_core.daily_pick import list_daily_pick_results, save_today_top_pick, update_pending_open_results


def main() -> None:
    parser = argparse.ArgumentParser(description="14:50 推送标的锁定与次日开盘验证")
    sub = parser.add_subparsers(dest="command", required=True)

    save = sub.add_parser("save", help="保存当前预测预期溢价最高的股票，仅用于命令行补录")
    save.add_argument("--force", action="store_true", help="非工作日也强制执行")

    update = sub.add_parser("update-open", help="更新待验证标的的次日开盘价")
    update.add_argument("--force", action="store_true", help="非工作日也强制执行")

    latest = sub.add_parser("latest", help="查看最近保存的标的")
    latest.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()
    if args.command == "save":
        print(save_today_top_pick(limit=10, force=args.force))
    elif args.command == "update-open":
        print(update_pending_open_results(force=args.force))
    elif args.command == "latest":
        print(list_daily_pick_results(limit=args.limit))


if __name__ == "__main__":
    main()
