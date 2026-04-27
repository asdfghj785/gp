from __future__ import annotations

import argparse

from quant_core.storage import database_overview, import_parquet_files, init_db
from quant_core.validation import validate_one_code, validate_repository


def main() -> None:
    parser = argparse.ArgumentParser(description="量化工作站数据库同步与三重验证工具")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="初始化本地数据库结构")

    sync = sub.add_parser("sync-parquet", help="将 Parquet 历史仓导入数据库索引")
    sync.add_argument("--limit", type=int, default=None, help="最多导入多少个 parquet 文件，空值表示全量")
    sync.add_argument("--code", action="append", help="只导入指定股票代码，可重复传入")

    validate = sub.add_parser("validate", help="运行数据三重验证")
    validate.add_argument("--sample", type=int, default=200, help="抽检文件数；0 表示全量")
    validate.add_argument("--source-check", action="store_true", help="启用实时行情源交叉核验")

    one = sub.add_parser("validate-code", help="验证单只股票")
    one.add_argument("code")
    one.add_argument("--source-check", action="store_true")

    sub.add_parser("overview", help="查看数据库与数据仓概况")
    args = parser.parse_args()

    if args.command == "init":
        init_db()
        print(database_overview())
    elif args.command == "sync-parquet":
        print(import_parquet_files(codes=args.code, limit=args.limit))
    elif args.command == "validate":
        sample = None if args.sample == 0 else args.sample
        print(validate_repository(sample=sample, source_check=args.source_check))
    elif args.command == "validate-code":
        print(validate_one_code(args.code, source_check=args.source_check))
    elif args.command == "overview":
        print(database_overview())


if __name__ == "__main__":
    main()
