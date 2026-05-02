#!/bin/bash

# 1. 进入你的代码所在的文件夹 (请把下面路径换成你 Mac 上的真实文件夹路径)
cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "legacy_realtime_sniper" "/Users/eudis/ths/cron_log.txt"

# 2. 运行脚本 (使用 Python 的绝对路径，防止后台环境找不到命令)
# 你可以在终端输入 which python 来确认你的 python 路径
/usr/bin/python3 scripts/utils/realtime_sniper.py >> cron_log.txt 2>&1
