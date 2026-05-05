#!/bin/bash
cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "push_top_pick" "/Users/eudis/ths/push_top_pick.log"
/usr/bin/python3 -m quant_core.execution.pushplus_tasks top-pick >> push_top_pick.log 2>&1
