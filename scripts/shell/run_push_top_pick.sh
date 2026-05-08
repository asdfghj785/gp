#!/bin/bash
cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
COMMAND="${1:-top-pick}"
shift || true
skip_if_not_trading_day "push_top_pick:${COMMAND}" "/Users/eudis/ths/push_top_pick.log"
/usr/bin/python3 -m quant_core.execution.pushplus_tasks "$COMMAND" "$@" >> push_top_pick.log 2>&1
