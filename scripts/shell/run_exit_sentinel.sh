#!/bin/bash
cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "exit_sentinel" "/Users/eudis/ths/exit_sentinel.log"
/usr/bin/python3 -m quant_core.execution.exit_sentinel "$@" >> exit_sentinel.log 2>&1
