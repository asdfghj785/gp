#!/bin/bash
cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "swing_patrol" "/Users/eudis/ths/swing_patrol.log"
/usr/bin/python3 -m quant_core.execution.swing_patrol >> swing_patrol.log 2>&1
