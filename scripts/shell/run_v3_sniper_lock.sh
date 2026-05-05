#!/bin/bash
cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "v3_sniper_lock" "/Users/eudis/ths/v3_sniper_lock.log"
/usr/bin/python3 scripts/utils/lock_v3_sniper_radar.py >> v3_sniper_lock.log 2>&1
