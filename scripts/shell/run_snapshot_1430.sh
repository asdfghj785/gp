#!/bin/zsh
set -euo pipefail

cd /Users/eudis/ths
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "snapshot_1430" "/Users/eudis/ths/snapshot_1430.log"
/usr/bin/python3 /Users/eudis/ths/scripts/utils/snapshot_1430.py
