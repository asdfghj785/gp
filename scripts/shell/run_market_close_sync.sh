#!/bin/bash
set -euo pipefail

cd /Users/eudis/ths
export PYTHONPATH="/Users/eudis/ths:${PYTHONPATH:-}"
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "market_close_sync" "/Users/eudis/ths/market_close_sync.log"

/usr/bin/python3 -m scripts.utils.quant_market_sync run >> market_close_sync.log 2>&1
