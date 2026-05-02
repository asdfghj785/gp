#!/bin/bash
set -euo pipefail

cd /Users/eudis/ths
mkdir -p logs
export PYTHONPATH="/Users/eudis/ths:${PYTHONPATH:-}"
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "daily_ashare_archiver" "/Users/eudis/ths/logs/daily_ashare_archiver.log"

/usr/bin/python3 /Users/eudis/ths/scripts/data_pipeline/daily_ashare_archiver.py --count 100 --sleep 0.05 >> logs/daily_ashare_archiver.log 2>> logs/daily_ashare_archiver.err.log
