#!/bin/bash
set -euo pipefail

cd /Users/eudis/ths
mkdir -p logs
export PYTHONPATH="/Users/eudis/ths:${PYTHONPATH:-}"
source /Users/eudis/ths/scripts/shell/trading_day_guard.sh
skip_if_not_trading_day "daily_ashare_archiver" "/Users/eudis/ths/logs/daily_ashare_archiver.log"

/usr/bin/python3 /Users/eudis/ths/scripts/data_pipeline/backfill_ashare_data.py --universe all --daily-count 1000 --m5-count 3000 --sleep 0.03 --retries 3 --replace-daily >> logs/daily_ashare_archiver.log 2>> logs/daily_ashare_archiver.err.log
