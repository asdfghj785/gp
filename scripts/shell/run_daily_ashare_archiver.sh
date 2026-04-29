#!/bin/bash
set -euo pipefail

cd /Users/eudis/ths
mkdir -p logs
export PYTHONPATH="/Users/eudis/ths:${PYTHONPATH:-}"

/usr/bin/python3 /Users/eudis/ths/scripts/data_pipeline/daily_ashare_archiver.py --count 100 --sleep 0.05 >> logs/daily_ashare_archiver.log 2>> logs/daily_ashare_archiver.err.log
