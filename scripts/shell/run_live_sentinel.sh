#!/bin/bash
set -euo pipefail

ROOT="/Users/eudis/ths"

cd "$ROOT"
mkdir -p logs
export PYTHONPATH="$ROOT:${PYTHONPATH:-}"
source "$ROOT/scripts/shell/trading_day_guard.sh"
skip_if_not_trading_day "live_sentinel" "$ROOT/logs/live_sentinel.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] live_sentinel disabled: exits are now handled by T+1 open settlement and T+3 15:00 close settlement." >> logs/live_sentinel.log
exit 0
