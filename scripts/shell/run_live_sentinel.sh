#!/bin/bash
set -euo pipefail

ROOT="/Users/eudis/ths"
LOCK_DIR="$ROOT/logs/live_sentinel.lock"

cd "$ROOT"
mkdir -p logs
export PYTHONPATH="$ROOT:${PYTHONPATH:-}"
source "$ROOT/scripts/shell/trading_day_guard.sh"
skip_if_not_trading_day "live_sentinel" "$ROOT/logs/live_sentinel.log"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] live_sentinel already running, skip this trigger." >> logs/live_sentinel.log
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "[$(date '+%Y-%m-%d %H:%M:%S')] start live_sentinel from latest 14:50 picks" >> logs/live_sentinel.log
/usr/bin/python3 /Users/eudis/ths/live_sentinel.py \
  --from-yesterday-picks \
  --interval 30 \
  >> logs/live_sentinel.log 2>> logs/live_sentinel.err.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] finish live_sentinel" >> logs/live_sentinel.log
