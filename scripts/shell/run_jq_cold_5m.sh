#!/bin/bash
set -euo pipefail

ROOT="/Users/eudis/ths"
LOCK_DIR="$ROOT/logs/jq_cold_5m.lock"

cd "$ROOT"
mkdir -p logs
export PYTHONPATH="$ROOT:${PYTHONPATH:-}"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] jq cold 5m already running, skip this trigger." >> logs/jq_cold_5m.log
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

echo "[$(date '+%Y-%m-%d %H:%M:%S')] start jq cold 5m quota run" >> logs/jq_cold_5m.log
/usr/bin/python3 /Users/eudis/ths/scripts/data_pipeline/batch_fetch_historical_min.py \
  --period 5 \
  --start-date "2025-01-19 09:30:00" \
  --end-date "2026-01-23 15:00:00" \
  --segment month \
  --quota-buffer 0 \
  >> logs/jq_cold_5m.log 2>> logs/jq_cold_5m.err.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] finish jq cold 5m quota run" >> logs/jq_cold_5m.log
