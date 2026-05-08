#!/bin/bash
set -euo pipefail

ROOT="/Users/eudis/ths"

cd "$ROOT"
mkdir -p logs
export PYTHONPATH="$ROOT:${PYTHONPATH:-}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] jq cold 5m disabled: JoinQuant fetching is no longer scheduled; local cache remains readable." >> logs/jq_cold_5m.log
exit 0
