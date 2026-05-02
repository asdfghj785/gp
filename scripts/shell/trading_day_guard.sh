#!/bin/bash

is_trading_day_today() {
  local root="${ROOT:-/Users/eudis/ths}"
  PYTHONPATH="$root:${PYTHONPATH:-}" /usr/bin/python3 - <<'PY'
from datetime import date
import os
from quant_core.data_pipeline.trading_calendar import is_trading_day
target = date.fromisoformat(os.getenv("TRADING_DAY_GUARD_DATE", date.today().isoformat())[:10])
raise SystemExit(0 if is_trading_day(target) else 1)
PY
}

skip_if_not_trading_day() {
  local task_name="${1:-scheduled task}"
  local log_path="${2:-/Users/eudis/ths/logs/non_trading_day_skip.log}"
  mkdir -p "$(dirname "$log_path")"
  if ! is_trading_day_today; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] non-trading day, skip $task_name." >> "$log_path"
    exit 0
  fi
}
