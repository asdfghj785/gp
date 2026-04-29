#!/bin/bash
set -euo pipefail

cd /Users/eudis/ths
export PYTHONPATH="/Users/eudis/ths:${PYTHONPATH:-}"

/usr/bin/python3 -m scripts.utils.quant_market_sync run >> market_close_sync.log 2>&1
