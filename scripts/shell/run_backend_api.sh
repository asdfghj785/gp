#!/bin/zsh
set -euo pipefail

cd /Users/eudis/ths
exec /usr/bin/python3 -m uvicorn quant_dashboard.backend.main:app --host 127.0.0.1 --port 8000
