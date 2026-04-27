#!/bin/zsh
set -euo pipefail

cd /Users/eudis/ths/quant_dashboard/frontend
export PATH="/Users/eudis/.nvm/versions/node/v24.14.1/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
exec /Users/eudis/.nvm/versions/node/v24.14.1/bin/npm run dev -- --host 127.0.0.1 --port 5173
