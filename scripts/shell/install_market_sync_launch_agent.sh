#!/bin/zsh
set -euo pipefail

SRC="/Users/eudis/ths/launch_agents/com.eudis.quant.market-close-sync.plist"
DST="$HOME/Library/LaunchAgents/com.eudis.quant.market-close-sync.plist"
UID_VALUE="$(id -u)"

chmod +x /Users/eudis/ths/scripts/shell/run_market_close_sync.sh
mkdir -p "$HOME/Library/LaunchAgents"
cp "$SRC" "$DST"

launchctl bootout "gui/$UID_VALUE" "$DST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$UID_VALUE" "$DST"
launchctl enable "gui/$UID_VALUE/com.eudis.quant.market-close-sync"

echo "Installed market close sync LaunchAgent."
