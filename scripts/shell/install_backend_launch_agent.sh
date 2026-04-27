#!/bin/zsh
set -euo pipefail

SRC="/Users/eudis/ths/launch_agents/com.eudis.quant.backend-api.plist"
DST="$HOME/Library/LaunchAgents/com.eudis.quant.backend-api.plist"
UID_VALUE="$(id -u)"

mkdir -p "$HOME/Library/LaunchAgents"
chmod +x /Users/eudis/ths/scripts/shell/run_backend_api.sh
cp "$SRC" "$DST"

launchctl bootout "gui/$UID_VALUE" "$DST" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$UID_VALUE" "$DST"
launchctl enable "gui/$UID_VALUE/com.eudis.quant.backend-api"

echo "Installed backend API LaunchAgent: com.eudis.quant.backend-api"
