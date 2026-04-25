#!/bin/zsh
set -euo pipefail

SRC_DIR="/Users/eudis/ths/launch_agents"
DST_DIR="$HOME/Library/LaunchAgents"

mkdir -p "$DST_DIR"
chmod +x /Users/eudis/ths/run_exit_sentinel.sh
cp "$SRC_DIR/com.eudis.quant.daily-pick-save.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.exit-sentinel.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.daily-pick-open.plist" "$DST_DIR/"

launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-save.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-open.plist" >/dev/null 2>&1 || true

launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-save.plist"
launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel.plist"
launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-open.plist"

launchctl enable "gui/$(id -u)/com.eudis.quant.daily-pick-save"
launchctl enable "gui/$(id -u)/com.eudis.quant.exit-sentinel"
launchctl enable "gui/$(id -u)/com.eudis.quant.daily-pick-open"

echo "Installed daily pick and 09:26 exit sentinel LaunchAgents."
