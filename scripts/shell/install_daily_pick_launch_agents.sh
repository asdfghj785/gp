#!/bin/zsh
set -euo pipefail

SRC_DIR="/Users/eudis/ths/launch_agents"
DST_DIR="$HOME/Library/LaunchAgents"

mkdir -p "$DST_DIR"
chmod +x /Users/eudis/ths/scripts/shell/run_exit_sentinel.sh
chmod +x /Users/eudis/ths/scripts/shell/run_swing_patrol.sh
cp "$SRC_DIR/com.eudis.quant.daily-pick-save.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.exit-sentinel-0916.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.exit-sentinel-0921.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.exit-sentinel-0925.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.swing-patrol.plist" "$DST_DIR/"

launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-save.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-open.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel-0916.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel-0921.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel-0925.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$(id -u)" "$DST_DIR/com.eudis.quant.swing-patrol.plist" >/dev/null 2>&1 || true

launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.daily-pick-save.plist"
launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel-0916.plist"
launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel-0921.plist"
launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.exit-sentinel-0925.plist"
launchctl bootstrap "gui/$(id -u)" "$DST_DIR/com.eudis.quant.swing-patrol.plist"

launchctl enable "gui/$(id -u)/com.eudis.quant.daily-pick-save"
launchctl enable "gui/$(id -u)/com.eudis.quant.exit-sentinel-0916"
launchctl enable "gui/$(id -u)/com.eudis.quant.exit-sentinel-0921"
launchctl enable "gui/$(id -u)/com.eudis.quant.exit-sentinel-0925"
launchctl enable "gui/$(id -u)/com.eudis.quant.swing-patrol"

echo "Installed daily pick, 09:16/09:21/09:25 staged exit sentinels and 14:45 swing patrol LaunchAgents."
