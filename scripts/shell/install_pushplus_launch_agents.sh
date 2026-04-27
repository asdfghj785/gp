#!/bin/zsh
set -euo pipefail

SRC_DIR="/Users/eudis/ths/launch_agents"
DST_DIR="$HOME/Library/LaunchAgents"
UID_VALUE="$(id -u)"

mkdir -p "$DST_DIR"
chmod +x /Users/eudis/ths/scripts/shell/run_push_heartbeat.sh /Users/eudis/ths/scripts/shell/run_push_top_pick.sh /Users/eudis/ths/scripts/shell/run_snapshot_1430.sh

# Disable older overlapping PushPlus automations to avoid duplicate 9:00 / 14:40 messages.
launchctl bootout "gui/$UID_VALUE" "$DST_DIR/com.quant.heartbeat.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$UID_VALUE" "$DST_DIR/com.quant.sniper.plist" >/dev/null 2>&1 || true

cp "$SRC_DIR/com.eudis.quant.push-heartbeat.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.push-top-pick.plist" "$DST_DIR/"
cp "$SRC_DIR/com.eudis.quant.snapshot-1430.plist" "$DST_DIR/"

launchctl bootout "gui/$UID_VALUE" "$DST_DIR/com.eudis.quant.push-heartbeat.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$UID_VALUE" "$DST_DIR/com.eudis.quant.push-top-pick.plist" >/dev/null 2>&1 || true
launchctl bootout "gui/$UID_VALUE" "$DST_DIR/com.eudis.quant.snapshot-1430.plist" >/dev/null 2>&1 || true

launchctl bootstrap "gui/$UID_VALUE" "$DST_DIR/com.eudis.quant.push-heartbeat.plist"
launchctl bootstrap "gui/$UID_VALUE" "$DST_DIR/com.eudis.quant.push-top-pick.plist"
launchctl bootstrap "gui/$UID_VALUE" "$DST_DIR/com.eudis.quant.snapshot-1430.plist"

launchctl enable "gui/$UID_VALUE/com.eudis.quant.push-heartbeat"
launchctl enable "gui/$UID_VALUE/com.eudis.quant.push-top-pick"
launchctl enable "gui/$UID_VALUE/com.eudis.quant.snapshot-1430"

echo "Installed PushPlus heartbeat, 14:30 snapshot, and top-pick LaunchAgents."
