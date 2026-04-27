#!/bin/zsh
set -euo pipefail

LABEL="com.eudis.quant.frontend-dev"
SRC="/Users/eudis/ths/launch_agents/${LABEL}.plist"
DST="${HOME}/Library/LaunchAgents/${LABEL}.plist"

mkdir -p "${HOME}/Library/LaunchAgents"
cp "${SRC}" "${DST}"
chmod 644 "${DST}"
chmod +x /Users/eudis/ths/scripts/shell/run_frontend_dev.sh

launchctl bootout "gui/$(id -u)" "${DST}" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "${DST}"
launchctl kickstart -k "gui/$(id -u)/${LABEL}"
launchctl print "gui/$(id -u)/${LABEL}"
