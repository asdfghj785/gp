#!/bin/zsh
set -euo pipefail

ROOT="/Users/eudis/ths"
SRC_DIR="$ROOT/launch_agents"
DST_DIR="$HOME/Library/LaunchAgents"
UID_VALUE="$(id -u)"

mkdir -p "$SRC_DIR" "$DST_DIR"
mkdir -p "$ROOT/logs"

chmod +x \
  "$ROOT/scripts/shell/run_backend_api.sh" \
  "$ROOT/scripts/shell/run_frontend_dev.sh" \
  "$ROOT/scripts/shell/run_exit_sentinel.sh" \
  "$ROOT/scripts/shell/run_snapshot_1430.sh" \
  "$ROOT/scripts/shell/run_swing_patrol.sh" \
  "$ROOT/scripts/shell/run_v3_sniper_lock.sh" \
  "$ROOT/scripts/shell/run_push_heartbeat.sh" \
  "$ROOT/scripts/shell/run_push_top_pick.sh" \
  "$ROOT/scripts/shell/run_market_close_sync.sh" \
  "$ROOT/scripts/shell/run_live_sentinel.sh" \
  "$ROOT/scripts/shell/run_jq_cold_5m.sh" \
  "$ROOT/scripts/shell/run_daily_ashare_archiver.sh" \
  "$ROOT/scripts/shell/trading_day_guard.sh"

/usr/bin/python3 - <<'PY'
from pathlib import Path
import plistlib

root = Path("/Users/eudis/ths")
launch_dir = root / "launch_agents"

def base(label, args, stdout, stderr, working_dir=None, schedule=None, run_at_load=False, keep_alive=False, env=None):
    data = {
        "Label": label,
        "ProgramArguments": args,
        "WorkingDirectory": str(working_dir or root),
        "StandardOutPath": str(root / stdout),
        "StandardErrorPath": str(root / stderr),
    }
    if schedule:
        hour, minute = schedule
        data["StartCalendarInterval"] = {"Hour": hour, "Minute": minute}
    if run_at_load:
        data["RunAtLoad"] = True
    if keep_alive:
        data["KeepAlive"] = True
    if env:
        data["EnvironmentVariables"] = env
    return data

node_path = "/Users/eudis/.nvm/versions/node/v24.14.1/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
plists = {
    "com.eudis.quant.backend-api.plist": base(
        "com.eudis.quant.backend-api",
        [str(root / "scripts/shell/run_backend_api.sh")],
        "quant_dashboard_backend.log",
        "quant_dashboard_backend_err.log",
        run_at_load=True,
        keep_alive=True,
    ),
    "com.eudis.quant.frontend-dev.plist": base(
        "com.eudis.quant.frontend-dev",
        [str(root / "scripts/shell/run_frontend_dev.sh")],
        "quant_dashboard/frontend/frontend_server.log",
        "quant_dashboard/frontend/frontend_server.err.log",
        working_dir=root / "quant_dashboard/frontend",
        run_at_load=True,
        keep_alive=True,
        env={"PATH": node_path},
    ),
    "com.eudis.quant.exit-sentinel-0916.plist": base(
        "com.eudis.quant.exit-sentinel-0916",
        [str(root / "scripts/shell/run_exit_sentinel.sh"), "--stage", "preopen"],
        "exit_sentinel_0916_agent.log",
        "exit_sentinel_0916_agent_err.log",
        schedule=(9, 16),
    ),
    "com.eudis.quant.exit-sentinel-0921.plist": base(
        "com.eudis.quant.exit-sentinel-0921",
        [str(root / "scripts/shell/run_exit_sentinel.sh"), "--stage", "audit", "--sleep-seconds", "5"],
        "exit_sentinel_0921_agent.log",
        "exit_sentinel_0921_agent_err.log",
        schedule=(9, 21),
    ),
    "com.eudis.quant.exit-sentinel-0925.plist": base(
        "com.eudis.quant.exit-sentinel-0925",
        [str(root / "scripts/shell/run_exit_sentinel.sh"), "--stage", "final", "--sleep-seconds", "5"],
        "exit_sentinel_0925_agent.log",
        "exit_sentinel_0925_agent_err.log",
        schedule=(9, 25),
    ),
    "com.eudis.quant.snapshot-1430.plist": base(
        "com.eudis.quant.snapshot-1430",
        [str(root / "scripts/shell/run_snapshot_1430.sh")],
        "snapshot_1430.log",
        "snapshot_1430_err.log",
        schedule=(14, 30),
    ),
    "com.eudis.quant.swing-patrol.plist": base(
        "com.eudis.quant.swing-patrol",
        [str(root / "scripts/shell/run_swing_patrol.sh")],
        "swing_patrol_agent.log",
        "swing_patrol_agent_err.log",
        schedule=(15, 10),
    ),
    "com.eudis.quant.v3-sniper-lock.plist": base(
        "com.eudis.quant.v3-sniper-lock",
        [str(root / "scripts/shell/run_v3_sniper_lock.sh")],
        "v3_sniper_lock_agent.log",
        "v3_sniper_lock_agent_err.log",
        schedule=(14, 50),
    ),
    "com.eudis.quant.push-top-pick.plist": base(
        "com.eudis.quant.push-top-pick",
        [str(root / "scripts/shell/run_push_top_pick.sh")],
        "push_top_pick_agent.log",
        "push_top_pick_agent_err.log",
        schedule=(14, 50),
    ),
    "com.eudis.quant.live-sentinel.plist": base(
        "com.eudis.quant.live-sentinel",
        [str(root / "scripts/shell/run_live_sentinel.sh")],
        "logs/live_sentinel_agent.log",
        "logs/live_sentinel_agent_err.log",
        schedule=(9, 15),
    ),
    "com.eudis.quant.market-close-sync.plist": base(
        "com.eudis.quant.market-close-sync",
        [str(root / "scripts/shell/run_market_close_sync.sh")],
        "market_close_sync_agent.log",
        "market_close_sync_agent_err.log",
        schedule=(15, 5),
    ),
    "com.quant.datasync.plist": base(
        "com.quant.datasync",
        ["/usr/bin/python3", str(root / "scripts/utils/data_recorder.py")],
        "datasync.log",
        "datasync_err.log",
        schedule=(15, 8),
    ),
    "com.quant.daily_ashare_archiver.plist": base(
        "com.quant.daily_ashare_archiver",
        [str(root / "scripts/shell/run_daily_ashare_archiver.sh")],
        "logs/daily_ashare_archiver_agent.log",
        "logs/daily_ashare_archiver_agent_err.log",
        schedule=(15, 15),
    ),
    "com.eudis.quant.jq-cold-5m.plist": base(
        "com.eudis.quant.jq-cold-5m",
        [str(root / "scripts/shell/run_jq_cold_5m.sh")],
        "logs/jq_cold_5m_agent.log",
        "logs/jq_cold_5m_agent_err.log",
        schedule=(1, 20),
    ),
    "com.eudis.quant.push-heartbeat.plist": base(
        "com.eudis.quant.push-heartbeat",
        [str(root / "scripts/shell/run_push_heartbeat.sh")],
        "push_heartbeat_agent.log",
        "push_heartbeat_agent_err.log",
        schedule=(9, 0),
    ),
    "com.eudis.quant.daily-pick-save.plist": base(
        "com.eudis.quant.daily-pick-save",
        ["/usr/bin/python3", "-m", "quant_core.execution.daily_pick_cli", "save"],
        "daily_pick_save.log",
        "daily_pick_save_err.log",
        schedule=(15, 30),
    ),
}

for filename, data in plists.items():
    with (launch_dir / filename).open("wb") as fh:
        plistlib.dump(data, fh, sort_keys=False)
PY

for plist in "$SRC_DIR"/*.plist; do
  /usr/bin/plutil -lint "$plist"
  cp "$plist" "$DST_DIR/"
done

labels=(
  com.eudis.quant.backend-api
  com.eudis.quant.frontend-dev
  com.eudis.quant.exit-sentinel-0916
  com.eudis.quant.exit-sentinel-0921
  com.eudis.quant.exit-sentinel-0925
  com.eudis.quant.snapshot-1430
  com.eudis.quant.swing-patrol
  com.eudis.quant.v3-sniper-lock
  com.eudis.quant.push-top-pick
  com.eudis.quant.live-sentinel
  com.eudis.quant.market-close-sync
  com.quant.datasync
  com.quant.daily_ashare_archiver
  com.eudis.quant.jq-cold-5m
  com.eudis.quant.push-heartbeat
  com.eudis.quant.daily-pick-save
)

for label in "${labels[@]}"; do
  plist="$DST_DIR/$label.plist"
  launchctl bootout "gui/$UID_VALUE" "$plist" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$UID_VALUE" "$plist"
  launchctl enable "gui/$UID_VALUE/$label"
done

echo "LaunchAgents updated and reloaded:"
for label in "${labels[@]}"; do
  plist="$SRC_DIR/$label.plist"
  args="$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments' "$plist" | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g')"
  schedule="$(/usr/libexec/PlistBuddy -c 'Print :StartCalendarInterval' "$plist" 2>/dev/null | tr '\n' ' ' | sed 's/[[:space:]]\\+/ /g' || true)"
  echo "- $label -> $args $schedule"
done
