#!/usr/bin/env bash
# Restart the bot only when no buy or sell is being sent (no "LANE … sending now" without its "done").
# With the supervisor (run_forever.sh) running, it starts the new copy; otherwise this script does.
# Waits until no trade is open (up to 5 min, then refuses unless --force).
# Usage: scripts/safe_restart.sh [--force] [logfile]   (default logs/run.out)
set -u
cd /home/user/sol-sniper || exit 1
. scripts/bot_lib.sh
FORCE=0
[ "${1:-}" = "--force" ] && { FORCE=1; shift; }
LOG="${1:-$BOT_LOG}"
# Never restart mid-trade: wait up to 5 min for every open trade to close (a restart once left one unmanaged).
if ! flat_now "$LOG"; then
  echo "a trade is open: $(open_trades "$LOG") — waiting up to 5 min for it to close"
  if ! wait_flat "$LOG" 300 && [ "$FORCE" != "1" ]; then
    echo "NOT restarted: still open: $(open_trades "$LOG"). Try again later, or --force to restart anyway."
    exit 2
  fi
fi
waited=$(wait_not_sending "$LOG")
# a buy may have opened a trade while we waited: check again right before the kill
if [ "$FORCE" != "1" ] && ! flat_now "$LOG"; then
  echo "NOT restarted: a trade opened meanwhile: $(open_trades "$LOG"). Try again later."
  exit 2
fi
rm -f "$BOT_STOP"
P=$(bot_pids)
[ -n "$P" ] && kill -9 $P
if supervised; then
  echo "killed${P:+ pid $P} (waited ${waited}s for in-flight trades); the supervisor starts the new copy in ~10s"
  exit 0
fi
sleep 1
cp "$LOG" "${LOG%.out}.prev-$(date +%H%M%S).out" 2>/dev/null
nohup venv/bin/python -m src.main --mode live-feed --config config/sniper.yaml --duration 0 > "$LOG" 2>&1 &
echo "restarted pid $! (waited ${waited}s for in-flight trades)"
