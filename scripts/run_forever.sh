#!/usr/bin/env bash
# Keeps the live bot running. Started at Windows login by the "SolSniperBot" scheduled task
# (wsl.exe -d Ubuntu -u user -- this script); that wsl.exe also keeps WSL from shutting down.
# - Starts the bot whenever no copy is running and logs/bot.stop is absent.
# - A copy started by hand or by the dashboard's Restart button is left alone (never two bots).
# - scripts/stop_bot.sh stops it and keeps it stopped; scripts/start_bot.sh clears that.
set -u
cd /home/user/sol-sniper || exit 1
. scripts/bot_lib.sh
exec 9>"$BOT_LOCK"
if ! flock -n 9; then
  echo "supervisor already running"
  # Stay alive anyway: this wsl.exe is what keeps WSL up for the task that started it.
  exec sleep infinity
fi
note() { echo "$(date '+%F %T') supervisor: $*" >> logs/supervisor.log; }
note "started (pid $$)"
while true; do
  if [ -f "$BOT_STOP" ] || [ -n "$(bot_pids)" ]; then
    sleep 5
    continue
  fi
  [ -s "$BOT_LOG" ] && cp "$BOT_LOG" "logs/run.prev-$(date +%m%d-%H%M%S).out"
  ls -t logs/run.prev-*.out 2>/dev/null | tail -n +21 | xargs -r rm -f
  note "starting the bot"
  venv/bin/python -m src.main --mode live-feed --config config/sniper.yaml --duration 0 > "$BOT_LOG" 2>&1 9>&-
  code=$?
  note "bot exited (code $code)"
  sleep 10
done
