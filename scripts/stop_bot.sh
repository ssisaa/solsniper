#!/usr/bin/env bash
# Stop the bot and keep it stopped (the supervisor will not restart it) — only when nothing is being sent.
# Refuses while a trade is open unless --force. Usage: scripts/stop_bot.sh [--force]
set -u
cd /home/user/sol-sniper || exit 1
. scripts/bot_lib.sh
waited=$(wait_not_sending)
if [ "${1:-}" != "--force" ] && ! flat_now; then
  echo "NOT stopped: a trade is open: $(open_trades). Use --force to stop anyway."
  exit 2
fi
touch "$BOT_STOP"
P=$(bot_pids)
[ -n "$P" ] && kill -9 $P
echo "stopped${P:+ pid $P} (waited ${waited}s for in-flight trades); stays stopped until scripts/start_bot.sh"
