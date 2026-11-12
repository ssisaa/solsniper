#!/usr/bin/env bash
# Start the bot (clears a stop from stop_bot.sh). With the supervisor running it starts within ~5s;
# without it, the supervisor is started here.
set -u
cd /home/user/sol-sniper || exit 1
. scripts/bot_lib.sh
rm -f "$BOT_STOP"
if [ -n "$(bot_pids)" ]; then echo "already running: $(bot_pids | tr '\n' ' ')"; exit 0; fi
if supervised; then
  echo "supervisor will start the bot within ~5s (log: $BOT_LOG)"
else
  setsid nohup scripts/run_forever.sh > /dev/null 2>&1 < /dev/null &
  echo "started the supervisor; the bot starts now (log: $BOT_LOG)"
fi
