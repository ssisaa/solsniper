# Shared by run_forever.sh, safe_restart.sh, stop_bot.sh, start_bot.sh. Source it from the repo root.
BOT_ROOT=/home/user/sol-sniper
BOT_LOG="$BOT_ROOT/logs/run.out"
BOT_STOP="$BOT_ROOT/logs/bot.stop"          # present = stay stopped (the supervisor will not start the bot)
BOT_LOCK="$BOT_ROOT/logs/run_forever.lock"  # held by the running supervisor
BOT_PAT='^(/home/user/sol-sniper/)?venv/bin/python -m src\.main'   # the bot itself, also the copy the dashboard's Restart button starts

bot_pids() { pgrep -f "$BOT_PAT"; }

supervised() { ! flock -n "$BOT_LOCK" true 2>/dev/null; }

# Wait (up to 60s) until no buy/sell is being sent. Prints the seconds waited.
wait_not_sending() {
  local log="${1:-$BOT_LOG}" i open=0
  for i in $(seq 1 120); do
    open=$(tail -400 "$log" 2>/dev/null | awk '/LANE .* — sending now/{n++} /LANE done/{n--} END{print (n>0)?n:0}')
    [ "$open" = "0" ] && break
    sleep 0.5
  done
  echo $((i / 2))
}

# Last open-trade list the bot logged: empty output = flat (or nothing logged yet).
open_trades() {
  local last
  # The bot's own list of open coins is the truth; the log line is the fallback. (A dashboard
  # "Clear session" once deleted run.out and this check then saw no open trade at all.)
  if [ -s "$BOT_ROOT/logs/open_mints.json" ]; then
    last=$(tr -d ' \n' < "$BOT_ROOT/logs/open_mints.json")
    [ "$last" = "[]" ] && return 0
    echo "$last"
    return 0
  fi
  last=$(grep -E "open_mints now" "${1:-$BOT_LOG}" 2>/dev/null | tail -1)
  [ -z "$last" ] && return 0
  echo "$last" | grep -q "now \[\]" && return 0
  echo "$last" | sed 's/.*open_mints now //'
}

# Wait (up to $2 seconds, default 300) until no trade is open. Returns 1 if one still is.
wait_flat() {
  local log="${1:-$BOT_LOG}" limit="${2:-300}" i
  for i in $(seq 1 "$limit"); do
    [ -z "$(open_trades "$log")" ] && return 0
    sleep 1
  done
  return 1
}
