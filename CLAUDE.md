# CLAUDE.md — sol-sniper

Read this first. It is the hand-over for any Claude session on a new machine: what the bot is, how it is
run, the rules the user set, and where the work stands. Continue from here; do not start over.
Setting up a fresh machine: [docs/NEW_MACHINE.md](docs/NEW_MACHINE.md). Full user guide: [docs/BOT_GUIDE.md](docs/BOT_GUIDE.md).

## What it is

A live Solana memecoin bot (pump.fun / PumpSwap) with a web dashboard on port 8080 (WebSocket 8765).
It trades **real SOL** from the user's wallet. One "Live Controls source" decides which strategy buys:
`launch`, `dayscalp`, `opencopy`, `callout`, `mayhem` (MayhemMode), `mayhem2`, `predict`.
The **Candles** page is a separate engine that can also support those sources (AutoTrade-Candles, below).

## Run, stop, test

```bash
bash scripts/start_bot.sh                 # start (or clear a stop); supervisor starts the bot
bash scripts/stop_bot.sh                  # stop and stay stopped (refuses with a trade open; --force)
bash scripts/safe_restart.sh              # restart only when flat (waits up to 5 min; --force waits, then restarts)
tail -f logs/run.out                      # live log (old runs: logs/run.prev-*.out)
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 venv/bin/python -m pytest -q tests   # anchorpy plugin breaks pytest without it
```

- 3 tests in `tests/test_hard_filters.py` fail already (rule-ID assertions, old); everything else must pass.
- After a code change: run tests, then `safe_restart.sh`. The bot only loads code at start.
- On the user's Windows PC the bot runs in WSL under a supervisor (`scripts/run_forever.sh`, started by the
  Windows scheduled task **SolSniperBot**). A plain `kill` is restarted by the supervisor in ~10 s.
- Before a restart, check the log line `open_mints now [...]` is empty and the heartbeat says `open=0`.

## Rules from the user (keep them)

- **Never restart or stop mid-trade.** Use `safe_restart.sh` / `stop_bot.sh`. Use `--force` only when the
  user says so (e.g. a paused coin that can never close).
- **Never log or commit secrets**: RPC URLs carry keys (log only the host), `config/sniper.yaml`,
  `logs/active-wallet.json` (the wallet private key). The repo is **public**.
- **Do not trade or move funds yourself**, and do not switch Candles to live via the API — the user flips Live.
- **No paid RPC WebSocket streams** (billed per GB). Caller stream = PumpPortal + public RPC logs.
- No extra SOL beyond Max position + fees + rent. Check Auto Trade / dry-run state before restarts.
- Plain, short replies; the user reads screenshots of the dashboard and asks "why" — answer with the log.

## Main files

| Area | File |
|---|---|
| Entry, sources, OpenHolds / CallOuts loops, wallet sweeps | `src/main.py` |
| Positions, exits, sell lane kicks, circuit breaker | `src/risk/position_manager.py` |
| Buy/sell routes (local pump build, Lightning, Jupiter), sell floor | `src/execution/tx_route.py`, `trader.py`, `pump_curve.py`, `fast_lane.py` |
| Candles engine (feeds, inverse ladder, AutoTrade-Candles support, guard) | `src/signal/candle_momo.py`, `candle_core.py` (Config), `candle_store.py` |
| Mayhem2 (follows Mayhem_Mode's BUY/SELL signals; strategies incl. `inv_ladder`) | `src/signal/mayhem2.py` |
| Dashboard API + data caches (OpenHolds, MayhemMode, Decoded) | `src/observability/dashboard.py`, `static/index.html` |
| Ledger (Trade Ledger page) | `src/observability/ledger.py` |
| Runtime settings (Settings page) | `src/config/runtime.py`; persisted in `logs/runtime_state.json` |
| Candles settings | `config/candle.yaml` (hot-reloaded within 1 s) |

## Where the work stands (2026-11-13)

**AutoTrade-Candles** — checkbox on OpenHolds (opencopy), CallOuts (callout), Decoded Tokens (launch *and*
dayscalp), MayhemMode (mayhem). Off by default. Ticking it works only on the page of the current Live
Controls source (else: "Change and Select Source to …"). When on:
- Candles clears its own data, its feeds become the source's held coins, Candles goes LIVE.
- The source buys lot 1 and keeps its rules; Candles adds lots and sells by its rule.
- **Guard:** the source's own sells (caller sold, FAST PACE, stops, time stop) go through only while the whole
  trade is in profit; in a loss Candles keeps the coin. Exceptions that always sell: pump paused the coin
  (`mayhem_paused`), manual Sell / −1, Candles' own sells. A held-back rule waits 5 s before re-trying.
- **MayhemMode:** Candles grabs Mayhem_Mode's youngest open (< 30 s old) straight into the feeds and queues a
  MayhemMode buy (one coin at a time; Auto Trade off = watch only). Only coins bought while the open was < 30 s
  are taken over (`pos._open_age`).
- **Dayscalp:** Dayscalp's scan picks the youngest coin (by pool/coin creation time) that passes its 5m band,
  volume and blacklist, and buys it at once as lot 1 (`quick_entry`: no 1s/1m re-check; RugCheck still runs
  when enabled). `src/signal/dayscalp.py` → `_candle_supports_dayscalp()`. That pick (the top row on Decoded
  Tokens, `LAST_CANDLE_PICK`) is fed to the Candles feeds at once, even when Auto Trade is off (watch only).
- Revert (Candles page banner) turns it off and puts Candles back to paper.

**Candles rule now (user's choice 2026-11-12/13)**: inverse ladder, Count `in_a_row`, Reds per lot **4** →
lots on reds 1, 5, 9; Max lots 3; Total cap 0.015 SOL; **green sells all only when the whole trade ≥ Min
profit 15 %, or the Settings Fast Pace margin if Fast Pace Sell is on and lower** (`green_target()`);
Max hold 90 s; Exit reserve 0; Min reserve 0.15 SOL (also blocks lots 2+ on very young coins).

**Mayhem2**: strategy `inv_ladder` added (Mayhem_Mode SELL = red buys, lots on reds 1/5/9, BUY = green sells
all, always). **No circuit breaker** for Mayhem2 or Candles (their losses don't count toward it).

**Other changes in the last sessions**: MayhemMode buys use the local pump build (exactly Max position SOL);
OpenHolds/CallOuts clicks and Decoded Tokens rows work with Auto Trade off; Decoded Tokens / Launch take
only pump coins (mint starts or ends with "pump"); Live Trades card shows the whole Candles trade (all lots);
non-SOL-quoted coins are valued on their own curve × the quote coin's SOL rate.

**Facts that matter** (measured): Mayhem coins usually crash and get paused within seconds to ~1 min (89 %
pause); no strategy was profitable on them at the bot's ~3 s reaction (backtests) — never promise profit.
pump frontend API 429s above ~10 req/s. Pump error 6042 = price moved past the slippage guard (buy refused,
no cost). A paused coin cannot be sold on its curve.

**Not built (proposed, waiting for the user)**: run only the background loops the selected source needs +
per-page Stop buttons; ticking AutoTrade-Candles also turning Auto Trade ON.

## Working style

- Investigate with the log (`logs/run.out`, `logs/candle_audit.jsonl`, `logs/candle_momo.sqlite`) before
  changing code; explain the cause with times and numbers.
- After code changes: tests → rebuild `deploy/caller_stream_update.tar.gz` (same file list) → safe restart →
  confirm no `Traceback` in `logs/run.out`.
- Commit with the user's name; push to `origin main` only when the user asks.
