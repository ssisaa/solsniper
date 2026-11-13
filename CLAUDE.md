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
- **Dayscalp:** Dayscalp's scan takes the **first scan token** (the scan lists the newest pump coins first)
  and buys it at once as lot 1 — no 5m band / volume / 1s / 1m checks (`quick_entry`); skips blacklist,
  non-pump, and coins Candles already traded or holds; RugCheck still runs when enabled. `src/signal/dayscalp.py` → `_candle_supports_dayscalp()`. That pick (the top row on Decoded
  Tokens, `LAST_CANDLE_PICK`) is fed to the Candles feeds at once, even when Auto Trade is off (watch only).
- Revert (Candles page banner) turns it off and puts Candles back to paper.

**Candles rule now (user's choice 2026-11-12/13)**: inverse ladder, Count `in_a_row`, Reds per lot **4** →
lots on reds 1, 5, 9; Max lots 3; Total cap 0.015 SOL; **green sells all only when the whole trade ≥ Min
profit 15 %, or the Settings Fast Pace margin if Fast Pace Sell is on and lower** (`green_target()`);
Max hold 90 s; Exit reserve 0; Min reserve 0.15 SOL (also blocks lots 2+ on very young coins).

**Mayhem2 feed (2026-11-13)**: two checkboxes on the Mayhem2 page (`mayhem2_feed`, one is always on).
*Mayhem_Mode* (default) = unchanged. *Mixed* = pins take turns New (< 3 min) → Movers → Graduated → New … (`_mixed_pick`, an empty board passes the turn), youngest
first in each board (boards read every 0.8 s / 1.5 s), Solana coins whose mint starts or ends with "pump" only, each board keeps its own coins (a coin on Movers and Graduated is in both); the pin needs no Mayhem_Mode holding, and
**every wallet's** BUY / SELL on the pinned coin is the signal (`ALL_TRADES_URL`); Mayhem_Mode positions are not
read. Coins off the pump curve (graduated) are valued by a Jupiter quote (`_jup_curve`, flat price, 1.5 s cache);
dry run skips the local build for them. Idle unpin (30 s without a trade) and one trade per coin still apply.

**Pro Trader (2026-11-13)**: `src/signal/pro_trader.py` reads the pinned coin's 1s candles (EMA 9/21, VWAP, RSI,
MACD, volume, 15 s BUY/SELL flow) → call BUY / SELL / HOLD / WAIT / AVOID with entry, target, stop, checklist and a
replay of the rules on the chart. Entry = "VWAP dip with volume" (≥10 % under VWAP, green candle, RSI < 45, last-5
volume ≥ 1.5× the 20 before); exit target +25 %, stop −5 %, 60 s time stop (engine: real value % minus 2 % fees).
Chosen by testing on 43 coins / 10,412 candles: 83 trades, 44 % wins, +2.3 %/trade after fees even 1 candle late;
plain trend-following lost on every setting. Small sample — never promise profit. Mayhem2 strategy `pro_trader`
trades it (live / dry run); its paper twin always runs in "Strategies side by side". `/api/candles` returns the read
as `pro`; the Mayhem2 chart draws EMA/VWAP lines, volume, an RSI pane, target/stop zones, replay tags and the
"Pro Trader desk" panel. **Candles** has the same: rule `pro` (Candles page Rule select) trades the Pro Trader call
(BUY when flat; SELL at target / stop / time stop, not held back by Sell-only-in-profit; no ladder);
`/api/candles?ctx=candle` follows the Candles trade; the Candles chart draws the same lines, RSI pane, desk and zones.

**Chart-strategy exits (2026-11-14)**: after a BUY call the price usually dips first (bottom ~3rd–4th candle) and
then rises over 20–30 candles, so the targets were raised to let it run: **Pro Trader +40 % / −5 % / 120 s**,
**Botpredict +40 % / −8 % / 60 s** (model still predicts +25 % first: `PRED_PCT`). On 76 fresh coins this beat the
old +25 % exits on training coins, fresh coins and late entries (bp +1.2 / +5.6 / +2.8 %/trade; pro +1.1 / +1.8 /
+1.1). Buying a deeper dip after the call (limit −3…−12 %) did worse.

**Candles rule `pro_hold` (2026-11-14, user's request)**: buys on **Botpredict's BUY** (◆ BP; user's choice after a
test with sell-in-profit +10 % / Max hold 300 s on 76 unseen coins: Botpredict trigger +5.0 %/trade, 84 % won, late
+4.4 %; Pro Trader setup −0.7 %; either +2.0 %). Earlier it bought on the chart's Pro Trader BUY; sell only once the whole
trade is in profit after fees (`green_target()`: Min profit, or Fast Pace margin), checked in the value loop; in a
loss hold until Max hold, then sell; no stop, no red-candle ladder. **Every new BUY signal buys again** (Max position) into
the open trade up to Total cap (`_fit_cap`); a signal that stays on for several candles buys once (`f.pro_was_buy`).
With AutoTrade-Candles on, **pro_hold buys lot 1 itself** (user's choice 2026-11-14): Dayscalp only picks the first
scan token and feeds it (`dayscalp._candle_rule() == "pro_hold"` → no Dayscalp buy); Candles buys on the chart BUY
(live, Auto Trade must be on, else `AUTO_TRADE_OFF`). Other rules keep `SUPPORT_NO_ENTRY` (the source buys lot 1).
MayhemMode's young-open grab still buys through MayhemMode.
Under pro_hold Candles watches **one coin at a time** (user's choice 2026-11-14, `PRO_HOLD_WATCH = 1`): Dayscalp's
first scan token; while a trade is open no new coin is fed; once it completes the next scan token (Candles' traded
coins are skipped). Its signals are rare (Botpredict's top 2 % moments), so expect quiet stretches.
This holds for every AutoTrade-Candles source (`_one_feed`): one feed = the coin in trade (oldest; a paused coin
does not count), else the source's next candidate. With MayhemMode as the source, pro_hold also owns entries
(`pro_hold_owns_entries("mayhem")`): MayhemMode's auto-open does not buy and the young-open grab only feeds. Backtest on 76 fresh coins: 45–50 % wins but −0.6 … −1.2 %
per trade (held losers lose more than winners make); rule `pro` did +1.8 % on the same coins. User chose to have it.

**Botpredict (2026-11-13)**: `src/signal/botpredict.py`, Mayhem2 strategy `botpredict` — the bot's own learned
model (logistic, 22 chart features, weights embedded in `MODEL`) for "price hits +25 % before −5 % within 60 s";
buys only its top 2 % moments; exits +25 % / **−8 %** / 60 s (own plan in `_pro_exit(…, name)`; pro_trader keeps −5 %).
**Stop widened to −8 % at the user's request (2026-11-14)** after a test on 76 fresh unseen coins showed every wider
stop did worse (−5 % +4.3 %/trade, −8 % +3.5 %, −10 % +2.4 %, close-based −2.6 %); weights refit for −8 % on 171 coins
(`thr` ≈ 21 %, normal 5.7 %). Earlier −5 % history: Trained on 43 coins, tested on 52 later unseen coins: +1.3 %/trade (24 trades), +3.6 % entering
1 candle late; final fit on all 95 coins. Pro Trader's dip setup LOST on those unseen coins (−1.7 %/trade).
Chart: gold "◆ BP chance" strip, ◆ BP replay tags, Botpredict row in the desk. Retrain = scripts in the session
scratchpad (not in the repo): features in botpredict.py, label = target-before-stop in 60 s.

**Mayhem2**: strategy `inv_ladder` added (Mayhem_Mode SELL = red buys, lots on reds 1/5/9, BUY = green sells
all, always). **No circuit breaker** for Mayhem2 or Candles (their losses don't count toward it).

**Other changes in the last sessions**: MayhemMode, Dayscalp and Launch buys on the pump curve use the local
pump build (exactly Max position SOL; Lightning once paid 0.005481 for a 0.005 buy); the chart's L1 marker of a
one-lot trade sits at the real entry price; Clear session empties `logs/run.out` instead of deleting it (and
keeps `bot.stop` / `run_forever.lock`); `scripts/bot_lib.sh open_trades` reads `logs/open_mints.json` first;
OpenHolds/CallOuts clicks and Decoded Tokens rows work with Auto Trade off; Decoded Tokens / Launch take
only pump coins (mint starts or ends with "pump"); Live Trades card shows the whole Candles trade (all lots);
non-SOL-quoted coins are valued on their own curve × the quote coin's SOL rate.

**Dashboard PnL (2026-11-13)**: Daily / 30 days / total cards = closed trades after fees from the ledger (same
rows as the win rate: the wallet's cash per sale, `_chain_cash`, else the ledger close); a win = net SOL after
fees > 0. Dayscalp never buys a coin the wallet already holds (an "unconfirmed" buy had landed → CS bought twice). "Wallet vs invested" = wallet SOL + value of every coin
still in the wallet − invested (its own figure). The wallet reconcile (`reconcile_wallet_sells`) counts every
wallet BUY of a trade (ladders), trims a buy only when it is ~one token-account rent over Max position, and
closes a ledger trade the wallet fully sold when the bot stopped before booking it.

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
