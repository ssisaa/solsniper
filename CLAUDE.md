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

**Candles Mixed feed (2026-11-13)**: Candles page Feed checkboxes (`auto_follow`: `mayhem_board` | `mixed`, one is
always on; only when AutoTrade-Candles is off). *Mixed* = same boards as Mayhem2's Mixed (`mayhem2.MIXED_BOARDS`,
reuses Mayhem2's fresh copy, else reads them itself), turns New → Movers → Graduated (`_mixed_want`, `_mx_turn`),
youngest first, pump mints only, New < 3 min; a coin stays while held, else until its trade completes or
`mixed_idle_s` (60 s) with no trade, then the next board's youngest (a handed-over coin is skipped 5 min). Max mints
slots (pro_hold: one). Paper / LIVE follow Candles' Live switch. Graduated coins are valued by Jupiter.

**Candles Mixed = New, Movers, Trending (2026-11-16/17, user's request)**: `candle_momo.CANDLE_MIXED_GROUPS` =
("new", "movers", "trending") — Graduated was removed and pump's Trending board (`TRENDING_URL`, social ranking, 1 h
window, read every 4 s) takes its place; turns New → Movers → Trending → New …, youngest first, mints that start or end
with "pump" only; page label "Mixed (New, Movers, Trending)". Trending coins are old (youngest seen ~4 h) and many are
graduated (Jupiter-priced). Mayhem2's own Mixed feed still uses New / Movers / Graduated. This overrides the
"Graduated" part of the Candles Mixed feed paragraph above.

**Paused coin took the one-feed slot (2026-11-17)**: after a restart a restored paused coin (ADAWDD; the dashboard's
`mayhem_state_of` does not know it) was pro_hold's one feed in support mode (`_one_feed`), `_single_feed` dropped it,
and it was re-added every 2 s ("Watching ADAWDD (mayhem holds it)" 116 times) — no other coin was fed in the MayhemMode
step. `_one_feed` and the support `want` now skip positions with `paused_at`. Same night pump answered HTTP 429 on every
advanced-indexer board for this IP (board errors are only logged at debug: an empty pick with no log line = look there).

**Unsold Coins: 1 h retry, Clear button (2026-11-18, user's request)**: `UNSOLD_RETRY_S` = 3600; after it the row's
status says it was retried for 1 h (N tries), still no price, not sellable, retrying stopped. The Retry column now sits
before Reason (it was off-screen at the right, so the user saw no retry although AI / Cyber had been re-priced) and each
row has a Clear button (`POST /api/candle {action: "unsold_clear", mint, ts}` → `unsold_clear`; the closed trade stays).

**One reading everywhere (2026-11-18, user's request)**: AI showed −8.95 % in Open positions and −34.3 % under the
chart. The table used the chart price (`live_reading`), while the chart footer, avg / break-even / target lines
(`chart_marks().open`) and the Exit hint (`_hint`) used the value loop's last Jupiter quote (up to 45 s old).
`reading(mint)` is now the one figure for all of them (live chart reading, else the last quote; the quote stays in
`quote_upct`). The `impact` measurement in the value loop now uses `_chart_value` (same unit conversion for a lot
bought on the other chart).

**Unsold after 3 failed sells, retried there (2026-11-18, user's request)**: NSA (Movers, graduated) bought at 00:17:57,
then 7 paper sells failed "price unreadable" over 6 min while the value loop priced it — the paper sell's Jupiter quote
(non-urgent) was skipped by the background Jupiter limiter. (1) `_paper_sell`: no Jupiter quote → priced from
`live_reading()` (chart × impact), else a value read ≤ 15 s old (reason "… priced from the chart"). (2) Still no price:
`pos["sell_fails"]` counts; at `SELL_FAILS_SET_ASIDE` 3 the trade goes to `_set_aside` at once (not after 90 s), booked at
its last value, next coin comes. (3) Unsold Coins page, paper rows: `retry` — `_unsold_retry` (value loop) re-reads the
price every 10 s for `UNSOLD_RETRY_S` 10 min; on a read the booked closed trade is corrected (`store.set_trade_cash`,
row `trade_id`), status shown in the new Retry column. LIVE set-aside is unchanged (only with no price route; coins stay
in the wallet). `_set_aside` runs once per trade (`pos["aside"]`).

**No candle close (2026-11-18, user's request)**: pumpcity sat in Open positions with no trade for 15 min. Candles page
"No candle close s" (`idle_exit_s`, default 30, 0 = off, next to Max hold): an open trade whose coin printed no new candle
on any of its charts for that long (counted from the newest candle, the buy or a resume — `idle_left()`) is closed by the
value loop (`_forced_exit`, reason "no candle for Ns (coin stopped trading)", paper and live). Open positions' Max hold
cell shows "no candle · closes in Ns" once 5 s pass without a candle (`idleLeft` in `/api/candle`). Paused coins are
not affected (their own watch).

**Graduated coins get a live chart (2026-11-17, user's request)**: pump's chart ran ahead of the bot's on GEKKO
(PumpSwap). The RPC logs subscription (public node, `mentions: [coin]`) already received every PumpSwap trade but only
pump's curve TradeEvent was decoded. `caller_stream.decode_amm_logs(logs, mint)` reads PumpSwap BuyEvent
(`67f4521f2cf57777`) / SellEvent (`3e2f370aa503dc2a`) from the "Program data:" lines — base amount at byte 16, quote
amount at 64, user pubkey at 152; the event has no mint, so the coin comes from the subscription id (`by_sid`, coin
subscriptions only). Checked live on GEKKO: 405 trades in 75 s, consistent prices. They feed the stream chart like any
trade (so the feed switches from pump_rest to stream). A lot priced on the other chart is converted by the two charts'
close ratio over their last common candles (`_unit_ratio`) in `_chart_value`. The Candles page redraws the chart every
0.3 s (was 1 s). Graduated coins only get this while they are among the RPC-logs subscriptions (Candles feeds first).

**Open positions follow the chart (2026-11-17, user's request)**: GEKKO (graduated, Jupiter-valued) showed 0.005046
SOL / +0.48 % for 20+ s while the candles rose ~4 %. Cause: background Jupiter quotes are cached up to 45 s
(`jupiter.BG_CACHE_S`), and the value loop re-scaled `impact` = old quote / chart price now every second, which pinned
the fast chart reading (`_fast_upct`, used by the sell at once) to the old quote as well. Now `_val_at[mint]` = when the
price behind the value was really read (`_jup_quote_time` from Jupiter's cache entry; curve reads = now) and `impact`
is only re-measured on a price ≤ 2 s old. `snapshot()` shows Value / uPnL / % from `live_reading()` (chart price now ×
impact, after fees; `value_src` "chart · …", the value loop's figure in `quote_upct`); a paused coin or a chart with no
candle for 15 s keeps the value loop's read. pump_rest charts still arrive 1–4 s late.

**Lotlow % / xtimes (2026-11-17, user's request)**: Candles page fields next to the pro_hold first-buy dropdown
(`lot_low_pct` default 25, `lot_x_times` default 1). Used by "buy green after red – low below" (`gar_low`) and "buy only
on ◆ BP BUY" (`bp`): lot 2 must be ≥ Lotlow % below lot 1 (gar_low: the red's low AND the green's open under the last
lot's low by that much; bp: the candle close under the last lot's candle), and each later lot xtimes the step before
(`lot_need_pct`: 33 / 2 → L2 33 %, L3 66 %, L4 132 %). A step ≥ 100 % can never happen → no further lot. Max lots /
Total cap still apply. gar_low was "any lower" before; Lotlow 0 restores that. Replaces the fixed `BP_LOWER_PCT`.

**Pump rate limit back-off (2026-11-17)**: every pump page showed "HTTPStatusError" and no coin was scanned for 20+ min —
pump answered HTTP 429 on all hosts for this connection and `fast_get` did not slow down on a 429 at all (only 403 had a
cool-down). Now `_rate_limited(url)`: more than 2 × 429 from a host inside 3 min pauses EVERY read of that host 5 s, 10,
20, 40, then 60 s at a time (`_rate_until`, checked by `_cooling`); each good answer takes one 429 off the count, so a
short overrun pauses nothing. Log: "PUMP rate limit (HTTP 429) on <host> — every read of it is paused N s".
`rate_limit_left()` gives the seconds left (not shown on the dashboard yet). Likely trigger that night: the ADAWDD
feed churn (a new feed every 2 s = chart + state reads) plus five restarts.

**Mixed feed stuck on a finished coin (2026-11-16)**: MONY stayed on the feed 8 min, flat, nothing else traded. Its
trade closed, the hand-over dropped `_mx_since[m]`, but a re-buy in flight (`busy`) kept the feed; from then on "since"
was "now" on every pick, so the coin was never done or idle. `_mixed_want` now restores the start time from
`traded[m]` (else now, once). Still open (offered): with Reuse same coin off, "gar" re-buys the coin in the ~1 s before
the next pick drops it (MONY was traded 3 times) — the guard exists only for `bp` / `signal`.

**Paused Coins (2026-11-14, user's request)**: a Candles trade whose coin pump pauses is NOT sold
(`_pause_hold`: `pos["paused_at"]`; board state for Mayhem board / Mixed feeds, pump's mayhem-state every 2 s for every
other open trade). New dashboard tab **Paused Coins** (`paused` in `/api/candle`, `paused_view()`), refreshed every
second. `_paused_loop` checks each held paused coin every second: mayhem-state active (or no state) → `_pause_resume`
(trade continues, Max hold counts from `resumed_at`, sells on profit); value < 0.0002 SOL or 2 % of cost →
`_pause_leave` (booked, coins left in the wallet). One trade at a time counts a buy still being sent (`trading()` = active + `busy` + `pending`; a second coin was bought in the ~1.6 s before a fill was booked). Open positions shows a paused coin as "paused · watching" (Max hold counts from `resumed_at`). While paused: no Max hold / stop / reserve exit, and the coin is not
fed or counted as open (`active_positions()`), so the next coin comes. Paused state is saved with the position (`positions.paused_at` / `pause_state` columns), and a restored trade's Max hold counts from the restart (`resumed_at`), so a restart neither forgets nor force-sells a paused coin. Pump refuses sells of a paused coin (error
0x1788 / 6024 seen on KRILLION; $ANIMAL sold in the first second after its board said paused).

**Completed is not paused (2026-11-15, user's request)**: pump state "completed" (the Mayhem run is over) can be sold, so
a held coin is sold off at once (`_completed_sell` → `_forced_exit`, retried every 5 s) instead of going to / staying on
the Paused Coins page — from the board state (`_pick`) and from `_paused_loop` when a paused coin turns completed. Only
"paused" is held. An open trade is also checked on pump's mayhem-state every 2 s (a coin that left the board has no
board state — HALH sat open while completed); a coin bought already completed (`was_completed`) trades normally. `paused_mints` is pruned of coins no longer held.

**Newest coin only; a resumed paused coin stays off the feed (2026-11-15, user's request)**: one trade and one feed at
a time, and the feed is always the newest coin at the moment the trade completes — no coin waits for a turn. (1) Mayhem
board: a coin whose trade completed is no longer kept (`keep` excludes `self.traded`); the pick is the board's youngest
right then (the same coin only if it still is the youngest). (2) A paused coin that moves again (G6S / PPAWS came back
next to OLRGTA5 = three open trades) never rejoins the feed / open trades (`_resume_off_feed`): it stays on the Paused
Coins watch, state "moving again — sells on profit or at Max hold", and `_moving_exit` sells it there — at once at
`green_target()`, else at Max hold counted from `moving_at`. OpenCopy + AutoTrade-Candles (several coins by design)
still resumes via `_pause_resume`. Offers / Mixed / Dayscalp / MayhemMode already took the newest at that moment.

**Completed dust (2026-11-16)**: a completed coin worth < 0.0002 SOL / 2 % of cost cannot be priced or sold (graduated, no
Jupiter route) — CHIMP was retried 124 times in 16 min and held the one-trade slot, so nothing traded. `_completed_sell`
now books it as dust (`_pause_leave`); any other completed sell that fails 6 times goes back to the normal exits.

**Pause recorder (2026-11-16, user's request)**: `CandleMomo._watch_rec` writes one line per mayhem-state read of an open
Candles trade to `logs/pause_watch.jsonl` (held_s, coin age, last 1s candle, value / upct, pump's whole state record incl.
the Mayhem agent's buy / sell counts and SOL; `event` = paused / paused_hold:…; rolls at 50 MB). The read is every second
for a trade's first 30 s, then every 2 s. Purpose: find a warning sign before a pause (paper run 2026-11-15/16: 35 of 101
buys paused, 20 within 11 s, none came back; 51 of 58 winners sold within 20 s). Not analysed yet.

**Instant take-profit (2026-11-16, user's request)**: a held coin (stream chart, not paused / closing) is re-evaluated on
EVERY trade from the trade socket, not only on the 0.5 s poll: `_on_stream_trade` (socket thread) → `_wake_trade` →
`_idle` (the wait between two polls) re-reads the stream candles (local) and runs `_evaluate` (≤ ~30/s; `m["instant_evals"]`).
A take-profit fired this way says "(instant: on a trade)" in its reason, so its refusals / fills can be compared with the
polled ones. Same rules, only checked sooner (also gar adds while holding). The sale still takes ~0.45 s to land.

**Paper exit of a coin with no price route (2026-11-16)**: a graduated / off-curve coin Jupiter gives no quote for could
not be paper-sold ("price unreadable — kept"), so SOB (−96 %) ignored every Exit click and Max hold. `_paper_sell` now
books it at its last known value (`value_sol`; reason "… no price route …") for a manual Exit (always) and for rule exits
without a candle signal (Max hold, stops) when the curve read says graduated / not_on_curve / not_sol_quote. A candle
signal sell and a failed RPC read still keep the position, as before.

**Unsold Coins (2026-11-16, user's request)**: an open Candles trade whose price cannot be read (`_value` gives nothing
or only an old cached quote; `pos["unread_since"]`, also set by a paper sell's "price unreadable — kept") for
`UNREAD_SET_ASIDE_S` 90 s is set aside by the value loop (`_set_aside`): out of Open positions (the next coin comes),
booked in Closed trades at its last known value, and listed on the new dashboard tab **Unsold Coins** (`unsold` in
`/api/candle`, `unsold_view()`, saved in `logs/candle_unsold.json`, last 200) with the reason. PAPER: always. LIVE: only
when the curve read says the coin has no price route (`NO_ROUTE`) — a failed RPC read never drops a real coin; its coins
stay in the wallet (`_live_closed`).

**CallOuts reader paused when unused (2026-11-16, user's request)**: `dashboard.callout_reader_needed()` — the CallOuts
loop (~8 pump requests / s) runs only while the Live Controls source is CallOuts or OpenCopy, or the CallOuts page was
opened in the last 30 s (`_callout_page_at`); else it sleeps (log "CALLOUTS reader paused", cached rows stay). Reason:
with MayhemMode as the source the bot went over pump's ~10 req/s and pump answered HTTP 403 in bursts (Candles' pump_rest
chart reads: 110 of 357 failed). First step of the "only the loops the source needs" idea.

**Pause after losses in a row (2026-11-16, user's request)**: common to every source, feed and rule on the Candles page.
`loss_pause_n` (2) losing closed trades in a row (`_note_result`, called at every whole close, paper and live; a win
resets `loss_streak`) → `loss_pause_until` = now + `loss_pause_s` (30 s; both on the Candles page next to Max hold, 0 =
off). While it runs: no new trade (`_route` skip `LOSS_PAUSE`, paper lot 1 too; manual Entry excepted) and, when flat,
`_pick` drops the watched coin and picks nothing — so the first pick afterwards is the source's newest coin. The header
pill shows "pause after losses · Ns". A source that buys lot 1 itself (Launch / Dayscalp live, OpenCopy live) is not held
back by it; Candles' own adds and sells are never blocked.

**Live buy limits (2026-11-14)**: Candles' live buys (source `candle`) are not under the Live Controls Max concurrent
(trader.buy: only a second buy of the same coin is refused; other open trades and leftover wallet coins don't block it; the wallet / second-buy gate is skipped for `candle`). Each lot's `px` is its REAL fill in chart units (paper: signal close × expected / got tokens; live: `_fill_px_chart` = chart close × fill price / curve price right after the fill) — GHOSTY's L1 filled 4× above its signal candle. The fast profit reading is scaled by `pos["impact"]` = real curve value / chart value from the value loop (selling a thin curve gets less than the chart shows). For the other sources a
coin pump paused (`mayhem_state_of`, or `CandleMomo.is_paused` / `paused_mints`) holds no Max concurrent slot.

**2026-11-15**: `_buy_live` still counted Candles' open trades against Max concurrent (2): FOMO + die (−96 %, Max hold 0 =
never sold) refused 707 Candles buys with `already_holding`; now only a second buy of the same coin is refused there too.
Candles Results show "Net PnL (closed)", "Open trades now" and "Net incl. open". Not built (asked the user): a rule for
dead held coins when Max hold is off.

**Candles buys: Lightning / Local only (2026-11-15, user's request)**: Candles page switch "Buy only via Lightning /
Local" (`buy_jupiter`, default false = on). `tx_route.jupiter_buy_off(side, reason)`: a Candles buy (`candle`,
`candle_add`) never goes to Jupiter; when Jupiter was the only route left, `run_swap` returns `no_route: Jupiter buys are
off …` and Candles drops the coin (`unusable`) and takes the next. Sells keep Jupiter as the last fallback. Paper drops a
`not_sol_quote` coin the same way. Reason: 30 of 39 buys that day were pump coins priced in another coin (USDC …) that
Lightning refuses ("Pool account not found") — Jupiter took ~4.5 s, opened extra accounts, and FOMO / J*B were among them.
This supersedes "non-SOL-quoted coins route Lightning first, Jupiter fallback" for buys unless the switch is turned off.

**Paid RPC picker (2026-11-15, user's request)**: Settings page card "Paid RPC" — dropdown of the paid RPCs (host only),
Add RPC field, Remove button (`/api/rpc` GET / POST `select` | `add` | `remove`; `rpc_pool.settings_view`, `select_rpc`,
`add_rpc`, `remove_rpc`). "Automatic" (default) = the ordered failover through all paid RPCs as before. One picked =
`paid_urls()` is that one alone, so a failure goes to the public nodes, not to another paid RPC. Pick / added / removed
are saved in `logs/rpc_choice.json` (URLs carry keys: never in Git, never sent to the browser — ids are `rpc_id`);
`config/sniper.yaml` is not rewritten (a removed yaml RPC is only hidden). `all_paid_urls()` keeps every known paid URL out
of the public lists. Takes effect within 10 s, no restart.

**PnL = the wallet's real SOL (2026-11-15, user's request)**: checked against all 68 on-chain transactions of the session
(wallet −0.0044 while the Dashboard said +0.0507 and Candles +0.0076). Fixes: (1) `exec_history._parse_wallet_tx` — only
the traded coin's own token account is a deposit; rent of any other account a route opens (~0.0015 SOL, Jupiter routes)
is part of the BUY / SELL amount. (2) Wallet list keeps 150 transactions (`WALLET_KEEP`, was 50; page still shows 50).
(3) `reconcile_wallet_sells` keeps the ledger's cost when the wallet list shows fewer buys than the ledger counted
(`incomplete`), counts each buy signature once, and never stores a ladder buy's signature as the open's own.
(4) `_chain_cash` uses the ledger close when the listed buys don't cover the coins sold. (5) Dashboard line "Bought and
not sold" (`pnl_unsold_sol`: OPEN rows of the session with no sale and no open trade — dust is still never booked as a
sale). (6) `CandleMomo._sync_wallet_cash` (every 20 s) copies the ledger's wallet figures into Candles' closed LIVE
trades (last 24 h, one ledger sale within 180 s). Known leftover: each restart adds a 0-PnL J*B row to Candles' Closed
trades (dust coin restored, then flagged dust again).

**Graduated / non-SOL-quoted pump coins (2026-11-14/15, user's request)**: Candles trades coins off the pump curve (graduated / PumpSwap) and pump coins whose curve is quoted in another coin (`not_sol_quote`, common among callers' coins).
Paper buys and sells are priced by Jupiter quotes at send and fill_lag_ms later (`_jup_quote_buy` / `_jup_quote_sell`,
pool fee inside the quote) instead of dropping the coin as NOT_TRADABLE; live buys go Lightning → Jupiter
(`tx_route`, off-curve remembered), and a floored (profit) sell of an off-curve coin first checks a Jupiter quote against
the floor, then sells via Lightning / Jupiter. Values use the existing Jupiter fallback in `_value`. Charts come from
pump's candle API (the stream has no trades for these). A pinned coin is not re-added while a trade is open. **Pinned coins (Pinned mints) under pro_hold enter ONLY on the chart's ▲ BUY** (Pro Trader call; lot 1, a new ▲ BUY adds up to Max lots / Total cap) — no first-candle / green-after-red / ◆ BP entries for them; they sell on their own rule: once the whole trade is ≥ **Pinned profit %** (`pinned_min_profit_pct`, default 1.5 %, after fees) it rides the run of greens and sells on the first red (forming candle) while still ≥ it; under it, hold (never a loss sale) until Max hold. Other sources / feeds unchanged.

**Trade source in the Mode column (2026-11-14)**: Candles Open positions / Closed trades show "paper · Launch",
"LIVE · Candles (Mixed: New)" … from `CandleMomo._src_label` (feed origin / support source), stored by `candle_store`
(`src` column on positions and trades; `Store.src_hook` fills it at open, close copies it).

**One feed + sell at once on profit (2026-11-14, user's request, repeated)**: `_pick` → `_pick_feeds` then
`_single_feed()`: for every source / feed, while a trade is open only the held coin(s) are fed; flat, one coin (Coins >1
no longer adds feeds). pro_hold sells AT ONCE when the whole trade (all lots, after ~2 % fees + network fees) is ≥
`green_target()` — read from the chart price on every 0.5 s poll via each lot's buy price (`_fast_upct`, falls back to
the value loop's fresh `upct`), retried every 1 s; no red-candle wait and no `_in_profit` RPC re-check (the break-even
floor in the sell stays). The forming-red / candle-close take-profit below is now only a backstop.

**Source Sequence (2026-11-14, user's request)**: Candles page button (`source_seq`, default off). On: `_seq_loop` steps
through `SEQ_STEPS` MayhemMode → Candles · Mayhem board → Candles · Mixed → (Dayscalp / CallOuts / OpenCopy removed 2026-11-14, Launch removed 2026-11-16) →
again. Source steps switch the Live Controls source through the dashboard config handler (`_seq_apply_source`, same as
the Settings page) with AutoTrade-Candles on (`update`, no wipe, Live/Paper untouched); board steps set `seq_board`
(`support_source()` = "", `follow()` = the board) while MayhemMode stays the source and only offers. A step waits up to
`SEQ_STEP_S` 60 s for a Candles trade; an opened trade is waited for until it closes (sell / Max hold), then next step
at once. Never switches while a Candles trade is open / in flight. Off: the current source stays.

**OpenCopy + AutoTrade-Candles, LIVE: OpenCopy buys first, Candles is the only exit (2026-11-16, user's request)**:
`CandleMomo.source_first()` (opencopy + Candles LIVE). A coin reaches Candles only after OpenCopy's own Auto Trade bought,
pinned and holds it (lot 1; OpenHolds buys instead of offering: `pro_hold_owns_entries("opencopy")` is False, whatever
the rule; `feeds_only("opencopy")` is False in LIVE, so no offers / `openhold_recent_coins` feed). `_support_sync` takes the
held coin over; Candles adds lots and sells by its rule and never opens lot 1 itself (`SUPPORT_NO_ENTRY`, manual Entry
excepted). **Exit: Candles' rule alone** — `source_sell_ok` returns False for these trades in profit or loss, so OpenHolds'
exits (caller sold, FAST PACE, stops, time stop) never sell; pump-paused, a manual Sell and Candles' sells still go
through. Several coins together as before (`support_source() == "opencopy"`). AutoTrade-Candles off = normal OpenCopy
(buy, sell, caller_sold). **In PAPER nothing changed**: OpenCopy stays feed-only (offers, Candles paper-buys), so a paper
test never starts live OpenCopy buys. This replaces the OpenCopy part of the feed-only paragraph below for LIVE.

**AutoTrade-Candles feed-only sources (2026-11-14, user's request)**: with AutoTrade-Candles on, CallOuts / OpenCopy /
MayhemMode (`FEED_ONLY_SOURCES`, `feeds_only()`) do not buy, pin or hold: OpenHolds' auto rows (callers' new opens,
Mayhem_Mode opens) and CallOuts' auto buy call `CandleMomo.offer(src, mint)` instead (main.py); Candles feeds the newest
offers (Max mints, pro_hold one; an offer drops after `mixed_idle_s` without a trade or once its trade closes) and buys lot 1
by its own rule (no SUPPORT_NO_ENTRY), Live or Paper as the Candles switch. Manual Buy clicks still buy. CallOuts also feeds directly (2026-11-15): with AutoTrade-Candles on, the 3 newest OPEN pump rows of the CallOuts page (`_callout_rows`) are offered every pick, without waiting for CallOuts' own buy trigger. OpenCopy likewise (2026-11-15): every coin the selected callers opened in the last 15 min that OpenHolds lists (`openhold_recent_coins`, stateless; pump mints only — start or end with "pump") is offered at once, and **OpenCopy alone may feed and trade several coins together** (`OPENCOPY_MAX_FEEDS` = 5; `_single_feed` / `_one_feed` exempt it). Every other source stays one coin at a time. OpenCopy feeds only coins opened after AutoTrade-Candles was ticked (`support_since`, set in `set_support`; 0 → the bot's start). **Live buys on the feed-only sources no longer need Auto Trade** (Candles' Live switch is the gate; Launch / Dayscalp still need it). A pump coin whose curve is quoted in another coin (USDC …) is routed **Lightning first, Jupiter fallback** (`tx_route`, was Jupiter only; not yet seen landing — unverified that PumpPortal can buy them). Off: sources
trade by their own rules. Dayscalp / Launch keep buying lot 1 themselves (Live, non-pro_hold).

**AutoTrade-Candles in Paper (2026-11-14, user's request)**: the Candles Live/Paper button works with AutoTrade-Candles
on (turning AutoTrade-Candles on still sets Live). In PAPER (`paper_support()`): Candles owns entries for every source
(`pro_hold_owns_entries` is true) — Dayscalp only feeds its scan token, MayhemMode's grab and auto-open only watch; the
source's lot 1 is played as a paper buy at once (`_paper_lot1`, sig `mirror`, rules other than pro_hold; pro_hold buys
on its signals, Auto Trade not needed); live source trades are not taken over (`_support_sync` returns, `_candle_support`
False, `supports()` False). OpenCopy / CallOuts / Launch still trade live by their own Auto Trade. Live existing support
trades keep being managed live.

**pro_hold extra entries (2026-11-14, user's request)**: besides ◆ BP, pro_hold also buys (a) a NEW coin (board age
≤ 60 s, `PRO_FIRST_MAX_AGE_S`) as `pro_hold_first_at` says: "now" = at once on arrival (0.00%, any coin it is given, skips FAR_BELOW_PEAK too), "gar" = buy every green after a red (forming candle, once per candle, `_gar`): flat and under the coin's high, or holding in a loss (adds up to Total cap), never in profit; it always starts with a buy at first sight (like "now": any age or history) and its signals skip SOL_OUTFLOW, Min reserve, FAR_BELOW_PEAK and the cooldown (sig `gar`) — only Total cap stops it; "flat" = only while the chart shows no candle yet (one reading moved < `FLAT_PCT` 0.05 %; 2+ candles = no first buy), "start" (default) = as soon as its chart STARTS (: the feed holds ≤ 2 candles, no move yet — `PRO_FIRST_START_CANDLES`; a coin first seen with more history waits for a green start), or ("green") the moment its first green STARTS (the forming candle trades above its open = last close; checked every 0.5 s poll, not at the candle close; a red waits while ≤ 60 s) (`pro_hold_first`, sig `first`: skips the SOL_OUTFLOW history check
and Min reserve; waits for the Mayhem check), and (b) the chart's green ▲ BUY (Pro Trader call, `pro_hold_pro_buy`).
Switches on the Candles page ("pro_hold also buys"). Tests on unseen coins with pro_hold exits (15 %): BP 75 % won,
median +21.6 %; first candle (3 s in) 58 %, +7.6 %; ▲ BUY 47 %, −0.6 % (−1.9 % w/o top 10 %) — data are Mayhem coins,
pauses not modelled. All sources: Mayhem board / Mixed know the coin age from the board; with AutoTrade-Candles,
Dayscalp's scan tokens carry `age` (LAST_CANDLE_PICKS) and MayhemMode's young opens their open age, so their feeds get
the first-candle buy too; OpenCopy / CallOuts / Launch buy lot 1 themselves (that is their first-candle buy) and
pro_hold adds on ◆ BP / ▲ BUY.

**pro_hold "buy green after red – low below" (2026-11-15, user's request)**: new entry in the pro_hold first-buy
dropdown (`pro_hold_first_at` = `gar_low`). Lot 1 at the start like "gar"; flat re-entries as "gar". While holding, a
green after a red adds a lot only when that red's low AND the green's open are not higher than the last lot's low
(`_lot_low`: lot 1 = the low of the candle it was bought in, `lot_t`; lots 2+ = their own min(red low, green open),
`low_ref`, both stored on the lot) — each lot sits below the one before; no trade-in-loss reading needed. Max lots /
Total cap still limit it. "gar" and every other choice are unchanged. Fixed the same day (SXSN): lot 1 was bought on pump_rest
(2.157e-06) and the chart then ran on the stream (2.5e-08, other units), so every candle looked below lot 1 and lot 2
was bought above it — a lot from another feed is now compared by this chart's candle at its fill time
(`_lot_low(…, f.using)`), and "below" is strictly lower. Chart lot markers sit on the candle the order was SENT in (`_mark_ts`, `lot_t`; fill
> 2 s later keeps the fill time) — drawn at the fill ~0.5 s later, L3 / L4 looked like the 2nd green after the red.

**pro_hold "buy only on the ▲ BUY signal" (2026-11-16, user's request)**: first-buy dropdown `pro_hold_first_at` =
`signal` (`sig_only` in `_pro_evaluate`): no first-candle, green-after-red or ◆ BP buy — a lot is bought only on the
chart's ▲ BUY (Pro Trader call, read at each closed candle; reason "▲ BUY signal — …"), every new ▲ BUY adds a lot up to
Max lots / Total cap, and it sells at `green_target()` like every pro_hold trade (not the pinned-coin rule). These buys skip the
SOL_OUTFLOW, FAR_BELOW_PEAK, Min reserve and cooldown checks (sig `first` + `now_buy` + `gar`; Doggles' first two ▲ BUY
tags were refused by SOL_OUTFLOW) — Skip Mayhem coins, Max lots and Total cap still apply. On a coin fed by pump_rest
(no stream trades) candles arrive 1–4 s late, so the lot lands that much after the tag: not fixable in the bot. The Pro Trader read needs 25 candles, so trades are rare on young coins. A ▲ BUY on a candle that arrived in a batch
(pump_rest delivers several at once; only the newest was read — tags at 02:35:10 / 02:35:14 on a pinned coin never
bought) still buys when it is ≤ 5 s before the newest candle (`_late_buy`, also for pinned coins; reason "… arrived late").

**pro_hold "buy only on ◆ BP BUY – each lot 25% lower" (2026-11-16, user's request)**: first-buy dropdown
`pro_hold_first_at` = `bp` (`bp_only` in `_pro_evaluate`): no first-candle, green-after-red or ▲ BUY buy (the ▲ BUY
checkbox is ignored). Lot 1 on the first ◆ BP BUY (Botpredict's call at a closed candle); while holding, a ◆ BP BUY
buys only when its candle closed ≥ `BP_LOWER_PCT` 25 % below the candle of the lot before (`_lot_close`, same feed-unit
handling as `_lot_low`) — not once per signal run; up to Max lots / Total cap; sells at `green_target()`. Like `signal`,
these buys skip SOL_OUTFLOW / FAR_BELOW_PEAK / Min reserve / cooldown and catch a signal on a late candle (`_late_buy`).
Fixed the same day (WARM / DICE bought twice): for `bp` and `signal`, no lot-1 signal while a buy of the coin is in
flight (a second ◆ BP BUY 1 s later was queued and bought as lot 2 with no 25 % check) and, with **Reuse same coin off**,
no new trade on a coin Candles already traded (it was re-bought 1 s after its profit sale; these buys skip the cooldown
and the feed is only dropped at the next pick). Why the chart shows ◆ BP tags in a row: the tags are a REPLAY of Botpredict's own plan (buy, exit at +40 % / −8 % / 60 s,
buy again the next candle while the chance is still ≥ 21 %) — on a crashed coin lying at its low the chance stays high
and each pretend trade is stopped out within a candle or two.

**Candles avoid_mayhem (2026-11-14, user's request)**: Candles page "Skip Mayhem coins" (default on): no new
Candles trade on a Mayhem coin (`MAYHEM_COIN` in `_entry_filters`; board `ms` or pump's mayhem-state, checked once per
coin `_check_mayhem`); Mixed skips Mayhem rows. Reason: pump pauses Mayhem coins and a paused coin can't sell —
pro_hold 18:06–23:57 on 2026-11-13: take-profit 32 trades +0.131 SOL, coin paused 27 trades −0.174, max hold 10 −0.070.
The Mayhem board feed and the MayhemMode source are exempt (all Mayhem coins by choice, `_avoid_mayhem_for`). Trade stream: Candles' coins go first in the
220-coin subscription list (`_HEAD_MINTS`); stream candles use each trade's own price (`trade_px`). Graduated
(PumpSwap) coins still get no stream trades (pump_rest only) — not solved.

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
trade is in profit after fees (`green_target()`: Min profit, or Fast Pace margin); **SELL signal (2026-11-13, user's
request)**: once the trade is ≥ that target the value loop only arms it (`pro_armed`, `pro_peak`) and the chart makes a
SELL signal on the first red 1s candle while still ≥ target (`_pro_hold_take`, fresh value ≤ 3 s, re-checked by
`_in_profit` before the sell; below target no SELL ever) — tested on 76 unseen coins at 15 %: median +21.6 % vs +20.1 %
selling at once, late entry +14.4 % vs +12.8 %; since 2026-11-14 the take-profit also fires on the candle still FORMING once it trades below its open (`take_t`, once per candle), not only at the red candle's close; Candles' live buys / sells (reason `candle*`) skip the separate simulateTransaction round trip before sending (`_send_keep_fee(presim=False)`; the RPC preflight still checks; a failing tx can occasionally pay its fee); Candles' pump sells use 95 % slippage (`tx_route.sell_slippage_bps`) and paper never refuses a sell for slippage, while pro_hold's profit sell always carries the break-even floor (`_floored`), so it goes through but never at a loss; Candles' first sell try skips the ~0.6 s pre-sell wallet read (`trader._fast_sell_at`, a retry within 10 s reads it); in a loss hold until Max hold, then sell; no stop, no red-candle ladder. **Every new BUY signal buys again** (Max position; limited by **Max lots** and Total cap — Max lots was ignored before 2026-11-14, `LADDER_MAX`) into
the open trade up to Total cap (`_fit_cap`); a signal that stays on for several candles buys once (`f.pro_was_buy`).
With AutoTrade-Candles on, **pro_hold buys lot 1 itself** (user's choice 2026-11-14): Dayscalp only picks the first
scan token and feeds it (`dayscalp._candle_rule() == "pro_hold"` → no Dayscalp buy); Candles buys on the chart BUY
(live, Auto Trade must be on, else `AUTO_TRADE_OFF`). Other rules keep `SUPPORT_NO_ENTRY` (the source buys lot 1).
MayhemMode's young-open grab still buys through MayhemMode.
Under pro_hold Candles watches **one coin at a time** (user's choice 2026-11-14, `PRO_HOLD_WATCH = 1`): Dayscalp's
first scan token; while a trade is open no new coin is fed; once it completes the next scan token (Candles' traded
coins are skipped). Its signals are rare (Botpredict's top 2 % moments), so expect quiet stretches.
This holds for every AutoTrade-Candles source (`_one_feed`) and, since 2026-11-14, for the Mayhem board feed too (no new pick while a trade is open): one feed = the coin in trade (oldest; a paused coin
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
- After code changes: tests → rebuild `deploy/caller_stream_update.tar.gz`
  (`tar czf deploy/caller_stream_update.tar.gz -T deploy/caller_stream_files.txt` — the list is in the repo since
  2026-11-16; it was rebuilt from the 2026-11-12 bundle after a reboot wiped the temp copy) → safe restart →
  confirm no `Traceback` in `logs/run.out`.
- Commit with the user's name; push to `origin main` only when the user asks.
