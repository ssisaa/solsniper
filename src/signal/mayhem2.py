"""Mayhem2: copy Mayhem_Mode's BUY / SELL trades on one young Mayhem coin at a time.

1. Discovery   advanced-indexer boards/mayhem: Mayhem coins, youngest first (age in seconds,
               ms = mayhem state). Only pump coins younger than 3 minutes are candidates.
2. Enrichment  mint-positions/<mint>: Mayhem_Mode's pnlUsd / pnlPercentage / amountHeld per coin.
3. Pin         the youngest active candidate that Mayhem_Mode is holding. One pin at a time.
               Its trades (trades/...?wallets=<Mayhem_Mode>) are read every 0.25s, and the
               on-chain trade socket (caller stream) feeds the same handler when it sees them
               first. Newest side wins: BUY -> buy Max position when flat; SELL -> sell 100%
               when holding. Signals older than the pin are never acted on.
4. Pause       coins-v2/<mint>/mayhem-state every 0.5s (and the board's ms every 0.8s).
               paused -> sell what is held, unpin, move to the next new coin. Mayhem_Mode no
               longer holding it (two reads) -> the same.

Trades only while Live Controls source is mayhem2 and Auto Trade is on. The page reads run while
the source is on or the Mayhem2 page was opened in the last 30s.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import deque
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("mayhem2")

BOARD_URL = "https://advanced-indexer.pump.fun/boards/mayhem?tier=web&offset=0&limit=30"
POS_URL = "https://frontend-api-v3.pump.fun/mint-positions/{mint}?sortBy=TOP&pageSize=50"
TRADES_URL = "https://frontend-api-v3.pump.fun/trades/solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/{mint}?wallets={wallet}"
STATE_URL = "https://frontend-api-v3.pump.fun/coins-v2/{mint}/mayhem-state"
# Mixed feed (Mayhem2 page checkbox "Mixed"): New first, then Movers, then Graduated.
MIXED_BOARDS = (
    ("new", "https://advanced-indexer.pump.fun/boards/new?tier=web&chains=solana&offset=0&limit=30", 0.8),
    ("movers", "https://advanced-indexer.pump.fun/boards/movers?tier=web&surface=WEB&platform=WEB&limit=150&chains=solana", 3.0),
    ("graduated", "https://advanced-indexer.pump.fun/boards/graduated?tier=web&offset=0&limit=30", 3.0),
)
MIXED_GROUPS = tuple(g for g, _u, _e in MIXED_BOARDS)
ALL_TRADES_URL = "https://frontend-api-v3.pump.fun/trades/solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/{mint}"
JUP_CURVE_S = 1.5        # a coin off pump's curve (graduated) is priced by a Jupiter quote, cached this long

YOUNG_S = 180.0          # discovery window: coins younger than 3 minutes
TRADES_EVERY = 0.25
STATE_EVERY = 0.5
BOARD_EVERY = 0.8
POS_EVERY = 0.35         # one mint-positions read per tick, pinned coin first
STALE_SIGNAL_S = 6.0     # a trade older than this when first seen is history, not a signal

MIN_SIGNAL_SOL = 0.001   # a Mayhem_Mode trade smaller than this is noise: no signal, no price

# Exits (percent since our buy, from the live price). Sell fees are ~2%, so +3% is a real profit.
PROFIT_MIN_PCT = 3.0     # Mayhem_Mode SELL: sell now only at or above this
WAIT_PROFIT_S = 10.0     # ...otherwise wait up to this long for it, then sell anyway
TAKE_PROFIT_PCT = 20.0   # sell at once, no signal needed (these coins spike, then crash)
TRAIL_ARM_PCT = 10.0     # after the peak reached this...
TRAIL_KEEP = 0.5         # ...sell when half of the peak gain is gone
STOP_PCT = -15.0         # sell at once
# Paper strategies run side by side: (enter on, exit on signal, uses TP/stop/max hold)
SHADOWS = {
    "copy":           ("buy",  "sell", 0.0),    # Mayhem_Mode BUY -> buy, its SELL -> sell
    "inverse":        ("sell", "buy",  0.0),    # its SELL (price pushed down) -> buy, its BUY -> sell
    "inverse_tp":     ("sell", "",     0.0),    # SELL -> buy, exit only by TP / stop / max hold
    "inverse_big":    ("sell", "buy",  0.05),   # only after a SELL of >= 0.05 SOL
    "inverse_big_tp": ("sell", "",     0.05),
    "ladder":         ("buy",  "sell", 0.0),    # BUY -> buy; SELL -> sell only in profit; BUY under last buy -> buy again
    "inv_ladder":     ("sell", "buy",  0.0),    # Candles inverse ladder: SELL (red) -> buy, every 3rd red -> lot, BUY (green) -> sell all
}
SHADOW_RULES = {
    "copy": "BUY→buy, SELL→sell", "inverse": "SELL→buy, BUY→sell", "inverse_tp": "SELL→buy, exit TP/stop/25s",
    "inverse_big": "SELL ≥0.05 SOL→buy, BUY→sell", "inverse_big_tp": "SELL ≥0.05 SOL→buy, exit TP/stop/25s",
    "ladder": "BUY→buy, SELL→sell if in profit, BUY below last buy→buy again (max 3)",
    "inv_ladder": "inverse ladder: SELL (red)→buy lot 1, leave 3 reds, next red→next lot below the last buy (lots on reds 1, 5, 9; max 3), BUY (green)→sell all",
}
LADDERS = ("ladder", "inv_ladder")      # multi-buy strategies: no stop, no max hold
INV_REDS_PER_LOT = 4                    # inv_ladder: after a lot, leave 3 reds, buy on the 4th (lots on reds 1, 5, 9)
# Ladder: up to this many buys into one trade. A SELL signal sells only at PROFIT_MIN_PCT or better
# on the whole trade; otherwise it holds. No stop and no max hold: pump pausing / completing the coin,
# or Mayhem_Mode no longer holding it, is the way out of a losing ladder.
LADDER_MAX_BUYS = 3
SHADOW_LAND_S = 0.45
USED_KEEP_S = 6 * 3600.0  # coins left / traded stay off-limits this long (they are past 3 minutes old long before)
IDLE_UNPIN_S = 30.0      # no Mayhem_Mode trade on the pin this long, nothing held: next new coin
MAX_HOLD_S = 25.0        # sell any trade held this long: half of Mayhem coins pause by ~34s old


def _wallet() -> str:
    from src.config.runtime import MAYHEM_WALLET
    return MAYHEM_WALLET


def _user() -> str:
    from src.config.runtime import MAYHEM_USER
    return MAYHEM_USER


def _f(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def _used_path():
    from pathlib import Path
    return Path(__file__).resolve().parents[2] / "logs" / "mayhem2_used.json"


def _load_used() -> Dict[str, Dict[str, Any]]:
    """Coins already traded / left, from before a restart: never pinned again."""
    import json
    try:
        data = json.loads(_used_path().read_text(encoding="utf-8"))
    except Exception:
        return {}
    cut = time.time() - USED_KEEP_S
    return {m: v for m, v in (data or {}).items() if isinstance(v, dict) and float(v.get("ts") or 0) >= cut}


def _fill_px(fill) -> float:
    """Lamports per raw token this buy paid (same units as the curve's virtual_sol / virtual_token)."""
    try:
        sol = float(fill.sol_in or 0)
        tok = float(fill.tokens_out or 0)
    except (TypeError, ValueError, AttributeError):
        return 0.0
    return sol * 1e9 / tok if sol > 0 and tok > 0 else 0.0


class Mayhem2:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.board: List[Dict[str, Any]] = []
        self.board_at = 0.0
        self.board_err = ""
        self.pos: Dict[str, Dict[str, Any]] = {}       # mint -> Mayhem_Mode position on that coin
        self.state: Dict[str, Dict[str, Any]] = {}     # mint -> {state, reason, at}
        self.pin: Optional[Dict[str, Any]] = None
        self.used: Dict[str, Dict[str, Any]] = _load_used()   # mints already traded / passed: never re-pinned
        self.seen_tx: Dict[str, float] = {}
        self.signals: deque = deque(maxlen=30)         # pinned coin's Mayhem_Mode trades, newest first
        self.notes: deque = deque(maxlen=200)
        self.view_at = 0.0
        self.want = ""                                  # newest signal side on the pin: buy / sell
        self.want_at = 0.0
        self.busy = ""                                  # "buy" while a buy is in flight
        self.last_px: Dict[str, float] = {}
        self.manager = None
        self.trader = None
        self.fill_px: Optional[Callable[[Any], float]] = None
        self.apply_since: Optional[Callable[[Any], None]] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.gone_reads: Dict[str, int] = {}
        self.kicked: Dict[str, float] = {}             # mint -> last sell start (retry at most every 2s)
        self.exiting: Dict[str, str] = {}              # mint -> why: sell decided, retried until gone
        self.wait_profit: Dict[str, float] = {}        # mint -> when Mayhem_Mode sold while we were not in profit
        self.sell_after_buy = False
        self.unresolved: Optional[Dict[str, Any]] = None   # a sent buy whose landing was not confirmed
        self.paper: Dict[str, Any] = {}                # mint -> dry-run holding (nothing on chain)
        self.paper_log: deque = deque(maxlen=200)      # closed dry-run trades
        self.lat: deque = deque(maxlen=200)            # ms from seeing the signal to a signed, simulated tx
        self.land: deque = deque(maxlen=200)           # live: ms from the signal to the buy landing
        # Side-by-side paper strategies on the same signals (curve-priced, 0.45s landing, 15% drift check).
        self.shadow: Dict[str, Dict[str, Any]] = {k: {"pos": None, "log": deque(maxlen=300), "skips": 0,
                                                      "marks": deque(maxlen=200), "done": {}}
                                                  for k in SHADOWS}
        # The bot's own buys / sells (live or dry run) for the chart: {ts, mint, side, label}.
        self.marks: deque = deque(maxlen=200)
        self.mixed_rows: Dict[str, List[Dict[str, Any]]] = {g: [] for g in MIXED_GROUPS}
        self.mixed_at: Dict[str, float] = {g: 0.0 for g in MIXED_GROUPS}
        self.feed_was = ""
        self.jup_curve: Dict[str, Any] = {}             # mint -> (pseudo curve, at): coins off pump's curve
        self.started = False

    # ---------- switches ----------
    def trading(self) -> bool:
        try:
            from src.config.runtime import bot_stopped, get_config, mayhem2_on
            cfg = get_config()
            return mayhem2_on() and bool(cfg.trade_enabled) and not self.dry() and not bot_stopped()
        except Exception:
            return False

    def dry(self) -> bool:
        """Dry run (default): every signal is built, signed and simulated, never sent."""
        try:
            from src.config.runtime import get_config
            return bool(getattr(get_config(), "mayhem2_dry_run", True))
        except Exception:
            return True

    def strategy(self) -> str:
        """Live Controls strategy for Mayhem2 (Settings / Mayhem2 page). Unknown -> inverse_tp."""
        try:
            from src.config.runtime import get_config
            name = str(getattr(get_config(), "mayhem2_strategy", "") or "")
        except Exception:
            name = ""
        return name if name in SHADOWS else "inverse_tp"

    def feed(self) -> str:
        """Coin feed (Mayhem2 page checkboxes): mayhem = Mayhem_Mode's coins and trades (unchanged);
        mixed = pump boards New, Movers, Graduated, and every wallet's trades on the pinned coin."""
        try:
            from src.config.runtime import get_config
            v = str(getattr(get_config(), "mayhem2_feed", "") or "").lower()
        except Exception:
            v = ""
        return "mixed" if v == "mixed" else "mayhem"

    def mixed(self) -> bool:
        return self.feed() == "mixed"

    def _who(self) -> str:
        return "the coin's traders" if self.mixed() else "Mayhem_Mode"

    def paper_ok(self) -> bool:
        try:
            from src.config.runtime import bot_stopped, mayhem2_on
            return mayhem2_on() and self.dry() and not bot_stopped()
        except Exception:
            return False

    def _held(self, mint: str = ""):
        """The open Mayhem2 holding on this coin: a live trade, else a dry-run one."""
        live = self._open_pos(mint)
        if live is not None:
            return live
        for m, p in list(self.paper.items()):
            if p.status == "OPEN" and (not mint or m == mint):
                return p
        return None

    def active(self) -> bool:
        try:
            from src.config.runtime import mayhem2_on
            if mayhem2_on():
                return True
        except Exception:
            pass
        return time.time() - self.view_at < 30

    def note(self, level: str, message: str, mint: str = "") -> None:
        now = time.time()
        with self.lock:
            if self.notes and self.notes[0]["m"] == message and now - self.notes[0]["ts"] < 20:
                self.notes[0]["n"] = int(self.notes[0].get("n") or 1) + 1
                self.notes[0]["ts"] = now
                return
            self.notes.appendleft({"ts": now, "level": level, "m": message, "mint": mint, "n": 1})
        log.info("MAYHEM2 %s", message)
        if level in ("buy", "sell", "ok", "warn"):
            try:
                from src.observability.action_log import record_action
                record_action("MAYHEM2", level="WARN" if level == "warn" else "INFO", detail=message[:220])
            except Exception:
                pass

    # ---------- reads ----------
    def _get(self, url: str, lane: str, timeout: float = 1.0) -> Any:
        from src.data.fast_get import mayhem2_get
        return mayhem2_get(url, lane=lane, timeout=timeout)

    def _open_pos(self, mint: str = ""):
        mgr = self.manager
        if mgr is None:
            return None
        for p in list(mgr.positions.values()):
            if p.status != "OPEN" or not p.mint:
                continue
            if "mayhem2" not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
                continue
            if mint and p.mint != mint:
                continue
            return p
        return None

    def _other_open(self, mint: str):
        """An open Mayhem2 trade on another coin that can still sell (dust does not count)."""
        mgr = self.manager
        if mgr is None:
            return None
        for p in list(mgr.positions.values()):
            if p.status != "OPEN" or not p.mint or p.mint == mint:
                continue
            if "mayhem2" not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
                continue
            if getattr(p, "_dust_value", False):
                continue
            try:
                from src.storage.mint_flags import mint_flag
                if mint_flag(p.mint) == "DUST":
                    continue
            except Exception:
                pass
            return p
        return None

    async def _mixed_boards(self) -> None:
        """Mixed feed: read New every 0.8s, Movers and Graduated every 3s (one request each)."""
        now = time.time()
        due = [(g, u) for g, u, every in MIXED_BOARDS if now - self.mixed_at.get(g, 0.0) >= every]
        if not due:
            return

        async def one(g: str, u: str):
            try:
                body = await asyncio.to_thread(self._get, u, "mayhem2board" + g[:3], 3.0)
                return g, list((body or {}).get("entries") or []) if isinstance(body, dict) else None, ""
            except Exception as exc:
                return g, None, f"{g}: {type(exc).__name__}: {exc}"[:120]

        res = await asyncio.gather(*(one(g, u) for g, u in due))
        with self.lock:
            errs = []
            for g, rows, err in res:
                if rows is not None:
                    self.mixed_rows[g] = [dict(r, _grp=g) for r in rows]
                    self.mixed_at[g] = time.time()
                elif err:
                    errs.append(err)
            self.board = [r for g in MIXED_GROUPS for r in self.mixed_rows.get(g) or []]
            self.board_at = max(self.mixed_at.values() or [0.0])
            self.board_err = "; ".join(errs)

    async def _board_loop(self) -> None:
        while True:
            if not self.active():
                await asyncio.sleep(0.5)
                continue
            t0 = time.time()
            feed = self.feed()
            if feed != self.feed_was:
                self._feed_changed(feed)
            if feed == "mixed":
                try:
                    await self._mixed_boards()
                except Exception:
                    log.debug("mayhem2 mixed boards failed", exc_info=True)
                await asyncio.sleep(max(0.05, BOARD_EVERY - (time.time() - t0)))
                continue
            try:
                body = await asyncio.to_thread(self._get, BOARD_URL, "mayhem2board", 1.5)
                rows = list((body or {}).get("entries") or []) if isinstance(body, dict) else []
                with self.lock:
                    self.board = rows
                    self.board_at = time.time()
                    self.board_err = ""
                    for r in rows:
                        mint = str(r.get("m") or "")
                        ms = str(r.get("ms") or "").lower()
                        if mint and ms:
                            prev = self.state.get(mint) or {}
                            # the coin's own mayhem-state read is newer than the board: keep it
                            if float(prev.get("at") or 0) < time.time() - 1.0 or prev.get("src") == "board":
                                self.state[mint] = {"state": ms, "reason": prev.get("reason", ""),
                                                    "at": time.time(), "src": "board"}
            except Exception as exc:
                with self.lock:
                    self.board_err = f"{type(exc).__name__}: {exc}"[:120]
            await asyncio.sleep(max(0.05, BOARD_EVERY - (time.time() - t0)))

    def _feed_changed(self, feed: str) -> None:
        """Checkbox switched: drop the other feed's board; a pin with nothing held is let go (not marked used)."""
        first = not self.feed_was
        self.feed_was = feed
        with self.lock:
            self.board = []
            self.board_at = 0.0
            self.board_err = ""
            for g in MIXED_GROUPS:
                self.mixed_rows[g] = []
                self.mixed_at[g] = 0.0
        if first:
            return
        pin = self.pin
        if pin and self._held(pin["mint"]) is None and self.busy != "buy":
            self.pin = None
            self.want = ""
        self.note("info", "Feed: Mixed — New, then Movers, then Graduated (youngest first in each); every wallet's "
                  "BUY / SELL on the pinned coin is the signal." if feed == "mixed"
                  else "Feed: Mayhem_Mode — its Mayhem coins and its BUY / SELL.")

    def _mixed_candidates(self) -> List[Dict[str, Any]]:
        """Mixed feed: New (under 3 minutes), then Movers, then Graduated; pump coins only; youngest
        first inside each group; a coin on two boards counts once (its first group)."""
        now = time.time()
        with self.lock:
            groups = {g: list(self.mixed_rows.get(g) or []) for g in MIXED_GROUPS}
            ats = dict(self.mixed_at)
            states = dict(self.state)
        out, seen = [], set()
        for g in MIXED_GROUPS:
            shift = max(0.0, now - ats.get(g, 0.0)) if ats.get(g) else 0.0
            part = []
            for r in groups[g]:
                mint = str(r.get("m") or "")
                if not mint or mint in seen or str(r.get("pg") or "") != "pump":
                    continue
                age = float(r.get("age") or 0) + shift
                if g == "new" and age > YOUNG_S:
                    continue
                st = str((states.get(mint) or {}).get("state") or r.get("ms") or "").lower()
                if st and st != "active":
                    continue          # a Mayhem coin that is paused / completed
                part.append(dict(r, _age=age, _grp=g))
            part.sort(key=lambda r: r["_age"])
            for r in part:
                seen.add(r["m"])
                out.append(r)
        return out

    def candidates(self) -> List[Dict[str, Any]]:
        """Board coins younger than 3 minutes, pump only, active, newest first."""
        if self.mixed():
            return self._mixed_candidates()
        now = time.time()
        with self.lock:
            rows = list(self.board)
            age_shift = max(0.0, now - self.board_at) if self.board_at else 0.0
            states = dict(self.state)
        out = []
        for r in rows:
            mint = str(r.get("m") or "")
            if not mint or str(r.get("pg") or "") != "pump":
                continue
            age = float(r.get("age") or 0) + age_shift
            if age > YOUNG_S:
                continue
            st = str((states.get(mint) or {}).get("state") or r.get("ms") or "").lower()
            if st != "active":
                continue
            out.append(dict(r, _age=age))
        out.sort(key=lambda r: r["_age"])
        return out

    async def _pos_loop(self) -> None:
        turn = 0
        while True:
            if not self.active() or self.mixed():      # Mayhem_Mode's positions: Mayhem feed only
                await asyncio.sleep(0.5)
                continue
            t0 = time.time()
            pin = self.pin
            mint = ""
            if pin and time.time() - float((self.pos.get(pin["mint"]) or {}).get("at") or 0) >= 0.9:
                mint = pin["mint"]
            else:
                cands = [r["m"] for r in self.candidates() if r["m"] not in self.used]
                if cands:
                    cands.sort(key=lambda m: float((self.pos.get(m) or {}).get("at") or 0))
                    mint = cands[0]
                turn += 1
            if mint:
                try:
                    body = await asyncio.to_thread(self._get, POS_URL.format(mint=mint), "mayhem2pos", 1.2)
                    rows = (body or {}).get("positions") if isinstance(body, dict) else None
                    mine = None
                    for row in rows or []:
                        if str(row.get("walletAddress") or "") == _wallet():
                            mine = row
                            break
                    rec = {
                        "at": time.time(),
                        "found": mine is not None,
                        "held": float((mine or {}).get("amountHeld") or 0),
                        "pnlUsd": _f((mine or {}).get("pnlUsd")),
                        "pnlPercentage": _f((mine or {}).get("pnlPercentage")),
                        "costBasisUsd": _f((mine or {}).get("costBasisUsd")),
                        "realizedPnlUsd": _f((mine or {}).get("realizedPnlUsd")),
                    }
                    with self.lock:
                        self.pos[mint] = rec
                        if len(self.pos) > 300:
                            for old in sorted(self.pos, key=lambda k: self.pos[k]["at"])[:100]:
                                self.pos.pop(old, None)
                except Exception:
                    pass
            # Pinned: trades (4/s) and state (2/s) carry the trading, so side reads slow to stay under
            # pump's ~10 req/s. Unpinned: full speed to find the next coin.
            every = 0.6 if self.pin else POS_EVERY
            await asyncio.sleep(max(0.05, every - (time.time() - t0)))

    async def _trades_loop(self) -> None:
        while True:
            pin = self.pin
            if not pin:
                await asyncio.sleep(0.05)
                continue
            t0 = time.time()
            mint = pin["mint"]
            mixed = self.mixed()
            try:
                url = ALL_TRADES_URL.format(mint=mint) if mixed else TRADES_URL.format(mint=mint, wallet=_wallet())
                body = await asyncio.to_thread(self._get, url, "mayhem2", 1.0)
                rows = (body or {}).get("trades") if isinstance(body, dict) else None
                if rows and self.pin and self.pin["mint"] == mint:
                    fresh = []
                    for t in rows:
                        tx = str(t.get("txId") or "")
                        side = str(t.get("side") or "").lower()
                        if not tx or side not in ("buy", "sell"):
                            continue
                        if str(t.get("kind") or "swap") != "swap":
                            continue
                        if not mixed and str((t.get("trader") or {}).get("address") or _wallet()) != _wallet():
                            continue
                        fresh.append({
                            "tx": tx, "side": side,
                            "ts": float(t.get("blockTimeMs") or 0) / 1000.0,
                            "usd": _f(t.get("valueUsd")), "sol": _f(t.get("valueNative")),
                            "px": _f(t.get("priceUsd")),
                        })
                    sized = [x for x in fresh if x.get("px") and (x.get("sol") or 0) >= 0.002]
                    if sized:
                        newest = max(sized, key=lambda x: x["ts"])
                        self._price(mint, float(newest["px"]))
                        self._signals(mint, fresh, "api")
            except Exception:
                pass
            await asyncio.sleep(max(0.02, TRADES_EVERY - (time.time() - t0)))

    async def _state_loop(self) -> None:
        while True:
            pin = self.pin
            if not pin or (self.mixed() and not pin.get("mh")):   # mayhem-state exists for Mayhem coins only
                await asyncio.sleep(0.1)
                continue
            t0 = time.time()
            mint = pin["mint"]
            try:
                body = await asyncio.to_thread(self._get, STATE_URL.format(mint=mint), "mayhem2state", 1.0)
                if isinstance(body, dict) and body.get("state"):
                    with self.lock:
                        self.state[mint] = {
                            "state": str(body.get("state") or "").lower(),
                            "reason": str(body.get("pause_reason") or ""),
                            "at": time.time(), "src": "state",
                        }
            except Exception:
                pass
            await asyncio.sleep(max(0.05, STATE_EVERY - (time.time() - t0)))

    # ---------- signals ----------
    def on_stream(self, ev: Dict[str, Any]) -> None:
        """Caller-stream thread: an on-chain trade. Mayhem_Mode on the pinned coin only."""
        pin = self.pin
        if not pin or ev.get("mint") != pin["mint"]:
            return
        if not self.mixed() and ev.get("wallet") != _wallet():
            return
        sig = str(ev.get("signature") or "")
        if not sig or self.loop is None:
            return
        row = {"tx": sig, "side": str(ev.get("kind") or ""), "ts": float(ev.get("ts") or time.time()),
               "sol": _f(ev.get("sol")), "usd": None, "px": None}
        self.loop.call_soon_threadsafe(self._signals, pin["mint"], [row], "chain")

    def _signals(self, mint: str, rows: List[Dict[str, Any]], src: str) -> None:
        pin = self.pin
        if not pin or pin["mint"] != mint:
            return
        now = time.time()
        new = []
        for r in rows:
            if r["tx"] in self.seen_tx:
                continue
            self.seen_tx[r["tx"]] = now
            if r["ts"] < float(pin["ts"]) - 1.0 or now - r["ts"] > STALE_SIGNAL_S:
                continue            # before the pin, or history: shown nowhere, never traded
            r["src"] = src
            r["seen"] = now
            new.append(r)
        if len(self.seen_tx) > 5000:
            cut = now - 600
            self.seen_tx = {k: v for k, v in self.seen_tx.items() if v >= cut}
        if not new:
            return
        new.sort(key=lambda r: r["ts"])
        pin["last_sig"] = now
        for r in new:
            self.signals.appendleft(r)
            if r.get("sol") is not None and float(r["sol"]) < MIN_SIGNAL_SOL:
                r["act"] = "ignored: tiny trade"
                continue
            self.want = r["side"]
            self.want_at = r["ts"]
            r["act"] = self._on_signal(r)
            try:
                r["plan"] = self._shadow_signal(r["side"], float(r.get("seen") or now), r.get("sol"))
            except Exception:
                log.debug("mayhem2 shadow failed", exc_info=True)
            log.info("MAYHEM2 SIGNAL %s %s via %s (%.2fs after the block) -> %s", r["side"].upper(),
                     pin.get("symbol") or mint[:8], src, max(0.0, now - r["ts"]), r["act"])

    def _price(self, mint: str, px: float) -> None:
        if px <= 0:
            return
        self.last_px[mint] = px
        pos = self._open_pos(mint)
        if pos is None:
            return
        pos._live_px = px
        pos._stream_px_at = time.time()
        if float(getattr(pos, "entry_price_usd", 0) or 0) <= 0:
            pos.entry_price_usd = px
            pos._pump_entry_px = px
        if self.apply_since is not None:
            try:
                self.apply_since(pos)
            except Exception:
                pass

    def _on_signal(self, r: Dict[str, Any]) -> str:
        """One signal, once. BUY buys only when flat and idle. SELL starts the exit once. A BUY that
        comes while buying, holding or selling is ignored: after the coins are sold, the next BUY buys."""
        pin = self.pin
        mint = pin["mint"]
        pos = self._held(mint)
        name = self.strategy()
        enter_on, exit_on, min_sol = SHADOWS[name]
        side = r["side"]
        if side == enter_on:
            if min_sol and (r.get("sol") is None or float(r["sol"]) < min_sol):
                return f"ignored: under {min_sol} SOL ({name})"
            if self.busy == "buy":
                return "ignored: buy already sending"
            if pos is not None:
                if getattr(pos, "_sell_busy", False) or mint in self.exiting:
                    return "ignored: selling"
                if name == "ladder":
                    return self._ladder_rebuy(pos, pin, float(r.get("seen") or time.time()))
                if name == "inv_ladder":
                    pos._m2_reds = int(getattr(pos, "_m2_reds", 0) or 0) + 1
                    if pos._m2_reds < INV_REDS_PER_LOT:
                        return f"red {pos._m2_reds}/{INV_REDS_PER_LOT} since the last lot: leave it"
                    out = self._ladder_rebuy(pos, pin, float(r.get("seen") or time.time()))
                    if "sent" in out or "BUY" in out:
                        pos._m2_reds = 0
                    return out
                return "ignored: holding"
            return self._try_buy(pin, float(r.get("seen") or time.time()))
        if not exit_on or side != exit_on:
            return f"ignored: {name} does not sell on {side.upper()}"
        if self.busy == "buy":
            self.sell_after_buy = True
            return "sell right after the buy lands"
        if pos is None:
            return "ignored: nothing held"
        if getattr(pos, "_sell_busy", False) or mint in self.exiting:
            return "ignored: already selling"
        return self._exit_on_signal(pos)

    def _ladder_rebuy(self, pos, pin: Dict[str, Any], seen: float) -> str:
        """Ladder: Mayhem_Mode BUY while we hold. Buy again only below our last buy price, max 3 buys."""
        n = int(getattr(pos, "_m2_buys", 1) or 1)
        if n >= LADDER_MAX_BUYS:
            return f"ignored: ladder full ({n}/{LADDER_MAX_BUYS} buys)"
        spot = float(getattr(pos, "_m2_spot", 0) or 0)
        last = float(getattr(pos, "_m2_last_px", 0) or 0)
        if spot <= 0 or last <= 0:
            return "ignored: no price for the ladder check yet"
        if spot >= last:
            return f"ignored: price {(spot / last - 1) * 100:+.1f}% vs last buy (ladder buys only below it)"
        return self._try_buy(pin, seen, add=True)

    def _try_buy(self, pin: Dict[str, Any], seen: float = 0.0, add: bool = False) -> str:
        mint = pin["mint"]
        seen = seen or time.time()
        tag = "LADDER BUY" if add else "BUY"
        if not add and (pin.get("traded") or mint in self.used):
            return "not bought: one trade per coin, this coin was already traded"
        if self.dry():
            if not self.paper_ok():
                return "not bought: source not mayhem2"
            if any(p.status == "OPEN" and m != mint for m, p in self.paper.items()):
                return "not bought: another dry-run trade is open"
            self.busy = "buy"
            self.sell_after_buy = False
            asyncio.ensure_future(self._paper_buy(dict(pin), seen, add=add))
            return f"DRY-RUN {tag} (built + simulated, not sent)"
        if not self.trading():
            return "not bought: Auto Trade off / source not mayhem2"
        if self.unresolved and self.unresolved.get("until", 0) > time.time():
            return "not bought: previous buy not confirmed yet, checking chain"
        if pin.get("buys_off_until", 0) > time.time():
            return "not bought: last buy failed, short wait"
        # No circuit breaker for Mayhem2 (removed at the user's request): losses in a row do not stop its buys.
        if self._other_open(mint) is not None:
            return "not bought: another Mayhem2 trade is open"
        self.busy = "buy"
        self.sell_after_buy = False
        asyncio.ensure_future(self._buy(dict(pin), seen, add=add))
        return f"{tag} sent"

    # ---------- exits ----------
    def _since(self, pos) -> Optional[float]:
        v = getattr(pos, "_m2_pct", None)       # what the curve would pay for our tokens now
        if v is None:
            v = getattr(pos, "since_buy_pct", None)
        try:
            return None if v is None else float(v)
        except (TypeError, ValueError):
            return None

    def _exit_on_signal(self, pos) -> str:
        """Mayhem_Mode sold. In profit after fees: sell now. Not yet: wait a short while for profit,
        inside the stop and the max hold (the coin can pause at any moment)."""
        since = self._since(pos)
        sig = "SOLD" if SHADOWS[self.strategy()][1] == "sell" else "BOUGHT"
        if self.strategy() == "inv_ladder":
            n = int(getattr(pos, "_m2_buys", 1) or 1)
            now_txt = "n/a" if since is None else f"{since:+.1f}%"
            self._sell(pos, f"green — Mayhem_Mode BOUGHT: sell all {n} lot(s) at {now_txt} (inverse ladder always sells on green)")
            return "SELL sent (green)"
        if self.strategy() == "ladder":
            sym = getattr(pos, "symbol", "") or pos.mint[:6]
            n = int(getattr(pos, "_m2_buys", 1) or 1)
            if since is not None and since >= PROFIT_MIN_PCT:
                self._sell(pos, f"Mayhem_Mode SOLD at {since:+.1f}% on {n} buy(s) (profit)")
                return "SELL sent (profit)"
            now_txt = "n/a" if since is None else f"{since:+.1f}%"
            self.note("warn", f"{sym} — Mayhem_Mode SOLD at {now_txt} on {n}/{LADDER_MAX_BUYS} buy(s). Not a profit: "
                      f"holding (ladder). A BUY below the last buy price buys again.", pos.mint)
            return f"holding: not in profit ({now_txt})"
        if since is None:
            self._sell(pos, f"Mayhem_Mode {sig} (no live % yet)")
            return "SELL sent"
        if since >= PROFIT_MIN_PCT:
            self._sell(pos, f"Mayhem_Mode {sig} at {since:+.1f}% (profit)")
            return "SELL sent (profit)"
        if since <= STOP_PCT:
            self._sell(pos, f"Mayhem_Mode {sig} at {since:+.1f}% (at the stop)")
            return "SELL sent (stop)"
        self.wait_profit[pos.mint] = time.time()
        self.note("warn", f"{getattr(pos, 'symbol', '') or pos.mint[:6]} — Mayhem_Mode {sig} at {since:+.1f}%. "
                  f"Not a profit yet: holding up to {WAIT_PROFIT_S:.0f}s for {PROFIT_MIN_PCT:+.0f}% (stop {STOP_PCT:.0f}%).", pos.mint)
        return f"waiting for profit (now {since:+.1f}%)"

    def _sell(self, pos, why: str) -> None:
        if pos.mint not in self.exiting:
            pct = self._since(pos)
            self.marks.append({"ts": time.time(), "mint": pos.mint, "side": "sell",
                               "label": "S" if pct is None else f"S {pct:+.0f}%"})
        self.exiting[pos.mint] = why
        self.wait_profit.pop(pos.mint, None)
        if getattr(pos, "_paper", False):
            if not pos._sell_busy:
                pos._sell_busy = True
                asyncio.ensure_future(self._paper_sell(pos, why))
                self.note("sell", f"{pos.symbol or pos.mint[:6]} — DRY RUN: {why}. Simulated sell.", pos.mint)
            return
        if self._may_kick(pos, 0.0) and self.manager.kick_now(pos, "mayhem2_sell"):
            self.kicked[pos.mint] = time.time()
        self.note("sell", f"{getattr(pos, 'symbol', '') or pos.mint[:6]} — {why}. Selling 100% now.", pos.mint)

    def _exit_check(self) -> None:
        """Every 0.1s on the open Mayhem2 trade: take profit, trail, stop, max hold, and the
        wait-for-profit after a SELL signal. Exits run even with Auto Trade off."""
        mgr = self.manager
        if mgr is None:
            return
        now = time.time()
        holders = [p for p in list(self.paper.values()) if p.status == "OPEN"] + list(mgr.positions.values())
        for pos in holders:
            if pos.status != "OPEN" or not pos.mint:
                continue
            if "mayhem2" not in (str(getattr(pos, "source", "") or ""), str(getattr(pos, "strategy", "") or "")):
                continue
            mint = pos.mint
            if getattr(pos, "_paper", False) and (mint in self.exiting or pos._sell_busy):
                continue
            if mint in self.exiting:
                if self._may_kick(pos, 2.0) and mgr.kick_now(pos, "mayhem2_sell"):
                    self.kicked[mint] = now      # the sell did not land: again, at most every 2s
                continue
            if getattr(pos, "_sell_busy", False):
                continue
            since = self._since(pos)
            peak = float(getattr(pos, "peak_since_pct", 0) or 0)
            held = now - float(getattr(pos, "entry_time", 0) or now)
            ladder = self.strategy() in LADDERS
            why = ""
            if since is not None and since >= TAKE_PROFIT_PCT:
                why = f"take profit {since:+.1f}%"
            elif since is not None and peak >= TRAIL_ARM_PCT and since <= peak * TRAIL_KEEP \
                    and (not ladder or since >= PROFIT_MIN_PCT):
                why = f"gave back from {peak:+.1f}% to {since:+.1f}% (trail)"
            elif ladder:
                pass                     # ladder: no stop, no max hold — it holds until profit or pause
            elif since is not None and since <= STOP_PCT:
                why = f"stop {since:+.1f}%"
            elif held >= MAX_HOLD_S:
                why = f"max hold {MAX_HOLD_S:.0f}s ({'n/a' if since is None else f'{since:+.1f}%'}) — pause risk"
            elif mint in self.wait_profit:
                if since is not None and since >= PROFIT_MIN_PCT:
                    why = f"profit {since:+.1f}% after Mayhem_Mode's exit signal"
                elif now - self.wait_profit[mint] >= WAIT_PROFIT_S:
                    why = f"no profit {WAIT_PROFIT_S:.0f}s after Mayhem_Mode's exit signal ({'n/a' if since is None else f'{since:+.1f}%'})"
            if why:
                self._sell(pos, why)
        for mint in list(self.exiting):
            if self._held(mint) is None:
                self.exiting.pop(mint, None)
                self.wait_profit.pop(mint, None)

    def _may_kick(self, pos, every: float = 2.0) -> bool:
        """A sell that did not land retries, but not faster than every 2s, and never on DUST."""
        if getattr(pos, "_sell_busy", False) or getattr(pos, "_dust_value", False):
            return False
        try:
            from src.storage.mint_flags import mint_flag
            if mint_flag(pos.mint) == "DUST":
                return False
        except Exception:
            pass
        return time.time() - float(self.kicked.get(pos.mint) or 0) >= every

    async def _buy(self, pin: Dict[str, Any], seen: float = 0.0, add: bool = False) -> None:
        mint = pin["mint"]
        seen = seen or time.time()
        symbol = pin.get("symbol") or mint[:6]
        try:
            fill = await self.trader.buy(mint=mint, size_multiplier=1.0, source="mayhem2", ref_price=1.0,
                                         add_to_open=add)
        except Exception as exc:
            fill = None
            log.exception("MAYHEM2 buy crashed mint=%s", mint[:8])
            err = f"{type(exc).__name__}: {exc}"[:160]
        else:
            err = "" if (fill and fill.ok) else str(getattr(fill, "error", None) or "buy_failed")
        try:
            if not (fill and fill.ok) and "not_confirmed" in err and not add:
                # Sent, but no confirmation in ~2s (RPC 429s / slow landing). It may still land:
                # no new buy until the chain says yes or no, and coins that arrive become this trade.
                from src.execution.tx_route import m2_buy_get
                info = m2_buy_get(mint, 120.0) or {}
                self.unresolved = {"mint": mint, "sig": info.get("sig", ""), "sol": info.get("sol", 0.0),
                                   "pin": dict(pin), "until": time.time() + 60.0, "seen": seen}
                self.note("warn", f"{symbol} — buy sent but not confirmed yet. Checking the chain for up to 60s; no new buy until then.", mint)
                asyncio.ensure_future(self._resolve())
                return
            if not (fill and fill.ok):
                cur = self.pin
                # A refusal costs nothing, but retrying every second chases a rising price until one
                # passes near the top (16:51 8vMg9sni). Every failed buy waits, 6042 included.
                if cur and cur["mint"] == mint:
                    cur["fails"] = int(cur.get("fails") or 0) + 1
                    cur["buys_off_until"] = time.time() + min(30.0, 3.0 * cur["fails"])
                self.note("warn", f"{symbol} — buy failed: {err}. Nothing bought.", mint)
                return
            ms = (time.time() - seen) * 1000.0
            self.land.append(ms)
            if add:
                n = self._ladder_merge(fill, pin)
                self.marks.append({"ts": time.time(), "mint": mint, "side": "buy", "label": f"B{n}"})
                self.note("buy", f"{symbol} — ladder buy {n}/{LADDER_MAX_BUYS}: {float(fill.sol_in or 0):.4f} SOL below the last buy price, "
                          f"landed {ms/1000:.2f}s after the signal.", mint)
                asyncio.ensure_future(self._fix_tokens(mint))
                return
            self._tag(fill, pin)
            self.marks.append({"ts": time.time(), "mint": mint, "side": "buy",
                               "label": "B1" if self.strategy() in LADDERS else "B"})
            self.note("buy", f"{symbol} — {self.strategy()} entry. Bought {float(fill.sol_in or 0):.4f} SOL, landed {ms/1000:.2f}s after the signal.", mint)
            asyncio.ensure_future(self._fix_tokens(mint))
        finally:
            self.busy = ""
            if self.sell_after_buy:
                self.sell_after_buy = False
                pos = self._held(mint)
                if pos is not None:
                    self._exit_on_signal(pos)      # a SELL came in while buying

    async def _resolve(self) -> None:
        """Follow an unconfirmed buy for up to 60s: its signature on any RPC, and the coins in the wallet."""
        u = self.unresolved
        if not u:
            return
        mint, sig = u["mint"], u.get("sig") or ""
        symbol = (u.get("pin") or {}).get("symbol") or mint[:6]
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_holding
        pk = pubkey_from_env()
        failed_on_chain = ""
        try:
            while time.time() < u["until"] and self.unresolved is u:
                if self._open_pos(mint) is not None:          # booked meanwhile (wallet sync)
                    self.note("buy", f"{symbol} — the unconfirmed buy landed; managed as this Mayhem2 trade.", mint)
                    return
                raw = 0
                try:
                    held = await asyncio.wait_for(get_token_holding(pk, mint, fast=True), timeout=2.0) if pk else None
                    raw = int((held or {}).get("raw") or 0)
                except Exception:
                    raw = 0
                if raw > 0:
                    self.adopt(mint, raw, float(u.get("sol") or 0), sig, u.get("pin") or {"mint": mint})
                    ms = (time.time() - float(u.get("seen") or time.time())) * 1000.0
                    self.note("buy", f"{symbol} — the unconfirmed buy DID land ({ms/1000:.1f}s after the signal). Now held as a Mayhem2 trade; exits apply.", mint)
                    return
                if sig and self.trader is not None:
                    try:
                        ok = await self.trader._confirm_signature(sig, tries=1, delay=0, paid_only=False, timeout=1.5)
                    except Exception:
                        ok = False
                    err = str(getattr(self.trader, "_last_tx_err", "") or "")
                    if not ok and err and err != "confirm_unreachable":
                        failed_on_chain = err
                        break
                await asyncio.sleep(1.0)
            if self.unresolved is u:
                if failed_on_chain:
                    self.note("warn", f"{symbol} — the unconfirmed buy failed on chain ({failed_on_chain[:60]}). Nothing bought.", mint)
                else:
                    self.note("warn", f"{symbol} — the unconfirmed buy never reached the wallet in 60s. Nothing bought.", mint)
                cur = self.pin
                if cur and cur["mint"] == mint:
                    cur["fails"] = int(cur.get("fails") or 0) + 1
                    cur["buys_off_until"] = time.time() + min(30.0, 3.0 * cur["fails"])
        finally:
            if self.unresolved is u:
                self.unresolved = None

    def adopt(self, mint: str, raw: int, sol: float, sig: str, pin: Dict[str, Any], pos=None) -> None:
        """Coins of a Mayhem2 buy found in the wallet: book (if needed) and tag them as a Mayhem2 trade,
        so Mayhem2's exits manage them and no OpenCopy / caller rule sells them."""
        import types
        if pos is None:
            pos = self._open_pos(mint)
        if pos is None and self.manager is not None and self.loop is not None:
            from src.risk.position_manager import PositionMode

            async def _open():
                p = await self.manager.open_position(mint=mint, mode=PositionMode.LIVE, entry_sol=sol or 0.005,
                                                     entry_price=(sol / raw) if raw and sol else 0.0,
                                                     tokens=float(raw), source="mayhem2", signature=sig or "")
                if p is not None:
                    self.adopt(mint, raw, sol, sig, pin, pos=p)
            asyncio.ensure_future(_open())
            return
        if pos is None:
            return
        fill = types.SimpleNamespace(sol_in=sol or float(getattr(pos, "entry_sol", 0) or 0), mode="live",
                                     tokens_out=float(raw), fees_sol=0.0,
                                     effective_price=(sol / raw) if raw and sol else 0.0, signature=sig or "")
        p2 = dict(pin or {})
        p2.setdefault("mint", mint)
        pos.source = "mayhem2"
        pos.strategy = "mayhem2"
        pos._from_openholds = False
        self._tag(fill, p2)
        pos._m2_tokens = raw
        if self.unresolved and self.unresolved.get("mint") == mint:
            self.unresolved = None

    def _ladder_merge(self, fill, pin: Dict[str, Any]) -> int:
        """A ladder buy landed: one trade, summed cost and coins, average entry price. Returns the buy count."""
        mint = pin["mint"]
        pos = self._open_pos(mint)
        if pos is None:
            log.warning("MAYHEM2 %s — ladder buy landed but no open trade to add it to", pin.get("symbol") or mint[:8])
            return 0
        add_sol = float(fill.sol_in or 0)
        add_tok = float(fill.tokens_out or 0)
        old_tok = float(getattr(pos, "_m2_tokens", 0) or getattr(pos, "tokens", 0) or 0)
        px = 0.0
        try:
            px = float(self.fill_px(fill)) if self.fill_px else 0.0
        except Exception:
            px = 0.0
        old_px = float(getattr(pos, "entry_price_usd", 0) or 0)
        if px > 0 and old_px > 0 and old_tok + add_tok > 0:
            avg = (old_px * old_tok + px * add_tok) / (old_tok + add_tok)
            pos.entry_price_usd = avg
            pos._pump_entry_px = avg
        pos.entry_sol = float(getattr(pos, "entry_sol", 0) or 0) + add_sol
        pos.fees_sol = float(getattr(pos, "fees_sol", 0) or 0) + float(fill.fees_sol or 0)
        pos.tokens = float(getattr(pos, "tokens", 0) or 0) + add_tok
        pos._m2_tokens = old_tok + add_tok
        pos._orig_raw = float(pos.tokens)
        pos._m2_buys = int(getattr(pos, "_m2_buys", 1) or 1) + 1
        pos._m2_last_px = _fill_px(fill) or getattr(pos, "_m2_last_px", 0)
        # The old peak belongs to the first buy's cost. Measured on the new average it would
        # trip the trail at once.
        pos.peak_since_pct = 0.0
        pos._m2_pct = None
        try:
            self.manager.set_price(mint, 1.0)
            self.manager._push_remote()
        except Exception:
            log.debug("mayhem2 ladder save failed", exc_info=True)
        try:
            from src.observability.ledger import get_ledger
            get_ledger().add_to_open(mint, add_sol, float(fill.fees_sol or 0), add_tok, fill.signature or "")
        except Exception:
            log.exception("mayhem2 ledger ladder add failed")
        log.info("MAYHEM2 LADDER %s buy %d sol=%.4f total=%.4f tokens=%.4g", pos.symbol or mint[:8], pos._m2_buys,
                 add_sol, pos.entry_sol, pos.tokens)
        return pos._m2_buys

    def _tag(self, fill, pin: Dict[str, Any]) -> None:
        mint = pin["mint"]
        cur = self.pin
        if cur and cur["mint"] == mint:
            cur["traded"] = True          # one trade per coin: when it closes, the bot moves on
        pos = None
        for p in self.manager.positions.values():
            if p.status == "OPEN" and p.mint == mint:
                pos = p
                break
        if pos is None:
            log.warning("MAYHEM2 %s — fill did not open a position", pin.get("symbol") or mint[:8])
            return
        px = 0.0
        try:
            px = float(self.fill_px(fill)) if self.fill_px else 0.0
        except Exception:
            px = 0.0
        if px <= 0:
            px = float(self.last_px.get(mint) or 0)
        pos.strategy = "mayhem2"
        pos.source = "mayhem2"
        pos.symbol = pin.get("symbol") or pos.symbol
        pos.name = pin.get("name") or getattr(pos, "name", "")
        pos.entry_price = 1.0
        pos.peak_price = 1.0
        pos.stop_pct = None
        pos.stop_price = None
        pos.min_hold_seconds = 0
        pos.entry_sol = float(fill.sol_in or pos.entry_sol or 0)
        pos.peak_since_pct = 0.0
        pos.since_buy_pct = 0.0
        pos.low_since_pct = 0.0
        pos._ath_from_entry = True
        pos._m2_buys = 1
        pos._m2_last_px = _fill_px(fill)
        pos.callout_user = _user()
        pos.callout_wallet = _wallet()
        pos.user_holds = "live"
        if px > 0:
            pos._pump_entry_px = px
            pos.entry_price_usd = px
            pos._entry_px_locked = True
        try:
            self.manager.set_price(mint, 1.0)
            self.manager._push_remote()
        except Exception:
            log.debug("mayhem2 entry save failed", exc_info=True)
        try:
            from src.observability.ledger import get_ledger
            get_ledger().record_open(
                id=f"m2-{mint[:8]}-{int(time.time())}",
                mint=mint,
                symbol=pos.symbol or mint[:6],
                mode=fill.mode,
                source="mayhem2",
                token_amount=fill.tokens_out,
                entry_sol=fill.sol_in,
                entry_fee_sol=fill.fees_sol,
                entry_tip_sol=0.0,
                price_sol_per_token=fill.effective_price,
                signature=fill.signature or "",
                price_usd=px or None,
                since_buy_pct=0.0,
                ath_since_pct=0.0,
                name=pos.name or "",
                callout_user=_user(),
            )
        except Exception:
            log.exception("mayhem2 ledger open failed")
        log.info("MAYHEM2 HOLDING %s sol=%.4f", pos.symbol or mint[:8], float(fill.sol_in or 0))

    # ---------- pin ----------
    def _set_pin(self, row: Dict[str, Any], why: str) -> None:
        mint = str(row.get("m") or row.get("mint") or "")
        self.pin = {
            "mint": mint,
            "symbol": str(row.get("t") or row.get("symbol") or ""),
            "name": str(row.get("n") or row.get("name") or ""),
            "image": str(row.get("i") or ""),
            "ts": time.time(),
            "age0": float(row.get("_age") or 0),
            "grp": str(row.get("_grp") or ""),
            "mh": bool(row.get("mh")),
            "feed": self.feed(),
        }
        self.want = ""
        self.want_at = time.time() - 1.0
        self.signals.clear()
        self.gone_reads.pop(mint, None)
        self.note("ok", f"{self.pin['symbol'] or mint[:6]} — pinned ({why}). Following "
                  f"{'every wallet' if self.mixed() else 'Mayhem_Mode'}'s BUY / SELL.", mint)

    def _save_used(self) -> None:
        import json
        try:
            path = _used_path()
            tmp = path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.used), encoding="utf-8")
            tmp.replace(path)
        except Exception:
            log.debug("mayhem2 used list save failed", exc_info=True)

    def _trade_done(self, pin: Dict[str, Any]) -> str:
        """One trade per coin: the trade on this pin has closed (gain or loss). Why, or empty."""
        mint = pin["mint"]
        if self._own():
            if not pin.get("traded"):
                return ""
            if self._held(mint) is not None or self.busy == "buy" or mint in self.exiting:
                return ""
            if self.unresolved and self.unresolved.get("mint") == mint:
                return ""
            return "its trade closed"
        sh = self.shadow[self.strategy()]
        if mint in sh["done"] and sh["pos"] is None and not sh.get("busy"):
            return f"its paper {self.strategy()} trade closed"
        return ""

    def _unpin(self, why: str, reason: str) -> None:
        pin = self.pin
        if not pin:
            return
        mint = pin["mint"]
        sym = pin.get("symbol") or mint[:6]
        self.used[mint] = {"why": why, "ts": time.time(), "symbol": sym}
        if len(self.used) > 400:
            for old in sorted(self.used, key=lambda k: self.used[k]["ts"])[:100]:
                self.used.pop(old, None)
        self._save_used()
        self.pin = None
        self.want = ""
        pos = self._held(mint)
        if pos is not None and getattr(pos, "_paper", False):
            self._sell(pos, why)
            self.note("sell", f"{sym} — {why}. Unpinned.", mint)
            return
        if pos is not None:
            self.exiting[mint] = why
            self.wait_profit.pop(mint, None)
            started = self.manager.kick_now(pos, reason)
            if started:
                self.kicked[mint] = time.time()
            self.note("sell", f"{sym} — {why}. {'Selling 100% now' if started else 'Sell already going'}; unpinned.", mint)
        else:
            self.note("warn", f"{sym} — {why}. Unpinned; nothing held.", mint)

    async def _brain_loop(self) -> None:
        last_wait_note = 0.0
        while True:
            await asyncio.sleep(0.1)
            try:
                self._exit_check()          # open trades are always watched, page open or not
            except Exception:
                log.debug("mayhem2 exit check failed", exc_info=True)
            try:
                if not self.active():
                    continue
                pin = self.pin
                if pin:
                    mint = pin["mint"]
                    st = self.state.get(mint) or {}
                    state_now = str(st.get("state") or "")
                    if state_now == "paused":
                        self._unpin(f"pump paused it ({st.get('reason') or 'paused'})", "mayhem2_paused")
                        continue
                    if state_now == "completed":
                        # Mayhem run is over for this coin: sell what is held and take the next new coin.
                        self._unpin("Mayhem state is completed", "mayhem2_paused")
                        continue
                    rec = {} if self.mixed() else (self.pos.get(mint) or {})
                    if rec.get("at") and float(rec["at"]) > float(pin["ts"]) and rec.get("held", 0) <= 0:
                        key = int(float(rec["at"]) * 10)
                        if self.gone_reads.get(mint + ":last") != key:
                            self.gone_reads[mint + ":last"] = key
                            self.gone_reads[mint] = int(self.gone_reads.get(mint) or 0) + 1
                        if self.gone_reads[mint] >= 2:
                            self._unpin("Mayhem_Mode no longer holds it", "mayhem2_paused")
                            continue
                    elif rec.get("held", 0) > 0:
                        self.gone_reads[mint] = 0
                    done = self._trade_done(pin)
                    if done:
                        self._unpin(f"{done} (gain or loss) — one trade per coin, next new coin", "mayhem2_sell")
                        continue
                    quiet = time.time() - max(float(pin["ts"]), float(pin.get("last_sig") or 0))
                    if quiet >= IDLE_UNPIN_S and self._held(mint) is None and self.busy != "buy":
                        self._unpin(f"{self._who()} made no trade on it for {quiet:.0f}s", "mayhem2_sell")
                        continue
                    self._retry_unpinned()
                    continue
                # No pin. A Mayhem2 trade still open (restart, or a sell that did not land) is pinned
                # again first, so its SELL / pause is still followed.
                self._retry_unpinned()
                held = self._open_pos()
                if held is not None and held.mint not in self.used:
                    self._set_pin({"m": held.mint, "t": getattr(held, "symbol", ""), "n": getattr(held, "name", "")},
                                  "the bot holds it")
                    if self.pin:
                        self.pin["traded"] = True
                    continue
                mixed = self.mixed()
                for row in self.candidates():
                    mint = row["m"]
                    if mint in self.used:
                        continue
                    if mixed:
                        self._set_pin(row, f"{row.get('_grp')} board, {row['_age']:.0f}s old")
                        break
                    rec = self.pos.get(mint) or {}
                    if not rec.get("at"):
                        continue          # its Mayhem_Mode read comes in a moment
                    if rec.get("held", 0) <= 0:
                        continue
                    self._set_pin(row, f"newest active coin, {row['_age']:.0f}s old, Mayhem_Mode holding")
                    break
                else:
                    if time.time() - last_wait_note > 60 and self.board_at:
                        last_wait_note = time.time()
                        self.note("info", "No new coin on the New / Movers / Graduated boards yet. Watching." if mixed
                                  else "No active Mayhem coin under 3 minutes that Mayhem_Mode holds yet. Watching.")
            except Exception:
                log.debug("mayhem2 brain failed", exc_info=True)

    def _retry_unpinned(self) -> None:
        """A coin left (paused / closed) whose sell did not land: sell again every 3s until DUST."""
        if not self.used or self.manager is None:
            return
        for p in list(self.manager.positions.values()):
            if p.status != "OPEN" or p.mint not in self.used:
                continue
            if "mayhem2" not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
                continue
            if self._may_kick(p, 3.0):
                why = str(self.used[p.mint].get("why") or "")
                if self.manager.kick_now(p, "mayhem2_paused" if "paus" in why or "no longer" in why else "mayhem2_sell"):
                    self.kicked[p.mint] = time.time()

    # ---------- real value (curve) + dry run ----------
    async def _curve_now(self, mint: str, fresh: bool = True):
        from src.execution import pump_curve as PC
        if fresh:
            PC.forget_curve(mint)
        curve, why = await PC._curve(mint)
        _fee, _bb, fee_bps = await PC._fees()
        if not curve and why in ("graduated", "not_on_curve") and self.mixed():
            curve = await self._jup_curve(mint)
        return curve, int(fee_bps or 100)

    def _jup(self):
        jup = getattr(self.manager, "_trader_jup", None) if self.manager is not None else None
        if jup is None and self.trader is not None:
            jup = getattr(self.trader, "jup", None)
        return jup

    async def _jup_curve(self, mint: str):
        """A coin off pump's curve (graduated, PumpSwap): a flat 'curve' at Jupiter's price for Max position
        SOL, so the same math values it (tiny next to the pool, so no price impact). Cached 1.5s."""
        hit = self.jup_curve.get(mint)
        if hit and time.time() - hit[1] < JUP_CURVE_S:
            return hit[0]
        jup = self._jup()
        if jup is None:
            return hit[0] if hit else None
        try:
            from src.config.runtime import get_config
            size = float(getattr(get_config(), "max_position_sol", 0.005) or 0.005)
            q = await asyncio.wait_for(jup.quote_sol_to_token(mint, size, slippage_bps=1000), timeout=2.0)
            out = int(getattr(q, "out_amount", 0) or 0) if q else 0
        except Exception:
            out = 0
        if out <= 0:
            return hit[0] if hit and time.time() - hit[1] < 10 else None
        vs = 10 ** 15                                   # lamports: huge reserves = no price impact
        vt = int(vs * out / (size * 1e9))
        curve = {"virtual_sol": vs, "virtual_token": vt, "real_sol": None, "src": "jupiter"}
        self.jup_curve[mint] = (curve, time.time())
        if len(self.jup_curve) > 50:
            for old in sorted(self.jup_curve, key=lambda k: self.jup_curve[k][1])[:20]:
                self.jup_curve.pop(old, None)
        return curve

    async def _value_loop(self) -> None:
        """Every 0.3s: what pump's curve would pay for each held bag right now (live and dry run).
        That is the profit % the exits use. The pinned coin's curve, fee and blockhash stay warm
        so a buy is built in ~1ms."""
        from src.execution import pump_curve as PC
        last_warm = 0.0
        while True:
            busy = any(p.status == "OPEN" for p in self.paper.values()) or self._open_pos() is not None \
                or any(v.get("pos") for v in self.shadow.values())
            await asyncio.sleep(0.15 if busy else 0.3)
            try:
                await self._shadow_tick()
            except Exception:
                log.debug("mayhem2 shadow tick failed", exc_info=True)
            try:
                held = [p for p in list(self.paper.values()) if p.status == "OPEN"]
                live = self._open_pos()
                if live is not None:
                    held.append(live)
                for h in held:
                    curve, bps = await self._curve_now(h.mint)
                    tokens = float(getattr(h, "_m2_tokens", 0) or getattr(h, "tokens", 0) or 0)
                    cost = float(getattr(h, "entry_sol", 0) or 0)
                    if not curve or tokens <= 0 or cost <= 0:
                        continue
                    value = PC._sol_for_tokens(int(tokens), curve["virtual_token"], curve["virtual_sol"], bps) / 1e9
                    pct = round((value / cost - 1.0) * 100.0, 2)
                    h._m2_pct = pct
                    h._m2_value = value
                    if curve.get("virtual_token"):
                        h._m2_spot = float(curve["virtual_sol"]) / float(curve["virtual_token"])
                    h.peak_since_pct = round(max(float(getattr(h, "peak_since_pct", 0) or 0), pct), 2)
                    if getattr(h, "_paper", False):
                        h.since_buy_pct = pct
                pin = self.pin
                if pin and time.time() - last_warm > 1.0 and (self.dry() or self.trading()):
                    last_warm = time.time()
                    await self._curve_now(pin["mint"])
                    await PC._blockhash()
            except Exception:
                log.debug("mayhem2 value loop failed", exc_info=True)

    async def _fix_tokens(self, mint: str) -> None:
        """A local pump buy reports its minimum tokens. Read what actually arrived."""
        try:
            from src.execution.signer import pubkey_from_env
            from src.execution.wallet_tokens import get_token_holding
            pk = pubkey_from_env()
            for _ in range(4):
                held = await get_token_holding(pk, mint, fast=True, paid_only=True) if pk else None
                raw = int((held or {}).get("raw") or 0)
                pos = self._open_pos(mint)
                if raw > 0 and pos is not None:
                    pos._m2_tokens = raw
                    if int(getattr(pos, "_m2_buys", 1) or 1) > 1:
                        pos.tokens = float(raw)      # ladder: all buys are in this one bag
                    return
                await asyncio.sleep(0.5)
        except Exception:
            log.debug("mayhem2 token read failed", exc_info=True)

    async def _paper_buy(self, pin: Dict[str, Any], seen: float, add: bool = False) -> None:
        """Dry run: build + sign the real local pump buy, simulate it on the paid RPC, never send.
        The fill is the curve ~0.45s later (about one slot after a send)."""
        import types
        from src.execution import pump_curve as PC
        mint = pin["mint"]
        sym = pin.get("symbol") or mint[:6]
        try:
            from src.config.runtime import get_config
            size = float(getattr(get_config(), "max_position_sol", 0.015) or 0.015)
            c0, _bps = await self._curve_now(mint)
            if c0 and c0.get("src") == "jupiter":
                signed, err, sim_err = "", "", ""          # graduated: priced by Jupiter, no local pump build
            else:
                signed, _minimum, err = await PC.pump_buy_tx(mint, size, quiet=True)
                if not signed:
                    self.note("warn", f"{sym} — DRY RUN: buy not built ({err}).", mint)
                    return
                sim_err = await self.trader._simulate_first(signed, 0.9) if self.trader else ""
            ms = (time.time() - seen) * 1000.0
            self.lat.append(ms)
            if sim_err:
                self.note("warn", f"{sym} — DRY RUN: buy would FAIL ({sim_err[:90]}). {ms:.0f} ms to decide.", mint)
                return
            await asyncio.sleep(0.45)
            curve, bps = await self._curve_now(mint)
            if not curve:
                self.note("warn", f"{sym} — DRY RUN: curve gone before the fill.", mint)
                return
            tokens = PC._tokens_for_sol(int(size * 1e9), curve["virtual_token"], curve["virtual_sol"], bps)
            first_val = PC._sol_for_tokens(int(tokens), curve["virtual_token"], curve["virtual_sol"], bps) / 1e9
            first_pct = round((first_val / size - 1.0) * 100.0, 2)    # the buy+sell fee cost, right at the fill
            old = self.paper.get(mint)
            if add and old is not None and old.status == "OPEN":
                old.entry_sol = float(old.entry_sol) + size
                old.tokens = float(old.tokens) + float(tokens)
                old._m2_tokens = float(old._m2_tokens) + float(tokens)
                old._m2_buys = int(getattr(old, "_m2_buys", 1) or 1) + 1
                old._m2_last_px = size * 1e9 / float(tokens) if tokens else old._m2_last_px
                old.peak_since_pct = 0.0
                self.marks.append({"ts": time.time(), "mint": mint, "side": "buy", "label": f"B{old._m2_buys}"})
                self.note("buy", f"{sym} — DRY RUN LADDER BUY {old._m2_buys}/{LADDER_MAX_BUYS}: {size:.4f} SOL below the last buy "
                          f"(trade now {old.entry_sol:.4f} SOL), simulated OK in {ms:.0f} ms (not sent).", mint)
                return
            self.paper[mint] = types.SimpleNamespace(
                mint=mint, symbol=sym, name=pin.get("name") or "", status="OPEN", source="mayhem2", strategy="mayhem2",
                entry_sol=size, entry_time=time.time(), tokens=float(tokens), _m2_tokens=float(tokens),
                since_buy_pct=first_pct, peak_since_pct=first_pct, _m2_pct=first_pct, _m2_value=first_val,
                _sell_busy=False, _paper=True, _dust_value=False,
                _m2_buys=1, _m2_last_px=(size * 1e9 / float(tokens)) if tokens else 0.0, _m2_spot=0.0,
            )
            cur = self.pin
            if cur and cur["mint"] == mint:
                cur["traded"] = True
            self.marks.append({"ts": time.time(), "mint": mint, "side": "buy",
                               "label": "B1" if self.strategy() in LADDERS else "B"})
            self.note("buy", f"{sym} — DRY RUN BUY {size:.4f} SOL: signed + simulated OK in {ms:.0f} ms after the signal (not sent).", mint)
        except Exception as exc:
            self.note("warn", f"{sym} — DRY RUN buy error {type(exc).__name__}.", mint)
            log.debug("mayhem2 paper buy failed", exc_info=True)
        finally:
            self.busy = ""
            if self.sell_after_buy:
                self.sell_after_buy = False
                pos = self._held(mint)
                if pos is not None:
                    self._exit_on_signal(pos)

    async def _paper_sell(self, pos, why: str) -> None:
        from src.execution import pump_curve as PC
        t0 = time.time()
        try:
            c0, _bps = await self._curve_now(pos.mint)
            if c0 and c0.get("src") == "jupiter":
                signed = ""                                # graduated: no local pump build
            else:
                signed, _min, err = await PC.pump_sell_tx(pos.mint, int(pos._m2_tokens), quiet=True)
            ms = (time.time() - t0) * 1000.0
            await asyncio.sleep(0.45)
            curve, bps = await self._curve_now(pos.mint)
            value = PC._sol_for_tokens(int(pos._m2_tokens), curve["virtual_token"], curve["virtual_sol"], bps) / 1e9 if curve else 0.0
            pnl = value - float(pos.entry_sol) - 0.00002 * (1 + int(getattr(pos, "_m2_buys", 1) or 1))   # one fee per buy + the sell
            pct = (pnl / float(pos.entry_sol)) * 100.0
            pos.status = "CLOSED"
            self.paper.pop(pos.mint, None)
            self.paper_log.appendleft({"ts": time.time(), "mint": pos.mint, "symbol": pos.symbol, "pnl": round(pnl, 6),
                                       "pct": round(pct, 2), "held": round(time.time() - pos.entry_time, 1), "why": why,
                                       "built": bool(signed), "buildMs": round(ms)})
            self.note("ok" if pnl > 0 else "warn",
                      f"{pos.symbol} — DRY RUN SOLD: {pnl:+.5f} SOL ({pct:+.1f}%) after {time.time() - pos.entry_time:.1f}s — {why}.", pos.mint)
        except Exception as exc:
            pos.status = "CLOSED"
            self.paper.pop(pos.mint, None)
            self.note("warn", f"{pos.symbol} — DRY RUN sell error {type(exc).__name__}.", pos.mint)
        finally:
            self.exiting.pop(pos.mint, None)

    def _own(self) -> bool:
        """The bot itself trades the selected strategy (live, or dry run). Else the page shows its paper twin."""
        return self.dry() or self.trading()

    def _paper_note(self, name: str, level: str, message: str, mint: str = "") -> None:
        """Messages for the selected strategy's paper trades, while the bot itself is not trading it."""
        if name == self.strategy() and not self._own():
            self.note(level, f"PAPER {name}: {message}", mint)

    def _shadow_signal(self, side: str, seen: float, sol=None) -> str:
        """Feed one Mayhem_Mode signal to every paper strategy. Returns what the selected one does."""
        pin = self.pin
        if not pin or not self.active():
            return ""
        chosen = self.strategy()
        plan = ""
        for name, (enter_on, exit_on, min_sol) in SHADOWS.items():
            sh = self.shadow[name]
            pos = sh["pos"]
            sym = pin.get("symbol") or pin["mint"][:6]
            what = ""
            if name == "inv_ladder":
                if sh.get("busy"):
                    what = "ignored: buy in progress"
                elif pos is not None and pos.get("closing"):
                    what = "ignored: selling"
                elif side == "sell" and pos is None and pin["mint"] in sh["done"]:
                    what = "ignored: already traded this coin (one trade per coin)"
                elif side == "sell" and pos is None:
                    sh["busy"] = True
                    asyncio.ensure_future(self._shadow_enter(name, pin["mint"], sym))
                    what = "BUY lot 1 (red)"
                elif side == "sell":
                    pos["reds"] = int(pos.get("reds") or 0) + 1
                    n = int(pos.get("buys") or 1)
                    spot, last = float(pos.get("spot") or 0), float(pos.get("last_px") or 0)
                    if pos["reds"] < INV_REDS_PER_LOT:
                        what = f"red {pos['reds']}/{INV_REDS_PER_LOT}: leave it"
                    elif n >= LADDER_MAX_BUYS:
                        what = f"ignored: ladder full ({n}/{LADDER_MAX_BUYS})"
                    elif spot <= 0 or last <= 0:
                        what = "ignored: no price yet"
                    elif spot >= last:
                        what = f"ignored: price {(spot / last - 1) * 100:+.1f}% vs last buy"
                    else:
                        pos["reds"] = 0
                        sh["busy"] = True
                        asyncio.ensure_future(self._shadow_enter(name, pin["mint"], sym, add=True))
                        what = f"LADDER BUY {n + 1}/{LADDER_MAX_BUYS} (red)"
                elif pos is None:
                    what = "ignored: nothing held"
                else:
                    pct = pos.get("pct")
                    pos["closing"] = True
                    asyncio.ensure_future(self._shadow_exit(
                        name, f"green — Mayhem_Mode BUY: sell all at {'n/a' if pct is None else f'{pct:+.1f}%'}"))
                    what = "SELL all (green)"
            elif name == "ladder":
                if sh.get("busy"):
                    what = "ignored: buy in progress"
                elif pos is not None and pos.get("closing"):
                    what = "ignored: selling"
                elif side == "buy" and pos is None and pin["mint"] in sh["done"]:
                    what = "ignored: already traded this coin (one trade per coin)"
                elif side == "buy" and pos is None:
                    sh["busy"] = True
                    asyncio.ensure_future(self._shadow_enter(name, pin["mint"], sym))
                    what = "BUY"
                elif side == "buy":
                    n = int(pos.get("buys") or 1)
                    spot, last = float(pos.get("spot") or 0), float(pos.get("last_px") or 0)
                    if n >= LADDER_MAX_BUYS:
                        what = f"ignored: ladder full ({n}/{LADDER_MAX_BUYS})"
                    elif spot <= 0 or last <= 0:
                        what = "ignored: no price yet"
                    elif spot >= last:
                        what = f"ignored: price {(spot / last - 1) * 100:+.1f}% vs last buy"
                    else:
                        sh["busy"] = True
                        asyncio.ensure_future(self._shadow_enter(name, pin["mint"], sym, add=True))
                        what = f"LADDER BUY {n + 1}/{LADDER_MAX_BUYS}"
                elif pos is None:
                    what = "ignored: nothing held"
                else:
                    pct = pos.get("pct")
                    if pct is not None and pct >= PROFIT_MIN_PCT:
                        pos["closing"] = True
                        asyncio.ensure_future(self._shadow_exit(name, f"Mayhem_Mode SELL at {pct:+.1f}% (profit)"))
                        what = f"SELL at {pct:+.1f}% (profit)"
                    else:
                        now_txt = "n/a" if pct is None else f"{pct:+.1f}%"
                        what = f"holding: {now_txt}, not in profit"
                        self._paper_note(name, "warn", f"{sym} — Mayhem_Mode SOLD at {now_txt} on "
                                         f"{int(pos.get('buys') or 1)}/{LADDER_MAX_BUYS} buy(s). Not a profit: holding.", pin["mint"])
            else:
                big_enough = not min_sol or (sol is not None and float(sol) >= min_sol)
                if side == enter_on and pos is None and pin["mint"] in sh["done"]:
                    what = "ignored: already traded this coin (one trade per coin)"
                elif side == enter_on and pos is None:
                    if not big_enough:
                        what = f"ignored: under {min_sol} SOL"
                    elif sh.get("busy"):
                        what = "ignored: buy in progress"
                    else:
                        sh["busy"] = True
                        asyncio.ensure_future(self._shadow_enter(name, pin["mint"], sym))
                        what = "BUY"
                elif exit_on and side == exit_on and pos is not None:
                    if pos.get("closing"):
                        what = "ignored: selling"
                    else:
                        pos["closing"] = True
                        asyncio.ensure_future(self._shadow_exit(name, f"Mayhem_Mode {side.upper()}"))
                        what = "SELL"
                elif side == enter_on:
                    what = "ignored: holding"
                elif exit_on and side == exit_on:
                    what = "ignored: nothing held"
                else:
                    what = f"ignored: {name} does not trade on {side.upper()}"
            if name == chosen:
                plan = what
        return plan

    async def _shadow_enter(self, name: str, mint: str, sym: str, add: bool = False) -> None:
        from src.execution import pump_curve as PC
        sh = self.shadow[name]
        try:
            from src.config.runtime import get_config
            size = float(getattr(get_config(), "max_position_sol", 0.015) or 0.015)
            c0, bps = await self._curve_now(mint)
            if not c0:
                return
            if add:
                held = sh["pos"]
                if not held or held["mint"] != mint or held.get("closing"):
                    return
                spot = float(c0["virtual_sol"]) / float(c0["virtual_token"]) if c0.get("virtual_token") else 0.0
                if spot <= 0 or spot >= float(held.get("last_px") or 0):
                    return            # ladder buys again only below the last buy price
            t0 = PC._tokens_for_sol(int(size * 1e9), c0["virtual_token"], c0["virtual_sol"], bps)
            await asyncio.sleep(SHADOW_LAND_S)
            c1, bps = await self._curve_now(mint)
            if not c1:
                return
            t1 = PC._tokens_for_sol(int(size * 1e9), c1["virtual_token"], c1["virtual_sol"], bps)
            if t1 < t0 * 0.85:
                sh["skips"] += 1          # pump would refuse it (6042): costs nothing
                self._paper_note(name, "warn", f"{sym} — buy refused: price moved over 15% before it landed (6042). Nothing bought.", mint)
                return
            if not self.pin or self.pin["mint"] != mint:
                return
            if add:
                held = sh["pos"]
                if not held or held.get("closing"):
                    return
                held["tokens"] += t1
                held["cost"] += size
                held["buys"] = int(held.get("buys") or 1) + 1
                held["last_px"] = size * 1e9 / t1 if t1 else held["last_px"]
                held["peak"] = None
                sh["marks"].append({"ts": time.time(), "mint": mint, "side": "buy", "label": f"B{held['buys']}"})
                self._paper_note(name, "info", f"{sym} — LADDER BUY {held['buys']}/{LADDER_MAX_BUYS}: {size:.4f} SOL below the "
                                 f"last buy (trade now {held['cost']:.4f} SOL).", mint)
                return
            sh["pos"] = {"mint": mint, "symbol": sym, "tokens": t1, "cost": size, "at": time.time(),
                         "pct": None, "peak": None, "buys": 1, "last_px": size * 1e9 / t1 if t1 else 0.0}
            sh["marks"].append({"ts": time.time(), "mint": mint, "side": "buy", "label": "B" if name not in LADDERS else "B1"})
            self._paper_note(name, "info", f"{sym} — BUY {size:.4f} SOL (paper, curve price).", mint)
        finally:
            sh["busy"] = False

    async def _shadow_exit(self, name: str, why: str) -> None:
        from src.execution import pump_curve as PC
        sh = self.shadow[name]
        pos = sh["pos"]
        if not pos:
            return
        try:
            await asyncio.sleep(SHADOW_LAND_S)
            c, bps = await self._curve_now(pos["mint"])
            value = PC._sol_for_tokens(int(pos["tokens"]), c["virtual_token"], c["virtual_sol"], bps) / 1e9 if c else 0.0
            pnl = value - pos["cost"] - 0.00004
            pnl -= 0.00002 * (int(pos.get("buys") or 1) - 1)     # one more network fee per extra buy
            pct = pnl / pos["cost"] * 100
            sh["log"].appendleft({"ts": time.time(), "pnl": round(pnl, 6), "pct": round(pct, 2),
                                  "held": round(time.time() - pos["at"], 1), "why": why, "symbol": pos["symbol"],
                                  "buys": int(pos.get("buys") or 1)})
            sh["marks"].append({"ts": time.time(), "mint": pos["mint"], "side": "sell", "label": f"S {pct:+.0f}%"})
            self._paper_note(name, "ok" if pnl > 0 else "warn",
                             f"{pos['symbol']} — SOLD {pnl:+.5f} SOL ({pct:+.1f}%) after {time.time() - pos['at']:.1f}s — {why}.", pos["mint"])
        finally:
            sh["done"][pos["mint"]] = time.time()
            if len(sh["done"]) > 400:
                for old in sorted(sh["done"], key=sh["done"].get)[:100]:
                    sh["done"].pop(old, None)
            sh["pos"] = None

    async def _shadow_tick(self) -> None:
        """Value each open paper-strategy holding; TP / stop / max hold; close when the pin moves."""
        from src.execution import pump_curve as PC
        open_ = [(n, sh["pos"]) for n, sh in self.shadow.items() if sh["pos"] and not sh["pos"].get("closing")]
        if not open_:
            return
        values: Dict[str, Any] = {}
        for name, pos in open_:
            mint = pos["mint"]
            if mint not in values:
                values[mint] = await self._curve_now(mint)
            c, bps = values[mint]
            if not c:
                continue
            value = PC._sol_for_tokens(int(pos["tokens"]), c["virtual_token"], c["virtual_sol"], bps) / 1e9
            pct = (value / pos["cost"] - 1.0) * 100.0
            pos["pct"] = pct
            if c.get("virtual_token"):
                pos["spot"] = float(c["virtual_sol"]) / float(c["virtual_token"])
            pos["peak"] = pct if pos["peak"] is None else max(pos["peak"], pct)
            why = ""
            if not self.pin or self.pin["mint"] != mint:
                why = "coin unpinned (paused / idle / closed)"
            elif pct >= TAKE_PROFIT_PCT:
                why = f"take profit {pct:+.1f}%"
            elif pos["peak"] >= TRAIL_ARM_PCT and pct <= pos["peak"] * TRAIL_KEEP \
                    and (name not in LADDERS or pct >= PROFIT_MIN_PCT):
                why = f"trail from {pos['peak']:+.1f}%"
            elif name in LADDERS:
                pass
            elif pct <= STOP_PCT:
                why = f"stop {pct:+.1f}%"
            elif time.time() - pos["at"] >= MAX_HOLD_S:
                why = f"max hold {MAX_HOLD_S:.0f}s"
            if why:
                pos["closing"] = True
                asyncio.ensure_future(self._shadow_exit(name, why))

    def _shadow_stats(self) -> Dict[str, Any]:
        out = {}
        for name, sh in self.shadow.items():
            rows = list(sh["log"])
            out[name] = {
                "trades": len(rows), "wins": sum(1 for x in rows if x["pnl"] > 0),
                "pnl": round(sum(x["pnl"] for x in rows), 6),
                "avgPct": round(sum(x["pct"] for x in rows) / len(rows), 2) if rows else None,
                "skips": sh["skips"], "open": bool(sh["pos"]),
                "rule": SHADOW_RULES.get(name, name),
            }
        return out

    def _paper_stats(self) -> Dict[str, Any]:
        rows = list(self.paper_log)
        lat = sorted(self.lat)
        land = sorted(self.land)
        def q(v, f):
            return round(v[min(len(v) - 1, int(len(v) * f))]) if v else None
        return {
            "trades": len(rows), "wins": sum(1 for x in rows if x["pnl"] > 0),
            "pnl": round(sum(x["pnl"] for x in rows), 6),
            "avgPct": round(sum(x["pct"] for x in rows) / len(rows), 2) if rows else None,
            "recent": rows[:12],
            "latP50": q(lat, 0.5), "latP95": q(lat, 0.95), "latN": len(lat),
            "landP50": q(land, 0.5), "landP95": q(land, 0.95), "landN": len(land),
        }

    # ---------- wiring ----------
    def start(self, manager, trader, fill_px=None, apply_since=None) -> None:
        if self.started:
            return
        self.started = True
        self.manager = manager
        self.trader = trader
        self.fill_px = fill_px
        self.apply_since = apply_since
        self.loop = asyncio.get_running_loop()
        try:
            from src.data.caller_stream import get_stream
            get_stream().add_raw_listener(self.on_stream)
        except Exception:
            log.debug("mayhem2 stream hook failed", exc_info=True)
        for coro in (self._board_loop(), self._pos_loop(), self._trades_loop(), self._state_loop(), self._brain_loop(),
                     self._value_loop()):
            asyncio.ensure_future(coro)
        log.info("MAYHEM2 engine started (board %.1fs, trades %.2fs, state %.1fs)", BOARD_EVERY, TRADES_EVERY, STATE_EVERY)

    def stream_mints(self) -> List[str]:
        pin = self.pin
        return [pin["mint"]] if pin else []

    # ---------- page ----------
    def snapshot(self) -> Dict[str, Any]:
        self.view_at = time.time()
        now = time.time()
        try:
            from src.config.runtime import get_config, mayhem2_on
            cfg = get_config()
            on = mayhem2_on()
            trade_on = bool(cfg.trade_enabled)
            max_pos = float(getattr(cfg, "max_position_sol", 0) or 0)
        except Exception:
            on, trade_on, max_pos = False, False, 0.0
        with self.lock:
            board = list(self.board)
            board_at = self.board_at
            err = self.board_err
            states = dict(self.state)
            posd = dict(self.pos)
            notes = [dict(n) for n in list(self.notes)[:200]]
            signals = [dict(s) for s in list(self.signals)[:20]]
        shift = max(0.0, now - board_at) if board_at else 0.0
        pin = dict(self.pin) if self.pin else None
        mixed = self.mixed()
        if mixed:
            board = self._mixed_candidates()[:60]
        rows = []
        for r in board:
            mint = str(r.get("m") or "")
            if not mint or str(r.get("pg") or "") != "pump":
                continue
            age = float(r["_age"]) if mixed else float(r.get("age") or 0) + shift
            st = states.get(mint) or {}
            state = str(st.get("state") or r.get("ms") or "")
            is_pin = bool(pin and pin["mint"] == mint)
            if age > YOUNG_S and not is_pin and not mixed:
                continue
            p = posd.get(mint) or {}
            rows.append({
                "mint": mint, "symbol": r.get("t") or "", "name": r.get("n") or "", "image": r.get("i") or "",
                "age": round(age, 1), "state": state, "mode": r.get("mm") or "",
                "mcUsd": _f(r.get("mc")), "tx5": r.get("tx5"), "volUsd": _f(r.get("vUsd")),
                "held": p.get("held"), "pnlUsd": p.get("pnlUsd"), "pnlPercentage": p.get("pnlPercentage"),
                "costBasisUsd": p.get("costBasisUsd"), "posAge": round(now - float(p["at"]), 1) if p.get("at") else None,
                "pinned": is_pin, "used": (self.used.get(mint) or {}).get("why", ""),
                "grp": r.get("_grp") or "", "graduated": bool(r.get("gd")),
            })
        rows.sort(key=lambda r: 0 if r["pinned"] else 1)     # the pinned coin always on top
        if pin and not any(r["pinned"] for r in rows):
            p = posd.get(pin["mint"]) or {}
            st = states.get(pin["mint"]) or {}
            rows.insert(0, {"mint": pin["mint"], "symbol": pin.get("symbol"), "name": pin.get("name"),
                            "image": pin.get("image"), "age": None, "state": st.get("state") or "", "mode": "",
                            "held": p.get("held"), "pnlUsd": p.get("pnlUsd"), "pnlPercentage": p.get("pnlPercentage"),
                            "costBasisUsd": p.get("costBasisUsd"), "pinned": True, "used": ""})
        bot = None
        if pin:
            pos = self._held(pin["mint"])
            if pos is not None:
                bot = {
                    "paper": bool(getattr(pos, "_paper", False)),
                    "valueSol": getattr(pos, "_m2_value", None),
                    "sol": float(getattr(pos, "entry_sol", 0) or 0),
                    "pct": self._since(pos),
                    "entryUsd": float(getattr(pos, "entry_price_usd", 0) or 0),
                    "liveUsd": float(getattr(pos, "_live_px", 0) or 0),
                    "since": round(now - float(getattr(pos, "entry_time", 0) or now), 1),
                    "selling": bool(getattr(pos, "_sell_busy", False)),
                    "buys": int(getattr(pos, "_m2_buys", 1) or 1),
                    "maxBuys": LADDER_MAX_BUYS if self.strategy() in LADDERS else 1,
                }
            elif not self._own():
                sp = self.shadow[self.strategy()]["pos"]
                if sp and sp.get("mint") == pin["mint"]:
                    bot = {
                        "paper": True, "shadow": True, "valueSol": None, "sol": float(sp.get("cost") or 0),
                        "pct": None if sp.get("pct") is None else round(float(sp["pct"]), 2),
                        "entryUsd": 0.0, "liveUsd": 0.0, "since": round(now - float(sp.get("at") or now), 1),
                        "selling": bool(sp.get("closing")), "buys": int(sp.get("buys") or 1),
                        "maxBuys": LADDER_MAX_BUYS if self.strategy() in LADDERS else 1,
                    }
            st = states.get(pin["mint"]) or {}
            pin.update({
                "pinnedFor": round(now - float(pin["ts"]), 1),
                "state": st.get("state") or "", "stateAge": round(now - float(st["at"]), 1) if st.get("at") else None,
                "want": self.want, "busy": self.busy, "bot": bot,
                "exiting": self.exiting.get(pin["mint"], ""),
                "waitProfit": round(WAIT_PROFIT_S - (now - self.wait_profit[pin["mint"]]), 1) if pin["mint"] in self.wait_profit else None,
                "lastPx": self.last_px.get(pin["mint"]),
            })
        used = sorted(
            ({"mint": m, **v} for m, v in self.used.items()), key=lambda x: -x["ts"],
        )[:12]
        # Chart, pin trades and messages follow the selected strategy: the bot's own trades when it
        # trades it (live / dry run), else that strategy's paper trades on the same signals.
        chosen = self.strategy()
        own = self._own()
        src_marks = self.marks if own else self.shadow[chosen]["marks"]
        marks = [dict(m) for m in list(src_marks) if pin and m.get("mint") == pin["mint"]][-60:]
        view = ("live" if self.trading() else "dry") if own else "paper"
        return {
            "ok": True, "on": on, "trading": on and trade_on and not self.dry(), "dry": self.dry(),
            "strategy": self.strategy(), "strategies": SHADOW_RULES,
            "paper": self._paper_stats(),
            "shadow": self._shadow_stats(), "tradeOn": trade_on, "maxPos": max_pos,
            "boardAge": round(now - board_at, 1) if board_at else None, "error": err,
            "pin": pin, "rows": rows, "signals": signals, "notes": notes, "used": used,
            "marks": marks, "view": view,
            "youngS": YOUNG_S, "feed": self.feed(),
            "boards": {g: len(self.mixed_rows.get(g) or []) for g in MIXED_GROUPS} if mixed else None,
            "rules": {"profitMin": PROFIT_MIN_PCT, "waitS": WAIT_PROFIT_S, "tp": TAKE_PROFIT_PCT,
                      "trailArm": TRAIL_ARM_PCT, "trailKeep": TRAIL_KEEP, "stop": STOP_PCT, "maxHoldS": MAX_HOLD_S},
        }


_engine: Optional[Mayhem2] = None


def get_engine() -> Mayhem2:
    global _engine
    if _engine is None:
        _engine = Mayhem2()
    return _engine
