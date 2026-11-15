"""Candle-momentum auto-trader (paper by default; live when mode is "live" and Auto Trade is on).

Polls 1s candles per watched pump coin, turns each new finished candle into a BUY / SELL signal
(see candle_core.evaluate), and fills it on paper against pump's bonding curve:

  signal  -> checks (kill, position, feed health, limits, cooldown, dedupe)
          -> intent: the real local pump transaction is built and signed (never sent) -> latency
          -> fill: the curve read fill_lag_ms later prices the trade (fee + slippage included)

Paper: nothing is ever sent. Live (mode "live", switched on from the Candles page with a confirm; its own
switch, independent of Auto Trade and of the circuit breaker): BUY goes through trader.buy(source=
"candle") — Max position size, the same local pump build + paid-RPC send as Mayhem2 — and SELL
through manager.kick_now(pos, "candle_sell"). No other exit rule touches a candle trade except the
coin pausing, max_hold_s, or your own -1 / Sell. Built on request 2026-11-10 although the paper
results were negative.

Providers (FR-1.8): pump_rest (frontend-api-v3 /candles) and stream (1s candles built from the
on-chain trade socket the bot already runs). The fallback takes over while the primary is degraded.

Files: config/candle.yaml (hot-reloaded), logs/candle_momo.sqlite, logs/candle_audit.jsonl.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from src.signal.candle_core import (Candle, CandleBook, Config, evaluate, ladder_decision, ladder_decision_down,
                                    ladder_lot_size,
                                    parse_pump, percentile, signal_id, validate)
from src.signal.candle_store import Store

log = logging.getLogger("candle")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(ROOT, "config", "candle.yaml")
DB_PATH = os.path.join(ROOT, "logs", "candle_momo.sqlite")
AUDIT_PATH = os.path.join(ROOT, "logs", "candle_audit.jsonl")
CANDLE_URL = ("https://frontend-api-v3.pump.fun/candles/solana:5eykt4UsFv8P8NJdTREpY1vzqKqZKvdp/{mint}"
              "?res={res}&count={count}&measure={measure}")
BOARD_URL = "https://advanced-indexer.pump.fun/boards/mayhem?tier=web&offset=0&limit=30"
BASE_FEE_LAMPORTS = 5000
FEED_ONLY_SOURCES = ("opencopy", "callout", "mayhem")
OPENCOPY_MAX_FEEDS = 5         # AutoTrade-Candles on OpenCopy: coins fed (and traded) at the same time
# Dayscalp, CallOuts and OpenCopy taken out at the user's request (2026-11-14)
SEQ_STEPS = (("launch", "Launch"), ("mayhem", "MayhemMode"), ("board:mayhem_board", "Candles · Mayhem board"),
             ("board:mixed", "Candles · Mixed"))
SEQ_STEP_S = 60.0              # Source Sequence: a step with no trade moves on after this   # AutoTrade-Candles: these sources only offer tokens
FLAT_PCT = 0.05                # "no candles yet": the only reading moved less than this (shows 0.00%)
PRO_FIRST_START_CANDLES = 2    # first-buy "start": the chart counts as just started up to this many candles
PAUSED_DUST_SOL = 0.0002       # Paused Coins: a held paused coin worth less than this (or 2% of its cost) is left
PAUSED_DUST_FRAC = 0.02
PRO_FIRST_MAX_AGE_S = 60.0     # pro_hold first-candle buy: only coins this young (a new coin's first candle)
SKIP_REASONS = ("KILLED", "NO_POSITION", "ALREADY_LONG", "ENTRIES_PAUSED", "FEED_DEGRADED", "STALE_FEED",
                "BLACKLIST", "NOT_WHITELISTED", "DAILY_LOSS", "RATE_LIMIT", "COOLDOWN", "MAX_EXPOSURE",
                "MAX_TRADE", "NOT_READY", "CIRCUIT_BREAKER", "BUY_UNRESOLVED", "DUPLICATE", "COALESCED", "STALE", "IN_FLIGHT", "NOT_TRADABLE", "LADDER_WAIT", "LADDER_MAX", "LADDER_CAP", "GREEN_CONFIRM", "NOT_IN_PROFIT", "LOW_RESERVES", "CRASH_CANDLE", "BELOW_BREAKEVEN", "SOL_OUTFLOW",
                "FAR_BELOW_PEAK", "LADDER_ABOVE", "SUPPORT_NO_ENTRY", "AUTO_TRADE_OFF", "MAYHEM_COIN")


# ---------------------------------------------------------------- providers

class CandleProvider:
    name = "base"

    async def fetch(self, mint: str, cfg: Config) -> List[Candle]:
        raise NotImplementedError


class PumpRestProvider(CandleProvider):
    """pump.fun frontend-api-v3 /candles. Unofficial: Chrome-fingerprint client, ~10 req/s per IP."""
    name = "pump_rest"

    async def fetch(self, mint: str, cfg: Config) -> List[Candle]:
        from src.data.fast_get import mayhem2_get
        url = CANDLE_URL.format(mint=mint, res=cfg.res, count=cfg.count, measure=cfg.measure)
        body = await asyncio.to_thread(mayhem2_get, url, "mayhem2candles", 1.5)
        rows = parse_pump(body)
        if not rows:
            raise ValueError("empty candle list")
        return rows


class StreamProvider(CandleProvider):
    """1s candles built from the trade socket (price in SOL per token; only ratios are used).
    Each candle opens at the previous candle's close, as on pump.fun's chart."""
    name = "stream"

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.bars: Dict[str, Dict[int, List[float]]] = {}    # mint -> t -> [open, high, low, close, vol]
        self.wanted: set = set()

    def on_trade(self, ev: Dict[str, Any]) -> None:
        mint = str(ev.get("mint") or "")
        if mint not in self.wanted:
            return
        # The trade's own price (SOL / tokens) from whichever source sent it first (PumpPortal or chain logs);
        # price_sol stays 0 for PumpPortal until it is matched to the chain, which never happens for most
        # graduated coins: their candles got no trades. Only ratios are used, so the small fee difference
        # between the two sources does not matter.
        px = float(ev.get("trade_px") or 0) or float(ev.get("price_sol") or 0)
        if px <= 0:
            return
        t = int(float(ev.get("ts") or time.time())) * 1000
        with self.lock:
            bars = self.bars.setdefault(mint, {})
            b = bars.get(t)
            if b is None:
                bars[t] = [px, px, px, px, float(ev.get("sol") or 0)]
                for old in sorted(bars)[:-300]:
                    bars.pop(old, None)
            else:
                b[1], b[2], b[3] = max(b[1], px), min(b[2], px), px
                b[4] += float(ev.get("sol") or 0)

    async def fetch(self, mint: str, cfg: Config) -> List[Candle]:
        now_ms = int(time.time() * 1000)
        with self.lock:
            bars = dict(self.bars.get(mint) or {})
        # Like pump.fun's chart: each candle opens at the previous candle's close. Most seconds on these
        # coins hold one trade, so a candle opened at its own first trade showed 0.0% and hid every move.
        rows: List[Candle] = []
        prev_close: Optional[float] = None
        for t, b in sorted(bars.items()):
            o = prev_close if prev_close is not None else b[0]
            rows.append(Candle(t=t, open=o, high=max(b[1], o), low=min(b[2], o), close=b[3], volume=b[4],
                               final=t + 1000 <= now_ms))
            prev_close = b[3]
        if not rows:
            raise ValueError("no stream trades yet")
        return rows


# ---------------------------------------------------------------- engine

class Feed:
    def __init__(self, mint: str, size: int) -> None:
        self.mint = mint
        self.books = {"pump_rest": CandleBook(size), "stream": CandleBook(size)}
        self.fails = {"pump_rest": 0, "stream": 0}
        self.ok_at = {"pump_rest": 0.0, "stream": 0.0}
        self.err = {"pump_rest": "", "stream": ""}
        self.last_eval_t: Optional[int] = None
        self.status = "STARTING"
        self.using = ""
        self.symbol = mint[:6]
        self.origin = "manual"
        self.board_state = ""
        self.age = None
        self.added = time.time()
        self.reserves: Deque[Tuple[float, float]] = deque(maxlen=90)   # (ts, SOL in the curve), ~1s apart


_EXIT_LABELS = {
    "fast_pace_sell": "the source's FAST PACE take-profit", "caller_closed": "the copied caller sold",
    "manual_now": "your -1 / Sell button", "copy_stop": "the source's copy stop", "trailing_stop": "a trailing stop",
    "stop_loss": "a stop loss", "hard_stop": "the hard stop", "take_profit": "a take-profit level",
    "not_in_wallet": "coins no longer in the wallet", "candle_sell": "Candles' sell",
    "mayhem_paused": "pump paused the coin — MayhemMode's pause exit sells at once (always allowed)",
}


def _exit_label(reason: str) -> str:
    r = str(reason or "")
    return f"bot rule: {_EXIT_LABELS.get(r, r.replace('_', ' ') or 'sold')}"


SUPPORT_SOURCES = ("opencopy", "callout", "launch", "mayhem", "dayscalp")
SUPPORT_NAMES = {"opencopy": "OpenCopy", "callout": "Callout", "launch": "Launch", "mayhem": "MayhemMode",
                 "dayscalp": "Dayscalp"}
MAYHEM_MAX_OPEN_AGE_S = 30.0   # MayhemMode: only a coin bought while Mayhem_Mode's open was < 30s old   # Live Controls sources AutoTrade-Candles can support


class CandleMomo:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.cfg = Config()
        self.cfg_mtime = 0.0
        self.cfg_errors: List[str] = []
        self.store: Optional[Store] = None
        self.providers: Dict[str, CandleProvider] = {"pump_rest": PumpRestProvider(), "stream": StreamProvider()}
        self.feeds: Dict[str, Feed] = {}
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.busy: Dict[str, str] = {}               # mint -> signal id in flight
        self.pending: Dict[str, Dict[str, Any]] = {}
        self.last_trade_at: Dict[str, float] = {}
        self.sig_times: Deque[float] = deque(maxlen=1000)
        self.recent: Deque[Dict[str, Any]] = deque(maxlen=60)
        self.notes: Deque[Dict[str, Any]] = deque(maxlen=60)
        self.lat_ms: Deque[float] = deque(maxlen=500)     # signal -> signed tx ready
        self.fill_ms: Deque[float] = deque(maxlen=500)    # signal -> priced fill
        self.feed_lag_ms: Deque[float] = deque(maxlen=500)
        self.slip_bps: Deque[float] = deque(maxlen=500)
        self.m = {"polls": 0, "poll_errors": 0, "signals_buy": 0, "signals_sell": 0, "executed": 0,
                  "exec_failed": 0, "skips": {}}
        self.exec_fail_streak = 0
        self.board: List[Dict[str, Any]] = []
        self.board_at = 0.0
        self.unusable: Dict[str, str] = {}          # mint -> why (this session): never watched again
        self.alerted: Dict[str, float] = {}
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.manager = None
        self.trader = None
        self.unresolved: Optional[Dict[str, Any]] = None
        self.streak: Dict[str, List[Any]] = {}      # mint -> [side, consecutive count] of signals
        self.reds_since: Dict[str, int] = {}         # mint -> red signals since the last lot (downside ladder)
        self.pro_px: Dict[str, float] = {}           # rule pro: mint -> chart price of the BUY call (its entry line)
        self.traded: Dict[str, float] = {}           # mint -> when its last trade closed (one-time-use mode)
        self._mx_rows: Dict[str, List[Dict[str, Any]]] = {}   # Mixed feed: board rows by group
        self._mx_at: Dict[str, float] = {}
        self.seq_board = ""                           # Source Sequence board step: "mayhem_board" | "mixed" | ""
        self.seq: Dict[str, Any] = {"i": -1, "at": 0.0, "trade_at": 0.0, "state": "off"}
        self.offers: Dict[str, Dict[str, Any]] = {}
        self._trade_ev: Dict[str, asyncio.Event] = {}      # held coin -> a trade just arrived (instant take-profit)
        self._instant = False
        self.paused_mints: Dict[str, float] = {}
        self._pin_warned: set = set()
        self._named: set = set()                       # mints whose name was looked up (pinned / unnamed feeds)      # coins pump paused / completed while Candles held them    # AutoTrade-Candles feed-only sources: mint -> offer
        self._mirrored: Dict[str, float] = {}         # Paper + AutoTrade-Candles: coins given a paper lot 1
        self.mayhem_of: Dict[str, Optional[bool]] = {}  # mint -> Mayhem coin? (None = still checking)
        self._mx_turn = 0                             # index in mayhem2.MIXED_GROUPS whose turn is next
        self._mx_since: Dict[str, float] = {}         # mint -> when Mixed started watching it
        self._mx_left: Dict[str, float] = {}          # mint -> when Mixed handed it over
        self.board_seen: Dict[str, float] = {}       # mint -> last time the board listed it
        self._jup_val: Dict[str, Tuple[float, float, int]] = {}   # mint -> (SOL value, ts, raw tokens)
        self._quote_dec: Dict[str, int] = {}         # quote coin -> decimals
        self._qrate: Dict[str, Tuple[float, float]] = {}   # quote coin -> (SOL per base unit, ts)
        self._grab: Dict[str, Dict[str, Any]] = {}   # MayhemMode support: young coins being bought (mint -> info)
        self.started = False
        self.started_at = time.time()

    # ---------- config
    def load_config(self, force: bool = False) -> None:
        try:
            mtime = os.path.getmtime(CONFIG_PATH)
        except OSError:
            self.save_config(self.cfg)
            return
        if not force and mtime == self.cfg_mtime:
            return
        import yaml
        try:
            with open(CONFIG_PATH, encoding="utf-8") as fh:
                data = yaml.safe_load(fh) or {}
        except Exception as exc:
            self.cfg_errors = [f"config unreadable: {exc}"[:160]]
            self.cfg_mtime = mtime
            return
        old = self.cfg.digest()
        self.cfg, self.cfg_errors = validate(data if isinstance(data, dict) else {}, Config())
        self.cfg_mtime = mtime
        if self.cfg.digest() != old and self.store:
            self.store.audit("config", digest=self.cfg.digest(), config=self.cfg.snapshot(), errors=self.cfg_errors)
            self.note("info", f"Config loaded ({self.cfg.digest()}): rule {self.cfg.rule}, Δ≥{self.cfg.min_delta_pct}%, "
                              f"mode {self.cfg.mode.upper()}." + (f" Errors: {'; '.join(self.cfg_errors)}" if self.cfg_errors else ""))

    def save_config(self, cfg: Config) -> None:
        import yaml
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write("# Candle-momentum auto-trader (paper only). Edited by the dashboard Candles page too;\n"
                     "# changes apply within 1s. See src/signal/candle_core.py Config for every field.\n")
            yaml.safe_dump(cfg.snapshot(), fh, sort_keys=False)
        os.replace(tmp, CONFIG_PATH)
        self.cfg_mtime = 0.0

    def update(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Runtime API (dashboard). Returns {ok, errors, config}. Kill takes effect at once."""
        cfg, errors = validate(data, self.cfg)
        self.cfg = cfg
        self.save_config(cfg)
        self.load_config(force=True)
        if "kill" in data and self.store:
            self.store.audit("kill", on=bool(self.cfg.kill))
            self.note("bad" if self.cfg.kill else "info", "KILL SWITCH ON: every new intent is skipped." if self.cfg.kill
                      else "Kill switch off.")
        return {"ok": not errors, "errors": errors, "config": self.cfg.snapshot()}

    # ---------- helpers
    def note(self, level: str, msg: str, mint: str = "") -> None:
        self.notes.appendleft({"ts": time.time(), "level": level, "msg": msg, "mint": mint})
        (log.warning if level in ("warn", "bad") else log.info)("CANDLE %s", msg)

    def alert(self, key: str, msg: str, every: float = 300.0) -> None:
        if time.time() - self.alerted.get(key, 0) < every:
            return
        self.alerted[key] = time.time()
        self.note("bad", "ALERT: " + msg)
        if self.store:
            self.store.audit("alert", key=key, msg=msg)

    def skip(self, sig: Dict[str, Any], reason: str) -> str:
        self.m["skips"][reason] = self.m["skips"].get(reason, 0) + 1
        sig["action"], sig["reason"] = "skipped", reason
        if self.store:
            self.store.signal(sig, "skipped", reason)
            self.store.audit("skip", signalId=sig["id"], mint=sig["mint"], side=sig["side"], reason=reason,
                             cfg=self.cfg.digest())
        return reason

    def _day_start(self) -> float:
        t = time.gmtime()
        return time.mktime((t.tm_year, t.tm_mon, t.tm_mday, 0, 0, 0, 0, 0, 0)) - time.timezone

    def watched(self) -> List[str]:
        return list(self.feeds)

    # ---------- watch list
    async def _board_loop(self) -> None:
        from src.data.fast_get import mayhem2_get
        while True:
            await asyncio.sleep(2.0)
            try:
                self.load_config()
                if not self.cfg.enabled:
                    continue
                rows: List[Dict[str, Any]] = []
                if self.follow() == "mixed" and not self.support_source():
                    rows = await self._mixed_boards()
                    self.board, self.board_at = rows, time.time()
                elif self.follow() == "mayhem_board":
                    try:
                        from src.signal.mayhem2 import get_engine as _m2
                        m2 = _m2()
                        if time.time() - float(m2.board_at or 0) < 3.0 and m2.board:
                            rows = list(m2.board)
                    except Exception:
                        rows = []
                    if not rows:
                        body = await asyncio.to_thread(mayhem2_get, BOARD_URL, "mayhem2candleboard", 1.5)
                        rows = list((body or {}).get("entries") or []) if isinstance(body, dict) else []
                    self.board, self.board_at = rows, time.time()
                self._pick(rows)
            except Exception as exc:
                log.debug("candle board failed: %s", exc)

    # ---------- support mode (AutoTrade-Candles on OpenHolds / CallOuts)
    def support_source(self) -> str:
        """'opencopy' / 'callout' / 'launch' while AutoTrade-Candles is on and that is the Live Controls source, else ''.
        A Source Sequence board step (Candles · Mayhem board / Mixed) has no support source: Candles picks itself."""
        if not self.cfg.support_enabled or self.seq_board:
            return ""
        src = self.live_filter()
        return src if src in SUPPORT_SOURCES else ""

    @staticmethod
    def live_filter() -> str:
        try:
            from src.config.runtime import get_config
            return str(getattr(get_config(), "filter_mode", "") or "")
        except Exception:
            return ""

    def follow(self) -> str:
        """Candles' own feed: the Source Sequence board step, else the Feed checkbox."""
        return self.seq_board or self.cfg.auto_follow

    @staticmethod
    def _eligible(p, src: str) -> bool:
        """The support source's coin that Candles takes over. MayhemMode: only the youngest opens —
        bought while Mayhem_Mode's open was < 30s old."""
        if src not in (str(getattr(p, "source", "") or ""), str(getattr(p, "strategy", "") or "")):
            return False
        if src == "mayhem":
            age = getattr(p, "_open_age", None)
            return age is not None and float(age) < MAYHEM_MAX_OPEN_AGE_S
        return True

    def _support_held(self, src: str) -> List[Any]:
        if self.manager is None:
            return []
        return [p for p in list(self.manager.positions.values())
                if p.status == "OPEN" and p.mint and (self._eligible(p, src) or p.mint in self.positions)]

    def _support_sync(self, src: str) -> None:
        """The source's held coins become Candles trades (lot 1 = the source's buy). Their source and rules
        stay; Candles only adds ladder lots and sells by its own rule. In PAPER nothing live is taken over."""
        if self.cfg.mode != "live":
            for p in self._support_held(src):
                rec = self.positions.get(p.mint)
                if not (rec and rec.get("mode") == "live"):
                    p._candle_support = False         # the source's own rules sell its live trade as usual
            return
        for p in self._support_held(src):
            p._candle_support = True        # the source's exit rules stop: Candles sells it by its rule
            if p.mint in self.positions:
                continue
            rec = {"mint": p.mint, "symbol": getattr(p, "symbol", "") or p.mint[:6], "mode": "live",
                   "tokens": float(p.tokens or 0), "entry_sol": float(p.entry_sol or 0),
                   "entry_ts": float(getattr(p, "entry_time", 0) or time.time()),
                   "entry_signal": f"support-{src}-{p.mint[:8]}", "costs_sol": float(getattr(p, "fees_sol", 0) or 0),
                   "support": src,
                   "lots": [{"n": 1, "sol": float(p.entry_sol or 0), "tokens": float(p.tokens or 0),
                             "ts": float(getattr(p, "entry_time", 0) or time.time()), "signal": f"{src} buy"}]}
            self.positions[p.mint] = rec
            self.reds_since[p.mint] = 0
            if self.store:
                self.store.open_position(rec)
            self.note("info", f"{rec['symbol']} — {src} bought it (lot 1). Candles adds lots and sells it by the {self.cfg.rule} "
                                         f"rule; {src}'s own rule sells only while the trade is in profit — in a loss Candles' rule takes over.", p.mint)

    def _paper_lot1(self, want: Dict[str, str], src: str) -> None:
        """Paper + AutoTrade-Candles, rules other than pro_hold: the source's lot 1 is played as a paper buy the
        moment the coin is fed (once per coin), then Candles adds lots and sells it by its rule on paper."""
        now = time.time()
        for m in want:
            if (m in self.positions or m in self.busy or m in self.unusable or m in self._mirrored
                    or len(self.trading()) >= max(1, int(self.cfg.max_mints or 1))):
                continue
            self._mirrored[m] = now
            f = self.feeds.get(m)
            sig = {"id": f"paper-lot1-{m}-{int(now * 1000)}", "mint": m, "ts": round(now * 1000), "side": "BUY",
                   "open": 0.0, "prevClose": None, "close": 0.0, "ref": 0.0, "deltaPct": 0.0,
                   "candleTs": int(now * 1000), "rule": self.cfg.rule, "provider": "paper-support", "feedLagMs": 0,
                   "seen": now, "mirror": True, "_counted": True, "symbol": (f.symbol if f else m[:6])}
            self.recent.appendleft(sig)
            self.note("info", f"{sig['symbol']} — PAPER lot 1 for {src} (what {src} would buy live).", m)
            self._start(sig)

    def feeds_only(self, src: str) -> bool:
        """AutoTrade-Candles on for CallOuts / OpenCopy / MayhemMode (user 2026-11-14): the source does not buy, pin or
        hold — every new token its callers (or Mayhem_Mode) open is offered to Candles, which buys and sells it by its
        own rule (Live or Paper as the Candles switch). Off: the source follows its own rules, no relation to Candles."""
        # the Live Controls source itself (not support_source()): on a Source Sequence board step MayhemMode stays
        # the source and must still only offer, not buy
        return src in FEED_ONLY_SOURCES and bool(self.cfg.support_enabled) and self.live_filter() == src

    def offer(self, src: str, mint: str, symbol: str = "", age: Optional[float] = None) -> bool:
        """A source hands a new token to Candles instead of buying it. True the first time (for logging)."""
        if not mint or mint in self.unusable:
            return False
        now = time.time()
        first = mint not in self.offers
        o = self.offers.setdefault(mint, {"src": src, "at": now, "symbol": symbol or mint[:6], "age": age})
        o["seen"] = now
        if first:
            self.note("info", f"{o['symbol']} — {src} offered it (new token); Candles buys / sells it by its "
                              f"{self.cfg.rule} rule ({self.cfg.mode.upper()}).", mint)
        return first

    # ---------- Source Sequence (Candles page button; user 2026-11-14)
    def _seq_switch(self, i: int) -> None:
        """Enter step i: a source step switches the Live Controls source (AutoTrade-Candles on, Candles' data and
        Live / Paper untouched); a board step lets Candles pick from that board itself."""
        key, name = SEQ_STEPS[i]
        now = time.time()
        self.seq.update(i=i, at=now, trade_at=0.0, state="watching")
        if key.startswith("board:"):
            self.seq_board = key.split(":", 1)[1]
            if not self.cfg.support_enabled:
                self.update({"support_enabled": True})      # the last source (MayhemMode) only offers meanwhile
        else:
            self.seq_board = ""
            self._seq_apply_source(key)
            if not self.cfg.support_enabled:
                self.update({"support_enabled": True})
        self.feeds.clear()
        self.providers["stream"].wanted = set()
        self.note("info", f"Source Sequence: step {i + 1}/{len(SEQ_STEPS)} — {name} ({self.cfg.mode.upper()}).")

    def _seq_apply_source(self, key: str) -> None:
        patch = {"filter_mode": key, "launch_enabled": key == "launch", "dayscalp_enabled": key == "dayscalp",
                 "callout_enabled": key == "callout", "opencopy_enabled": key == "opencopy",
                 "mayhem_enabled": key == "mayhem", "mayhem2_enabled": False, "predict_enabled": False}
        try:
            from src.observability.dashboard import bus as _bus
            if _bus._on_config:
                res = _bus._on_config(patch)                     # same path as the Settings page source switch
                if asyncio.iscoroutine(res):
                    asyncio.ensure_future(res)
            else:
                from src.config.runtime import update_config
                update_config(patch)
        except Exception:
            log.warning("Source Sequence: switch to %s failed", key, exc_info=True)

    async def _seq_loop(self) -> None:
        """Each step: up to SEQ_STEP_S for a Candles trade to open; a trade that opened is waited for until it
        closes (its sell, or Max hold). Then the next step; after Candles · Mixed, Launch again. Never switches
        while a Candles trade is open or in flight."""
        while True:
            await asyncio.sleep(1.0)
            try:
                if not self.cfg.source_seq:
                    if self.seq["state"] != "off":
                        self.seq_board = ""
                        self.seq.update(state="off")
                        self.note("info", "Source Sequence OFF — the current source stays; sources follow their own rules.")
                    continue
                now = time.time()
                if self.seq["i"] < 0 or self.seq["state"] == "off":
                    self._seq_switch(0 if self.seq["i"] < 0 else self.seq["i"])
                    continue
                busy = bool(self.active_positions()) or bool(self.busy) or bool(self.unresolved)
                if busy:
                    if not self.seq["trade_at"]:
                        self.seq["trade_at"] = now
                    self.seq["state"] = "trade open — waiting for its sell or Max hold"
                    continue
                if self.seq["trade_at"] or now - self.seq["at"] >= SEQ_STEP_S:
                    self._seq_switch((self.seq["i"] + 1) % len(SEQ_STEPS))
                else:
                    self.seq["state"] = "watching"
            except Exception:
                log.debug("source sequence failed", exc_info=True)

    def seq_status(self) -> Dict[str, Any]:
        i = self.seq["i"]
        on = bool(self.cfg.source_seq) and i >= 0 and self.seq["state"] != "off"
        return {"on": bool(self.cfg.source_seq), "step": (i + 1) if on else 0, "steps": len(SEQ_STEPS),
                "name": SEQ_STEPS[i][1] if on else "", "next": SEQ_STEPS[(i + 1) % len(SEQ_STEPS)][1] if on else "",
                "state": self.seq["state"] if on else "off",
                "left": max(0, round(SEQ_STEP_S - (time.time() - self.seq["at"]))) if on and not self.seq["trade_at"] else None}

    def pro_hold_owns_entries(self, src: str) -> bool:
        """pro_hold with AutoTrade-Candles on this source: Candles buys (on Botpredict's BUY); the source only feeds."""
        return ((self.cfg.rule == "pro_hold" or self.paper_support()) and self.support_source() == src) or self.feeds_only(src)

    def paper_support(self) -> bool:
        """AutoTrade-Candles on with Candles in PAPER (user 2026-11-14: test a source without real SOL). Candles
        paper-buys lot 1 itself on the source's coins, the source's picks only feed it (no live buys started by
        AutoTrade-Candles), and the source's live trades are not taken over or held back."""
        return self.cfg.mode != "live" and bool(self.support_source())

    def _one_feed(self, want: Dict[str, str]) -> Dict[str, str]:
        """pro_hold: one coin at a time. The coin in trade (Candles' own or the source's, oldest first; a coin pump
        paused cannot sell and does not count), else the source's next candidate."""
        try:
            from src.observability.dashboard import mayhem_state_of
        except Exception:
            mayhem_state_of = lambda _m: ""
        held = [m for m in sorted(self.positions, key=lambda k: float(self.positions[k].get("entry_ts") or 0))
                if mayhem_state_of(m) != "paused"]
        if held:
            return {held[0]: want.get(held[0], "held")}
        for m, origin in want.items():
            if origin in ("scan", "mayhem-new", "mayhem-watch", "offer"):
                return {m: origin}
        for m, origin in want.items():
            if mayhem_state_of(m) != "paused":
                return {m: origin}
        return {}

    def _mayhem_grab(self) -> List[str]:
        """AutoTrade-Candles for MayhemMode: Mayhem_Mode's youngest open (< 30s old) goes to the feeds at
        once, and MayhemMode buys it (lot 1, pinned and held like a MayhemMode buy) — no waiting for
        MayhemMode's own auto-open. One coin at a time (Coins). Returns the mints being bought."""
        now = time.time()
        grab = self._grab
        for m, g in list(grab.items()):
            if m in self.positions or now - g["at"] > 25:      # bought (Candles took it) or the buy did not land
                grab.pop(m, None)
                if m not in self.positions:
                    self.traded[m] = now
        try:
            from src.config.runtime import MAYHEM_USER, MAYHEM_WALLET, get_config
            from src.observability.dashboard import mayhem_young_opens, queue_openhold_order
            cfg_now = get_config()
        except Exception:
            return list(grab)
        try:
            from src.observability.dashboard import mayhem_state_of
        except Exception:
            mayhem_state_of = lambda _m: ""
        # A coin pump paused cannot be sold (its sell keeps retrying): it does not take the Coins slot,
        # so the next young coin can still be bought.
        held = len([m for m, r in self.positions.items()
                    if r.get("support") == "mayhem" and mayhem_state_of(m) != "paused"]) + len(grab)
        if self.pro_hold_owns_entries("mayhem"):
            # pro_hold (or Paper) buys itself: the youngest open is only watched, and only while flat
            if held or self.trading():
                return []
            young = [str(r.get("coinMint") or "") for r in mayhem_young_opens(MAYHEM_MAX_OPEN_AGE_S)]
            young = [m for m in young if m and m not in self.traded and m not in self.unusable]
            return young[:1]
        if held >= max(1, int(self.cfg.max_mints)) or self.cfg.kill or self.cfg.entries_paused:
            return list(grab)
        if not bool(getattr(cfg_now, "trade_enabled", False)):
            self.alert_once("grab-off", "MayhemMode: Auto Trade is OFF — the youngest Mayhem_Mode coin is shown, not bought. "
                                        "Turn Auto Trade on to buy it.")
            young = [str(r.get("coinMint") or "") for r in mayhem_young_opens(MAYHEM_MAX_OPEN_AGE_S)[:1]]
            return list(grab) + [m for m in young if m]          # watch only
        held_mints = {p.mint for p in (self.manager.positions.values() if self.manager else []) if p.status == "OPEN"}
        for row in mayhem_young_opens(MAYHEM_MAX_OPEN_AGE_S):
            m = str(row.get("coinMint") or "")
            if m in held_mints or m in grab or m in self.traded or m in self.unusable:
                continue
            sym = str(row.get("symbol") or row.get("coinSymbol") or m[:6])
            grab[m] = {"at": now, "symbol": sym}
            queue_openhold_order("buy", m, sym, str(row.get("coinName") or row.get("name") or ""),
                                 MAYHEM_USER, MAYHEM_WALLET, force=False, age=float(row["age"]), candle=True)
            self.note("info", f"{sym} — Mayhem_Mode opened it {row['age']:.0f}s ago (youngest, < "
                              f"{MAYHEM_MAX_OPEN_AGE_S:.0f}s): watching it now and buying it as MayhemMode lot 1.", m)
            if self.store:
                self.store.audit("mayhem_grab", mint=m, age=round(float(row["age"]), 1))
            break                                       # the very first (youngest) only
        return list(grab)

    def green_target(self) -> Tuple[float, str]:
        """Whole-trade % a green candle needs to sell (Sell only in profit): Min profit, or the Settings
        Fast Pace margin when Fast Pace Sell is on and lower. -> (pct, where it comes from)."""
        mp = float(self.cfg.min_profit_pct)
        try:
            from src.config.runtime import get_config
            rc = get_config()
            if bool(getattr(rc, "fast_pace_sell", False)):
                fp = float(getattr(rc, "fast_pace_margin_pct", 0) or 0)
                if 0 < fp < mp:
                    return fp, "Fast Pace"
        except Exception:
            pass
        return mp, "Min profit"

    def alert_once(self, key: str, msg: str, every: float = 120.0) -> None:
        if time.time() - self.alerted.get(key, 0) >= every:
            self.alerted[key] = time.time()
            self.note("warn", msg)

    def set_support(self, on: bool, src: str = "") -> Dict[str, Any]:
        """Turn AutoTrade-Candles on (Candles stops its own trading, clears it, and supports the source live)
        or off (Revert: Candles back to normal, paper). src = the page's source (OpenHolds -> opencopy,
        CallOuts -> callout, Decoded Tokens -> launch): turning on is refused unless it is the Live Controls source."""
        if on and src:
            try:
                from src.config.runtime import get_config
                live = str(getattr(get_config(), "filter_mode", "") or "")
            except Exception:
                live = ""
            if live != src:
                name = SUPPORT_NAMES.get(src, src)
                return {"ok": False, "error": f"Change and Select Source to {name} (Live Controls source is now {live or '—'})."}
        if bool(on) == bool(self.cfg.support_enabled):
            return {"ok": True, "support": bool(on), "config": self.cfg.snapshot()}
        if self.busy or self.unresolved:
            return {"ok": False, "error": "A Candles trade is in progress — try again in a few seconds"}
        if on and any(p.get("mode") == "live" and not p.get("support") for p in self.positions.values()):
            return {"ok": False, "error": "Candles has its own LIVE trade open — sell it (Exit all) first"}
        if not on and self.manager is not None:
            for p in list(self.manager.positions.values()):
                if getattr(p, "_candle_support", False):
                    p._candle_support = False      # Revert: the source's own exit rules apply again
        # stop and clear what Candles is doing (history stays until Reset data)
        for mint, p in list(self.positions.items()):
            if p.get("mode") == "paper" or p.get("support"):
                self.positions.pop(mint, None)
                if self.store:
                    self.store.drop_position(mint)
        self.pending.clear()
        self.streak.clear()
        self.reds_since.clear()
        self.feeds.clear()
        self.providers["stream"].wanted = set()
        if on:
            # A clean page for the source: results, trades, signals, messages and stats start from zero
            # (the audit log is archived, not deleted).
            moved = self._wipe()
            if self.store:
                self.store.audit("reset", archived=os.path.basename(moved) if moved else "", why="support on")
        else:
            self.recent.clear()           # the source's signals; closed support trades stay in the table
            self.notes.clear()
            self.m["skips"] = {}
        self.offers.clear()
        out = self.update({"support_enabled": bool(on), "mode": "live" if on else "paper",
                           "support_since": time.time() if on else 0.0})
        if on:
            self.note("info", "AutoTrade-Candles ON — Candles stopped its own trading and now supports the Live Controls "
                              "source: its held coins are the feeds; Candles adds lots and sells them by its rule (LIVE).")
        else:
            self.note("info", "Reverted — Candles is back to normal, in PAPER mode. The source keeps its own coins and rules.")
        return dict(out, support=bool(on))

    # ---------- Mixed feed (Candles page checkbox, same boards as Mayhem2's "Mixed")
    async def _mixed_boards(self) -> List[Dict[str, Any]]:
        """New / Movers / Graduated boards (pump's own lists). Reuses Mayhem2's fresh copy when it reads them,
        else reads them here (New every 2 s, Movers / Graduated every 4 s). Rows carry _grp and _age."""
        from src.data.fast_get import mayhem2_get
        from src.signal.mayhem2 import MIXED_BOARDS, MIXED_GROUPS
        now = time.time()
        try:
            from src.signal.mayhem2 import get_engine as _m2
            m2 = _m2()
            if m2.mixed():
                with m2.lock:
                    for g in MIXED_GROUPS:
                        at = float(m2.mixed_at.get(g) or 0)
                        if now - at < 3.0 and m2.mixed_rows.get(g):
                            self._mx_rows[g] = list(m2.mixed_rows[g])
                            self._mx_at[g] = at
        except Exception:
            pass
        due = [(g, u) for g, u, _e in MIXED_BOARDS
               if now - float(self._mx_at.get(g) or 0) >= (2.0 if g == "new" else 4.0)]

        async def one(g: str, u: str):
            try:
                body = await asyncio.to_thread(mayhem2_get, u, "candlemixed" + g[:3], 3.0)
                return g, list((body or {}).get("entries") or []) if isinstance(body, dict) else None
            except Exception:
                return g, None
        for g, rows in await asyncio.gather(*(one(g, u) for g, u in due)):
            if rows is not None:
                self._mx_rows[g] = rows
                self._mx_at[g] = time.time()
        out = []
        for g in MIXED_GROUPS:
            shift = max(0.0, now - float(self._mx_at.get(g) or now))
            for r in self._mx_rows.get(g) or []:
                out.append(dict(r, _grp=g, _age=float(r.get("age") or 0) + shift))
        return out

    def _mixed_candidates(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Solana coins whose mint starts or ends with 'pump', New only under max age, a Mayhem coin only while
        active, not blacklisted / unusable / already traded (unless Reuse coin); youngest first in each board."""
        from src.signal.mayhem2 import MIXED_GROUPS, YOUNG_S
        cfg = self.cfg
        out = []
        for g in MIXED_GROUPS:
            part, seen = [], set()
            for r in rows:
                m = str(r.get("m") or "")
                if r.get("_grp") != g or not m or m in seen or not (m.startswith("pump") or m.endswith("pump")):
                    continue
                if not str(r.get("c") or "solana").startswith("solana"):
                    continue
                seen.add(m)
                if g == "new" and r["_age"] > min(YOUNG_S, cfg.max_age_s):
                    continue
                ms = str(r.get("ms") or "").lower()
                if ms and (cfg.avoid_mayhem or ms not in ("active", "enabled")):
                    continue                      # a Mayhem coin (pump can pause it), or one already paused
                if m in cfg.blacklist or m in self.unusable or (not cfg.reuse_coin and m in self.traded):
                    continue
                part.append(r)
            part.sort(key=lambda r: r["_age"])
            out.extend(part)
        return out

    def _mixed_want(self, rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Mixed: takes turns New -> Movers -> Graduated -> New ... A watched coin stays while held, and while
        flat until a trade on it completes or it sat mixed_idle_s with no trade; then the next board's youngest
        coin is fed. An empty board passes the turn on. Up to Max mints (pro_hold: one)."""
        from src.signal.mayhem2 import MIXED_GROUPS
        cfg = self.cfg
        now = time.time()
        n = len(MIXED_GROUPS)
        cands = self._mixed_candidates(rows)
        cmints = {r["m"] for r in cands}
        slots = 1 if cfg.rule == "pro_hold" else max(1, int(cfg.max_mints or 1))
        want: Dict[str, Dict[str, Any]] = {}
        for m, f in list(self.feeds.items()):
            if not str(f.origin).startswith("mixed"):
                continue
            since = float(self._mx_since.get(m) or now)
            done = m in self.traded and float(self.traded[m]) >= since
            idle = now - since >= cfg.mixed_idle_s and m not in self.busy
            if m in self.positions or (not done and not idle and (m in cmints or now - since < 10)):
                want[m] = {"origin": f.origin}
        if cfg.rule == "pro_hold" and self.trading():
            return want                       # one coin at a time: the open trade completes first
        used = set(want) | set(self.positions)
        while len(want) < slots:
            pick = None
            for k in range(n):
                i = (self._mx_turn + k) % n
                g = MIXED_GROUPS[i]
                pick = next((r for r in cands if r["_grp"] == g and r["m"] not in used
                             and now - float(self._mx_left.get(r["m"]) or 0) > 300), None)
                if pick:
                    self._mx_turn = (i + 1) % n
                    break
            if not pick:
                break
            used.add(pick["m"])
            self._mx_since[pick["m"]] = now
            want[pick["m"]] = {"origin": "mixed-" + pick["_grp"], "row": pick}
        return want

    def _pick(self, rows: List[Dict[str, Any]]) -> None:
        self._pick_feeds(rows)
        self._single_feed()
        self._name_feeds()

    def _src_label(self, mint: str) -> str:
        """Where a Candles trade came from, for the Mode column: Launch, Dayscalp, MayhemMode, Callout, OpenCopy,
        Candles (Mayhem board), Candles (Mixed: New / Movers / Graduated), Candles (pinned)."""
        pos = self.positions.get(mint) or {}
        if pos.get("support"):
            return SUPPORT_NAMES.get(pos["support"], pos["support"])
        f = self.feeds.get(mint)
        origin = str(getattr(f, "origin", "") or "")
        if origin in SUPPORT_NAMES:
            return SUPPORT_NAMES[origin]
        if origin.startswith("mixed-"):
            return f"Candles (Mixed: {origin[6:].title()})"
        if origin == "auto":
            return "Candles (Mayhem board)"
        if origin == "manual":
            return "Candles (pinned)"
        src = self.support_source()
        return SUPPORT_NAMES.get(src, "Candles") if src else "Candles"

    # ---------- Paused Coins (user 2026-11-14): a paused coin is held and watched, not sold
    def _pause_hold(self, mint: str, pos: Dict[str, Any], state: str) -> None:
        """pump paused the coin while held: do not sell (pump refuses sells then, and paused coins often move
        again). It goes to the Paused Coins page, watched every second by _paused_loop."""
        now = time.time()
        self._watch_rec(mint, pos, None, "paused_hold:" + str(state))
        pos["paused_at"] = now
        pos["pause_state"] = state
        pos.pop("closing", None)
        self.paused_mints[mint] = now
        self.feeds.pop(mint, None)
        if self.store:
            self.store.open_position(pos)      # saved: a restart keeps it paused (it was sold at Max hold before)
        self.note("warn", f"{pos.get('symbol') or mint[:6]} PAUSED by pump — held, not sold; watched every second on "
                          f"the Paused Coins page. It continues (sells on profit) if it moves again.", mint)
        if self.store:
            self.store.audit("paused_hold", mint=mint, state=state)

    async def _paused_loop(self) -> None:
        """Every second, for each held paused coin: pump's mayhem-state and the coin's value. Moving again
        (active) -> the trade continues (Max hold restarts, sells on profit). Worth almost nothing (dust) -> left:
        booked, no longer watched. Never sold while paused."""
        from src.data.fast_get import mayhem2_get
        from src.signal.mayhem2 import STATE_URL
        while True:
            await asyncio.sleep(1.0)
            tick = getattr(self, "_paused_tick", 0) + 1
            self._paused_tick = tick
            for m in [m for m in self.paused_mints if m not in self.positions]:
                self.paused_mints.pop(m, None)          # sold / closed: no longer a paused coin
            for mint, pos in list(self.positions.items())[:8]:
                if mint in self.busy:
                    continue
                if not pos.get("paused_at"):
                    # an open trade from any source / feed: is pump pausing it? (every 2s; the board only covers
                    # the Mayhem board / Mixed feeds)
                    # every second for the first 30 s of a trade (20 of 35 pauses hit within 11 s), then every 2 s
                    young = time.time() - float(pos.get("entry_ts") or 0) <= 30
                    if ((tick % 2 and not young) or pos.get("closing")
                            or time.time() - float(pos.get("resumed_at") or 0) < 15):
                        continue
                    try:
                        body = await asyncio.to_thread(mayhem2_get, STATE_URL.format(mint=mint), "candlepaused", 2.0)
                        st = str((body or {}).get("state") or "").lower() if isinstance(body, dict) else ""
                        self._watch_rec(mint, pos, body, "paused" if st == "paused" else "")
                        if st == "paused":
                            self._pause_hold(mint, pos, st)
                        elif st == "completed":
                            # A coin that left the board has no board state (HALH 2026-11-15 sat open, completed).
                            # Bought already completed (first reading within 10 s of the buy): a normal trade.
                            if "was_completed" not in pos:
                                pos["was_completed"] = time.time() - float(pos.get("entry_ts") or 0) < 10
                            if not pos["was_completed"]:
                                self._completed_sell(mint, pos)
                        else:
                            pos.setdefault("was_completed", False)
                    except Exception:
                        pass
                    continue
                try:
                    body = await asyncio.to_thread(mayhem2_get, STATE_URL.format(mint=mint), "candlepaused", 2.0)
                    state = str((body or {}).get("state") or "").lower() if isinstance(body, dict) else ""
                    if body is None or (isinstance(body, dict) and not body.get("state")):
                        state = "active"          # not a Mayhem coin any more / no pause record: trading
                    pos["pause_state"] = state or pos.get("pause_state")
                    pos["pause_checked"] = time.time()
                    v, _real, vsrc = await self._value(mint, pos["tokens"])
                    if v is not None:
                        pos["value_sol"] = v
                        pos["value_src"] = vsrc
                        pos["value_at"] = time.time()
                        pos["upnl_sol"] = v - pos["entry_sol"] - 2 * self._network_fee_sol()
                        pos["upct"] = pos["upnl_sol"] / pos["entry_sol"] * 100.0 if pos["entry_sol"] else 0.0
                    cost = float(pos.get("entry_sol") or 0)
                    if v is not None and cost > 0 and v < max(PAUSED_DUST_SOL, cost * PAUSED_DUST_FRAC):
                        self._pause_leave(mint, pos, v)
                    elif state == "completed":
                        self._completed_sell(mint, pos)
                    elif state == "active":
                        if self._resume_off_feed():
                            # one trade at a time, and the feed is always the newest coin (user 2026-11-15: G6S and
                            # PPAWS came back while OLRGTA5 was open — three open trades; a coin that waited is no
                            # longer the latest): it never goes back to the feed. It is sold from this watch.
                            self._moving_exit(mint, pos)
                        else:
                            self._pause_resume(mint, pos)
                    else:
                        pos.pop("moving_at", None)          # paused again
                except Exception:
                    log.debug("paused watch failed", exc_info=True)

    def _resume_off_feed(self) -> bool:
        """A paused coin that moves again does not rejoin the feed / open trades (one trade at a time, newest coin
        only). OpenCopy with AutoTrade-Candles trades several coins together: there it rejoins as before."""
        return not (self.support_source() == "opencopy" and self.feeds_only("opencopy"))

    def _moving_exit(self, mint: str, pos: Dict[str, Any]) -> None:
        """A paused coin trades again: it stays on the Paused Coins watch (valued every second) and is sold from
        there — at once when the whole trade is >= the profit target, else at Max hold counted from the moment it
        moved again. It never takes the feed or the one-trade slot."""
        now = time.time()
        moving = float(pos.setdefault("moving_at", now))
        pos["pause_state"] = "moving again — sells on profit or at Max hold"
        if pos.get("closing") or mint in self.busy:
            return
        up, fresh = pos.get("upct"), now - float(pos.get("value_at") or 0) <= 3
        tgt, _from = self.green_target()
        why = ""
        if up is not None and fresh and float(up) >= tgt:
            why = f"moved again after a pause — take profit, trade {float(up):+.1f}% ≥ {tgt:g}% after fees"
        elif self.cfg.max_hold_s and now - moving > self.cfg.max_hold_s:
            why = f"max hold {self.cfg.max_hold_s:.0f}s (counted from when it moved again after a pause)"
        if why:
            pos["closing"] = True
            asyncio.ensure_future(self._forced_exit(mint, why))

    def _pause_resume(self, mint: str, pos: Dict[str, Any]) -> None:
        held = time.time() - float(pos.get("paused_at") or time.time())
        pos.pop("paused_at", None)
        pos["resumed_at"] = time.time()
        pos.pop("pro_armed", None)
        self.paused_mints.pop(mint, None)
        if self.store:
            self.store.open_position(pos)
        f = self.feeds.get(mint)
        if f is not None:
            f.board_state = "active"
        self.note("info", f"{pos.get('symbol') or mint[:6]} moving again after {held:.0f}s paused — the trade continues "
                          f"(sells on profit; Max hold restarts).", mint)
        if self.store:
            self.store.audit("paused_resume", mint=mint, after_s=round(held, 1))

    def _watch_rec(self, mint: str, pos: Dict[str, Any], body: Any, event: str = "") -> None:
        """Pause recorder (user 2026-11-16): one line per state read of an open trade in logs/pause_watch.jsonl —
        seconds held, coin age, the last 1s candle, the trade's value and pump's whole mayhem-state record (state,
        mode, the Mayhem agent's buy / sell counts and SOL). Read later to look for a warning sign before a pause.
        Never raises; the file rolls over at 50 MB."""
        try:
            now = time.time()
            f = self.feeds.get(mint)
            c = None
            if f is not None and f.using and f.books[f.using].rows:
                c = f.books[f.using].rows[-1]
            row = {"ts": round(now, 2), "mint": mint, "symbol": pos.get("symbol"), "mode": pos.get("mode"),
                   "event": event, "held_s": round(now - float(pos.get("entry_ts") or now), 1),
                   "lots": len(pos.get("lots") or []) or 1, "entry_sol": pos.get("entry_sol"),
                   "value_sol": pos.get("value_sol"), "upct": pos.get("upct"),
                   "age": getattr(f, "age", None) if f is not None else None,
                   "board": getattr(f, "board_state", "") if f is not None else "",
                   "feed": f.using if f is not None else "", "feed_status": f.status if f is not None else "",
                   "candle": ({"t": c.t, "o": c.open, "h": c.high, "l": c.low, "c": c.close, "v": c.volume}
                              if c is not None else None),
                   "state": body if isinstance(body, dict) else None}
            # tests must not write into the live bot's file (they set _watch_path to a temp file)
            path = getattr(self, "_watch_path", None) or (
                "" if "PYTEST_CURRENT_TEST" in os.environ else os.path.join("logs", "pause_watch.jsonl"))
            if not path:
                return
            try:
                if os.path.getsize(path) > 50_000_000:
                    os.replace(path, path + ".1")
            except OSError:
                pass
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, default=str) + "\n")
        except Exception:
            log.debug("pause watch record failed", exc_info=True)

    def _completed_sell(self, mint: str, pos: Dict[str, Any]) -> None:
        """pump's state is "completed" (the Mayhem run is over), not "paused": the coin can be sold, so it is sold
        off now instead of sitting on the Paused Coins page (user 2026-11-15). A failed sell is retried: a coin on
        the watch stays there (next second), any other trade keeps its normal exits."""
        if pos.get("closing") or mint in self.busy:
            return
        last = float(pos.get("completed_try") or 0)
        if time.time() - last < 5:
            return
        pos["completed_try"] = time.time()
        pos["pause_state"] = "completed"
        # A completed coin worth almost nothing cannot be priced or sold (graduated, no Jupiter route): CHIMP
        # 2026-11-16 was retried 124 times over 16 minutes and held the one-trade slot. Dust is booked and left;
        # anything else that still fails after 6 tries (30 s) goes back to the normal exits (Max hold).
        v, cost = pos.get("value_sol"), float(pos.get("entry_sol") or 0)
        if v is not None and cost > 0 and float(v) < max(PAUSED_DUST_SOL, cost * PAUSED_DUST_FRAC):
            return self._pause_leave(mint, pos, float(v))
        pos["completed_tries"] = int(pos.get("completed_tries") or 0) + 1
        if pos["completed_tries"] > 6:
            pos["was_completed"] = True
            if pos.get("paused_at"):
                self._pause_resume(mint, pos)
            return
        pos["closing"] = True
        self.note("warn", f"{pos.get('symbol') or mint[:6]} is COMPLETED on pump (Mayhem run over, not paused) — "
                          f"selling it off.", mint)
        if self.store:
            self.store.audit("completed_sell", mint=mint)
        asyncio.ensure_future(self._forced_exit(mint, "Mayhem completed (not paused) — sold off"))

    def _pause_leave(self, mint: str, pos: Dict[str, Any], v: float) -> None:
        """Gone to dust while paused: stop watching. Booked at its value; the coins are left in the wallet."""
        why = f"paused coin went to dust ({v:.6f} SOL) — left in the wallet, no longer watched"
        pos.pop("paused_at", None)
        self.paused_mints.pop(mint, None)
        if pos.get("mode") == "live":
            self._live_closed(mint, None, why)
        else:
            pos["closing"] = True
            asyncio.ensure_future(self._forced_exit(mint, "coin " + why))     # paper: booked at its dust value

    def paused_view(self) -> List[Dict[str, Any]]:
        now = time.time()
        return [{"mint": m, "symbol": p.get("symbol"), "mode": p.get("mode"), "src": p.get("src"),
                 "lots": len(p.get("lots") or []) or 1, "entry_sol": p.get("entry_sol"), "value_sol": p.get("value_sol"),
                 "upct": p.get("upct"), "state": p.get("pause_state"), "paused_s": round(now - float(p["paused_at"])),
                 "checked_s": round(now - float(p.get("pause_checked") or now), 1)}
                for m, p in self.positions.items() if p.get("paused_at")]

    def trading(self) -> set:
        """Coins with an active trade or one being sent right now (a buy is booked only when its fill lands,
        up to ~1.6s later — 2026-11-14 a second coin was bought in that gap). One trade at a time uses this."""
        return set(self.active_positions()) | set(self.busy) | set(self.pending)

    def active_positions(self) -> Dict[str, Dict[str, Any]]:
        """Open trades that are not on the Paused Coins watch: a paused coin never blocks the next coin."""
        return {m: p for m, p in self.positions.items() if not p.get("paused_at")}

    def is_paused(self, mint: str) -> bool:
        """pump paused (or completed) this coin: it cannot be sold, so it is not an active trade."""
        if mint in self.paused_mints:
            return True
        f = self.feeds.get(mint)
        return bool(f and f.board_state in ("paused", "completed"))

    def _name_feeds(self) -> None:
        """A watched coin with no name (pinned mints, some sources) gets its symbol from pump / DexScreener, once."""
        need = [m for m, f in self.feeds.items() if m not in self._named and (not f.symbol or f.symbol == m[:6])]
        if not need or not self.loop:
            return
        self._named.update(need)

        async def go(mints: List[str]) -> None:
            try:
                from src.execution.wallet_tokens import quote_mints
                got = await quote_mints(mints, named=True)
                for m in mints:
                    sym = str((got.get(m) or {}).get("symbol") or "")
                    if sym and m in self.feeds:
                        self.feeds[m].symbol = sym
                    if sym and m in self.positions and self.positions[m].get("symbol") in (None, "", m[:6]):
                        self.positions[m]["symbol"] = sym
            except Exception:
                log.debug("candle name lookup failed", exc_info=True)
        try:
            asyncio.get_running_loop()
            asyncio.ensure_future(go(need))
        except RuntimeError:
            asyncio.run_coroutine_threadsafe(go(need), self.loop)

    def _single_feed(self) -> None:
        """One coin at a time, for every source and feed (user 2026-11-14): while a trade is open only the coin(s)
        held are watched; flat, one coin. The next coin comes once the trade completes."""
        active = self.trading()
        if self.support_source() == "opencopy" and self.feeds_only("opencopy"):
            # OpenCopy only (user 2026-11-15): several coins opened by the callers are watched and traded together,
            # up to OPENCOPY_MAX_FEEDS — held coins first
            keep = ([m for m in self.feeds if m in active]
                    + [m for m in self.feeds if m not in active and m not in self.positions])[:max(OPENCOPY_MAX_FEEDS, len(active))]
        elif active:
            keep = [m for m in self.feeds if m in active]
        else:
            # paused coins are watched on the Paused Coins page, not fed: the next coin can come. A pinned coin
            # (Pinned mints) takes the slot first.
            free = [m for m in self.feeds if m not in self.positions]
            pinned = [m for m in free if m in (self.cfg.mints or [])]
            keep = (pinned or free)[:1]
        for m in list(self.feeds):
            if m not in keep:
                self.feeds.pop(m, None)
        self.providers["stream"].wanted = set(self.feeds)

    def _pick_feeds(self, rows: List[Dict[str, Any]]) -> None:
        cfg = self.cfg
        src = self.support_source()
        if src:
            # Support mode: feeds = the source's held coins (plus anything Candles still holds). No board picks.
            self._support_sync(src)
            want = {p.mint: "support" for p in self._support_held(src)}
            ages: Dict[str, float] = {}           # coin age where the source knows it (first-candle buy)
            for m in [m for m, p in self.positions.items() if not p.get("paused_at")] + [m for m in self.busy if m in self.feeds]:
                want.setdefault(m, "held")
            if src == "mayhem":
                for m in self._mayhem_grab():
                    want.setdefault(m, "mayhem-new")
                try:
                    from src.observability.dashboard import mayhem_young_opens
                    for r in mayhem_young_opens(MAYHEM_MAX_OPEN_AGE_S):
                        # Mayhem_Mode opened it < 30s ago on a fresh coin: its open age stands for the coin's age
                        ages.setdefault(str(r.get("coinMint") or ""), float(r.get("age") or 0))
                except Exception:
                    pass
            if src == "dayscalp":
                try:
                    from src.signal.dayscalp import LAST_CANDLE_PICK as _pk, LAST_CANDLE_PICKS as _pks
                    for gm, gv in list(self._grab.items()):
                        if time.time() - float(gv.get("at") or 0) > 60:
                            self._grab.pop(gm, None)
                    owns = self.pro_hold_owns_entries("dayscalp")
                    picks = list(_pks) if owns else [_pk]
                    if owns and self.trading():
                        picks = []        # one coin at a time: the open trade completes before the next coin is fed
                    for pk in picks:
                        if pk.get("mint") and time.time() - float(pk.get("ts") or 0) < 20:
                            want.setdefault(pk["mint"], "scan")      # Decoded Tokens' top pick(s): watch now
                            if pk.get("age") is not None:
                                ages[pk["mint"]] = float(pk["age"]) + (time.time() - float(pk.get("ts") or time.time()))
                            self._grab.setdefault(pk["mint"], {"at": time.time(), "symbol": pk.get("symbol") or ""})
                except Exception:
                    pass
            if src == "opencopy" and self.feeds_only("opencopy"):
                # AutoTrade-Candles on OpenCopy (user 2026-11-15): every new open of the callers selected on OpenHolds
                # is offered to Candles at once — no OpenCopy buy / pin / hold, no auto-open range wait
                try:
                    # the coins the OpenHolds page lists (opened in the last 15 minutes), not only OpenCopy's 1-minute
                    # auto-open window; pump coins only (mint starts or ends with "pump", user 2026-11-15)
                    from src.observability.dashboard import openhold_recent_coins
                    # only coins the callers opened AFTER AutoTrade-Candles was ticked (user 2026-11-15): ticking
                    # it does not pick up what is already listed
                    since = float(cfg.support_since or 0) or self.started_at
                    new = [r for r in openhold_recent_coins(900.0) if float(r.get("openedAt") or 0) >= since]
                    for r in new[:OPENCOPY_MAX_FEEDS]:
                        m = str(r.get("coinMint") or "")
                        if (m.startswith("pump") or m.endswith("pump")) and m not in self.traded and m not in self.unusable:
                            self.offer("opencopy", m, str(r.get("symbol") or ""))
                except Exception:
                    log.debug("openholds rows for Candles failed", exc_info=True)
            if src == "callout" and self.feeds_only("callout"):
                # AutoTrade-Candles on CallOuts (user 2026-11-15): no waiting for CallOuts' own buy rules — the newest
                # OPEN pump coins on the CallOuts page are offered to Candles straight away
                try:
                    from src.observability.dashboard import _callout_rows
                    co_rows, _err = _callout_rows()
                    fresh = sorted((r for r in co_rows if str(r.get("call") or "").upper() == "OPEN"
                                    and (str(r.get("coinMint") or "").startswith("pump")
                                         or str(r.get("coinMint") or "").endswith("pump"))),
                                   key=lambda r: -float(r.get("openedAt") or 0))[:3]
                    for r in fresh:
                        m = str(r["coinMint"])
                        if m not in self.traded and m not in self.unusable:
                            self.offer("callout", m, str(r.get("symbol") or ""))
                except Exception:
                    log.debug("callout rows for Candles failed", exc_info=True)
            if self.feeds_only(src):
                now = time.time()
                for m, o in list(self.offers.items()):
                    stale = now - float(o.get("seen") or o["at"]) > max(cfg.mixed_idle_s, 30.0)
                    done = m in self.traded and float(self.traded[m]) >= float(o["at"])
                    if m in self.positions:
                        continue
                    if o["src"] != src or stale or done or m in self.unusable:
                        self.offers.pop(m, None)     # an untradable coin no longer cycles back into the feed
                slots = 1 if cfg.rule == "pro_hold" else max(1, int(cfg.max_mints or 1))
                if src == "opencopy":
                    slots = OPENCOPY_MAX_FEEDS      # OpenCopy only: coins opened together are fed together
                fresh = sorted((o["at"], m) for m, o in self.offers.items() if m not in self.positions)
                for _at, m in reversed(fresh):                       # newest offer first
                    if len([x for x in want if x not in self.positions]) >= slots:
                        break
                    want.setdefault(m, "offer")
                    o = self.offers[m]
                    if o.get("age") is not None:
                        ages[m] = float(o["age"]) + (now - float(o["at"]))
                    self._grab.setdefault(m, {"at": now, "symbol": o.get("symbol") or ""})
            if cfg.rule == "pro_hold" and src != "opencopy":
                want = self._one_feed(want)
            for m, origin in want.items():
                if cfg.avoid_mayhem and m not in self.mayhem_of:
                    self._check_mayhem(m)
                if m in ages and m in self.feeds and self.feeds[m].age is None:
                    self.feeds[m].age = ages[m]
                if m not in self.feeds:
                    f = Feed(m, cfg.count)
                    f.origin = src
                    f.age = ages.get(m)
                    f.symbol = ((self.positions.get(m) or {}).get("symbol") or (self._grab.get(m) or {}).get("symbol")
                                or m[:6])
                    self.feeds[m] = f
                    what = {"offer": f"{src} offered it — Candles buys it by its {cfg.rule} rule ({cfg.mode.upper()})",
                            "mayhem-new": ("Mayhem_Mode just opened it — pro_hold buys it on Botpredict's BUY (◆ BP)"
                                           if cfg.rule == "pro_hold" else "Mayhem_Mode just opened it — buying"),
                            "scan": ("Dayscalp's top pick on Decoded Tokens — pro_hold buys it on Botpredict's BUY (◆ BP)"
                                     if cfg.rule == "pro_hold" else
                                     "Dayscalp's top pick on Decoded Tokens — bought as lot 1 when Auto Trade is on")}
                    self.note("info", f"Watching {f.symbol} ({what.get(origin, src + ' holds it')}).", m)
            for m in list(self.feeds):
                if m not in want:
                    self.feeds.pop(m, None)
            self.providers["stream"].wanted = set(self.feeds)
            if self.paper_support() and cfg.rule != "pro_hold" and not self.feeds_only(src):
                self._paper_lot1(want, src)
            return
        want: Dict[str, Dict[str, Any]] = {}
        for m in cfg.mints:
            if m in self.unusable:
                if m not in self._pin_warned:
                    self._pin_warned.add(m)
                    self.note("warn", f"Pinned {m[:6]}… is not a pump.fun bonding-curve coin ({self.unusable[m]}) — "
                                      "Candles can only chart and trade pump.fun coins. Remove it from Pinned mints.", m)
                continue
            if m not in cfg.blacklist:
                busy_now = self.trading()
                if busy_now and m not in busy_now:
                    continue          # a trade is open: the pinned coin is fed once it closes (no add / drop churn)
                want[m] = {"origin": "manual"}
        state = {str(r.get("m") or ""): r for r in rows}
        if self.follow() == "mixed" and not cfg.whitelist_only:
            for m, info in self._mixed_want(rows).items():
                want.setdefault(m, info)
            for m in list(self.feeds):
                if str(self.feeds[m].origin).startswith("mixed") and m not in want:
                    self._mx_left[m] = time.time()        # handed over: not picked again for 5 minutes
                    self._mx_since.pop(m, None)
        elif self.follow() == "mayhem_board" and not cfg.whitelist_only:
            now = time.time()
            for r in rows:
                if r.get("m"):
                    self.board_seen[str(r["m"])] = now
            ok = lambda r: (str(r.get("ms") or "").lower() in ("active", "enabled")
                            and str(r.get("m") or "") not in cfg.blacklist and str(r.get("m") or "").endswith("pump")
                            and str(r.get("m") or "") not in self.unusable
                            and (cfg.reuse_coin or str(r.get("m") or "") not in self.traded))
            young = [r for r in rows if ok(r) and cfg.min_age_s <= float(r.get("age") or 0) <= cfg.max_age_s]
            young.sort(key=lambda r: float(r.get("age") or 0))
            # Keep the coin already watched. Reuse on: until it is paused / completed (no age limit) —
            # it stays while the board lists it active. Reuse off: dropped once one trade on it closed.
            # A coin whose trade completed is NOT kept (user 2026-11-15): the moment a trade completes the feed is
            # the board's youngest coin right then (the same coin again only if it still is the youngest).
            keep = [r for r in rows if ok(r) and r["m"] in self.feeds and self.feeds[r["m"]].origin == "auto"
                    and r["m"] not in self.traded
                    and (cfg.reuse_coin or float(r.get("age") or 0) <= cfg.max_age_s)]
            fresh = [r for r in young if r["m"] not in {k["m"] for k in keep}]
            picks = (keep + fresh)[:cfg.max_mints]
            if cfg.rule == "pro_hold":
                # one coin at a time (user 2026-11-14): while a trade is open only that coin is fed; the next
                # board coin comes once it closes
                picks = [] if self.trading() else picks[:1]
            for r in picks:
                want.setdefault(r["m"], {"origin": "auto"})
        for mint in [m for m, p in self.positions.items() if not p.get("paused_at")] + [m for m in self.busy if m in self.feeds]:
            want.setdefault(mint, {"origin": self.feeds[mint].origin if mint in self.feeds else "held"})   # or are buying
        for mint, info in want.items():
            if mint not in self.feeds:
                f = Feed(mint, cfg.count)
                f.origin = info["origin"]
                self.feeds[mint] = f
                r0 = info.get("row") or state.get(mint) or {}
                what = info["origin"]
                if what.startswith("mixed-"):
                    what = (f"Mixed: {what[6:].title()} board's youngest, age {float(r0.get('_age') or 0):.0f}s — "
                            f"next turn {('New', 'Movers', 'Graduated')[self._mx_turn % 3]}")
                self.note("info", f"Watching {r0.get('t') or mint[:6]} ({what}).", mint)
            f = self.feeds[mint]
            r = state.get(mint)
            if r and r.get("ms"):
                self.mayhem_of[mint] = True
            if cfg.avoid_mayhem and mint not in self.mayhem_of:
                self._check_mayhem(mint)
            if r:
                f.symbol = str(r.get("t") or f.symbol)
                f.board_state = str(r.get("ms") or "").lower()
                f.age = r.get("age")
        for mint in list(self.feeds):
            if mint not in want:
                self.feeds.pop(mint, None)
        self.providers["stream"].wanted = set(self.feeds)
        for mint, pos in list(self.positions.items()):
            f = self.feeds.get(mint)
            if (f and f.board_state in ("paused", "completed") and not pos.get("closing") and not pos.get("paused_at")
                    and time.time() - float(pos.get("resumed_at") or 0) > 15):
                if f.board_state == "completed":
                    self._completed_sell(mint, pos)      # Mayhem run over, not paused: it can be sold
                else:
                    self._pause_hold(mint, pos, f.board_state)

    # ---------- polling
    async def _poll_loop(self) -> None:
        tasks: Dict[str, asyncio.Task] = {}
        while True:
            await asyncio.sleep(0.05)
            for mint in list(self.feeds):
                t = tasks.get(mint)
                if t is None or t.done():
                    tasks[mint] = asyncio.ensure_future(self._poll_one(mint))
            for mint in list(tasks):
                if mint not in self.feeds and tasks[mint].done():
                    tasks.pop(mint, None)

    async def _poll_one(self, mint: str) -> None:
        cfg = self.cfg
        t0 = time.time()
        f = self.feeds.get(mint)
        if f is None or not cfg.enabled:
            await asyncio.sleep(0.5)
            return
        names = [cfg.provider] + ([cfg.fallback] if cfg.fallback and cfg.fallback != cfg.provider else [])
        for name in names:
            prov = self.providers[name]
            try:
                rows = await prov.fetch(mint, cfg)
                if name == "pump_rest":
                    self.m["polls"] += 1
                f.books[name].merge(rows)
                f.fails[name] = 0
                f.ok_at[name] = time.time()
                f.err[name] = ""
            except Exception as exc:
                if name == "pump_rest":
                    self.m["polls"] += 1
                    self.m["poll_errors"] += 1
                f.fails[name] += 1
                f.err[name] = f"{type(exc).__name__}: {exc}"[:100]
        self._health(f)
        if f.using:
            self._evaluate(f, time.time())     # the signal exists once the data has arrived
        await self._idle(mint, f, max(0.0, cfg.poll_ms / 1000.0 - (time.time() - t0)))

    # ---------- instant take-profit (user 2026-11-16): a held coin is re-read on EVERY trade, not every 0.5 s
    def _on_stream_trade(self, ev: Dict[str, Any]) -> None:
        """Trade socket thread. The trade goes into the stream candles as before; for a coin with an open trade
        the poll's wait is ended at once so the profit check runs on this very trade."""
        self.providers["stream"].on_trade(ev)
        try:
            mint = str(ev.get("mint") or "")
            if mint in self.positions and self.loop is not None:
                self.loop.call_soon_threadsafe(self._wake_trade, mint)
        except Exception:
            pass

    def _wake_trade(self, mint: str) -> None:
        ev = self._trade_ev.get(mint)
        if ev is not None:
            ev.set()

    async def _idle(self, mint: str, f: Feed, wait: float) -> None:
        """The pause between two polls. Flat: a plain sleep. Holding (stream chart): every trade on the coin ends
        the wait, the stream candles are re-read (local, no request) and the rules run right then — a coin that
        touched the target and fell back inside the 0.5 s between polls was never seen before. At most ~30 a second."""
        end = time.time() + wait
        if mint not in self.positions:
            self._trade_ev.pop(mint, None)
            await asyncio.sleep(wait)
            return
        ev = self._trade_ev.setdefault(mint, asyncio.Event())
        while True:
            left = end - time.time()
            if left <= 0:
                return
            try:
                await asyncio.wait_for(ev.wait(), left)
            except asyncio.TimeoutError:
                return
            ev.clear()
            pos = self.positions.get(mint)
            if (pos is None or pos.get("paused_at") or pos.get("closing") or mint in self.busy
                    or f.using != "stream" or self.feeds.get(mint) is not f):
                continue
            try:
                rows = await self.providers["stream"].fetch(mint, self.cfg)
                f.books["stream"].merge(rows)
                f.ok_at["stream"] = time.time()
                self.m["instant_evals"] = int(self.m.get("instant_evals") or 0) + 1
                self._instant = True
                try:
                    self._evaluate(f, time.time())
                finally:
                    self._instant = False
            except Exception:
                log.debug("instant evaluation failed", exc_info=True)
            await asyncio.sleep(0.03)

    def _healthy(self, f: Feed, name: str) -> bool:
        return f.fails[name] < self.cfg.fail_degrade and time.time() - f.ok_at[name] <= self.cfg.stale_s

    def _health(self, f: Feed) -> None:
        cfg = self.cfg
        prev = f.status
        using = ""
        for name in [cfg.provider] + ([cfg.fallback] if cfg.fallback else []):
            if name and self._healthy(f, name):
                using = name
                break
        f.using = using
        if not using:
            f.status = "DEGRADED"
        else:
            last = f.books[using].last()
            f.status = "OK" if last and time.time() - (last.t + 1000) / 1000.0 <= cfg.stale_s else "STALE"
            if using != cfg.provider:
                f.status = "FALLBACK" if f.status == "OK" else f.status
        if f.status != prev:
            if f.status == "DEGRADED":
                self.alert(f"degraded:{f.mint}", f"{f.symbol} candle feed DEGRADED ({f.err.get(cfg.provider) or 'no data'}) — entries blocked.", 60)
            elif prev in ("DEGRADED",) or (f.status == "FALLBACK" and time.time() - f.added > 15):
                # A just-picked coin has no stream trade for a second or two: that brief fallback is normal.
                # A quiet coin flips STALE <-> FALLBACK every few seconds: say it once per 5 minutes.
                k = f"feed:{f.mint}:{f.status}"
                if prev == "DEGRADED" or time.time() - self.alerted.get(k, 0) >= 300:
                    self.alerted[k] = time.time()
                    self.note("info", f"{f.symbol} feed {f.status} via {using}.", f.mint)
            if self.store:
                self.store.audit("feed", mint=f.mint, status=f.status, provider=using)

    def pro_context(self, mint: str) -> Dict[str, Any]:
        """The open Candles trade on this coin for the Pro Trader read: entry (chart price) and time held."""
        held = self.positions.get(mint)
        if not held or self.cfg.rule == "pro_hold":       # pro_hold exits on profit / max hold, not the chart plan
            return {"flow": None, "holding": None}
        ts = float(held.get("entry_ts") or time.time())
        px = float(self.pro_px.get(mint) or 0)
        if px <= 0:
            lot = (held.get("lots") or [{}])[0]
            px = float(lot.get("px") or 0)
        f = self.feeds.get(mint)
        if px <= 0 and f is not None and f.using:
            px = float(self._px_at(f, ts) or 0)
        return {"flow": None, "holding": {"entry": px, "held": time.time() - ts} if px > 0 else None}

    def _gar(self, f: Feed, live_rows: List[Candle], held: Optional[Dict[str, Any]], seen: float) -> bool:
        """pro_hold "buy green after red" (user 2026-11-14): the candle now forming trades above its open right after
        a red candle = a green after a red. Buys then when flat and the coin is down from its high, or adds a lot
        (Max position, up to Total cap) when the open trade is in a loss. In profit: no buy — it sells on profit.
        Once per candle, checked every 0.5s poll on the forming candle. True when a signal was sent."""
        c, prev = live_rows[-1], live_rows[-2]
        if c.t == getattr(f, "gar_t", None) or not (prev.close < prev.open) or not (c.close > c.open):
            return False
        low_ref = None
        if held is not None and self.cfg.pro_hold_first_at == "gar_low":
            # "buy green after red – low below" (user 2026-11-15): a new lot only when the red candle's low AND this
            # green's open are not higher than the previous lot's low (L1: the low of the candle it was bought in;
            # later lots: their own red's low / green's open). Each lot sits lower than the one before.
            ref = self._lot_low(held, live_rows, f.using)
            low_ref = min(prev.low, c.open)
            if ref is None or held.get("closing") or not (prev.low < ref and c.open < ref):
                return False
            why = (f"green after red, low below — red low {prev.low:.4g} and green open {c.open:.4g} are under the "
                   f"last lot's low {ref:.4g}: buys again lower (sells on profit)")
        elif held is None:
            peak = max(x.high for x in live_rows)
            if not (peak > 0 and c.close < peak):
                return False
            down = (1 - c.close / peak) * 100
            why = f"green after red — the coin is {down:.0f}% under its high"
        else:
            up = held.get("upct")
            if up is None or float(up) >= 0 or time.time() - float(held.get("value_at") or 0) > 3:
                return False
            why = f"green after red — trade {float(up):+.1f}%: buys again to lower the average (sells on profit)"
        f.gar_t = c.t
        # "every green after red" means every one: the SOL-outflow / Min reserve / far-below-high / cooldown checks
        # refused 57 of 77 (2026-11-14) — skipped here; Total cap still limits the adds, Max hold still exits
        self._pro_emit(f, live_rows, "BUY", {"call": "BUY", "px": c.close, "why": why}, seen,
                       first=True, now_buy=True, gar=True, low_ref=low_ref)
        return True

    @staticmethod
    def _lot_low(held: Dict[str, Any], rows: List[Candle], using: str = "") -> Optional[float]:
        """The last lot's low for "low below", in the units of the chart now used (`rows`).
        Same feed as the lot: its stored red low / green open (lots 2+), else the low of the candle it was bought in.
        Other feed (the lot was bought on pump_rest, the chart is now the stream — their prices are in different
        units: SXSN 2026-11-15, lot 1 at 2.157e-06 vs 2.5e-08, so every later candle looked "below"): the low of this
        chart's candle at the lot's fill time (lots 2+: also the candle before it). No such candle -> None (no buy)."""
        lots = held.get("lots") or []
        last = lots[-1] if lots else {}
        same = not using or last.get("src") in (None, using)
        ref = float(last.get("low_ref") or 0)
        if same and ref > 0:
            return ref
        t = last.get("lot_t") if same else None
        ts = float(last.get("ts") or held.get("entry_ts") or 0) * 1000.0
        for k in range(len(rows) - 1, -1, -1):
            x = rows[k]
            if (t is not None and x.t == t) or (t is None and x.t <= ts):
                if t is None and ts - x.t > 5000:
                    return None                       # the chart has no candle near the buy
                low = float(x.low)
                if len(lots) > 1 and k > 0:
                    low = min(low, float(rows[k - 1].low))
                return low if low > 0 else None
        return None

    def _fast_upct(self, held: Dict[str, Any], f: Feed, price: float) -> Optional[float]:
        """Whole trade % after fees from the chart price: each lot's buy price (same feed units) against the price
        now; fees ~2% (pump 1% each way) + both network fees. Falls back to the value loop's fresh read."""
        lots = held.get("lots") or []
        cost = sum(float(l.get("sol") or 0) for l in lots)
        if price > 0 and cost > 0 and all(float(l.get("px") or 0) > 0 and (l.get("src") in (None, f.using))
                                         for l in lots):
            value = sum(float(l["sol"]) * price / float(l["px"]) for l in lots) * float(held.get("impact") or 1.0)
            net = 2 * self._network_fee_sol() * max(1, len(lots))
            # pump's 1% each way is not charged on a graduated coin valued by Jupiter quotes: its pool fee is already
            # in the quotes (and in `impact`), so no extra 2% (2026-11-14: AUfKbx needed ~+12% instead of +10%)
            fee_pct = 0.0 if str(held.get("value_src") or "").startswith("jupiter") else 2.0
            return (value - cost - net) / cost * 100.0 - fee_pct
        up = held.get("upct")
        if up is not None and time.time() - float(held.get("value_at") or 0) <= 2.0:
            return float(up)
        return None

    def _pro_hold_take(self, held: Dict[str, Any], cur) -> Optional[tuple]:
        """pro_hold take-profit read: (trade %, target, from) when the trade is in profit after fees and this
        1s candle is red, else None. Never on the loss side: below the target it keeps holding (Max hold)."""
        up = held.get("upct")
        if up is None or held.get("closing") or not (cur.close < cur.open):
            return None
        if time.time() - float(held.get("value_at") or 0) > 3:
            return None                      # stale value: no sell signal on an old price
        tgt, tgt_from = self.green_target()
        return (float(up), tgt, tgt_from) if float(up) >= tgt else None

    @staticmethod
    def _late_buy(PT: Any, cs: List[Dict[str, Any]], prev_eval: Optional[int]) -> Optional[str]:
        """The chart's ▲ BUY on a candle that arrived in a batch (user 2026-11-16: pump_rest delivered several
        candles at once, only the newest was read, and the ▲ BUY tags at 02:35:10 / 02:35:14 never bought). Every
        candle that is new since the last read is read too; a ▲ BUY on one of them, at most 5 s before the newest
        candle, still buys. Returns the reason text, else None."""
        if prev_eval is None or len(cs) < 2:
            return None
        last_t = cs[-1]["t"]
        for i in range(len(cs) - 2, max(len(cs) - 7, 0), -1):
            t = cs[i]["t"]
            if t <= prev_eval or last_t - t > 5000:
                break
            try:
                p = PT.analyze(cs[:i + 1], None, None)
            except Exception:
                return None
            if p.get("call") == "BUY":
                return (f"{p.get('why') or ''} (signal {(last_t - t) / 1000:.0f}s ago — its candle arrived late)").strip()
        return None

    def _pro_evaluate(self, f: Feed, seen: float) -> None:
        """Rule "pro": a day trader's read of the 1s chart (src/signal/pro_trader.py) once per new candle.
        Flat + BUY call -> BUY; holding + SELL call (target / stop / time stop) -> SELL."""
        from src.signal import pro_trader as PT
        rows = f.books[f.using].finals()
        cfg = self.cfg
        live_rows = f.books[f.using].rows           # finals + the candle still forming this second
        # a pinned coin (Pinned mints) enters ONLY on the chart's ▲ BUY (user 2026-11-14): no first-candle / green-
        # after-red / ◆ BP entries for it; it still sells on profit like every pro_hold trade
        pinned = f.mint in (cfg.mints or [])
        # "only on the ▲ BUY signal" (user 2026-11-16): no first-candle / green-after-red / ◆ BP buys at all — a lot
        # is bought only when the chart shows ▲ BUY (each new ▲ BUY adds one, up to Max lots / Total cap); it sells
        # at the profit target like every pro_hold trade
        sig_only = cfg.rule == "pro_hold" and cfg.pro_hold_first and cfg.pro_hold_first_at == "signal" and not pinned
        if (cfg.rule == "pro_hold" and cfg.pro_hold_first and live_rows and not pinned and not sig_only
                and not getattr(f, "first_done", False)
                and f.mint not in self.positions
                and not (self._avoid_mayhem_for(f) and self.mayhem_of.get(f.mint) is None and not f.board_state)):
            # pro_hold: a NEW coin is bought on its first GREEN candle (user's choice 2026-11-14; first candle on
            # 74 unseen coins, 3s in, with pro_hold's exits: 58% won, median +7.6%). A red first candle waits for
            # the next green one while the coin is still "new" (60s or younger).
            now = time.time()
            if not hasattr(f, "first_t0"):
                f.first_t0, f.first_age0 = now, f.age
            age_now = None if f.first_age0 is None else float(f.first_age0) + (now - f.first_t0)
            if cfg.pro_hold_first_at in ("now", "gar", "gar_low"):
                # "at once (0.00%)" (user 2026-11-14): every coin Candles is given is bought the first time its chart
                # is read — before any move, whatever its age or candle count. Only the Mayhem check, Kill / Pause
                # and the live gates can still stop it.
                c = live_rows[-1]
                f.first_done = True
                gar = cfg.pro_hold_first_at in ("gar", "gar_low")
                # "buy green after red" always starts with this buy too (user 2026-11-14), then _gar buys each green
                # after a red
                self._pro_emit(f, live_rows, "BUY", {"call": "BUY", "px": c.close,
                                                     "why": ("bought at the start" if gar else "bought at once on arrival (0.00%)")
                                                            + (f" — coin age {age_now:.0f}s" if age_now is not None else "")
                                                            + ("; then buys every green after a red" if gar else "")
                                                            + (" whose low is below the last lot's" if cfg.pro_hold_first_at == "gar_low" else "")},
                               seen, first=True, now_buy=True, gar=gar)
                return
            if cfg.pro_hold_first_at == "flat":
                # "no candles yet (0.00%)" (user 2026-11-14): the chart has only its price line — one reading, no
                # candle body, 0.00%. Bought right then. While the first candle is still forming and has moved, it
                # waits for a flat reading; once a second candle exists the moment is gone (no first buy).
                c = live_rows[-1]
                if len(live_rows) > 1:
                    f.first_done = True                 # candles already showing: the moment has passed
                elif (c.open > 0 and abs(c.close / c.open - 1) * 100 < FLAT_PCT
                      and c.high <= c.open * (1 + FLAT_PCT / 100) and c.low >= c.open * (1 - FLAT_PCT / 100)):
                    f.first_done = True
                    self._pro_emit(f, live_rows, "BUY", {"call": "BUY", "px": c.close,
                                                         "why": "no candles yet — 0.00%, bought on the price line"},
                                   seen, first=True, now_buy=True)
                    return
            elif age_now is None or age_now > PRO_FIRST_MAX_AGE_S:
                f.first_done = True
            else:
                # The turn: the red ends and the price starts up. Checked on the candle still FORMING (every poll,
                # 0.5s): it opens at the last close, so trading above its open = the first green has begun —
                # bought then, not 1s later when it closes (user 2026-11-14).
                c = live_rows[-1]
                if cfg.pro_hold_first_at == "start" and len(live_rows) <= PRO_FIRST_START_CANDLES:
                    # "start" (user 2026-11-14): the chart has just begun — one or two candles, no move up or down
                    # yet — buy right there. A coin first seen with more history than that waits for a green start.
                    f.first_done = True
                    self._pro_emit(f, live_rows, "BUY", {"call": "BUY", "px": c.close,
                                                         "why": f"chart just started — a new coin (age {age_now:.0f}s, "
                                                                f"{len(live_rows)} candle{'s' if len(live_rows) > 1 else ''})"},
                                   seen, first=True)
                    return
                if c.close > c.open:
                    f.first_done = True
                    self._pro_emit(f, live_rows, "BUY", {"call": "BUY", "px": c.close,
                                                         "why": f"first green starting — a new coin (age {age_now:.0f}s)"
                                                                + ("" if c.final else ", candle still forming")},
                                   seen, first=True)
                    return
        held_now = self.positions.get(f.mint)
        if (cfg.rule == "pro_hold" and cfg.pro_hold_first and cfg.pro_hold_first_at in ("gar", "gar_low") and not pinned
                and len(live_rows) >= 2 and self._gar(f, live_rows, held_now, seen)):
            return
        if cfg.rule == "pro_hold" and held_now is not None and live_rows and not held_now.get("closing"):
            # Sell on profit AT ONCE (user 2026-11-14: +35% and +18% passed unsold, then -45%). The whole trade
            # (every lot, after fees) is read from the 1s chart on every 0.5s poll — not the once-a-second curve
            # read — and sold the moment it is >= the target; no waiting for a red candle. The break-even floor
            # in the sell keeps slippage from turning it into a loss.
            c = live_rows[-1]
            up = self._fast_upct(held_now, f, c.close)
            if f.mint in (cfg.mints or []):
                # pinned coin (user 2026-11-14): after a ▲ BUY the price climbs in a run of small greens
                # (+0.6%, +1%, +1.6% …). Once the whole trade is >= Pinned profit % after fees, ride the run and sell
                # on the first red (still forming) while still in profit; under it, hold (never a loss sale).
                pmin = float(cfg.pinned_min_profit_pct)
                if up is not None and up >= pmin:
                    held_now["pro_peak"] = max(float(held_now.get("pro_peak") or up), up)
                    if c.close < c.open and time.time() - float(getattr(f, "take_at", 0) or 0) >= 1.0:
                        f.take_at = time.time()
                        self._pro_emit(f, live_rows, "SELL", {"call": "SELL", "px": c.close,
                                                               "why": f"pinned: green run ended — trade {up:+.1f}% ≥ "
                                                                      f"{pmin:g}% after fees (peak "
                                                                      f"{float(held_now['pro_peak']):+.1f}%), sold in profit"},
                                       seen)
                        return
                up = None                   # pinned coins do not use the general target below
            tgt, tgt_from = self.green_target()
            if up is not None and up >= tgt and time.time() - float(getattr(f, "take_at", 0) or 0) >= 1.0:
                f.take_at = time.time()
                held_now["pro_peak"] = max(float(held_now.get("pro_peak") or up), up)
                self._pro_emit(f, live_rows, "SELL", {"call": "SELL", "px": c.close,
                                                       "why": f"take profit — trade {up:+.1f}% ≥ {tgt:g}% after fees "
                                                              f"({tgt_from}), sold at once"
                                                              + (" (instant: on a trade)" if getattr(self, "_instant", False) else "")},
                                       seen)
                return
        if len(rows) < 2:
            return
        if f.last_eval_t is None:
            f.last_eval_t = rows[-1].t
            return
        if rows[-1].t <= f.last_eval_t:
            return
        prev_eval = f.last_eval_t
        f.last_eval_t = rows[-1].t
        cur = rows[-1]
        held = self.positions.get(f.mint)
        if not held:
            self.pro_px.pop(f.mint, None)
        ctx = self.pro_context(f.mint)
        cs = [{"t": c.t, "o": c.open, "h": c.high, "l": c.low, "c": c.close, "v": c.volume} for c in rows]
        if self.cfg.rule == "pro_hold" and (pinned or sig_only):
            a = PT.analyze(cs, None, None)
            if a.get("call") == "BUY":
                a = {"call": "BUY", "px": a.get("px"),
                     "why": ("▲ BUY (pinned coin) — " if pinned else "▲ BUY signal — ") + str(a.get("why") or "")}
            else:
                late = self._late_buy(PT, cs, prev_eval)
                if late is not None:
                    a = {"call": "BUY", "px": cur.close,
                         "why": ("▲ BUY (pinned coin) — " if pinned else "▲ BUY signal — ") + late}
                else:
                    a = {"call": "WAIT"}             # its exits are the pro_hold profit sell / Max hold
        elif self.cfg.rule == "pro_hold":
            # pro_hold buys on Botpredict's call (user's choice 2026-11-14: tested +5.0%/trade on unseen coins with
            # sell-in-profit / Max hold, vs -0.7% on the Pro Trader setup); its own exits are profit / Max hold
            from src.signal import botpredict as BPm
            a = BPm.analyze(cs, None)
            if a.get("call") == "SELL":
                a["call"] = "HOLD"
            if a.get("call") != "BUY" and cfg.pro_hold_pro_buy:
                # the chart's green ▲ BUY (Pro Trader) also buys (user's choice 2026-11-14; weaker in tests:
                # 47% won, -1.9% per trade without the top 10% — switch off on the Candles page if it loses)
                p = PT.analyze(cs, None, None)
                if p.get("call") == "BUY":
                    a = {"call": "BUY", "px": p.get("px"), "why": "▲ Pro Trader BUY — " + str(p.get("why") or "")}
        else:
            a = PT.analyze(cs, None, ctx["holding"])
        was_buy = bool(getattr(f, "pro_was_buy", False))
        f.pro_was_buy = a.get("call") == "BUY"
        if a.get("call") == "BUY" and held is None:
            side = "BUY"
        elif a.get("call") == "BUY" and self.cfg.rule == "pro_hold" and not was_buy:
            side = "BUY"                     # pro_hold: each new BUY signal buys again into the open trade
        elif held is not None and a.get("call") == "SELL" and self.cfg.rule == "pro":
            side = "SELL"
        elif held is not None and self.cfg.rule == "pro_hold" and self._pro_hold_take(held, cur) is not None:
            # pro_hold SELL signal, profit side only: the whole trade is >= target after fees and the green run
            # just ended (a red 1s candle). Tested on 76 unseen coins: beat selling at once at the target.
            side = "SELL"
            up, tgt, tgt_from = self._pro_hold_take(held, cur)
            a = {"call": "SELL", "px": cur.close,
                 "why": (f"take profit — trade {up:+.1f}% ≥ {tgt:g}% after fees ({tgt_from}), green run ended "
                         f"(red candle; peak {float(held.get('pro_peak') or up):+.1f}%)")}
        else:
            return
        self._pro_emit(f, rows, side, a, seen)

    def _pro_emit(self, f: Feed, rows: List[Candle], side: str, a: Dict[str, Any], seen: float,
                  first: bool = False, now_buy: bool = False, gar: bool = False,
                  low_ref: Optional[float] = None) -> None:
        cur = rows[-1]
        lag_ms = (seen - (cur.t + 1000) / 1000.0) * 1000.0
        sig = {"id": signal_id(f.mint, cur.t, "pro"), "mint": f.mint, "ts": round(seen * 1000),
               "side": side, "open": cur.open, "prevClose": rows[-2].close if len(rows) >= 2 else cur.open,
               "close": cur.close, "first": first, "now_buy": now_buy, "gar": gar, "low_ref": low_ref,
               "ref": cur.open, "deltaPct": round((cur.close / cur.open - 1) * 100, 4) if cur.open else 0.0,
               "candleTs": cur.t, "rule": "pro", "provider": f.using, "feedLagMs": round(lag_ms),
               "symbol": f.symbol, "seen": seen, "pro": True, "pro_why": str(a.get("why") or ""),
               "pro_px": float(a.get("px") or 0), "pro_hold_sell": side == "SELL" and self.cfg.rule == "pro_hold"}
        self.feed_lag_ms.append(lag_ms)
        self.m["signals_buy" if side == "BUY" else "signals_sell"] += 1
        self.recent.appendleft(sig)
        if self.store:
            self.store.audit("signal", signalId=sig["id"], mint=f.mint, side=side, deltaPct=sig["deltaPct"],
                             candleTs=cur.t, open=cur.open, prevClose=sig["prevClose"], close=cur.close,
                             provider=f.using, feedLagMs=sig["feedLagMs"], cfg=self.cfg.digest(), pro=sig["pro_why"])
        if self._route(sig) in ("intent", "queued") and side == "BUY":
            self.pro_px[f.mint] = sig["pro_px"]

    def _evaluate(self, f: Feed, seen: float) -> None:
        cfg = self.cfg
        if cfg.rule in ("pro", "pro_hold"):
            self._pro_evaluate(f, seen)
            return
        book = f.books[f.using]
        rows = book.finals()          # finished candles only: an in-progress candle's close is not final
        if len(rows) < 2:
            return
        if f.last_eval_t is None:                 # first read: history is not a signal
            f.last_eval_t = rows[-1].t
            return
        new = [i for i, c in enumerate(rows) if c.t > f.last_eval_t]
        if not new:
            return
        f.last_eval_t = rows[-1].t
        for i in new:
            cur, prev = rows[i], rows[i - 1] if i > 0 else None
            side, delta, ref = evaluate(cfg.rule, prev, cur, cfg.min_delta_pct)
            if not side:
                continue
            lag_ms = (seen - (cur.t + (1000 if cur.final else 0)) / 1000.0) * 1000.0
            sig = {"id": signal_id(f.mint, cur.t, cfg.rule), "mint": f.mint, "ts": round(seen * 1000),
                   "side": side, "open": cur.open, "prevClose": prev.close if prev else None, "close": cur.close,
                   "ref": ref, "deltaPct": delta, "candleTs": cur.t, "rule": cfg.rule, "provider": f.using,
                   "feedLagMs": round(lag_ms), "symbol": f.symbol, "seen": seen}
            self.feed_lag_ms.append(lag_ms)
            self.m["signals_buy" if side == "BUY" else "signals_sell"] += 1
            self.recent.appendleft(sig)
            if self.store:
                self.store.audit("signal", signalId=sig["id"], mint=f.mint, side=side, deltaPct=delta,
                                 candleTs=cur.t, open=cur.open, prevClose=sig["prevClose"], close=cur.close,
                                 provider=f.using, feedLagMs=sig["feedLagMs"], cfg=cfg.digest())
            self._route(sig)

    # ---------- routing
    def _route(self, sig: Dict[str, Any]) -> str:
        cfg = self.cfg
        mint, side = sig["mint"], sig["side"]
        f = self.feeds.get(mint)
        held = self.positions.get(mint)
        if self.store and self.store.claimed(sig["id"]):
            return self.skip(sig, "DUPLICATE")
        if not sig.get("_counted"):                 # consecutive same-side signals (a requeue counts once)
            sig["_counted"] = True
            st = self.streak.get(mint)
            if st and st[0] == side:
                st[1] += 1
            else:
                self.streak[mint] = st = [side, 1]
            sig["streak"] = st[1]
            if side == "BUY" and mint in self.positions:
                self.reds_since[mint] = self.reds_since.get(mint, 0) + 1
        n = int(sig.get("streak") or 1)
        ladder = self.ladder_on()
        if cfg.kill:
            return self.skip(sig, "KILLED")
        if side == "SELL" and held is None:
            return self.skip(sig, "NO_POSITION")
        if side == "SELL" and ladder and n < cfg.sell_on_green_confirm:
            return self.skip(sig, "GREEN_CONFIRM")
        if side == "BUY" and held is None and self.support_source():
            if not ((cfg.rule == "pro_hold" and sig.get("pro")) or sig.get("mirror")
                    or (self.paper_support() and sig.get("pro")) or self.feeds_only(self.support_source())):
                return self.skip(sig, "SUPPORT_NO_ENTRY")        # the source buys lot 1; Candles only adds / sells
            # pro_hold: Candles buys lot 1 itself on the chart BUY (the source only picks / feeds the coin),
            # and like the source it buys only while Auto Trade is on (off = watch only)
            try:
                from src.config.runtime import get_config as _rc
                # Feed-only sources (OpenCopy / CallOuts / MayhemMode do not buy themselves): Candles' own Live
                # switch is the gate, as for its own feeds — Auto Trade is not needed (user 2026-11-15: every
                # live buy was skipped AUTO_TRADE_OFF). Launch / Dayscalp still need it.
                if (cfg.mode == "live" and not self.feeds_only(self.support_source())
                        and not bool(getattr(_rc(), "trade_enabled", False))):
                    return self.skip(sig, "AUTO_TRADE_OFF")     # Paper: Auto Trade not needed
            except Exception:
                pass
        if (side == "BUY" and held is None and cfg.rule == "inverse" and cfg.max_entry_red_pct > 0
                and float(sig.get("deltaPct") or 0) < -cfg.max_entry_red_pct):
            return self.skip(sig, "CRASH_CANDLE")          # opening on a -15%+ second buys the crash, not a dip
        if side == "BUY" and held is not None and sig.get("pro") and cfg.rule == "pro_hold":
            # pro_hold: a new BUY signal buys again (Max position), no red-candle ladder; Max lots and Total cap
            # both limit it (user 2026-11-14: Max lots 5 was ignored — Total cap 0.015 / 0.005 allowed 3)
            from src.signal.candle_core import _fit_cap
            lots = len(held.get("lots") or []) or 1
            if lots >= max(1, int(cfg.max_ladder_buys)):
                return self.skip(sig, "LADDER_MAX")
            what, size = _fit_cap(float(held.get("entry_sol") or 0), self._lot_base(held.get("mode") or cfg.mode), cfg)
            if what != "add":
                return self.skip(sig, "LADDER_CAP")
            sig["add"], sig["ladder_size"], sig["lot_no"] = True, size, lots + 1
            if held.get("mode") == "live" and cfg.mode != "live":
                return self.skip(sig, "LIVE_NOT_BUILT")
        elif side == "BUY" and held is not None:
            if not ladder:
                return self.skip(sig, "ALREADY_LONG")
            base = self._lot_base(held.get("mode") or cfg.mode)
            lots = len(held.get("lots") or []) or 1
            if cfg.ladder_count == "since_last_lot":
                last = (held.get("lots") or [{}])[-1]
                if not last.get("px") and last.get("ts") and f is not None and f.using:
                    px = self._px_at(f, float(last["ts"]))       # adopted lot 1 (support): price from the feed
                    if px:
                        last["px"], last["src"] = px, f.using
                same_src = last.get("src") in (None, sig.get("provider"))
                what, size = ladder_decision_down(self.reds_since.get(mint, 0), lots, float(held.get("entry_sol") or 0),
                                                  base, cfg, price=sig.get("close"),
                                                  last_px=last.get("px") if same_src else None)
            else:
                what, size = ladder_decision(n, lots, float(held.get("entry_sol") or 0), base, cfg)
            if what != "add":
                return self.skip(sig, {"wait": "LADDER_WAIT", "max": "LADDER_MAX", "cap": "LADDER_CAP",
                                       "above": "LADDER_ABOVE"}[what])
            self.reds_since[mint] = 0
            sig["add"], sig["ladder_size"], sig["lot_no"] = True, size, lots + 1
            if held.get("mode") == "live" and cfg.mode != "live":
                return self.skip(sig, "LIVE_NOT_BUILT")     # never add live lots while switched to paper
        if side == "BUY" and cfg.mode == "live":
            why = self.live_block()
            if why:
                return self.skip(sig, why)
            if self.unresolved and self.unresolved.get("until", 0) > time.time():
                return self.skip(sig, "BUY_UNRESOLVED")
        if side == "BUY":
            if cfg.entries_paused:
                return self.skip(sig, "ENTRIES_PAUSED")
            if f is None or f.status == "DEGRADED":
                return self.skip(sig, "FEED_DEGRADED")
            if f.status == "STALE":
                return self.skip(sig, "STALE_FEED")
            if mint in cfg.blacklist:
                return self.skip(sig, "BLACKLIST")
            if cfg.whitelist_only and mint not in cfg.mints:
                return self.skip(sig, "NOT_WHITELISTED")
            # Live only: paper risks nothing, and blocking it would stop the test collecting data.
            if (cfg.mode == "live" and self.store
                    and self.store.realized_since(self._day_start(), "live") <= -cfg.max_daily_loss_sol):
                self.alert("daily_loss", f"live daily loss limit {cfg.max_daily_loss_sol} SOL reached — live entries blocked until 00:00 UTC.", 3600)
                return self.skip(sig, "DAILY_LOSS")
            if not sig.get("add") and cfg.buy_size_sol > cfg.max_trade_sol:
                return self.skip(sig, "MAX_TRADE")
            if not sig.get("add") and cfg.buy_size_sol > cfg.max_open_sol_per_mint:
                return self.skip(sig, "MAX_EXPOSURE")
            # max_signals_per_min counts acted-on signals (intents), not every candle that crossed Δ
            if sum(1 for t in self.sig_times if time.time() - t <= 60) >= cfg.max_signals_per_min:
                return self.skip(sig, "RATE_LIMIT")
            # the cooldown stops re-entry churn after a round trip; ladder adds come 1s apart by design
            if (not sig.get("add") and not sig.get("gar")
                    and time.time() - self.last_trade_at.get(mint, 0) < cfg.cooldown_s):
                return self.skip(sig, "COOLDOWN")
        if mint in self.busy:
            old = self.pending.get(mint)
            if old:
                self.skip(old, "COALESCED")
            self.pending[mint] = sig
            sig["action"] = "queued"
            return "queued"
        return self._start(sig)

    def _start(self, sig: Dict[str, Any]) -> str:
        if self.store and not self.store.claim(sig["id"]):
            return self.skip(sig, "DUPLICATE")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self.skip(sig, "NOT_READY")          # not on the engine loop: never mark the coin busy
        self.busy[sig["mint"]] = sig["id"]
        self.sig_times.append(time.time())
        sig["action"] = "intent"
        if self.store:
            self.store.signal(sig, "intent")
            self.store.audit("intent", signalId=sig["id"], mint=sig["mint"], side=sig["side"], mode=self.cfg.mode,
                             size=self.cfg.buy_size_sol if sig["side"] == "BUY" else None, cfg=self.cfg.digest())
        asyncio.ensure_future(self._run(sig))
        return "intent"

    async def _run(self, sig: Dict[str, Any]) -> None:
        mint = sig["mint"]
        try:
            held = self.positions.get(mint) or {}
            if sig["side"] == "BUY" and not sig.get("add") and not sig.get("manual") and not sig.get("mirror"):
                why, detail = self._entry_filters(mint, first=bool(sig.get("first")), now_buy=bool(sig.get("now_buy")))
                if why:
                    self.skip(sig, why)
                    sig["reason"] = f"{why} ({detail})"
                    return
            if (sig["side"] == "BUY" and self.cfg.min_reserve_sol > 0 and not sig.get("manual") and not sig.get("first")
                    and not sig.get("mirror")):
                try:
                    c, _bps, _why = await self._curve(mint)
                except Exception:
                    c = None                  # unreadable: the buy itself will find out; the guard only blocks on data
                real = (c or {}).get("real_sol")
                if real is not None and real / 1e9 < self.cfg.min_reserve_sol:
                    self.skip(sig, "LOW_RESERVES")
                    sig["reason"] = f"LOW_RESERVES ({real / 1e9:.3f} SOL in the curve)"
                    return
            if sig["side"] == "BUY" and sig.get("add"):
                if held.get("mode") == "live":
                    await self._live_add(sig)
                else:
                    await self._paper_buy(sig)
            elif sig["side"] == "BUY":
                if self.cfg.mode == "live":
                    await self._live_buy(sig)
                else:
                    await self._paper_buy(sig)
            elif (self.cfg.sell_only_in_profit and not sig.get("pro")
                  and not sig.get("manual") and not await self._in_profit(sig)):
                pass                                 # skipped NOT_IN_PROFIT: keep holding, wait for a better green
            else:
                why = ("manual Exit all (chart button) — sold at the market, not a rule sell" if sig.get("manual")
                       else f"Pro Trader: {sig.get('pro_why') or 'sell call'}" if sig.get("pro")
                       else f"green candle (+{sig['deltaPct']:.2f}% candle) with the whole trade ≥ {self.green_target()[0]:g}% "
                            f"({self.green_target()[1]})" if self.cfg.sell_only_in_profit
                       else f"green candle +{sig['deltaPct']:.2f}% (any green sells; that % is the candle, not the trade)")
                if held.get("mode") == "live":
                    await self._live_sell(sig, why)
                else:
                    await self._paper_sell(sig, why)
        except Exception as exc:
            log.exception("candle paper %s failed", sig["side"])
            self._failed(sig, f"{type(exc).__name__}: {exc}")
        finally:
            self.busy.pop(mint, None)
            self.last_trade_at[mint] = time.time()
            nxt = self.pending.pop(mint, None)
            if nxt:
                if time.time() - nxt["seen"] > self.cfg.signal_max_age_s:
                    self.skip(nxt, "STALE")
                else:
                    self._route(nxt)

    def _failed(self, sig: Dict[str, Any], err: str) -> None:
        self.m["exec_failed"] += 1
        self.exec_fail_streak += 1
        sig["action"], sig["reason"] = "failed", err[:120]
        if self.store and "candleTs" in sig:          # forced exits (max hold / pause) are not candle signals
            self.store.signal(sig, "failed", err[:200])
            self.store.fill({"signal_id": sig["id"], "mint": sig["mint"], "side": sig["side"], "ts": time.time(),
                             "mode": "paper", "status": "failed", "error": err[:200]})
            self.store.audit("fill", signalId=sig["id"], status="failed", error=err[:200])
        held = self.positions.get(sig["mint"]) or {}
        label = "LIVE" if (held.get("mode") == "live" or (sig["side"] == "BUY" and self.cfg.mode == "live")) else "paper"
        self.note("warn", f"{sig.get('symbol')} {label} {sig['side']} failed: {err[:100]}", sig["mint"])
        if self.exec_fail_streak >= 3:
            self.alert("exec_fail", f"{self.exec_fail_streak} executions failed in a row (last: {err[:80]}).", 300)

    # ---------- paper execution
    async def _curve(self, mint: str):
        from src.execution import pump_curve as PC
        PC.forget_curve(mint)
        curve, why = await PC._curve(mint)
        _fee, _bb, bps = await PC._fees()
        return curve, int(bps or 100), why

    async def _fill_px_chart(self, mint: str, sol: float, tokens: float, sig: Dict[str, Any]) -> float:
        """A live fill's price in chart units: the real SOL/token paid against the curve's price right now, on
        the chart's price right now. Falls back to the signal candle's close."""
        base = float(sig.get("close") or 0)
        try:
            from src.execution import pump_curve as PC  # noqa: F401
            f = self.feeds.get(mint)
            rows = f.books[f.using].rows if f and f.using else []
            c, _bps, _ = await self._curve(mint)
            if c and rows and sol > 0 and tokens > 0 and c.get("virtual_token"):
                curve_px = float(c["virtual_sol"]) / float(c["virtual_token"])        # lamports per raw token
                fill_px = sol * 1e9 / float(tokens)
                if curve_px > 0 and rows[-1].close > 0:
                    return rows[-1].close * fill_px / curve_px
        except Exception:
            log.debug("fill px failed", exc_info=True)
        return base

    async def _value(self, mint: str, tokens: float) -> Tuple[Optional[float], Optional[float], str]:
        """(SOL the whole holding sells for now, SOL left in the curve, source). The SOL curve is read
        fresh; a coin quoted in another coin (OpenCopy / CallOuts often hold these) is valued on its own
        curve and scaled by that coin's SOL rate; anything else falls back to a Jupiter quote (<= every 4s)."""
        from src.execution import pump_curve as PC
        raw = int(tokens or 0)
        if raw <= 0:
            return None, None, ""
        c, bps, why = await self._curve(mint)
        if c:
            real = c.get("real_sol")
            v = PC._sol_for_tokens(raw, c["virtual_token"], c["virtual_sol"], bps) / 1e9
            return v, (real / 1e9 if real is not None else None), "curve"
        if why == "not_sol_quote":
            try:
                v, real = await self._value_quoted(mint, raw, bps)
                if v is not None:
                    return v, real, "curve·quote"
            except Exception:
                log.debug("candle quoted value failed", exc_info=True)
        hit = self._jup_val.get(mint)
        if hit and hit[2] == raw and time.time() - hit[1] < 4.0:
            return hit[0], None, "jupiter"
        jup = self._jup()
        if jup is not None and (not hit or time.time() - hit[1] >= 4.0):
            try:
                q = await asyncio.wait_for(jup.quote_token_to_sol(mint, raw, slippage_bps=1000), timeout=2.0)
                if q and q.out_amount > 0:
                    self._jup_val[mint] = (q.out_amount / 1e9, time.time(), raw)
                    return q.out_amount / 1e9, None, "jupiter"
            except Exception:
                pass
        return (hit[0], None, "jupiter (old)") if hit and hit[2] == raw and time.time() - hit[1] < 20 else (None, None, "")

    async def _value_quoted(self, mint: str, raw: int, bps: int) -> Tuple[Optional[float], Optional[float]]:
        """Value on a pump curve quoted in another coin: quote out of the curve x SOL per quote coin."""
        from src.execution import pump_curve as PC
        curve = PC._pda([b"bonding-curve", bytes(PC._pk(mint))], PC.PUMP)
        res = await PC._rpc("getMultipleAccounts", [[str(curve)], {"encoding": "base64"}])
        vals = (res or {}).get("value") if isinstance(res, dict) else None
        if not vals or not vals[0]:
            return None, None
        acct = PC._raw_account(vals[0])
        if len(acct) < 115 or acct[48] != 0:
            return None, None
        from solders.pubkey import Pubkey
        quote = str(Pubkey.from_bytes(acct[83:115]))
        dec = self._quote_dec.get(quote)
        if dec is None:
            r2 = await PC._rpc("getMultipleAccounts", [[quote], {"encoding": "base64"}])
            v2 = (r2 or {}).get("value") if isinstance(r2, dict) else None
            m = PC._raw_account(v2[0]) if v2 and v2[0] else b""
            if len(m) < 45:
                return None, None
            dec = self._quote_dec[quote] = int(m[44])
        vq = PC._u64(acct, 16)
        scale = await self._quote_rate(mint, quote, dec, vq)      # SOL per 1 base unit of the quote coin
        if not scale:
            return None, None
        out = PC._sol_for_tokens(raw, PC._u64(acct, 8), vq, bps)
        return out * scale, PC._u64(acct, 32) * scale

    @staticmethod
    def _px_at(f: "Feed", ts: float) -> Optional[float]:
        """Close of the feed's candle at that second (None if the feed starts later)."""
        book = f.books.get(f.using)
        t = int(ts) * 1000
        best = None
        for c in (book.rows if book else []):
            if c.t <= t:
                best = c
            else:
                break
        return best.close if best else None

    def _jup(self):
        jup = getattr(self.manager, "_trader_jup", None) if self.manager is not None else None
        if jup is None and self.trader is not None:
            jup = getattr(self.trader, "jup", None)
        return jup

    async def _quote_rate(self, mint: str, quote: str, dec: int, vq: int) -> Optional[float]:
        """SOL per base unit of a quote coin, cached 30s: pump's rate for SOL-like quotes (hSOL), else a
        Jupiter quote of ~1% of the curve's quote reserve (e.g. pumpCmXq, ~0.00004 SOL each)."""
        from src.observability.dashboard import quote_ratio
        hit = self._qrate.get(quote)
        if hit and time.time() - hit[1] < 30:
            return hit[0]
        r = quote_ratio(mint)
        if r and r != 1.0:
            self._qrate[quote] = (r / (10 ** dec), time.time())
            return self._qrate[quote][0]
        jup = self._jup()
        amt = max(int(vq // 100), 10 ** max(0, dec - 2))
        if jup is not None and amt > 0:
            try:
                q = await asyncio.wait_for(jup.quote_token_to_sol(quote, amt, slippage_bps=300), timeout=2.0)
                if q and q.out_amount > 0:
                    self._qrate[quote] = (q.out_amount / 1e9 / amt, time.time())
                    return self._qrate[quote][0]
            except Exception:
                pass
        return hit[0] if hit and time.time() - hit[1] < 300 else None

    def _network_fee_sol(self) -> float:
        cfg = self.cfg
        return (BASE_FEE_LAMPORTS + cfg.compute_units * cfg.priority_fee_micro_lamports / 1e6) / 1e9

    @staticmethod
    async def _jup_quote_buy(jup, mint: str, sol: float) -> int:
        try:
            q = await asyncio.wait_for(jup.quote_sol_to_token(mint, sol, slippage_bps=1000), timeout=2.5)
            return int(q.out_amount) if q and q.out_amount > 0 else 0
        except Exception:
            return 0

    @staticmethod
    async def _jup_quote_sell(jup, mint: str, raw: int) -> Optional[float]:
        try:
            q = await asyncio.wait_for(jup.quote_token_to_sol(mint, int(raw), slippage_bps=1000), timeout=2.5)
            return q.out_amount / 1e9 if q and q.out_amount > 0 else None
        except Exception:
            return None

    async def _paper_buy(self, sig: Dict[str, Any]) -> None:
        from src.execution import pump_curve as PC
        cfg = self.cfg
        mint = sig["mint"]
        size = float(sig.get("ladder_size") or cfg.buy_size_sol)
        add = bool(sig.get("add")) and mint in self.positions
        # Build + sign first on the warm curve / blockhash (what a live send would do), then read the
        # curve fresh: that is the price at send time, and the fill is priced fill_lag_ms after the send.
        signed, _min, err = await PC.pump_buy_tx(mint, size, slippage_bps=cfg.max_slippage_bps, quiet=True)
        sent = time.time()
        lat = (sent - sig["seen"]) * 1000.0
        self.lat_ms.append(lat)
        c0, bps, why = await self._curve(mint)
        # graduated (PumpSwap) or quoted in another coin (not SOL): Jupiter prices them (user 2026-11-15)
        # ...unless Jupiter buys are off: a coin priced in another coin cannot be bought by Lightning / Local
        jup = self._jup() if (not c0 and why in ("graduated", "not_on_curve", "not_sol_quote")
                              and (cfg.buy_jupiter or why != "not_sol_quote")) else None
        if not c0 and jup is None:
            if why in ("not_sol_quote", "graduated", "not_on_curve"):
                self.unusable[mint] = why          # e.g. an hSOL-quoted coin: drop it, pick another
                self.feeds.pop(mint, None)
                self.note("warn", f"{sig.get('symbol')} dropped: {why} (not a SOL bonding-curve coin).", mint)
                return self.skip(sig, "NOT_TRADABLE")
            return self._failed(sig, f"not on curve ({why})")
        lam = int(size * 1e9)
        if jup is not None:
            # graduated pump coin (PumpSwap pool, user 2026-11-14): priced by Jupiter quotes — at send, and again
            # fill_lag_ms later for the fill; the pool fee is inside the quote
            q0 = await self._jup_quote_buy(jup, mint, size)
            if not q0:
                return self._failed(sig, "graduated coin: no Jupiter quote")
            expected = q0
            await asyncio.sleep(max(0.0, cfg.fill_lag_ms / 1000.0 - (time.time() - sent)))
            if self.cfg.kill:
                return self._failed(sig, "KILLED before fill")
            tokens = await self._jup_quote_buy(jup, mint, size) or expected
            bps1 = 0
        else:
            expected = PC._tokens_for_sol(lam, c0["virtual_token"], c0["virtual_sol"], bps)
            await asyncio.sleep(max(0.0, cfg.fill_lag_ms / 1000.0 - (time.time() - sent)))
            if self.cfg.kill:
                return self._failed(sig, "KILLED before fill")
            c1, bps1, why1 = await self._curve(mint)
            if not c1:
                return self._failed(sig, f"curve gone before fill ({why1})")
            tokens = PC._tokens_for_sol(lam, c1["virtual_token"], c1["virtual_sol"], bps1)
        slip = (expected - tokens) / expected * 1e4 if expected else 0.0
        fill_ms = (time.time() - sig["seen"]) * 1000.0
        pump_fee = size * bps1 / (1e4 + bps1)
        net_fee = self._network_fee_sol()
        rec = {"signal_id": sig["id"], "mint": mint, "side": "BUY", "ts": time.time(), "mode": "paper",
               "sol": size, "tokens": tokens, "expected_tokens": expected, "slippage_bps": round(slip, 1),
               "pump_fee_sol": pump_fee, "network_fee_sol": net_fee, "latency_ms": round(lat, 1),
               "fill_ms": round(fill_ms, 1), "error": None if signed else f"not built: {err}", "signature": None}
        if slip > cfg.max_slippage_bps:
            rec["status"] = "failed"
            rec["error"] = f"slippage {slip:.0f} bps > {cfg.max_slippage_bps} (would revert)"
            self.store.fill(rec)
            return self._failed(sig, rec["error"])
        rec["status"] = "confirmed"
        self.slip_bps.append(slip)
        self.fill_ms.append(fill_ms)
        self.m["executed"] += 1
        self.exec_fail_streak = 0
        # the lot's price in chart units at the REAL fill (2026-11-14: GHOSTY's L1 filled 0.7s later at a spike
        # top, 4x the signal candle — the marker and the fast profit reading used the signal price)
        px = float(sig.get("close") or 0)
        if px > 0 and expected and tokens:
            px = px * float(expected) / float(tokens)
        lot = {"n": 1, "sol": size, "tokens": float(tokens), "ts": time.time(), "signal": sig["id"],
               "px": px or sig.get("close"), "src": sig.get("provider"),
               "lot_t": sig.get("candleTs"), "low_ref": sig.get("low_ref")}
        if add and mint not in self.positions:
            # the trade closed (pause / max hold / stop) while this lot was being priced: no lot
            return self._failed(sig, "ladder add skipped — the trade closed meanwhile")
        if add:
            pos = self.positions[mint]
            pos.setdefault("lots", [{"n": 1, "sol": pos["entry_sol"], "tokens": pos["tokens"],
                                     "ts": pos["entry_ts"], "signal": pos["entry_signal"]}])
            lot["n"] = len(pos["lots"]) + 1
            pos["lots"].append(lot)
            pos["tokens"] = float(pos["tokens"]) + float(tokens)
            pos["entry_sol"] = float(pos["entry_sol"]) + size
            pos["costs_sol"] = float(pos.get("costs_sol") or 0) + pump_fee + net_fee
        else:
            self.reds_since[mint] = 0
            pos = {"mint": mint, "symbol": sig.get("symbol"), "mode": "paper", "tokens": float(tokens), "entry_sol": size,
                   "entry_ts": time.time(), "entry_signal": sig["id"], "costs_sol": pump_fee + net_fee, "lots": [lot]}
            self.positions[mint] = pos
        self.store.fill(rec)
        self.store.open_position(pos)
        self.store.signal(sig, "filled")
        self.store.audit("fill", signalId=sig["id"], **{k: rec[k] for k in ("side", "status", "sol", "tokens", "slippage_bps",
                                                                            "pump_fee_sol", "network_fee_sol", "latency_ms", "fill_ms")})
        sig["action"] = "filled"
        what = (f"PAPER LADDER BUY lot {lot['n']} (red #{sig.get('streak')}, total {pos['entry_sol']:.4f} SOL)"
                if add else "PAPER BUY")
        self.note("buy", f"{sig.get('symbol')} {what} {size:.4f} SOL (Δ{sig['deltaPct']:+.2f}%, signed in {lat:.0f} ms, "
                         f"filled {fill_ms:.0f} ms, slip {slip:+.0f} bps, feed lag {sig['feedLagMs']} ms).", mint)

    async def _paper_sell(self, sig: Optional[Dict[str, Any]], why: str, paused: bool = False) -> None:
        from src.execution import pump_curve as PC
        cfg = self.cfg
        mint = sig["mint"] if sig else ""
        pos = self.positions.get(mint)
        if pos is None:
            return
        ratio = cfg.sell_ratio if sig and not paused else 1.0
        part = int(pos["tokens"] * ratio)
        signed = ""
        if part > 0:
            signed, _m, _e = await PC.pump_sell_tx(mint, part, slippage_bps=cfg.max_slippage_bps, quiet=True)
        sent = time.time()
        lat = (sent - sig["seen"]) * 1000.0 if sig and sig.get("seen") and "candleTs" in sig else None
        if lat is not None:
            self.lat_ms.append(lat)
        c0, bps, why0 = await self._curve(mint)
        jup = self._jup() if (not c0 and why0 in ("graduated", "not_on_curve", "not_sol_quote")) else None
        if jup is not None and part > 0:
            # graduated coin: Jupiter quotes (pool fee inside), at send and fill_lag_ms later
            q0 = await self._jup_quote_sell(jup, mint, part)
            await asyncio.sleep(max(0.0, cfg.fill_lag_ms / 1000.0 - (time.time() - sent)))
            q1 = await self._jup_quote_sell(jup, mint, part)
            expected, got, bps1 = (q0 or q1 or 0.0), (q1 or q0 or 0.0), 0
            c0 = c1 = ({"jupiter": True} if (q0 or q1) else None)
        else:
            expected = PC._sol_for_tokens(part, c0["virtual_token"], c0["virtual_sol"], bps) / 1e9 if c0 else 0.0
            await asyncio.sleep(max(0.0, cfg.fill_lag_ms / 1000.0 - (time.time() - sent)))
            c1, bps1, why1 = await self._curve(mint)
            if c1:
                got = PC._sol_for_tokens(part, c1["virtual_token"], c1["virtual_sol"], bps1) / 1e9
            else:
                got, bps1 = expected, bps          # curve gone during the lag: last known value
        if not c0 and not c1 and not paused:
            # No price at all (RPC 429 / read failed). Booking 0 SOL out made fake -100% trades
            # (20:35-20:42): keep the position, the next SELL candle or max hold retries.
            pos.pop("closing", None)
            if sig and "candleTs" in sig:
                return self._failed(sig, "price unreadable — paper position kept, will retry")
            return
        slip = (expected - got) / expected * 1e4 if expected else 0.0
        fill_ms = (time.time() - sig["seen"]) * 1000.0 if sig and sig.get("seen") else None
        if fill_ms is not None:
            self.fill_ms.append(fill_ms)
        # A sell is never refused for slippage (user 2026-11-14 "must sell"): live sells carry 95% slippage, so
        # paper books the worse price too; a profit sell's break-even floor is still checked below.
        real = bool(sig) and "candleTs" in sig            # a candle signal, not a forced exit
        share = part / pos["tokens"] if pos["tokens"] else 1.0
        net_fee = self._network_fee_sol()
        if self._floored(sig):
            floor = pos["entry_sol"] * share + net_fee * (1 + share)
            if got < floor:
                # Live, the break-even minimum makes this sell revert (no fee) and the coins stay held.
                pos.pop("closing", None)
                return self._refused(sig, got, floor)
        sell_pump_fee = got * bps1 / max(1, 1e4 - bps1) if got else 0.0
        cost_part = pos["entry_sol"] * share
        entry_costs = float(pos.get("costs_sol") or 0.0) * share       # buy pump fee + buy network fee
        pnl = got - cost_part - net_fee - net_fee * share               # sell + buy network fees
        costs = entry_costs + sell_pump_fee + net_fee
        trade = {"mint": mint, "symbol": pos.get("symbol"), "mode": "paper", "entry_ts": pos["entry_ts"],
                 "exit_ts": time.time(), "entry_sol": cost_part, "exit_sol": got, "pnl_sol": round(pnl, 9),
                 "pct": round(pnl / cost_part * 100.0, 2) if cost_part else 0.0, "costs_sol": round(costs, 9),
                 "gross_pnl_sol": round(pnl + costs, 9), "entry_signal": pos["entry_signal"],
                 "exit_signal": sig["id"] if sig else None, "why": why, "paused_exit": 1 if paused else 0,
                 "lots": len(pos.get("lots") or []) or 1}
        self.store.fill({"signal_id": sig["id"], "mint": mint, "side": "SELL", "ts": time.time(), "mode": "paper",
                         "status": "confirmed", "sol": got, "tokens": part, "expected_sol": expected,
                         "slippage_bps": round(slip, 1), "pump_fee_sol": sell_pump_fee, "network_fee_sol": net_fee,
                         "latency_ms": round(lat, 1) if lat is not None else None,
                         "fill_ms": round(fill_ms, 1) if fill_ms is not None else None,
                         "error": None if signed else "sell not built", "signature": None})
        if real:
            self.store.signal(sig, "filled")
            sig["action"] = "filled"
            self.slip_bps.append(slip)
            self.m["executed"] += 1
            self.exec_fail_streak = 0
        whole = part >= pos["tokens"] or ratio >= 1.0
        if whole:
            self.positions.pop(mint, None)
        else:
            pos["tokens"] -= part
            pos["entry_sol"] -= cost_part
            pos["costs_sol"] = float(pos.get("costs_sol") or 0.0) - entry_costs
            pos.pop("closing", None)
        self.store.close_position(mint, trade, keep=None if whole else pos)
        if whole:
            self.traded[mint] = time.time()
            self.reds_since.pop(mint, None)
        self.store.audit("close", mint=mint, signalId=trade["exit_signal"], pnlSol=trade["pnl_sol"], pct=trade["pct"],
                         why=why, pausedExit=paused, cfg=self.cfg.digest())
        nlots = len(pos.get("lots") or []) or 1
        self.note("sell" if pnl > 0 else "warn",
                  f"{pos.get('symbol')} PAPER SELL{f' ({nlots} lots together)' if nlots > 1 else ''} {pnl:+.5f} SOL ({trade['pct']:+.1f}%) after "
                  f"{trade['exit_ts'] - trade['entry_ts']:.1f}s — {why}" + (" (paused: live sell may fail)" if paused else "") + ".", mint)

    async def _forced_exit(self, mint: str, why: str) -> None:
        if mint in self.busy:
            await asyncio.sleep(1.0)
        held = self.positions.get(mint) or {}
        paused = why.startswith("coin ")
        sig = {"id": f"forced-{mint}-{int(time.time())}", "mint": mint, "side": "SELL", "seen": time.time(),
               "symbol": held.get("symbol") or mint[:6]}
        if held.get("mode") == "live":
            await self._live_sell(sig, why)
        else:
            await self._paper_sell(sig, why, paused=paused)

    # ---------- live execution
    def live_block(self) -> str:
        """'' when a live buy may go out, else the skip reason. Candles' own Live switch is the gate:
        it does not need Auto Trade (that is the switch for the Live Controls source, e.g. Mayhem2)."""
        if self.trader is None or self.manager is None:
            return "NOT_READY"
        return ""          # no circuit breaker for Candles (removed on request 2026-11-10)

    async def _in_profit(self, sig: Dict[str, Any]) -> bool:
        """Whole trade valued on the curve right now, after fees. False -> skip NOT_IN_PROFIT (hold on)."""
        from src.execution import pump_curve as PC
        mint = sig["mint"]
        pos = self.positions.get(mint)
        if pos is None:
            return False
        pct = None
        try:
            value, _real, vsrc = await self._value(mint, pos["tokens"])
            if value is not None and vsrc != "jupiter (old)":
                pnl = value - float(pos["entry_sol"]) - 2 * self._network_fee_sol()
                pct = pnl / float(pos["entry_sol"]) * 100.0 if pos["entry_sol"] else None
        except Exception:
            pct = None
        # No fresh price -> no sell. The 2s-old valuation sold two "profits" at -28% / -32%.
        if pct is None or pct < self.green_target()[0]:
            sig["reason_detail"] = f"trade {pct:+.1f}%" if pct is not None else "price unknown"
            self.skip(sig, "NOT_IN_PROFIT")
            sig["reason"] = f"NOT_IN_PROFIT ({sig['reason_detail']})"
            return False
        return True

    def supports(self, pos) -> bool:
        """True for a coin the support source holds while AutoTrade-Candles is on: the guard applies from the
        moment the source's buy lands, not only after Candles' next 2s scan has adopted it."""
        if self.cfg.mode != "live":
            rec = self.positions.get(getattr(pos, "mint", ""))
            return bool(rec and rec.get("mode") == "live" and getattr(pos, "_candle_support", False))
        if getattr(pos, "_candle_support", False):
            return True
        src = self.support_source()
        return bool(src) and self._eligible(pos, src)

    async def source_sell_ok(self, pos, reason: str) -> bool:
        """AutoTrade-Candles gate for the source's own sell (caller sold, FAST PACE, stops ...): allowed only
        while the whole trade (all lots, after fees) is in profit (> 0) on a fresh valuation.
        Otherwise Candles keeps the coin and sells it by its rule (green candle in profit)."""
        mint = pos.mint
        rec = self.positions.get(mint) or {}
        cost = float(rec.get("entry_sol") or getattr(pos, "entry_sol", 0) or 0)
        tokens = float(getattr(pos, "tokens", 0) or rec.get("tokens") or 0)
        pct = None
        try:
            v, _real, vsrc = await self._value(mint, tokens)
            if v is not None and vsrc != "jupiter (old)" and cost > 0:
                pct = (v - cost - 2 * self._network_fee_sol()) / cost * 100.0
        except Exception:
            pct = None
        ok = pct is not None and pct > 0          # in profit: the source's own rule sells; in a loss: Candles' rule
        src = rec.get("support") or getattr(pos, "source", "") or "source"
        label = _EXIT_LABELS.get(reason, reason.replace("_", " "))
        key = f"gate:{mint}:{ok}"
        if time.time() - self.alerted.get(key, 0) >= 30:
            self.alerted[key] = time.time()
            sym = rec.get("symbol") or getattr(pos, "symbol", "") or mint[:6]
            now = f"{pct:+.1f}%" if pct is not None else "price unknown"
            if ok:
                self.note("info", f"{sym} — {src}'s rule ({label}) sells it: the trade is in profit ({now}).", mint)
            else:
                self.note("info", f"{sym} — {src}'s rule ({label}) wanted to sell at {now} (a loss): held back. Candles' rule takes over — it keeps the coin, "
                                  f"adds lots on reds and sells on a green once ≥ {self.green_target()[0]:g}%.", mint)
            if self.store:
                self.store.audit("support_gate", mint=mint, reason=reason, pct=pct, allowed=ok)
        if ok and mint in self.positions:
            self.positions[mint]["source_exit"] = label
        return ok

    def call_threadsafe(self, fn, *args, timeout: float = 5.0) -> Dict[str, Any]:
        """Run fn(*args) on the engine's event loop and return its result. The dashboard's HTTP handlers
        run on their own threads; starting a trade from there crashed (no event loop) and left the coin
        marked busy (01:16 ASX6)."""
        loop = self.loop
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if loop is None or running is loop:
            return fn(*args)

        async def _call():
            return fn(*args)
        try:
            return asyncio.run_coroutine_threadsafe(_call(), loop).result(timeout)
        except Exception as exc:
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:160]}

    def manual(self, action: str, mint: str) -> Dict[str, Any]:
        """Chart buttons. entry: open a trade, or add one lot to the open one (ignores the candle rules and
        guards, keeps Max lots / Total cap). exit: sell every lot now, at any price."""
        cfg = self.cfg
        mint = str(mint or "")
        if cfg.kill:
            return {"ok": False, "error": "Kill switch is on"}
        if mint in self.busy:
            return {"ok": False, "error": "A trade on this coin is already in progress — try again in a second"}
        held = self.positions.get(mint)
        f = self.feeds.get(mint)
        now = time.time()
        sig = {"id": f"manual-{action}-{mint}-{int(now * 1000)}", "mint": mint, "ts": round(now * 1000),
               "open": 0.0, "prevClose": None, "close": 0.0, "ref": 0.0, "deltaPct": 0.0, "candleTs": int(now * 1000),
               "rule": cfg.rule, "provider": "manual", "feedLagMs": 0, "seen": now, "manual": True, "_counted": True,
               "symbol": (f.symbol if f else None) or (held or {}).get("symbol") or mint[:6]}
        if action == "exit":
            if held is None:
                return {"ok": False, "error": "Nothing held on this coin"}
            sig["side"] = "SELL"
        elif action == "entry":
            sig["side"] = "BUY"
            mode = (held or {}).get("mode") or cfg.mode
            if mode == "live" and cfg.mode != "live":
                return {"ok": False, "error": "This trade is live but Candles is in paper mode"}
            if mode == "live":
                why = self.live_block()
                if why:
                    return {"ok": False, "error": why}
            if held is not None:
                lots = len(held.get("lots") or []) or 1
                if lots >= cfg.max_ladder_buys:
                    return {"ok": False, "error": f"Max lots ({cfg.max_ladder_buys}) reached"}
                size = ladder_lot_size(self._lot_base(mode), lots + 1, cfg.ladder_size_mode)
                from src.signal.candle_core import _fit_cap
                what, size = _fit_cap(float(held.get("entry_sol") or 0), size, cfg)
                if what != "add":
                    return {"ok": False, "error": f"Total cap {cfg.ladder_max_total_sol} SOL reached"}
                sig.update(add=True, ladder_size=size, lot_no=lots + 1)
        else:
            return {"ok": False, "error": "action must be entry or exit"}
        self.recent.appendleft(sig)
        self.note("info", f"{sig['symbol']} manual {'EXIT (sell all)' if action == 'exit' else ('ENTRY lot ' + str(sig.get('lot_no')) if sig.get('add') else 'ENTRY')} from the chart.", mint)
        return {"ok": True, "result": self._start(sig)}

    def reset(self) -> Dict[str, Any]:
        """Candles page "Reset data": empty the results, trades, signals and paper positions; keep settings.
        Refused while a live trade is open (its coins are real) or a trade is in flight."""
        if any(p.get("mode") == "live" and not p.get("support") for p in self.positions.values()):
            return {"ok": False, "error": "A LIVE trade is open — sell it (Exit all) first"}
        if self.busy or self.unresolved:
            return {"ok": False, "error": "A trade is in progress — try again in a few seconds"}
        moved = self._wipe()
        if self.store:
            self.store.audit("reset", archived=os.path.basename(moved) if moved else "", config=self.cfg.snapshot())
        self.note("info", "Candles data reset — results, trades, signals and paper positions cleared; settings kept."
                  + (f" Old audit log kept as {os.path.basename(moved)}." if moved else ""))
        return {"ok": True, "archived": os.path.basename(moved) if moved else ""}

    def _wipe(self) -> str:
        """Empty results, trades, signals, messages and stats. Support positions (the source's coins that
        Candles manages) are kept. Returns the archived audit file name."""
        keep = {m: p for m, p in self.positions.items() if p.get("support")}
        moved = self.store.reset() if self.store else ""
        self.positions.clear()
        self.positions.update(keep)
        if self.store:
            for p in keep.values():
                self.store.open_position(p)
        self.pending.clear()
        self.streak.clear()
        self.traded.clear()
        self.last_trade_at.clear()
        self.sig_times.clear()
        self.recent.clear()
        self.notes.clear()
        for d in (self.lat_ms, self.fill_ms, self.feed_lag_ms, self.slip_bps):
            d.clear()
        self.m = {"polls": 0, "poll_errors": 0, "signals_buy": 0, "signals_sell": 0, "executed": 0,
                  "exec_failed": 0, "skips": {}}
        self.exec_fail_streak = 0
        self.alerted.clear()
        self.started_at = time.time()
        return moved

    @staticmethod
    def _mark_ts(lot: Dict[str, Any]) -> Any:
        """Where a lot's marker sits on the chart: the candle its order was SENT in (`lot_t`), not the fill ~0.5 s
        later — a buy sent late in a green was drawn under the NEXT candle (SXSN 2026-11-15: L3 / L4 looked like the
        2nd green after the red). A fill more than 2 s after its candle (slow feed) keeps the fill time."""
        ts, t = lot.get("ts"), lot.get("lot_t")
        try:
            if t and ts and 0 <= float(ts) - float(t) / 1000.0 <= 2.0:
                return float(t) / 1000.0 + 0.5
        except (TypeError, ValueError):
            pass
        return ts

    def chart_marks(self, mint: str) -> Dict[str, Any]:
        """Entries / exits to draw on the chart: the open trade's lots and the last closed trades."""
        held = self.positions.get(mint) or {}
        lots = [{"ts": self._mark_ts(l), "n": l.get("n"), "sol": l.get("sol"), "px": l.get("px")} for l in (held.get("lots") or [])]
        if held and not lots:
            lots = [{"ts": held.get("entry_ts"), "n": 1, "sol": held.get("entry_sol")}]
        closed = []
        if self.store:
            with self.store.lock:
                closed = [{"entry_ts": r[0], "exit_ts": r[1], "pct": r[2], "lots": r[3], "mode": r[4]} for r in
                          self.store.db.execute("SELECT entry_ts, exit_ts, pct, lots, mode FROM trades WHERE mint=? "
                                                "ORDER BY exit_ts DESC LIMIT 20", (mint,)).fetchall()]
        sigs = [{"ts": (s.get("ts") or 0) / 1000.0, "side": s.get("side"), "action": s.get("action"),
                 "reason": s.get("reason"), "delta": s.get("deltaPct")}
                for s in list(self.recent) if s.get("mint") == mint and not s.get("manual")][:80]
        return {"mint": mint, "open": {"mode": held.get("mode"), "entry_sol": held.get("entry_sol"), "support": held.get("support"),
                                       "upct": held.get("upct"), "value_sol": held.get("value_sol"),
                                       "value_src": held.get("value_src"), "fees_sol": 2 * self._network_fee_sol(),
                                       "lots": lots} if held else None, "closed": closed,
                "signals": sigs, "hint": self._hint(mint)}

    def _hint(self, mint: str) -> Dict[str, Any]:
        """What the rule will do next on this coin: text for the Entry / Exit buttons + chart levels."""
        cfg = self.cfg
        held = self.positions.get(mint)
        f = self.feeds.get(mint)
        inverse = cfg.rule == "inverse"
        buy_on, sell_on = ("red", "green") if inverse else ("green", "red")
        tgt, tgt_from = self.green_target()
        out: Dict[str, Any] = {"minProfit": tgt, "minProfitFrom": tgt_from, "profitOnly": bool(cfg.sell_only_in_profit)}
        if cfg.rule == "pro_hold":
            out.update({"profitOnly": True, "pro": True})
            if held:
                up = held.get("upct")
                n = len(held.get("lots") or []) or 1
                room = max(0.0, cfg.ladder_max_total_sol - float(held.get("entry_sol") or 0))
                lot = self._lot_base(held.get("mode") or cfg.mode)
                cap_lots = n + int((room + 1e-9) // lot) if lot > 0 else n
                limit = min(int(cfg.max_ladder_buys), cap_lots)
                if n >= int(cfg.max_ladder_buys):
                    out["entry"] = f"Max lots {cfg.max_ladder_buys} reached ({n} lots)"
                elif room < 0.001:
                    out["entry"] = f"Total cap {cfg.ladder_max_total_sol:g} SOL reached ({n} lots)"
                else:
                    out["entry"] = (f"pro_hold: buy {n + 1} on the next signal — lots {n}/{limit} "
                                    f"(Max lots {cfg.max_ladder_buys}, Total cap {cfg.ladder_max_total_sol:g} SOL allows {cap_lots}"
                                    + (f" — raise Total cap to {cfg.max_ladder_buys * lot:g} for all {cfg.max_ladder_buys}"
                                       if cap_lots < cfg.max_ladder_buys else "") + ")")
                hold_txt = f"in a loss it holds up to Max hold {cfg.max_hold_s:.0f}s" if cfg.max_hold_s else "in a loss it holds (Max hold is off)"
                out["exit"] = (f"{'now ' + format(up, '+.1f') + '% — ' if up is not None else ''}SELL signal on the first red "
                               f"candle while the trade is ≥ {tgt:g}% after fees ({tgt_from}) — rides the green run; "
                               f"{hold_txt}")
                out["ready"] = bool(up is not None and up >= tgt)
            else:
                from src.signal import botpredict as BPm
                if mint in (cfg.mints or []):
                    out["entry"] = "pinned coin: lot 1 on the chart's ▲ BUY (Pro Trader call); sells on profit"
                    out["exit"] = "—"
                    return out
                extra = [x for x, on in (("a new coin's first candle", cfg.pro_hold_first),
                                         ("the green ▲ BUY", cfg.pro_hold_pro_buy)) if on]
                out["entry"] = (f"pro_hold: lot 1 on Botpredict's BUY (◆ BP — chance of +{BPm.PRED_PCT:.0f}% first "
                                f"≥ {BPm.MODEL['thr'] * 100:.0f}%)" + (", " + ", ".join(extra) if extra else ""))
                out["exit"] = "—"
            return out
        if cfg.rule == "pro":
            from src.signal import pro_trader as PT
            out.update({"minProfit": PT.TARGET_PCT, "minProfitFrom": "Pro Trader target", "profitOnly": False, "pro": True})
            if held:
                up = held.get("upct")
                out["entry"] = "Pro Trader: one entry per trade (no ladder)"
                out["exit"] = (f"{'now ' + format(up, '+.1f') + '% — ' if up is not None else ''}Pro Trader sells at "
                               f"{PT.TARGET_PCT:+.0f}% / {PT.STOP_PCT:+.0f}% / {PT.TIME_STOP_S:.0f}s")
                out["ready"] = bool(up is not None and up >= PT.TARGET_PCT - PT.FEE_PCT)
            else:
                out["entry"] = ("support mode: the source buys lot 1" if self.support_source()
                                else "Pro Trader BUY call: ≥10% under VWAP, green, RSI<45, volume ≥1.5x")
                out["exit"] = "—"
            return out
        if held:
            lots = len(held.get("lots") or []) or 1
            up = held.get("upct")
            reds = self.reds_since.get(mint, 0)
            every = max(1, int(cfg.reds_per_lot))
            if not (inverse and cfg.ladder_enabled):
                out["entry"] = "ladder off — manual Entry adds a lot"
            elif lots >= cfg.max_ladder_buys:
                out["entry"] = f"lots full ({lots}/{cfg.max_ladder_buys})"
            elif float(held.get("entry_sol") or 0) >= cfg.ladder_max_total_sol - 1e-9:
                out["entry"] = f"total cap {cfg.ladder_max_total_sol} SOL reached"
            elif cfg.ladder_count == "since_last_lot":
                left = max(0, every - reds)
                out["entry"] = (f"lot {lots + 1} in {left} more red{'s' if left != 1 else ''}, below the last buy"
                                if left else f"lot {lots + 1} on the next red below the last buy")
            else:
                nth = lots * every + 1
                out["entry"] = f"lot {lots + 1} on red {nth} in a row (a green sells all)"
            last = (held.get("lots") or [{}])[-1]
            out["lastLotTs"] = last.get("ts")
            if up is None:
                out["exit"] = f"waiting for a price · sells on {sell_on} at ≥ {tgt:g}% ({tgt_from})"
            elif cfg.sell_only_in_profit and up < tgt:
                out["exit"] = f"now {up:+.1f}% — sells on a {sell_on} candle once ≥ {tgt:g}% ({tgt_from})"
            else:
                out["exit"] = f"now {up:+.1f}% — sells on the next {sell_on} candle"
            out["ready"] = bool(up is not None and (not cfg.sell_only_in_profit or up >= tgt))
        else:
            if self.support_source():
                out["entry"] = "support mode: the source buys lot 1"
            else:
                why = []
                if f is not None:
                    r, detail = self._entry_filters(mint)
                    if r:
                        why.append(detail)
                out["entry"] = (f"opens on a {buy_on} candle (Δ ≥ {cfg.min_delta_pct:g}%"
                                + (f", ≤ {cfg.max_entry_red_pct:g}%" if inverse and cfg.max_entry_red_pct else "") + ")"
                                + (" · now blocked: " + "; ".join(why) if why else " · filters OK"))
            out["exit"] = "nothing held"
        return out

    def ladder_on(self) -> bool:
        return self.cfg.rule == "inverse" and bool(self.cfg.ladder_enabled)

    def _lot_base(self, mode: str) -> float:
        """Lot 1 size: paper uses Buy SOL; live uses the bot's Max position (what trader.buy spends)."""
        if mode == "live":
            try:
                from src.config.runtime import get_config
                return float(getattr(get_config(), "max_position_sol", 0) or 0.005)
            except Exception:
                return 0.005
        return float(self.cfg.buy_size_sol)

    async def _live_add(self, sig: Dict[str, Any]) -> None:
        """Ladder lot N on a live trade: same fast pump send as any buy, merged into the open trade."""
        mint, sym = sig["mint"], sig.get("symbol") or sig["mint"][:6]
        size = float(sig.get("ladder_size") or 0)
        rec = self.positions.get(mint)
        pos = self._mgr_pos(mint)
        if rec is None or pos is None or size <= 0:
            return self._failed(sig, "ladder add: no open live trade")
        try:
            bal = await self.trader.get_sol_balance(fast=True)
        except Exception:
            bal = None
        if bal is not None and float(bal) < size + 0.0015:
            return self._failed(sig, f"ladder add: insufficient_balance bal={float(bal):.4f} need={size + 0.0015:.4f}")
        from src.execution.signer import pubkey_from_env
        from src.execution.tx_route import run_swap
        from src.execution.wallet_tokens import get_token_holding
        pk = pubkey_from_env()
        before = float(pos.tokens or 0)
        t_add = time.time()
        try:
            routed = await run_swap(self.trader, "buy", mint, sol=size, reason="candle_add") or {}
        except Exception as exc:
            return self._failed(sig, f"ladder add crashed {type(exc).__name__}")
        ms = (time.time() - sig["seen"]) * 1000.0
        raw_now = 0
        for _ in range(4 if not routed.get("ok") else 2):   # the wallet is the truth for the new total
            try:
                held = await asyncio.wait_for(get_token_holding(pk, mint, fast=True, paid_only=True), timeout=1.5) if pk else None
                raw_now = int((held or {}).get("raw") or 0)
            except Exception:
                raw_now = 0
            if raw_now > before * 1.001:
                break
            await asyncio.sleep(0.75)
        got = raw_now - before if raw_now > before * 1.001 else (float(routed.get("tokens") or 0) if routed.get("ok") else 0.0)
        if got <= 0:
            err = str(routed.get("err") or "not landed")
            self.store.fill({"signal_id": sig["id"], "mint": mint, "side": "BUY", "ts": time.time(), "mode": "live",
                             "status": "failed", "error": ("ladder: " + err)[:200], "latency_ms": round(ms, 1)})
            return self._failed(sig, f"live ladder add: {err[:110]}")
        fee = 0.00002
        pos.tokens = float(pos.tokens or 0) + got
        pos.entry_sol = float(pos.entry_sol or 0) + size
        pos.fees_sol = float(getattr(pos, "fees_sol", 0) or 0) + fee
        try:
            from src.observability.ledger import get_ledger
            get_ledger().add_to_open(mint, sol=size, fee_sol=fee, tokens=got,
                                     signature=str(routed.get("sig") or "") or self._recent_buy_sig(mint, t_add))
            self.manager._push_remote()
        except Exception:
            log.exception("candle ladder ledger add failed")
        rec.setdefault("lots", [{"n": 1, "sol": rec["entry_sol"], "tokens": rec["tokens"], "ts": rec["entry_ts"],
                                 "signal": rec["entry_signal"]}])
        lot = {"n": len(rec["lots"]) + 1, "sol": size, "tokens": got, "ts": time.time(), "signal": sig["id"],
               "px": await self._fill_px_chart(mint, size, got, sig), "src": sig.get("provider"), "lot_t": sig.get("candleTs"), "low_ref": sig.get("low_ref")}
        rec["lots"].append(lot)
        rec["tokens"] = float(rec["tokens"]) + got
        rec["entry_sol"] = float(rec["entry_sol"]) + size
        rec["costs_sol"] = float(rec.get("costs_sol") or 0) + fee
        self.store.open_position(rec)
        self.store.fill({"signal_id": sig["id"], "mint": mint, "side": "BUY", "ts": time.time(), "mode": "live",
                         "status": "confirmed", "sol": size, "tokens": got, "network_fee_sol": fee,
                         "latency_ms": round(ms, 1), "fill_ms": round(ms, 1), "error": None,
                         "signature": routed.get("sig")})
        self.store.signal(sig, "filled")
        self.store.audit("fill", signalId=sig["id"], side="BUY", status="confirmed", mode="live", ladder_lot=lot["n"],
                         sol=size, tokens=got, signature=routed.get("sig"), latency_ms=round(ms, 1))
        sig["action"] = "filled"
        self.lat_ms.append(ms)
        self.m["executed"] += 1
        self.exec_fail_streak = 0
        self.note("buy", f"{sym} LIVE LADDER BUY lot {lot['n']} {size:.4f} SOL (red #{sig.get('streak')}, total "
                         f"{rec['entry_sol']:.4f} SOL) — landed {ms/1000:.2f}s after the signal.", mint)

    @staticmethod
    def _recent_buy_sig(mint: str, since: float) -> str:
        """The signature of a ladder buy whose route reply came back empty but whose coins landed: the
        candle send cache, else the wallet's newest BUY row for this coin. Without it the ledger counts
        the lot but cannot tell it apart, and the on-chain cost check cut the trade back to lot 1."""
        try:
            from src.execution.tx_route import candle_buy_get
            row = candle_buy_get(mint, 60.0)
            if row and row.get("sig") and float(row.get("ts") or 0) >= since - 1:
                return str(row["sig"])
        except Exception:
            pass
        try:
            from src.observability.exec_history import get_exec_history
            for e in reversed(list(get_exec_history().wallet_entries)):
                if (e.mint == mint and (e.side or "").upper() == "BUY" and float(e.ts or 0) >= since - 2
                        and getattr(e, "signature", "")):
                    return str(e.signature)
        except Exception:
            pass
        return ""

    def _mgr_pos(self, mint: str):
        if self.manager is None:
            return None
        for p in list(self.manager.positions.values()):
            if p.status == "OPEN" and p.mint == mint:
                return p
        return None

    def _tag_live(self, pos, sol: float, raw: float, sig_id: str, signature: str, symbol: str) -> None:
        """A landed live buy: tag it candle (only candle's exits apply), book it, track it."""
        pos.source = "candle"
        pos.strategy = "candle"
        pos.symbol = symbol or getattr(pos, "symbol", "") or pos.mint[:6]
        pos.entry_price = 1.0
        pos.peak_price = 1.0
        pos.stop_pct = None
        pos.stop_price = None
        pos.min_hold_seconds = 0
        pos.since_buy_pct = 0.0
        pos.peak_since_pct = 0.0
        pos._from_openholds = False
        try:
            self.manager.set_price(pos.mint, 1.0)
            self.manager._push_remote()
        except Exception:
            pass
        try:
            from src.observability.ledger import get_ledger
            get_ledger().record_open(
                id=f"cd-{pos.mint[:8]}-{int(time.time())}", mint=pos.mint, symbol=pos.symbol, mode="live",
                source="candle", token_amount=float(raw), entry_sol=float(sol), entry_fee_sol=0.0, entry_tip_sol=0.0,
                price_sol_per_token=(sol / raw) if raw else 0.0, signature=signature or "", since_buy_pct=0.0,
                ath_since_pct=0.0, name=getattr(pos, "name", "") or "")
        except Exception:
            log.exception("candle ledger open failed")
        rec = {"mint": pos.mint, "symbol": pos.symbol, "mode": "live", "tokens": float(raw), "entry_sol": float(sol),
               "entry_ts": time.time(), "entry_signal": sig_id, "costs_sol": float(getattr(pos, "fees_sol", 0) or 0),
               "lots": [{"n": 1, "sol": float(sol), "tokens": float(raw), "ts": time.time(), "signal": sig_id}]}
        self.positions[pos.mint] = rec
        self.reds_since[pos.mint] = 0
        self.store.open_position(rec)

    def _floored(self, sig: Optional[Dict[str, Any]]) -> bool:
        """A green-candle sell (not a pause / max-hold / reserves exit, not your manual Exit) must return
        at least the trade's cost."""
        # pro_hold's profit sell is always floored (user 2026-11-14 "must sell but on profit"): with 95% slippage
        # the break-even floor is what keeps the sale from landing at a loss
        return (bool(sig) and "candleTs" in sig and not sig.get("manual")
                and (bool(self.cfg.sell_only_in_profit) or bool(sig.get("pro_hold_sell"))))

    def _refused(self, sig: Dict[str, Any], got: Optional[float], floor: float) -> None:
        """Sell refused below break-even: nothing sold, no fee, coins kept for the next green."""
        sig["action"] = "skipped"
        detail = f"would get {got:.5f} < break-even {floor:.5f} SOL" if got is not None else f"price under break-even {floor:.5f} SOL"
        self.skip(sig, "BELOW_BREAKEVEN")
        sig["reason"] = f"BELOW_BREAKEVEN ({detail})"
        self.note("info", f"{sig.get('symbol') or sig['mint'][:6]} sell refused — {detail}. Holding; the next green tries again.", sig["mint"])

    async def _live_buy(self, sig: Dict[str, Any]) -> None:
        mint, sym = sig["mint"], sig.get("symbol") or sig["mint"][:6]
        try:
            fill = await self.trader.buy(mint=mint, size_multiplier=1.0, source="candle", ref_price=1.0)
        except Exception as exc:
            return self._failed(sig, f"buy crashed {type(exc).__name__}")
        done = time.time()
        ms = (done - sig["seen"]) * 1000.0
        if not (fill and fill.ok):
            err = str(getattr(fill, "error", "") or "buy_failed")
            if "not_confirmed" in err:
                from src.execution.tx_route import candle_buy_get
                info = candle_buy_get(mint, 120.0) or {}
                self.unresolved = {"mint": mint, "sig": info.get("sig", ""), "sol": info.get("sol", 0.0),
                                   "until": time.time() + 60.0, "signal": sig}
                self.note("warn", f"{sym} LIVE BUY sent but not confirmed yet — checking the wallet for 60s, no new buy until then.", mint)
                asyncio.ensure_future(self._live_resolve())
                return
            self.store.fill({"signal_id": sig["id"], "mint": mint, "side": "BUY", "ts": done, "mode": "live",
                             "status": "failed", "error": err[:200], "latency_ms": round(ms, 1)})
            low = err.lower()
            if any(k in low for k in ("not_sol_quote", "no_route", "not_on_curve", "graduated", "not a pump pool")):
                # e.g. an hSOL-quoted coin: the fast pump route cannot buy it. Drop it, watch the next coin.
                self.unusable[mint] = err[:40]
                self.feeds.pop(mint, None)
                self.note("warn", f"{sym} dropped: {err[:60]} (cannot be bought on pump's SOL curve). Watching the next coin.", mint)
                return self.skip(sig, "NOT_TRADABLE")
            return self._failed(sig, f"live buy: {err[:120]}")
        pos = self._mgr_pos(mint)
        if pos is None:
            return self._failed(sig, "live buy filled but no position opened")
        sol, raw = float(fill.sol_in or 0), float(fill.tokens_out or 0)
        self._tag_live(pos, sol, raw, sig["id"], fill.signature or "", sym)
        try:
            self.positions[mint]["lots"][0].update(px=await self._fill_px_chart(mint, sol, raw, sig),
                                                   src=sig.get("provider"))
        except (KeyError, IndexError):
            pass
        self.lat_ms.append(ms)
        self.fill_ms.append(ms)
        self.m["executed"] += 1
        self.exec_fail_streak = 0
        self.store.fill({"signal_id": sig["id"], "mint": mint, "side": "BUY", "ts": done, "mode": "live",
                         "status": "confirmed", "sol": sol, "tokens": raw, "pump_fee_sol": None,
                         "network_fee_sol": float(fill.fees_sol or 0), "latency_ms": round(ms, 1), "fill_ms": round(ms, 1),
                         "error": None, "signature": fill.signature})
        self.store.signal(sig, "filled")
        self.store.audit("fill", signalId=sig["id"], side="BUY", status="confirmed", mode="live", sol=sol, tokens=raw,
                         signature=fill.signature, latency_ms=round(ms, 1))
        sig["action"] = "filled"
        self.note("buy", f"{sym} LIVE BUY {sol:.4f} SOL — landed {ms/1000:.2f}s after the candle signal (Δ{sig['deltaPct']:+.2f}%).", mint)

    async def _live_resolve(self) -> None:
        u = self.unresolved
        if not u:
            return
        mint = u["mint"]
        sig = u["signal"]
        from src.execution.signer import pubkey_from_env
        from src.execution.wallet_tokens import get_token_holding
        pk = pubkey_from_env()
        try:
            while time.time() < u["until"] and self.unresolved is u:
                if mint in self.positions:
                    return
                pos = self._mgr_pos(mint)
                raw = 0
                try:
                    held = await asyncio.wait_for(get_token_holding(pk, mint, fast=True), timeout=2.0) if pk else None
                    raw = int((held or {}).get("raw") or 0)
                except Exception:
                    raw = 0
                if raw > 0 or pos is not None:
                    self.adopt(mint, raw or float(getattr(pos, "tokens", 0) or 0), float(u.get("sol") or 0), u.get("sig") or "",
                               sig.get("symbol") or mint[:6], pos=pos, sig_id=sig["id"])
                    self.note("buy", f"{sig.get('symbol')} the unconfirmed LIVE BUY landed — tracked as a candle trade.", mint)
                    return
                await asyncio.sleep(1.0)
            if self.unresolved is u:
                self._failed(sig, "live buy never reached the wallet in 60s")
        finally:
            if self.unresolved is u:
                self.unresolved = None

    def adopt(self, mint: str, raw: float, sol: float, signature: str, symbol: str, pos=None, sig_id: str = "") -> None:
        """Coins of a candle live buy found in the wallet (late landing / wallet sync): track as candle."""
        if mint in self.positions:
            return
        if pos is None:
            pos = self._mgr_pos(mint)
        if pos is None and self.manager is not None:
            from src.risk.position_manager import PositionMode

            async def _open():
                p = await self.manager.open_position(mint=mint, mode=PositionMode.LIVE, entry_sol=sol or 0.005,
                                                     entry_price=1.0, tokens=float(raw), source="candle",
                                                     signature=signature or "")
                if p is not None:
                    self._tag_live(p, sol or 0.005, raw, sig_id or f"adopt-{mint}", signature, symbol)
            asyncio.ensure_future(_open())
            return
        if pos is not None:
            self._tag_live(pos, sol or float(getattr(pos, "entry_sol", 0) or 0.005), raw, sig_id or f"adopt-{mint}", signature, symbol)
        if self.unresolved and self.unresolved.get("mint") == mint:
            self.unresolved = None

    async def _live_sell(self, sig: Dict[str, Any], why: str) -> None:
        mint = sig["mint"]
        rec = self.positions.get(mint)
        if rec is None:
            return
        pos = self._mgr_pos(mint)
        if pos is None:
            self._live_closed(mint, None, why + " (already sold elsewhere)")
            return
        t0 = time.time()
        floored = self._floored(sig)
        if floored:
            from src.execution.tx_route import clear_sell_floor, set_sell_floor
            floor = float(rec.get("entry_sol") or 0) + 2 * self._network_fee_sol()
            set_sell_floor(mint, int(floor * 1e9))          # break-even minimum written into the sell itself
        pos._candle_selling = time.time()          # the source's rule exits stand aside while Candles sells
        try:
            started = self.manager.kick_now(pos, "candle_sell")
            if not started and not getattr(pos, "_sell_busy", False):
                return self._failed(sig, "live sell did not start")
            last_kick = time.time()
            # A floored sell retries for 10s (each try is rebuilt and re-checks the break-even price);
            # a forced exit keeps trying for 30s. Re-kick as soon as the previous try has finished.
            limit = 10.0 if floored else 30.0
            held = lambda: pos.status == "OPEN" and any(p is pos for p in list(self.manager.positions.values()))
            while held() and time.time() - t0 < limit:
                await asyncio.sleep(0.15)
                if (held() and not getattr(pos, "_sell_busy", False)
                        and time.time() - last_kick > 0.5):
                    self.manager.kick_now(pos, "candle_sell")      # the sell did not land: again
                    last_kick = time.time()
        finally:
            pos._candle_selling = 0.0
            if floored:
                clear_sell_floor(mint)
        if pos.status == "OPEN" and not any(p is pos for p in list(self.manager.positions.values())):
            # The bot dropped this trade (its wallet sweep saw the coins gone): sold if a sell landed.
            if getattr(pos, "_last_sell_sig", "") or float(getattr(pos, "_sol_out", 0) or 0) > 0:
                pos.status = "CLOSED"
            else:
                return self._live_closed(mint, None, why + " (coins left the wallet)")
        if pos.status == "OPEN":
            rec.pop("closing", None)
            if floored:
                return self._refused(sig, None, floor)
            return self._failed(sig, "live sell not landed in 30s (still held; next SELL signal retries)")
        ms = (time.time() - float(sig.get("seen") or t0)) * 1000.0
        if "candleTs" in sig:
            self.lat_ms.append(ms)
            self.m["executed"] += 1
            self.exec_fail_streak = 0
            self.store.signal(sig, "filled")
            sig["action"] = "filled"
        self._live_closed(mint, pos, why, sig_id=sig.get("id"), ms=ms)

    def _find_exit(self, mint: str, since: float) -> Optional[Tuple[float, float, str]]:
        """SOL that came back for this coin after `since`: the bot's closed trade, else the wallet's SELL rows."""
        try:
            from datetime import datetime
            for t in reversed(list(getattr(self.manager, "closed", []) or [])):
                if t.mint != mint or float(t.exit_sol or 0) <= 0:
                    continue
                try:
                    closed_at = datetime.fromisoformat(str(t.closed_at)).timestamp()
                except (TypeError, ValueError):
                    closed_at = 0.0
                if closed_at >= since:                      # this trade's close, not an older one on the coin
                    return float(t.exit_sol), float(t.fees_sol or 0), _exit_label(getattr(t, 'exit_reason', '') or '')
        except Exception:
            pass
        try:
            from src.observability.exec_history import get_exec_history
            out = 0.0
            for e in list(get_exec_history().wallet_entries):
                if (e.mint == mint and (e.side or "").upper() == "SELL" and float(e.ts or 0) >= since - 5
                        and (e.status or "") != "failed"):
                    out += float(e.sol_amount or 0)
            if out > 0:
                return out, 0.0, "wallet history"
        except Exception:
            pass
        return None

    def _live_closed(self, mint: str, pos, why: str, sig_id: Optional[str] = None, ms: Optional[float] = None) -> None:
        rec = self.positions.pop(mint, None)
        if rec is None:
            return
        self.traded[mint] = time.time()
        self.reds_since.pop(mint, None)
        entry = float(rec.get("entry_sol") or 0)
        if pos is not None:
            out = float(getattr(pos, "_sol_out", 0) or 0)
            fees = float(getattr(pos, "fees_sol", 0) or 0) + float(getattr(pos, "_exit_fee", 0) or 0)
            pnl = out - entry - fees
        else:
            out, fees, pnl = 0.0, 0.0, None
            found = self._find_exit(mint, float(rec.get("entry_ts") or 0))
            if found:
                out, fees, src = found
                pnl = out - entry - fees
                why = f"{why} — sold for {out:.5f} SOL ({src})"
            if rec.get("support"):
                if rec.get("source_exit"):
                    why += " — allowed: the trade was in profit (in a loss Candles' rule would have kept it)"
                elif "pump paused" in why:
                    pass                  # paused exits always go through: a paused coin will not recover
                elif pnl is not None and pnl < 0:
                    why += " — went out before Candles' guard saw it (no guard check on this sell)"
        trade = {"mint": mint, "symbol": rec.get("symbol"), "mode": "live", "entry_ts": rec.get("entry_ts"),
                 "exit_ts": time.time(), "entry_sol": entry, "exit_sol": out,
                 "pnl_sol": round(pnl, 9) if pnl is not None else 0.0,
                 "pct": round(pnl / entry * 100.0, 2) if pnl is not None and entry else 0.0, "costs_sol": round(fees, 9),
                 "gross_pnl_sol": round((pnl or 0) + fees, 9), "entry_signal": rec.get("entry_signal"),
                 "exit_signal": sig_id, "why": why, "paused_exit": 1 if why.startswith("coin ") else 0,
                 "lots": len(rec.get("lots") or []) or 1}
        if pnl is None:
            self.store.close_position(mint, dict(trade, pnl_sol=0.0, why=why + " — PnL in the Trade Ledger"))
        else:
            self.store.close_position(mint, trade)
        self.store.audit("close", mint=mint, mode="live", signalId=sig_id, pnlSol=trade["pnl_sol"], why=why)
        if pnl is None:
            self.note("info", f"{rec.get('symbol')} LIVE trade closed elsewhere — {why}.", mint)
        else:
            nl = len(rec.get("lots") or []) or 1
            self.note("sell" if pnl > 0 else "warn", f"{rec.get('symbol')} LIVE SELL{f' ({nl} lots together)' if nl > 1 else ''} {pnl:+.5f} SOL ({trade['pct']:+.1f}%) "
                      f"after {trade['exit_ts'] - float(rec.get('entry_ts') or time.time()):.1f}s — {why}"
                      + (f", {ms/1000:.2f}s after the signal" if ms else "") + ".", mint)

    async def _reserve_loop(self) -> None:
        """Every second: SOL in each watched coin's curve, one batched chain read. Feeds the inflow filter."""
        import base64
        from src.execution import pump_curve as PC
        while True:
            await asyncio.sleep(1.0)
            mints = list(self.feeds)[:8]
            if not mints or not self.cfg.enabled:
                continue
            try:
                pdas = [str(PC._pda([b"bonding-curve", bytes(PC._pk(m))], PC.PUMP)) for m in mints]
                res = await PC._rpc("getMultipleAccounts", [pdas, {"encoding": "base64"}])
                now = time.time()
                for m, v in zip(mints, (res or {}).get("value") or []):
                    f = self.feeds.get(m)
                    if f is None or not v:
                        continue
                    raw = base64.b64decode(v["data"][0])
                    if len(raw) >= 40:
                        real = int.from_bytes(raw[32:40], "little") / 1e9
                        f.reserves.append((now, real))
                        pos = self.positions.get(m)
                        # Drain exit on this fresh chain read (the general curve read can fall back to
                        # pump's lagging site price under RPC limits — drains were caught at -93%).
                        if (pos is not None and self.cfg.exit_reserve_sol > 0 and real < self.cfg.exit_reserve_sol
                                and not pos.get("closing") and m not in self.busy):
                            pos["closing"] = True
                            asyncio.ensure_future(self._forced_exit(m, f"reserves draining ({real:.3f} SOL left)"))
            except Exception:
                log.debug("candle reserve read failed", exc_info=True)

    def _avoid_mayhem_for(self, f: Optional[Feed]) -> bool:
        """Skip Mayhem coins applies to Mixed and the AutoTrade-Candles sources. The Mayhem board feed lists only
        Mayhem coins: choosing it means trading them, so its own picks ("auto") are never skipped (user 2026-11-14)."""
        if not self.cfg.avoid_mayhem:
            return False
        if (f is not None and f.origin == "auto" and self.follow() == "mayhem_board"
                and not self.support_source()):
            return False
        if self.support_source() == "mayhem":
            return False          # MayhemMode only has Mayhem coins: choosing it means trading them (user 2026-11-14)
        return True

    def _check_mayhem(self, mint: str) -> None:
        """Ask pump once whether this coin is a Mayhem coin (its mayhem-state is null for a normal coin)."""
        if mint in self.mayhem_of or not self.loop:
            return
        self.mayhem_of[mint] = None

        async def go() -> None:
            from src.data.fast_get import mayhem2_get
            from src.signal.mayhem2 import STATE_URL
            try:
                body = await asyncio.to_thread(mayhem2_get, STATE_URL.format(mint=mint), "candlemayhem", 3.0)
                self.mayhem_of[mint] = bool(isinstance(body, dict) and body.get("state"))
            except Exception:
                self.mayhem_of.pop(mint, None)          # try again on the next signal
        try:
            asyncio.get_running_loop()
            asyncio.ensure_future(go())
        except RuntimeError:
            asyncio.run_coroutine_threadsafe(go(), self.loop)

    def _entry_filters(self, mint: str, first: bool = False, now_buy: bool = False) -> Tuple[str, str]:
        """New trades only (not ladder lots, not manual): money still flowing in, not far below the peak.
        -> (skip reason, detail) or ("", "")."""
        cfg = self.cfg
        f = self.feeds.get(mint)
        if f is None:
            return "", ""
        if self._avoid_mayhem_for(f):
            # pump pauses Mayhem coins (89% within ~1 min) and a paused coin cannot be sold: 27 pro_hold trades
            # held in a loss were frozen there (-0.174 SOL, 2026-11-13 18:09-22:50). Normal coins are never paused.
            mh = self.mayhem_of.get(mint)
            if mh is None and f.board_state:
                mh = True
            if mh is None:
                self._check_mayhem(mint)
                mh = self.mayhem_of.get(mint)
            if mh is None:
                return "MAYHEM_COIN", "checking whether pump can pause this coin"
            if mh:
                return "MAYHEM_COIN", "a Mayhem coin — pump can pause it and a paused coin cannot be sold"
        if cfg.inflow_window_s > 0 and not first:     # a first-candle buy has no reserve history yet
            now = time.time()
            hist = list(f.reserves)
            old = [r for t, r in hist if now - t >= cfg.inflow_window_s]
            if not hist or not old:
                return "SOL_OUTFLOW", f"no {cfg.inflow_window_s:.0f}s reserve history yet"
            cur = hist[-1][1]
            if cur < old[-1]:
                return "SOL_OUTFLOW", f"curve SOL {cur:.3f} < {old[-1]:.3f} {cfg.inflow_window_s:.0f}s ago"
        if cfg.max_below_peak_pct > 0 and not now_buy:
            book = f.books.get(f.using or cfg.provider)
            rows = book.rows if book else []
            if rows:
                peak = max(r.high for r in rows)
                price = rows[-1].close
                if peak > 0 and price < peak * (1 - cfg.max_below_peak_pct / 100.0):
                    return "FAR_BELOW_PEAK", f"{(1 - price / peak) * 100:.0f}% below the coin's high"
        return "", ""

    def _sync_wallet_cash(self) -> int:
        """Closed LIVE trades take the Trade Ledger's wallet figures (SOL really paid and got back).

        Candles books a trade at the planned lot size and the sell reply; the wallet also paid for accounts a
        route opened (~0.0015 SOL each) and lots that filled above / below the size (2026-11-15: Candles showed
        +0.0076 SOL while the wallet was −0.0044). The ledger is matched to the wallet's transactions a few
        seconds after each sale, so its row is the truer one. Rows with no ledger sale are left as they are."""
        if not self.store:
            return 0
        from src.observability.ledger import get_ledger
        closes = [e for e in get_ledger().entries if e.side == "CLOSE" and e.mint and float(e.exit_sol or 0) > 0]
        if not closes:
            return 0
        n = 0
        cut = time.time() - 86400
        for t in self.store.trades(200):
            if t.get("mode") != "live" or float(t.get("exit_ts") or 0) < cut or str(t.get("why") or "").startswith("sold by "):
                continue
            near = [e for e in closes if e.mint == t.get("mint") and abs(float(e.ts or 0) - float(t["exit_ts"])) <= 180]
            if len(near) != 1:
                continue          # no ledger sale, or a sale in parts: keep Candles' own figures
            e = near[0]
            cost = float(e.entry_sol or 0) + float(e.entry_fee_sol or 0) + float(e.entry_tip_sol or 0)
            pnl = float(e.exit_sol or 0) - cost - float(e.exit_fee_sol or 0)
            if cost <= 0 or abs(pnl - float(t.get("pnl_sol") or 0)) <= 0.00005:
                continue
            self.store.set_trade_cash(t["id"], cost, float(e.exit_sol or 0), pnl)
            n += 1
        return n

    # ---------- valuation
    async def _value_loop(self) -> None:
        from src.execution import pump_curve as PC
        warm_at = 0.0
        cash_at = 0.0
        while True:
            await asyncio.sleep(1.0 if self.positions else 2.0)   # 1s while holding: the pause guard reacts fast
            if time.time() - cash_at >= 20.0:
                cash_at = time.time()
                try:
                    self._sync_wallet_cash()
                except Exception:
                    log.debug("candle wallet-cash sync failed", exc_info=True)
            if time.time() - warm_at >= 1.9:      # warm-up stays every 2s
                warm_at = time.time()
                try:
                    # keep watched coins' curve (3s cache) and the blockhash warm, so a build takes ~1 ms
                    for mint in list(self.feeds)[:4]:
                        if mint not in self.positions:
                            await PC._curve(mint)
                    if self.feeds:
                        await PC._blockhash()
                        await PC._fees()
                except Exception:
                    log.debug("candle warm failed", exc_info=True)
            for mint, pos in list(self.positions.items()):
                if pos.get("mode") == "live":
                    mp = self._mgr_pos(mint)
                    if mp is not None and pos.get("support"):
                        mp._candle_support = True     # after a restart too: Candles alone sells it
                    if mp is not None and getattr(mp, "source", "") != "candle" and not pos.get("support"):
                        mp.source = mp.strategy = "candle"        # restored after a restart: candle owns it
                    if (mp is None and mint not in self.busy and not pos.get("closing")
                            and (pos.get("support") or time.time() - self.started_at > 90)):
                        # Wait a few seconds for the bot to book the sale, so its SOL and PnL are known.
                        gone = pos.setdefault("gone_at", time.time())
                        if time.time() - gone < 10 and not self._find_exit(mint, float(pos.get("entry_ts") or 0)):
                            continue
                        self._live_closed(mint, None, f"sold by {pos['support']}'s own rule"
                                          if pos.get("support") else "sold — the sale confirmed late (or a manual Sell), amount taken from the wallet")
                        continue
                    pos.pop("gone_at", None)
                if pos.get("paused_at"):
                    continue          # on the Paused Coins watch: no Max hold / stop / reserve exit (_paused_loop)
                if (self.cfg.max_hold_s and not pos.get("closing") and mint not in self.busy
                        and time.time() - float(pos.get("resumed_at") or pos.get("entry_ts") or time.time())
                        > self.cfg.max_hold_s):
                    pos["closing"] = True
                    asyncio.ensure_future(self._forced_exit(mint, f"max hold {self.cfg.max_hold_s:.0f}s"))
                try:
                    v, real, vsrc = await self._value(mint, pos["tokens"])
                    if real is not None:
                        pos["real_sol"] = real
                    if (real is not None and self.cfg.exit_reserve_sol > 0 and real < self.cfg.exit_reserve_sol
                            and not pos.get("closing") and mint not in self.busy):
                        # Mayhem pauses a coin once its curve SOL drains: get out while it still sells.
                        pos["closing"] = True
                        asyncio.ensure_future(self._forced_exit(mint, f"reserves draining ({real:.3f} SOL left)"))
                    if v is not None:
                        pos["value_sol"] = v
                        pos["value_src"] = vsrc
                        pos["value_at"] = time.time()
                        pos["upnl_sol"] = v - pos["entry_sol"] - 2 * self._network_fee_sol()
                        pos["upct"] = pos["upnl_sol"] / pos["entry_sol"] * 100.0 if pos["entry_sol"] else 0.0
                        try:
                            # selling the whole holding moves a thin curve: real value / chart value at this moment
                            # scales the fast chart reading (2026-11-14: chart said +10%, the sale was under cost)
                            fv = self.feeds.get(mint)
                            px = fv.books[fv.using].rows[-1].close if fv and fv.using and fv.books[fv.using].rows else 0.0
                            lots_ = pos.get("lots") or []
                            if px > 0 and lots_ and all(float(l.get("px") or 0) > 0 for l in lots_):
                                chart_v = sum(float(l.get("sol") or 0) * px / float(l["px"]) for l in lots_)
                                if chart_v > 0:
                                    pos["impact"] = max(0.2, min(1.2, v / chart_v))
                        except Exception:
                            pass
                        if self.cfg.rule == "pro_hold" and not pos.get("closing") and mint not in self.busy:
                            tgt, tgt_from = self.green_target()
                            if pos["upct"] >= tgt and not pos.get("pro_armed"):
                                # pro_hold: in profit after fees -> ride the green run; the chart's SELL signal
                                # (first red candle while still >= target, _pro_evaluate) takes the profit
                                pos["pro_armed"] = True
                                pos["pro_peak"] = pos["upct"]
                            if pos.get("pro_armed"):
                                pos["pro_peak"] = max(float(pos.get("pro_peak") or 0), pos["upct"])
                        if (self.cfg.stop_loss_pct and pos["upct"] <= -self.cfg.stop_loss_pct
                                and not pos.get("closing") and mint not in self.busy):
                            pos["closing"] = True
                            asyncio.ensure_future(self._forced_exit(mint, f"stop loss {pos['upct']:.1f}%"))
                except Exception:
                    log.debug("candle value failed", exc_info=True)

    # ---------- lifecycle
    def start(self, manager=None, trader=None) -> None:
        if self.started:
            return
        self.started = True
        self.manager = manager
        self.trader = trader
        self.loop = asyncio.get_running_loop()
        self.store = Store(DB_PATH, AUDIT_PATH)
        self.store.src_hook = self._src_label
        self.load_config(force=True)
        self.store.audit("start", digest=self.cfg.digest(), config=self.cfg.snapshot(), errors=self.cfg_errors)
        for p in self.store.positions():
            self.positions[p["mint"]] = p
            if p.get("paused_at"):
                self.paused_mints[p["mint"]] = float(p["paused_at"])     # still paused: stays on the watch
            else:
                # Max hold counts from the restart: a restored trade is not force-sold at once (2026-11-14 restored
                # paused coins were sold at Max hold before the pause check could see them)
                p["resumed_at"] = time.time()
        if self.positions:
            live = sum(1 for p in self.positions.values() if p.get("mode") == "live")
            self.note("info", f"Restored {len(self.positions)} position(s) from SQLite ({live} live: matched to the bot's open trades).")
        try:
            from src.data.caller_stream import get_stream
            get_stream().add_raw_listener(self._on_stream_trade)
        except Exception:
            log.debug("candle stream hook failed", exc_info=True)
        for coro in (self._board_loop(), self._poll_loop(), self._value_loop(), self._reserve_loop(), self._seq_loop(),
                     self._paused_loop()):
            asyncio.ensure_future(coro)
        self.note("info", f"Candle engine started — {self.cfg.mode.upper()}, rule {self.cfg.rule}, poll {self.cfg.poll_ms} ms.")

    def stream_mints(self) -> List[str]:
        return list(self.feeds) if self.cfg.enabled and "stream" in (self.cfg.provider, self.cfg.fallback) else []

    # ---------- observability
    def stats(self) -> Dict[str, Any]:
        def q(d, p):
            v = percentile(list(d), p)
            return round(v, 1) if v is not None else None
        tot = self.store.totals() if self.store else {}
        polls = self.m["polls"]
        return {"polls": polls, "pollErrors": self.m["poll_errors"],
                "pollOkPct": round(100.0 * (polls - self.m["poll_errors"]) / polls, 2) if polls else None,
                "pollsPerSec": round(polls / max(1.0, time.time() - self.started_at), 2),
                "signals": {"BUY": self.m["signals_buy"], "SELL": self.m["signals_sell"]},
                "executed": self.m["executed"], "execFailed": self.m["exec_failed"], "skips": dict(self.m["skips"]),
                "latencyMs": {"p50": q(self.lat_ms, .5), "p95": q(self.lat_ms, .95), "n": len(self.lat_ms)},
                "fillMs": {"p50": q(self.fill_ms, .5), "p95": q(self.fill_ms, .95)},
                "feedLagMs": {"p50": q(self.feed_lag_ms, .5), "p95": q(self.feed_lag_ms, .95)},
                "slippageBps": {"p50": q(self.slip_bps, .5), "p95": q(self.slip_bps, .95)},
                "all": tot}

    def metrics_text(self) -> str:
        s = self.stats()
        lines = [
            "# TYPE candle_polls_total counter", f"candle_polls_total {s['polls']}",
            "# TYPE candle_poll_errors_total counter", f"candle_poll_errors_total {s['pollErrors']}",
            "# TYPE candle_signals_total counter",
            f'candle_signals_total{{side="BUY"}} {s["signals"]["BUY"]}',
            f'candle_signals_total{{side="SELL"}} {s["signals"]["SELL"]}',
            "# TYPE candle_executed_total counter", f"candle_executed_total {s['executed']}",
            "# TYPE candle_exec_failed_total counter", f"candle_exec_failed_total {s['execFailed']}",
            "# TYPE candle_skips_total counter",
        ]
        lines += [f'candle_skips_total{{reason="{k}"}} {v}' for k, v in sorted(s["skips"].items())]
        for name, key in (("candle_signal_to_send_ms", "latencyMs"), ("candle_fill_ms", "fillMs"),
                          ("candle_feed_lag_ms", "feedLagMs"), ("candle_slippage_bps", "slippageBps")):
            lines.append(f"# TYPE {name} summary")
            for qn, qk in (("0.5", "p50"), ("0.95", "p95")):
                v = s[key].get(qk)
                if v is not None:
                    lines.append(f'{name}{{quantile="{qn}"}} {v}')
        a = s.get("all") or {}
        lines += ["# TYPE candle_pnl_sol gauge", f'candle_pnl_sol{{kind="net"}} {a.get("netPnlSol", 0)}',
                  f'candle_pnl_sol{{kind="gross"}} {a.get("grossPnlSol", 0)}',
                  f'candle_pnl_sol{{kind="worst_case"}} {a.get("worstCasePnlSol", 0)}',
                  "# TYPE candle_feed_degraded gauge"]
        lines += [f'candle_feed_degraded{{mint="{m}"}} {1 if f.status == "DEGRADED" else 0}' for m, f in self.feeds.items()]
        return "\n".join(lines) + "\n"

    def snapshot(self) -> Dict[str, Any]:
        now = time.time()
        feeds = []
        for m, f in list(self.feeds.items()):
            book = f.books.get(f.using or self.cfg.provider)
            last = book.last() if book else None
            feeds.append({"mint": m, "symbol": f.symbol, "origin": f.origin, "status": f.status, "using": f.using,
                          "boardState": f.board_state, "age": f.age, "errors": dict(f.err),
                          "candles": len(book.rows) if book else 0,
                          "lastAgeS": round(now - (last.t + 1000) / 1000.0, 1) if last else None,
                          "outOfOrder": book.out_of_order if book else 0, "duplicates": book.duplicates if book else 0,
                          "tail": [{"t": c.t, "o": c.open, "c": c.close, "f": c.final} for c in (book.rows[-12:] if book else [])]})
        pos = [dict({k: p.get(k) for k in ("mint", "symbol", "mode", "tokens", "entry_sol", "entry_ts", "value_sol",
                                           "upnl_sol", "upct", "support", "value_src", "src", "paused_at", "resumed_at",
                                           "pause_state")}, lots=len(p.get("lots") or []) or 1)
               for p in self.positions.values()]
        recent = [{k: s.get(k) for k in ("id", "mint", "symbol", "ts", "side", "deltaPct", "candleTs", "provider",
                                         "feedLagMs", "action", "reason")} for s in list(self.recent)[:40]]
        try:
            from src.config.runtime import get_config
            auto = bool(getattr(get_config(), "trade_enabled", False))
            max_pos = float(getattr(get_config(), "max_position_sol", 0) or 0)
        except Exception:
            auto, max_pos = False, 0.0
        sup = self.support_source()
        try:
            from src.config.runtime import get_config as _gc
            src_now = str(getattr(_gc(), "filter_mode", "") or "")
        except Exception:
            src_now = ""
        return {"ok": True, "seq": self.seq_status(), "paused": self.paused_view(), "support": sup, "supportEnabled": bool(self.cfg.support_enabled), "liveSource": src_now,
                "paperOnly": self.cfg.mode != "live", "live": self.cfg.mode == "live",
                "liveBlock": self.live_block() if self.cfg.mode == "live" else "", "autoTrade": auto,
                "maxPositionSol": max_pos, "unresolved": bool(self.unresolved), "config": self.cfg.snapshot(), "configErrors": self.cfg_errors,
                "digest": self.cfg.digest(), "feeds": feeds, "positions": pos, "signals": recent,
                "trades": self.store.trades(30) if self.store else [], "notes": list(self.notes)[:40],
                "stats": self.stats(), "busy": list(self.busy), "uptimeS": round(now - self.started_at)}


_engine: Optional[CandleMomo] = None


def get_engine() -> CandleMomo:
    global _engine
    if _engine is None:
        _engine = CandleMomo()
    return _engine
