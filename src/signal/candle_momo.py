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
SKIP_REASONS = ("KILLED", "NO_POSITION", "ALREADY_LONG", "ENTRIES_PAUSED", "FEED_DEGRADED", "STALE_FEED",
                "BLACKLIST", "NOT_WHITELISTED", "DAILY_LOSS", "RATE_LIMIT", "COOLDOWN", "MAX_EXPOSURE",
                "MAX_TRADE", "NOT_READY", "CIRCUIT_BREAKER", "BUY_UNRESOLVED", "DUPLICATE", "COALESCED", "STALE", "IN_FLIGHT", "NOT_TRADABLE", "LADDER_WAIT", "LADDER_MAX", "LADDER_CAP", "GREEN_CONFIRM", "NOT_IN_PROFIT", "LOW_RESERVES", "CRASH_CANDLE", "BELOW_BREAKEVEN", "SOL_OUTFLOW",
                "FAR_BELOW_PEAK", "LADDER_ABOVE", "SUPPORT_NO_ENTRY", "AUTO_TRADE_OFF")


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
        px = float(ev.get("price_sol") or 0)
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
                if self.cfg.auto_follow == "mayhem_board":
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
        """'opencopy' / 'callout' / 'launch' while AutoTrade-Candles is on and that is the Live Controls source, else ''."""
        if not self.cfg.support_enabled:
            return ""
        try:
            from src.config.runtime import get_config
            src = str(getattr(get_config(), "filter_mode", "") or "")
        except Exception:
            return ""
        return src if src in SUPPORT_SOURCES else ""

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
        stay; Candles only adds ladder lots and sells by its own rule."""
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

    def pro_hold_owns_entries(self, src: str) -> bool:
        """pro_hold with AutoTrade-Candles on this source: Candles buys (on Botpredict's BUY); the source only feeds."""
        return self.cfg.rule == "pro_hold" and self.support_source() == src

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
            if origin in ("scan", "mayhem-new", "mayhem-watch"):
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
        if self.cfg.rule == "pro_hold":
            # pro_hold buys on Botpredict's BUY itself: the youngest open is only watched, and only while flat
            if held or self.positions:
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
        out = self.update({"support_enabled": bool(on), "mode": "live" if on else "paper"})
        if on:
            self.note("info", "AutoTrade-Candles ON — Candles stopped its own trading and now supports the Live Controls "
                              "source: its held coins are the feeds; Candles adds lots and sells them by its rule (LIVE).")
        else:
            self.note("info", "Reverted — Candles is back to normal, in PAPER mode. The source keeps its own coins and rules.")
        return dict(out, support=bool(on))

    def _pick(self, rows: List[Dict[str, Any]]) -> None:
        cfg = self.cfg
        src = self.support_source()
        if src:
            # Support mode: feeds = the source's held coins (plus anything Candles still holds). No board picks.
            self._support_sync(src)
            want = {p.mint: "support" for p in self._support_held(src)}
            for m in self.positions:
                want.setdefault(m, "held")
            if src == "mayhem":
                for m in self._mayhem_grab():
                    want.setdefault(m, "mayhem-new")
            if src == "dayscalp":
                try:
                    from src.signal.dayscalp import LAST_CANDLE_PICK as _pk, LAST_CANDLE_PICKS as _pks
                    for gm, gv in list(self._grab.items()):
                        if time.time() - float(gv.get("at") or 0) > 60:
                            self._grab.pop(gm, None)
                    picks = list(_pks) if cfg.rule == "pro_hold" else [_pk]
                    if cfg.rule == "pro_hold" and self.positions:
                        picks = []        # one coin at a time: the open trade completes before the next coin is fed
                    for pk in picks:
                        if pk.get("mint") and time.time() - float(pk.get("ts") or 0) < 20:
                            want.setdefault(pk["mint"], "scan")      # Decoded Tokens' top pick(s): watch now
                            self._grab.setdefault(pk["mint"], {"at": time.time(), "symbol": pk.get("symbol") or ""})
                except Exception:
                    pass
            if cfg.rule == "pro_hold":
                want = self._one_feed(want)
            for m, origin in want.items():
                if m not in self.feeds:
                    f = Feed(m, cfg.count)
                    f.origin = src
                    f.symbol = ((self.positions.get(m) or {}).get("symbol") or (self._grab.get(m) or {}).get("symbol")
                                or m[:6])
                    self.feeds[m] = f
                    what = {"mayhem-new": ("Mayhem_Mode just opened it — pro_hold buys it on Botpredict's BUY (◆ BP)"
                                           if cfg.rule == "pro_hold" else "Mayhem_Mode just opened it — buying"),
                            "scan": ("Dayscalp's top pick on Decoded Tokens — pro_hold buys it on Botpredict's BUY (◆ BP)"
                                     if cfg.rule == "pro_hold" else
                                     "Dayscalp's top pick on Decoded Tokens — bought as lot 1 when Auto Trade is on")}
                    self.note("info", f"Watching {f.symbol} ({what.get(origin, src + ' holds it')}).", m)
            for m in list(self.feeds):
                if m not in want:
                    self.feeds.pop(m, None)
            self.providers["stream"].wanted = set(self.feeds)
            return
        want: Dict[str, Dict[str, Any]] = {}
        for m in cfg.mints:
            if m not in cfg.blacklist:
                want[m] = {"origin": "manual"}
        state = {str(r.get("m") or ""): r for r in rows}
        if cfg.auto_follow == "mayhem_board" and not cfg.whitelist_only:
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
            keep = [r for r in rows if ok(r) and r["m"] in self.feeds and self.feeds[r["m"]].origin == "auto"
                    and (cfg.reuse_coin or float(r.get("age") or 0) <= cfg.max_age_s)]
            fresh = [r for r in young if r["m"] not in {k["m"] for k in keep}]
            for r in (keep + fresh)[:cfg.max_mints]:
                want.setdefault(r["m"], {"origin": "auto"})
        for mint in list(self.positions):           # never drop a coin we hold
            want.setdefault(mint, {"origin": self.feeds[mint].origin if mint in self.feeds else "held"})
        for mint, info in want.items():
            if mint not in self.feeds:
                f = Feed(mint, cfg.count)
                f.origin = info["origin"]
                self.feeds[mint] = f
                self.note("info", f"Watching {state.get(mint, {}).get('t') or mint[:6]} ({info['origin']}).", mint)
            f = self.feeds[mint]
            r = state.get(mint)
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
            if f and f.board_state in ("paused", "completed") and not pos.get("closing") and self.loop:
                pos["closing"] = True
                asyncio.ensure_future(self._forced_exit(mint, f"coin {f.board_state}"))

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
        await asyncio.sleep(max(0.0, cfg.poll_ms / 1000.0 - (time.time() - t0)))

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

    def _pro_evaluate(self, f: Feed, seen: float) -> None:
        """Rule "pro": a day trader's read of the 1s chart (src/signal/pro_trader.py) once per new candle.
        Flat + BUY call -> BUY; holding + SELL call (target / stop / time stop) -> SELL."""
        from src.signal import pro_trader as PT
        rows = f.books[f.using].finals()
        if len(rows) < 2:
            return
        if f.last_eval_t is None:
            f.last_eval_t = rows[-1].t
            return
        if rows[-1].t <= f.last_eval_t:
            return
        f.last_eval_t = rows[-1].t
        cur = rows[-1]
        held = self.positions.get(f.mint)
        if not held:
            self.pro_px.pop(f.mint, None)
        ctx = self.pro_context(f.mint)
        cs = [{"t": c.t, "o": c.open, "h": c.high, "l": c.low, "c": c.close, "v": c.volume} for c in rows]
        if self.cfg.rule == "pro_hold":
            # pro_hold buys on Botpredict's call (user's choice 2026-11-14: tested +5.0%/trade on unseen coins with
            # sell-in-profit / Max hold, vs -0.7% on the Pro Trader setup); its own exits are profit / Max hold
            from src.signal import botpredict as BPm
            a = BPm.analyze(cs, None)
            if a.get("call") == "SELL":
                a["call"] = "HOLD"
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
        lag_ms = (seen - (cur.t + 1000) / 1000.0) * 1000.0
        sig = {"id": signal_id(f.mint, cur.t, "pro"), "mint": f.mint, "ts": round(seen * 1000),
               "side": side, "open": cur.open, "prevClose": rows[-2].close, "close": cur.close,
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
            if not (cfg.rule == "pro_hold" and sig.get("pro")):
                return self.skip(sig, "SUPPORT_NO_ENTRY")        # the source buys lot 1; Candles only adds / sells
            # pro_hold: Candles buys lot 1 itself on the chart BUY (the source only picks / feeds the coin),
            # and like the source it buys only while Auto Trade is on (off = watch only)
            try:
                from src.config.runtime import get_config as _rc
                if not bool(getattr(_rc(), "trade_enabled", False)):
                    return self.skip(sig, "AUTO_TRADE_OFF")
            except Exception:
                pass
        if (side == "BUY" and held is None and cfg.rule == "inverse" and cfg.max_entry_red_pct > 0
                and float(sig.get("deltaPct") or 0) < -cfg.max_entry_red_pct):
            return self.skip(sig, "CRASH_CANDLE")          # opening on a -15%+ second buys the crash, not a dip
        if side == "BUY" and held is not None and sig.get("pro") and cfg.rule == "pro_hold":
            # pro_hold: a new BUY signal buys again (Max position), no red-candle ladder; Total cap still holds
            from src.signal.candle_core import _fit_cap
            lots = len(held.get("lots") or []) or 1
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
            if not sig.get("add") and time.time() - self.last_trade_at.get(mint, 0) < cfg.cooldown_s:
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
            if sig["side"] == "BUY" and not sig.get("add") and not sig.get("manual"):
                why, detail = self._entry_filters(mint)
                if why:
                    self.skip(sig, why)
                    sig["reason"] = f"{why} ({detail})"
                    return
            if sig["side"] == "BUY" and self.cfg.min_reserve_sol > 0 and not sig.get("manual"):
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
            elif (((self.cfg.sell_only_in_profit and not sig.get("pro")) or sig.get("pro_hold_sell"))
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
        if not c0:
            if why in ("not_sol_quote", "graduated", "not_on_curve"):
                self.unusable[mint] = why          # e.g. an hSOL-quoted coin: drop it, pick another
                self.feeds.pop(mint, None)
                self.note("warn", f"{sig.get('symbol')} dropped: {why} (not a SOL bonding-curve coin).", mint)
                return self.skip(sig, "NOT_TRADABLE")
            return self._failed(sig, f"not on curve ({why})")
        lam = int(size * 1e9)
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
        lot = {"n": 1, "sol": size, "tokens": float(tokens), "ts": time.time(), "signal": sig["id"],
               "px": sig.get("close"), "src": sig.get("provider")}
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
        c0, bps, _ = await self._curve(mint)
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
        real = bool(sig) and "candleTs" in sig            # a candle signal, not a forced exit
        if real and expected and slip > cfg.max_slippage_bps:
            self.store.fill({"signal_id": sig["id"], "mint": mint, "side": "SELL", "ts": time.time(), "mode": "paper",
                             "status": "failed", "error": f"slippage {slip:.0f} bps (would revert)"})
            pos.pop("closing", None)
            return self._failed(sig, f"slippage {slip:.0f} bps (would revert)")
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

    def chart_marks(self, mint: str) -> Dict[str, Any]:
        """Entries / exits to draw on the chart: the open trade's lots and the last closed trades."""
        held = self.positions.get(mint) or {}
        lots = [{"ts": l.get("ts"), "n": l.get("n"), "sol": l.get("sol"), "px": l.get("px")} for l in (held.get("lots") or [])]
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
                out["entry"] = (f"pro_hold: buy {n + 1} on the next Botpredict BUY (◆ BP) (no red-candle lots; {room:.4f} SOL left of Total cap "
                                f"{cfg.ladder_max_total_sol:g})" if room >= 0.001 else f"Total cap {cfg.ladder_max_total_sol:g} SOL reached")
                hold_txt = f"in a loss it holds up to Max hold {cfg.max_hold_s:.0f}s" if cfg.max_hold_s else "in a loss it holds (Max hold is off)"
                out["exit"] = (f"{'now ' + format(up, '+.1f') + '% — ' if up is not None else ''}SELL signal on the first red "
                               f"candle while the trade is ≥ {tgt:g}% after fees ({tgt_from}) — rides the green run; "
                               f"{hold_txt}")
                out["ready"] = bool(up is not None and up >= tgt)
            else:
                from src.signal import botpredict as BPm
                out["entry"] = (f"pro_hold: lot 1 on Botpredict's BUY (◆ BP — chance of +{BPm.PRED_PCT:.0f}% first "
                                f"≥ {BPm.MODEL['thr'] * 100:.0f}%)")
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
               "px": sig.get("close"), "src": sig.get("provider")}
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
        return bool(sig) and "candleTs" in sig and not sig.get("manual") and bool(self.cfg.sell_only_in_profit)

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
            self.positions[mint]["lots"][0].update(px=sig.get("close"), src=sig.get("provider"))
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

    def _entry_filters(self, mint: str) -> Tuple[str, str]:
        """New trades only (not ladder lots, not manual): money still flowing in, not far below the peak.
        -> (skip reason, detail) or ("", "")."""
        cfg = self.cfg
        f = self.feeds.get(mint)
        if f is None:
            return "", ""
        if cfg.inflow_window_s > 0:
            now = time.time()
            hist = list(f.reserves)
            old = [r for t, r in hist if now - t >= cfg.inflow_window_s]
            if not hist or not old:
                return "SOL_OUTFLOW", f"no {cfg.inflow_window_s:.0f}s reserve history yet"
            cur = hist[-1][1]
            if cur < old[-1]:
                return "SOL_OUTFLOW", f"curve SOL {cur:.3f} < {old[-1]:.3f} {cfg.inflow_window_s:.0f}s ago"
        if cfg.max_below_peak_pct > 0:
            book = f.books.get(f.using or cfg.provider)
            rows = book.rows if book else []
            if rows:
                peak = max(r.high for r in rows)
                price = rows[-1].close
                if peak > 0 and price < peak * (1 - cfg.max_below_peak_pct / 100.0):
                    return "FAR_BELOW_PEAK", f"{(1 - price / peak) * 100:.0f}% below the coin's high"
        return "", ""

    # ---------- valuation
    async def _value_loop(self) -> None:
        from src.execution import pump_curve as PC
        warm_at = 0.0
        while True:
            await asyncio.sleep(1.0 if self.positions else 2.0)   # 1s while holding: the pause guard reacts fast
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
                                          if pos.get("support") else "no longer open in the bot (sold or closed elsewhere)")
                        continue
                    pos.pop("gone_at", None)
                if (self.cfg.max_hold_s and not pos.get("closing") and mint not in self.busy
                        and time.time() - float(pos.get("entry_ts") or time.time()) > self.cfg.max_hold_s):
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
        self.load_config(force=True)
        self.store.audit("start", digest=self.cfg.digest(), config=self.cfg.snapshot(), errors=self.cfg_errors)
        for p in self.store.positions():
            self.positions[p["mint"]] = p
        if self.positions:
            live = sum(1 for p in self.positions.values() if p.get("mode") == "live")
            self.note("info", f"Restored {len(self.positions)} position(s) from SQLite ({live} live: matched to the bot's open trades).")
        try:
            from src.data.caller_stream import get_stream
            get_stream().add_raw_listener(self.providers["stream"].on_trade)
        except Exception:
            log.debug("candle stream hook failed", exc_info=True)
        for coro in (self._board_loop(), self._poll_loop(), self._value_loop(), self._reserve_loop()):
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
                                           "upnl_sol", "upct", "support", "value_src")}, lots=len(p.get("lots") or []) or 1)
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
        return {"ok": True, "support": sup, "supportEnabled": bool(self.cfg.support_enabled), "liveSource": src_now,
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
