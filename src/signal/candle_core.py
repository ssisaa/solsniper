"""Candle-momentum core: pure pieces with no I/O, so tests can replay recorded candles.

Candle      one normalized 1s candle (t = open time in ms).
CandleBook  rolling, de-duplicated, ordered buffer per mint.
evaluate    the signal rule on the newest candles.
Config      schema-checked settings (config/candle.yaml).

Measured on pump's frontend-api-v3 /candles (2026-11-10, 203 consecutive pairs): every candle's
open equals the previous candle's close, gaps included. The spec's literal rule
(open > previous close) therefore never fires on that feed. It is kept as rule "literal" (it can fire
on the stream-built fallback, whose open is the first trade of the second). The default rule
"own_candle" uses the information the feed actually carries: a finished candle's close vs its open.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Tuple

RULES = ("own_candle", "inverse", "literal", "pro", "pro_hold")
# pro: src/signal/pro_trader.py chart read (+40 / -5 / 120s). pro_hold: buy on Botpredict's BUY (botpredict.py), sell only once the
# whole trade is in profit (Min profit / Fast Pace after fees); in a loss hold until Max hold, then sell.
MODES = ("paper", "live")
_NS = uuid.UUID("6f1e0c52-6c1b-4b8e-9d0e-2c6a1d0b7a11")


@dataclass(frozen=True)
class Candle:
    t: int              # open time, ms
    open: float
    high: float
    low: float
    close: float
    volume: float
    final: bool = True


def _num(v: Any) -> Optional[float]:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def parse_pump(body: Any) -> List[Candle]:
    """pump /candles body -> candles. Rows with a missing or non-positive price are dropped."""
    rows = body.get("candles") if isinstance(body, dict) else body
    out: List[Candle] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        t = r.get("openTimeMs", r.get("t", r.get("timestamp")))
        o, h, lo, c = (_num(r.get(k)) for k in ("open", "high", "low", "close"))
        v = _num(r.get("volumeUsd", r.get("volume"))) or 0.0
        try:
            t = int(t)
        except (TypeError, ValueError):
            continue
        if t < 1_000_000_000_000:          # seconds -> ms
            t *= 1000
        if not o or not c or o <= 0 or c <= 0:
            continue
        out.append(Candle(t=t, open=o, high=h or max(o, c), low=lo or min(o, c), close=c, volume=v,
                          final=bool(r.get("final", True))))
    return out


class CandleBook:
    """Last N candles of one mint, ordered by t. Duplicates keep the first final version; an older
    candle arriving after a newer one is discarded and counted."""

    def __init__(self, size: int = 300) -> None:
        self.size = max(2, int(size))
        self.rows: List[Candle] = []
        self.out_of_order = 0
        self.duplicates = 0

    def merge(self, candles: List[Candle]) -> int:
        """Merge one response. Returns how many candle times were new. A response whose newest
        candle is older than what we already hold is a stale / out-of-order read: counted, merged
        only where it fills a hole."""
        if not candles:
            return 0
        by_t: Dict[int, Candle] = {c.t: c for c in self.rows}
        newest = self.rows[-1].t if self.rows else None
        if newest is not None and max(c.t for c in candles) < newest:
            self.out_of_order += 1
        cutoff = self.rows[0].t if len(self.rows) >= self.size else None
        added = 0
        for c in candles:
            old = by_t.get(c.t)
            if old is not None:
                if old.final:
                    if old != c:
                        self.duplicates += 1
                    continue                 # keep the first final version
                by_t[c.t] = c                # a live candle is replaced by its newer state
                continue
            if cutoff is not None and c.t < cutoff:
                continue                     # older than the window: history we no longer keep
            by_t[c.t] = c
            added += 1
        self.rows = sorted(by_t.values(), key=lambda x: x.t)[-self.size:]
        return added

    def finals(self) -> List[Candle]:
        return [c for c in self.rows if c.final]

    def last(self) -> Optional[Candle]:
        return self.rows[-1] if self.rows else None


def signal_id(mint: str, candle_t: int, rule: str) -> str:
    """Deterministic: one candle on one mint under one rule is one signal, across restarts."""
    return str(uuid.uuid5(_NS, f"{mint}|{int(candle_t)}|{rule}"))


def evaluate(rule: str, prev: Optional[Candle], cur: Candle, min_delta_pct: float
             ) -> Tuple[str, float, float]:
    """-> (side, delta_pct, reference). side is BUY, SELL or "" (below threshold).

    own_candle: cur.close vs cur.open (cur must be final).   BUY when up by >= min_delta_pct.
    inverse:    the same delta, sides swapped (buy the red candle, sell the green one).
    literal:    cur.open vs prev.close (the spec as written).
    """
    if rule == "literal":
        if prev is None or prev.close <= 0:
            return "", 0.0, 0.0
        ref, now = prev.close, cur.open
    else:
        if not cur.final or cur.open <= 0:
            return "", 0.0, 0.0
        ref, now = cur.open, cur.close
    delta = (now / ref - 1.0) * 100.0
    if round(abs(delta), 9) < float(min_delta_pct) or delta == 0:
        return "", round(delta, 4), ref
    side = "BUY" if delta > 0 else "SELL"
    if rule == "inverse":
        side = "SELL" if side == "BUY" else "BUY"
    return side, round(delta, 4), ref


# ---------------------------------------------------------------- config

@dataclass
class Config:
    enabled: bool = True
    mode: str = "paper"                      # paper | live (live is refused: not built)
    rule: str = "own_candle"
    res: str = "1s"
    count: int = 300
    measure: str = "price_usd"
    poll_ms: int = 500
    stale_s: float = 5.0
    fail_degrade: int = 3                    # consecutive poll failures -> DEGRADED
    min_delta_pct: float = 0.5
    max_signals_per_min: int = 30
    cooldown_s: float = 2.0
    signal_max_age_s: float = 4.0            # a queued signal older than this is dropped (STALE)
    buy_size_sol: float = 0.01
    sell_ratio: float = 1.0
    max_slippage_bps: int = 2500
    priority_fee_micro_lamports: int = 50000
    compute_units: int = 120000
    fill_lag_ms: int = 450                   # paper: fill priced on the curve this long after the decision
    max_trade_sol: float = 0.02
    max_open_sol_per_mint: float = 0.02
    max_daily_loss_sol: float = 0.05
    entries_paused: bool = False
    kill: bool = False
    provider: str = "pump_rest"              # pump_rest | stream
    fallback: str = "stream"                 # "" | stream | pump_rest
    auto_follow: str = "mayhem_board"        # none | mayhem_board | mixed (New -> Movers -> Graduated, youngest first)
    pro_hold_first: bool = True              # pro_hold: also buy a NEW coin (age <= 60s) on its first candle
    pro_hold_first_at: str = "start"         # first-candle buy: "start" = as the chart starts (1-2 candles, before it
                                             # moves) | "green" = when the first green starts
    pro_hold_pro_buy: bool = True            # pro_hold: also buy on the chart's green ▲ BUY (Pro Trader call)
    source_seq: bool = False                 # Source Sequence: every step (>= 60s) the source moves on in SEQ_STEPS
    avoid_mayhem: bool = True                # never buy a Mayhem coin (pump can pause it; a paused coin cannot sell)
    mixed_idle_s: float = 60.0               # mixed: a watched coin with no trade for this long hands over to the next turn
    max_mints: int = 1
    min_age_s: float = 3.0
    max_age_s: float = 180.0                 # only coins younger than this are PICKED (see reuse_coin)
    support_enabled: bool = False            # AutoTrade-Candles (OpenHolds / CallOuts pages): Candles supports the
                                             # Live Controls source (opencopy / callout) — its held coins are the
                                             # feeds; the source buys lot 1, Candles adds lots and sells by its rule
    reuse_coin: bool = True                  # on: keep trading the same coin until it is paused / completed
                                             # off: one round trip per coin, then move on to a new coin
    # Inverse ladder (rule "inverse" only): lot 1 on the first red; more lots on consecutive reds
    # from red number min_red_count_to_ladder + 1 (default: the 3rd red); the first green (or
    # sell_on_green_confirm greens in a row) sells every lot together. stop_loss_pct 0 = off.
    ladder_enabled: bool = True
    max_ladder_buys: int = 5                 # lots per trade, lot 1 included
    min_red_count_to_ladder: int = 2         # old setting, no longer used (see reds_per_lot)
    reds_per_lot: int = 3                    # one lot per N reds: buys on reds 1, N+1, 2N+1 … (3: 1, 4, 7)
    ladder_count: str = "since_last_lot"     # since_last_lot: count reds since the last lot (greens do not reset)
                                             # and add only below the last lot's price (averaging down)
                                             # in_a_row: reds in a row only (a green resets the count)
    ladder_size_mode: str = "fixed"          # fixed: every lot = lot 1 · martingale: 1x, 1x, 2x, 4x, …
    ladder_max_total_sol: float = 0.015      # hard cap on all lots of one trade together (SOL)
    sell_on_green_confirm: int = 1           # green candles in a row needed to sell
    stop_loss_pct: float = 0.0               # e.g. 30 = sell all at -30% (overrides waiting for green)
    sell_only_in_profit: bool = True         # a SELL candle sells only if the whole trade is up >= min_profit_pct
    min_profit_pct: float = 5.0              # net of fees, valued on pump's curve at that moment (buffer
                                             # for the ~0.5s between the decision and the fill)
    # Pause guard (measured 2026-11-11: Mayhem coins are paused after the SOL in their curve drains to ~0)
    max_entry_red_pct: float = 15.0          # inverse: never OPEN a trade on a red bigger than this (a crash, not a
                                             # dip — backtest 2026-11-11: avg -2.2% -> +4.1%/trade). 0 = off
    # Entry filters (replay of 80 live coins, 2026-11-11: avg +8.4% -> +21.6%/trade, win 56% -> 69%)
    inflow_window_s: float = 5.0             # open only if the curve holds >= the SOL it held this many s ago
                                             # (money still flowing in: a dip, not a drain). 0 = off
    max_below_peak_pct: float = 85.0         # don't open if the price is this far below the coin's high. 0 = off
    min_reserve_sol: float = 0.15            # no buy / ladder lot while the curve holds less than this
    exit_reserve_sol: float = 0.08           # holding and the curve drops below this: sell everything now
    max_hold_s: float = 60.0                 # close any trade held this long (0 = off): sparse candles
                                             # on a dying coin may never print the SELL candle
    mints: List[str] = field(default_factory=list)       # always watched (whitelist when strict)
    whitelist_only: bool = False
    blacklist: List[str] = field(default_factory=list)

    def snapshot(self) -> Dict[str, Any]:
        return asdict(self)

    def digest(self) -> str:
        return hashlib.sha1(json.dumps(self.snapshot(), sort_keys=True).encode()).hexdigest()[:10]


_LIMITS: Dict[str, Tuple[float, float]] = {
    "count": (2, 1000), "poll_ms": (200, 10000), "stale_s": (1, 120), "fail_degrade": (1, 20),
    "min_delta_pct": (0, 100), "max_signals_per_min": (1, 600), "cooldown_s": (0, 600),
    "signal_max_age_s": (0.5, 60), "buy_size_sol": (0.0001, 1), "sell_ratio": (0.01, 1),
    "max_slippage_bps": (1, 10000), "priority_fee_micro_lamports": (0, 10_000_000),
    "compute_units": (10_000, 1_400_000), "fill_lag_ms": (0, 10000), "max_trade_sol": (0.0001, 5),
    "max_open_sol_per_mint": (0.0001, 5), "max_daily_loss_sol": (0.0001, 100), "max_mints": (0, 10),
    "min_age_s": (0, 86400), "max_age_s": (1, 864000), "max_hold_s": (0, 3600), "max_ladder_buys": (1, 20), "min_red_count_to_ladder": (1, 20), "reds_per_lot": (1, 20),
    "ladder_max_total_sol": (0.0001, 5), "sell_on_green_confirm": (1, 10), "stop_loss_pct": (0, 100), "min_profit_pct": (-50, 1000), "min_reserve_sol": (0, 100), "max_entry_red_pct": (0, 100), "inflow_window_s": (0, 60),
    "max_below_peak_pct": (0, 99.9), "exit_reserve_sol": (0, 100), "mixed_idle_s": (5, 3600),
}
_CHOICES = {"mode": MODES, "rule": RULES, "res": ("1s", "5s", "15s", "1m"), "measure": ("price_usd", "price"),
            "provider": ("pump_rest", "stream"), "fallback": ("", "stream", "pump_rest"),
            "auto_follow": ("none", "mayhem_board", "mixed"), "ladder_size_mode": ("fixed", "martingale"),
            "ladder_count": ("since_last_lot", "in_a_row"), "pro_hold_first_at": ("start", "green")}
HOT_KEYS = set(Config.__dataclass_fields__)      # everything is hot-reloadable (no keys live here)


_REMOVED = {"support_source_min_pct"}    # taken out: the source sells while in profit, Candles' rule in a loss


def validate(data: Dict[str, Any], base: Optional[Config] = None) -> Tuple[Config, List[str]]:
    """Merge data onto base. Returns (config, errors); a bad field keeps the base value."""
    cfg = Config(**(base.snapshot() if base else {}))
    errors: List[str] = []
    for key, val in (data or {}).items():
        if key in _REMOVED:
            continue                      # an old setting still in candle.yaml: ignored, not an error
        if key not in Config.__dataclass_fields__:
            errors.append(f"unknown field {key}")
            continue
        cur = getattr(cfg, key)
        try:
            if isinstance(cur, bool):
                val = val if isinstance(val, bool) else str(val).strip().lower() in ("1", "true", "yes", "on")
            elif isinstance(cur, int):
                val = int(val)
            elif isinstance(cur, float):
                val = float(val)
            elif isinstance(cur, list):
                if isinstance(val, str):
                    val = [x for x in val.replace(",", " ").split()]
                val = [str(x).strip() for x in (val or []) if str(x).strip()]
                bad = [x for x in val if not (32 <= len(x) <= 44 and x.isalnum())]
                if bad:
                    raise ValueError(f"not a mint: {bad[0][:12]}")
            else:
                val = "" if val is None else str(val).strip()
        except (TypeError, ValueError) as exc:
            errors.append(f"{key}: {exc}")
            continue
        if key in _LIMITS:
            lo, hi = _LIMITS[key]
            if not lo <= val <= hi:
                errors.append(f"{key}: {val} outside {lo}..{hi}")
                continue
        if key in _CHOICES and val not in _CHOICES[key]:
            errors.append(f"{key}: {val!r} not one of {', '.join(map(str, _CHOICES[key]))}")
            continue
        setattr(cfg, key, val)
    if cfg.buy_size_sol > cfg.max_trade_sol:
        errors.append(f"buy_size_sol {cfg.buy_size_sol} above max_trade_sol {cfg.max_trade_sol}; capped")
        cfg.buy_size_sol = cfg.max_trade_sol
    return cfg, errors


def percentile(values: List[float], q: float) -> Optional[float]:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def replay(candles: List[Candle], rule: str, min_delta_pct: float, mint: str = "replay",
           chunk: int = 1) -> List[Dict[str, Any]]:
    """Deterministic replay: feed candles `chunk` at a time through a CandleBook, evaluate every new
    candle exactly like the engine, and gate by position (BUY only when flat, SELL only when long).
    Returns the signals with action "enter" / "exit" / "NO_POSITION" / "ALREADY_LONG"."""
    book = CandleBook(len(candles) + 2)
    out: List[Dict[str, Any]] = []
    last_t: Optional[int] = None
    long = False
    for i in range(0, len(candles), max(1, chunk)):
        book.merge(candles[i:i + chunk])
        rows = book.finals()
        if len(rows) < 2:
            if rows and last_t is None:
                last_t = rows[-1].t
            continue
        if last_t is None:
            last_t = rows[-1].t
            continue
        for j, cur in enumerate(rows):
            if cur.t <= last_t:
                continue
            side, delta, _ref = evaluate(rule, rows[j - 1] if j else None, cur, min_delta_pct)
            if not side:
                continue
            if side == "BUY":
                act = "ALREADY_LONG" if long else "enter"
                long = True
            else:
                act = "exit" if long else "NO_POSITION"
                long = False
            out.append({"id": signal_id(mint, cur.t, rule), "side": side, "deltaPct": delta, "candleTs": cur.t,
                        "price": cur.close, "action": act})
        last_t = rows[-1].t
    return out



def ladder_lot_size(base: float, lot_no: int, mode: str) -> float:
    """Size of lot number lot_no (1-based). fixed: base each. martingale: 1x, 1x, 2x, 4x, 8x …"""
    if mode != "martingale" or lot_no <= 2:
        return base
    return base * (2 ** (lot_no - 2))


def _fit_cap(held_sol: float, size: float, cfg: "Config") -> Tuple[str, float]:
    """A lot that would pass Total cap shrinks to what is left (lot 1 bought by OpenCopy / CallOuts is
    often 0.0051, so a full last lot would miss the cap by 0.0001). Under half a lot left -> cap."""
    room = cfg.ladder_max_total_sol - held_sol
    if size <= room + 1e-12:
        return "add", size
    if room >= max(0.5 * size, 0.001):
        return "add", round(room - 1e-9, 9)
    return "cap", 0.0


def ladder_decision(streak: int, lots: int, held_sol: float, base: float, cfg: "Config") -> Tuple[str, float]:
    """Inverse ladder on a red candle while holding. -> ("add", size) or ("wait"|"max"|"cap", 0).
    streak = reds in a row (a green resets it). One lot per reds_per_lot reds: with 3, lots on reds
    1, 4, 7, 10 … Red 1 of a new run while already holding waits (the next lot comes on its red 4)."""
    if lots >= cfg.max_ladder_buys:
        return "max", 0.0
    every = max(1, int(cfg.reds_per_lot))
    if streak <= every or (streak - 1) % every != 0:
        return "wait", 0.0
    size = ladder_lot_size(base, lots + 1, cfg.ladder_size_mode)
    return _fit_cap(held_sol, size, cfg)



def ladder_decision_down(reds_since: int, lots: int, held_sol: float, base: float, cfg: "Config",
                         price: Optional[float] = None, last_px: Optional[float] = None) -> Tuple[str, float]:
    """Downside ladder: one more lot after reds_per_lot reds since the last lot (greens do not reset),
    and only while the price is below the last lot's buy price. -> ("add", size) or (wait|above|max|cap, 0)."""
    if lots >= cfg.max_ladder_buys:
        return "max", 0.0
    if reds_since < max(1, int(cfg.reds_per_lot)):
        return "wait", 0.0
    if price is not None and last_px and price >= last_px:
        return "above", 0.0
    size = ladder_lot_size(base, lots + 1, cfg.ladder_size_mode)
    return _fit_cap(held_sol, size, cfg)
