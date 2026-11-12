"""Pro Trader: a day-trader's read of a 1s candle chart as one call (BUY / SELL / HOLD / WAIT / AVOID) with an
entry, a target, a stop, a checklist and the reasons, plus the indicator lines the Mayhem2 chart draws.

The setup (chosen by testing, 2026-11-13, 43 pump coins / 10,412 1s candles, split in two halves):
  "VWAP dip with volume" (buy the flush, sell the snap-back):
    price at least 10% under VWAP (the average price everyone paid)  - it is cheap vs the crowd
    a green candle                                                   - buyers took it back this second
    RSI 14 under 45                                                  - sold off, not stretched up
    volume of the last 5 candles >= 1.5x the 20 before               - real money is stepping in
  Exit: target +25%, stop -5%, or 60s time stop. Reward:risk 5:1, so it can lose more often than it wins.
  Tested: 83 trades, 44% wins, +2.3% per trade after ~2% fees even entering one candle late; both halves of the
  data were positive. Plain trend-following (EMA 9 > 21, over VWAP, RSI 50-78) lost on every setting tried,
  as momentum did before (memory: Mayhem coin economics). Small sample: no promise, losses will come.
  OUT OF SAMPLE (2026-11-13, 52 coins recorded later): 121 trades, 22% wins, -1.7% per trade — the edge did
  not hold. Botpredict (src/signal/botpredict.py) held up on those same coins; prefer it.

The trend / momentum read (EMA 9 / 21, VWAP, RSI, MACD, volume, on-chain flow) is shown as context and as the
confidence, not as the entry trigger.

Everything here is pure math on the candles (no I/O), so the engine, the page and the tests all see the same call.
"""

from typing import Any, Dict, List, Optional

EMA_FAST = 9
EMA_SLOW = 21
RSI_N = 14
ATR_N = 14
SWING_N = 30
MIN_CANDLES = 25
DIP_PCT = 10.0          # close at least this far under VWAP
DIP_RSI = 45.0          # RSI under this
VOL_X = 1.5             # last 5 candles' volume vs the 20 before
TARGET_PCT = 25.0
STOP_PCT = -5.0
TIME_STOP_S = 60.0
FEE_PCT = 2.0           # round trip, for the replay


def _ema(xs: List[float], n: int) -> List[float]:
    out: List[float] = []
    k = 2.0 / (n + 1)
    for i, x in enumerate(xs):
        out.append(x if i == 0 else out[-1] + k * (x - out[-1]))
    return out


def _rsi(xs: List[float], n: int = RSI_N) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(xs)
    if len(xs) <= n:
        return out
    gain = loss = 0.0
    for i in range(1, n + 1):
        d = xs[i] - xs[i - 1]
        gain += max(d, 0.0)
        loss += max(-d, 0.0)
    gain /= n
    loss /= n
    for i in range(n, len(xs)):
        if i > n:
            d = xs[i] - xs[i - 1]
            gain = (gain * (n - 1) + max(d, 0.0)) / n
            loss = (loss * (n - 1) + max(-d, 0.0)) / n
        out[i] = 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)
    return out


def _atr(cs: List[Dict[str, float]], n: int = ATR_N) -> List[float]:
    out: List[float] = []
    for i, c in enumerate(cs):
        prev = cs[i - 1]["c"] if i else c["o"]
        tr = max(c["h"] - c["l"], abs(c["h"] - prev), abs(c["l"] - prev))
        out.append(tr if i == 0 else (out[-1] * (n - 1) + tr) / n)
    return out


def _vwap(cs: List[Dict[str, float]]) -> List[float]:
    out, pv, vv = [], 0.0, 0.0
    for c in cs:
        v = max(float(c.get("v") or 0.0), 0.0)
        tp = (c["h"] + c["l"] + c["c"]) / 3.0
        pv += tp * v
        vv += v
        out.append(pv / vv if vv > 0 else tp)
    return out


def indicators(cs: List[Dict[str, float]]) -> Dict[str, List[Any]]:
    closes = [c["c"] for c in cs]
    e12, e26 = _ema(closes, 12), _ema(closes, 26)
    macd = [a - b for a, b in zip(e12, e26)]
    sig = _ema(macd, 9)
    return {
        "ema9": _ema(closes, EMA_FAST), "ema21": _ema(closes, EMA_SLOW), "vwap": _vwap(cs),
        "rsi": _rsi(closes), "atr": _atr(cs), "hist": [a - b for a, b in zip(macd, sig)],
    }


def _read(cs: List[Dict[str, float]], ind: Dict[str, List[Any]], i: int,
          flow: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """A flat trader's read at candle i: the setup checklist (the entry trigger) and the trend context."""
    c = cs[i]
    px = c["c"]
    e9, e21, vw = ind["ema9"][i], ind["ema21"][i], ind["vwap"][i]
    rsi = ind["rsi"][i]
    atr = ind["atr"][i] or px * 0.01
    hist, hist_prev = ind["hist"][i], ind["hist"][i - 1] if i else 0.0
    hi = max(x["h"] for x in cs[max(0, i - SWING_N + 1):i + 1])
    v5 = sum(float(x.get("v") or 0) for x in cs[max(0, i - 4):i + 1]) / 5.0
    prev = cs[max(0, i - 24):max(0, i - 4)]
    v20 = sum(float(x.get("v") or 0) for x in prev) / max(1, len(prev))
    vx = v5 / v20 if v20 > 0 else 0.0
    under = (1 - px / vw) * 100 if vw > 0 else 0.0
    green = px > c["o"]
    checks = [
        {"ok": under >= DIP_PCT, "t": f"{under:.1f}% under VWAP (need {DIP_PCT:.0f}%)" if under > 0 else f"{-under:.1f}% over VWAP (need {DIP_PCT:.0f}% under)"},
        {"ok": green, "t": "green candle — buyers took the second" if green else "red candle — still falling"},
        {"ok": rsi is not None and rsi < DIP_RSI, "t": f"RSI {rsi:.0f} (need under {DIP_RSI:.0f})" if rsi is not None else "RSI warming up"},
        {"ok": vx >= VOL_X, "t": f"volume {vx:.1f}x the 20 before (need {VOL_X}x)"},
    ]
    up, dn = [], []
    score = 0
    if e9 > e21:
        score += 1
        up.append("EMA 9 over EMA 21 (uptrend)")
    else:
        score -= 1
        dn.append("EMA 9 under EMA 21 (downtrend)")
    if px > vw:
        score += 1
        up.append("price over VWAP")
    else:
        dn.append("price under VWAP")
    if rsi is not None:
        if rsi > 80:
            dn.append(f"RSI {rsi:.0f} overbought")
        elif rsi < 30:
            up.append(f"RSI {rsi:.0f} oversold (bounce zone)")
    if hist > 0 and hist > hist_prev:
        score += 1
        up.append("MACD momentum turning up")
    elif hist < 0 and hist < hist_prev:
        score -= 1
        dn.append("MACD momentum falling")
    if flow:
        b, s_ = int(flow.get("buy") or 0), int(flow.get("sell") or 0)
        if b + s_ >= 4 and b >= 1.5 * s_:
            score += 1
            up.append(f"flow {b} buys / {s_} sells (15s)")
        elif b + s_ >= 4 and s_ >= 1.5 * b:
            score -= 1
            dn.append(f"flow {s_} sells / {b} buys (15s)")
    off_hi = (1 - px / hi) * 100 if hi > 0 else 0.0
    n_ok = sum(1 for x in checks if x["ok"])
    buy = n_ok == len(checks)
    return {
        "buy": buy, "checks": checks, "nOk": n_ok, "score": score, "up": up, "dn": dn, "px": px, "rsi": rsi,
        "atr": atr, "ema9": e9, "ema21": e21, "vwap": vw, "offHigh": round(off_hi, 1), "underVwap": round(under, 1),
        "volX": round(vx, 2), "stopPct": STOP_PCT, "targetPct": TARGET_PCT,
        "stop": px * (1 + STOP_PCT / 100), "target": px * (1 + TARGET_PCT / 100),
    }


def exit_reason(cs: List[Dict[str, float]], i: int, entry: float, held_s: float,
                stop_pct: float = STOP_PCT, target_pct: float = TARGET_PCT) -> str:
    """Why a long held since `entry` closes at candle i (empty: keep it)."""
    c = cs[i]
    if entry <= 0:
        return ""
    if (c["l"] / entry - 1) * 100 <= stop_pct:
        return f"stop {stop_pct:+.0f}% hit"
    if (c["h"] / entry - 1) * 100 >= target_pct:
        return f"target {target_pct:+.0f}% hit"
    if held_s >= TIME_STOP_S:
        return f"time stop {TIME_STOP_S:.0f}s ({(c['c'] / entry - 1) * 100:+.1f}%)"
    return ""


def replay(cs: List[Dict[str, float]], ind: Dict[str, List[Any]], fee_pct: float = FEE_PCT) -> Dict[str, Any]:
    """The same rules walked over the chart: where it would have bought and sold (the chart's BUY / % tags)."""
    marks, trades = [], []
    pos = None
    for i in range(MIN_CANDLES, len(cs)):
        c = cs[i]
        if pos is None:
            if _read(cs, ind, i)["buy"]:
                pos = {"i": i, "entry": c["c"]}
                marks.append({"t": c["t"], "side": "buy", "px": c["c"], "label": "BUY"})
            continue
        why = exit_reason(cs, i, pos["entry"], (c["t"] - cs[pos["i"]]["t"]) / 1000)
        if not why:
            continue
        if why.startswith("stop"):
            px = min(c["o"], pos["entry"] * (1 + STOP_PCT / 100))
        elif why.startswith("target"):
            px = pos["entry"] * (1 + TARGET_PCT / 100)
        else:
            px = c["c"]
        pct = (px / pos["entry"] - 1) * 100 - fee_pct
        trades.append(pct)
        marks.append({"t": c["t"], "side": "sell", "px": px, "label": f"{pct:+.0f}%", "why": why})
        pos = None
    wins = sum(1 for x in trades if x > 0)
    return {"marks": marks, "trades": len(trades), "wins": wins,
            "sumPct": round(sum(trades), 1), "avgPct": round(sum(trades) / len(trades), 1) if trades else None,
            "open": pos is not None}


def analyze(cs: List[Dict[str, float]], flow: Optional[Dict[str, int]] = None,
            holding: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The live call on the last candle. `holding` = {entry, held} (entry in the chart's price) when a long is open."""
    cs = [c for c in cs or [] if c.get("c") and c.get("o")]
    if len(cs) < MIN_CANDLES:
        return {"ok": False, "call": "WAIT", "why": f"reading the chart ({len(cs)}/{MIN_CANDLES} candles)",
                "confidence": 0, "up": [], "dn": [], "checks": [], "lines": None, "replay": None}
    ind = indicators(cs)
    i = len(cs) - 1
    r = _read(cs, ind, i, flow)
    if holding:
        why = exit_reason(cs, i, float(holding.get("entry") or 0), float(holding.get("held") or 0))
        call = "SELL" if why else "HOLD"
        text = why or f"in the trade — target {TARGET_PCT:+.0f}%, stop {STOP_PCT:+.0f}%, {TIME_STOP_S:.0f}s max"
    elif r["buy"]:
        call, text = "BUY", "VWAP dip with volume — buy the flush"
    elif r["score"] <= -2 and r["nOk"] <= 1:
        call, text = "AVOID", "sellers in control and no dip setup — stay out"
    else:
        call, text = "WAIT", f"setup {r['nOk']}/{len(r['checks'])} — wait for all four"
    conf = max(0, min(100, int(r["nOk"] * 20 + 10 + r["score"] * 5)))
    entry = float(holding.get("entry") or 0) if holding else 0.0
    if entry <= 0:
        entry = r["px"]
    pnl = round((r["px"] / entry - 1) * 100, 2) if holding else None
    keep = 180
    lines = {k: [round(v, 14) for v in ind[k][-keep:]] for k in ("ema9", "ema21", "vwap")}
    lines["rsi"] = [None if v is None else round(v, 1) for v in ind["rsi"][-keep:]]
    lines["t"] = [c["t"] for c in cs[-keep:]]
    return {
        "ok": True, "call": call, "why": text, "confidence": conf, "score": r["score"],
        "px": r["px"], "entry": entry, "stop": entry * (1 + STOP_PCT / 100), "target": entry * (1 + TARGET_PCT / 100),
        "holding": bool(holding), "pnlPct": pnl, "held": round(float(holding.get("held") or 0), 1) if holding else None,
        "stopPct": STOP_PCT, "targetPct": TARGET_PCT, "timeStopS": TIME_STOP_S, "rr": round(TARGET_PCT / -STOP_PCT, 1),
        "rsi": None if r["rsi"] is None else round(r["rsi"], 1), "atrPct": round(r["atr"] / r["px"] * 100, 2) if r["px"] else None,
        "ema9": r["ema9"], "ema21": r["ema21"], "vwap": r["vwap"], "offHigh": r["offHigh"],
        "underVwap": r["underVwap"], "volX": r["volX"], "checks": r["checks"],
        "up": r["up"], "dn": r["dn"], "flow": flow or None,
        "lines": lines, "replay": replay(cs, ind),
    }
