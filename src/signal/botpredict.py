"""Botpredict: the bot's own prediction for a meme coin's next minute, learned from data.

What it predicts: the chance that, buying at this 1s candle's close, the price reaches +25% before it falls 8%,
within 60 seconds. A logistic model on 22 chart features (returns over 1-30 candles, distance to VWAP / EMA 9 /
EMA 21, RSI, volume surge, distance from the 30s high / low, candle body and wicks, red / green streaks,
volatility, share of volume on green candles). It buys only its top 2% most confident moments.

History (2026-11-13/14):
- First built with a -5% stop: trained on 43 coins, +1.3%/trade on 52 later unseen coins; then on 76 more fresh
  unseen coins +4.3%/trade (197 trades, 39% won), +2.3% entering one candle late.
- The user asked for a wider stop so 1s wiggles do not hit it. Tested (retrained per stop, same 76 unseen coins):
  -8% +3.5% (late +1.6%), -10% +2.4%, -12% +1.8%, -15% +1.2%, -20% +1.5%, 3x ATR +1.7%, close-below-stop -2.6%.
  Every wider stop did worse: a -5% dip right after the entry usually is the turn, not a wiggle. The user chose to
  widen anyway; -8% is the least costly wider stop. Weights below are fit for -8% on all 171 coins
  (36,847 moments, 5.7% of them hit +25% first). Small samples: an edge, not a promise.

- Exits (2026-11-14, 76 fresh coins): the price after a BUY call usually dips first (bottom ~3rd candle) and then
  runs (median +3.3% after 10 candles, +41% after 30; 73% reach +25% within 60). Selling at +40% instead of +25%
  (same -8% stop, 60s): +1.2% / +5.6% / +2.8% per trade on training coins / fresh coins / fresh entering late, vs
  +0.1% / +3.6% / +1.7%. The model still predicts +25% first (PRED_PCT); only the sell target moved.

Pure math, no I/O: the engine, the page and the tests all see the same prediction.
"""

import math
from typing import Any, Dict, List, Optional

PRED_PCT = 25.0        # what the model predicts: +25% before the stop within 60s (its training label)
TARGET_PCT = 40.0      # where it sells (2026-11-14): after a BUY the move usually keeps going; +40% let it run
STOP_PCT = -8.0       # widened from -5% at the user's request (2026-11-14); -5% tested better, see the docstring
TIME_STOP_S = 60.0
FEE_PCT = 2.0
MIN_CANDLES = 31

MODEL = {
    "mu": [-0.000409, -0.00108, -0.001499, -0.002374, -0.002361, -0.043009, -0.004615, -0.009977, -0.005361, 0.489627, 0.684204, 0.509974, -0.141108, 0.148682, -0.000409, 0.018815, 0.023297, 0.186003, 0.193996, 0.025102, 0.289791, 0.481307],
    "sd": [0.056367, 0.099685, 0.127808, 0.179045, 0.310874, 0.331385, 0.079871, 0.137293, 0.073247, 0.176144, 0.485411, 0.499901, 0.234437, 0.243424, 0.056367, 0.097099, 0.110887, 0.263133, 0.266176, 0.036871, 0.337098, 0.29111],
    "w": [-0.007438, -0.046834, -0.092237, -0.199649, -0.011041, 0.105814, 0.178685, 0.100518, -0.006434, 0.058437, 0.015501, -0.003084, -0.102818, 0.092764, -0.007438, 0.121897, 0.125249, 0.001148, 0.106625, 0.288984, 0.138492, -0.086985],
    "b": -2.982798,
    "thr": 0.2061,      # top 2% of the model's scores
    "base": 0.057,      # how often +25% came first at a random moment
}

LABELS = {
    "r1": ("last candle up", "last candle down"),
    "r3": ("up over 3s", "down over 3s"),
    "r5": ("up over 5s", "down over 5s"),
    "r10": ("up over 10s", "dipped over 10s"),
    "r30": ("up over 30s", "down over 30s"),
    "vwap_d": ("above VWAP", "below VWAP"),
    "ema9_d": ("above EMA 9", "below EMA 9"),
    "ema21_d": ("above EMA 21", "below EMA 21"),
    "ema_x": ("EMA 9 over 21", "EMA 9 under 21"),
    "rsi": ("RSI high", "RSI low"),
    "volx": ("volume surging", "volume quiet"),
    "vol_up": ("green candle", "red candle"),
    "off_hi": ("near the 30s high", "far under the 30s high"),
    "off_lo": ("well off the 30s low", "at the 30s low"),
    "body": ("strong green body", "red body"),
    "uwick": ("long upper wick", "no upper wick"),
    "lwick": ("long lower wick (buyers)", "no lower wick"),
    "red_streak": ("reds in a row", "no red streak"),
    "green_streak": ("greens in a row", "no green streak"),
    "atr": ("volatile (big 1s ranges)", "calm (small ranges)"),
    "range30": ("wide 30s range", "tight 30s range"),
    "buy_share10": ("volume on greens", "volume on reds"),
}



NAMES = ["r1","r3","r5","r10","r30","vwap_d","ema9_d","ema21_d","ema_x","rsi","volx","vol_up","off_hi","off_lo","body","uwick","lwick","red_streak","green_streak","atr","range30","buy_share10"]

def ema(xs,n):
    out=[];k=2/(n+1)
    for i,x in enumerate(xs): out.append(x if i==0 else out[-1]+k*(x-out[-1]))
    return out

def rsi(xs,n=14):
    out=[None]*len(xs)
    if len(xs)<=n: return out
    g=l=0.0
    for i in range(1,n+1):
        d=xs[i]-xs[i-1]; g+=max(d,0); l+=max(-d,0)
    g/=n;l/=n
    for i in range(n,len(xs)):
        if i>n:
            d=xs[i]-xs[i-1]; g=(g*(n-1)+max(d,0))/n; l=(l*(n-1)+max(-d,0))/n
        out[i]=100.0 if l==0 else 100-100/(1+g/l)
    return out

def features(cs):
    """Feature rows for every candle index >= 30 (list of (i, vec))."""
    c=[x["c"] for x in cs]; v=[max(0.0,float(x.get("v") or 0)) for x in cs]
    e9=ema(c,9); e21=ema(c,21); r=rsi(c)
    pv=vv=0.0; vw=[]
    for x in cs:
        tp=(x["h"]+x["l"]+x["c"])/3; pv+=tp*max(0,x.get("v") or 0); vv+=max(0,x.get("v") or 0); vw.append(pv/vv if vv>0 else tp)
    atr=[];
    for i,x in enumerate(cs):
        p=cs[i-1]["c"] if i else x["o"]; tr=max(x["h"]-x["l"],abs(x["h"]-p),abs(x["l"]-p))/max(p,1e-18)
        atr.append(tr if i==0 else (atr[-1]*13+tr)/14)
    out=[]
    lg=lambda a,b: math.log(a/b) if a>0 and b>0 else 0.0
    for i in range(30,len(cs)):
        x=cs[i]
        hi=max(y["h"] for y in cs[i-29:i+1]); lo=min(y["l"] for y in cs[i-29:i+1])
        v5=sum(v[i-4:i+1])/5; v20=sum(v[i-24:i-4])/20
        rs=gs=0
        j=i
        while j>0 and cs[j]["c"]<cs[j]["o"]: rs+=1; j-=1
        j=i
        while j>0 and cs[j]["c"]>cs[j]["o"]: gs+=1; j-=1
        rng=max(x["h"]-x["l"],1e-18)
        upv=sum(v[k] for k in range(i-9,i+1) if cs[k]["c"]>=cs[k]["o"]); tv=sum(v[i-9:i+1])
        f=[lg(c[i],c[i-1]),lg(c[i],c[i-3]),lg(c[i],c[i-5]),lg(c[i],c[i-10]),lg(c[i],c[i-30]),
           lg(c[i],vw[i]),lg(c[i],e9[i]),lg(c[i],e21[i]),lg(e9[i],e21[i]),
           (r[i] if r[i] is not None else 50)/100,
           math.log1p(v5/v20) if v20>0 else 0.0, 1.0 if x["c"]>=x["o"] else 0.0,
           lg(c[i],hi), lg(c[i],lo), lg(x["c"],x["o"]),
           (x["h"]-max(x["o"],x["c"]))/rng, (min(x["o"],x["c"])-x["l"])/rng,
           min(rs,6)/6, min(gs,6)/6, atr[i], lg(hi,lo), upv/tv if tv>0 else 0.5]
        out.append((i,f))
    return out

def label(cs,i,tp,sl,ts):
    """+1 if, entering at close i, high reaches +tp% before low hits -sl% within ts seconds. Also the realized %."""
    e=cs[i]["c"]; t0=cs[i]["t"]
    for k in range(i+1,len(cs)):
        y=cs[k]
        if (y["t"]-t0)/1000>ts: return 0, (cs[k-1]["c"]/e-1)*100
        if (y["l"]/e-1)*100<=-sl: return 0, (min(y["o"],e*(1-sl/100))/e-1)*100
        if (y["h"]/e-1)*100>=tp: return 1, tp
    return None, None


def _score(f: List[float]) -> (float, List[float]):
    mu, sd, w = MODEL["mu"], MODEL["sd"], MODEL["w"]
    contrib = [w[k] * (f[k] - mu[k]) / sd[k] for k in range(len(w))]
    z = sum(contrib) + MODEL["b"]
    z = max(-30.0, min(30.0, z))
    return 1.0 / (1.0 + math.exp(-z)), contrib


def _exit(cs, i, entry, held):
    y = cs[i]
    if (y["l"] / entry - 1) * 100 <= STOP_PCT:
        return f"stop {STOP_PCT:+.0f}% hit"
    if (y["h"] / entry - 1) * 100 >= TARGET_PCT:
        return f"target {TARGET_PCT:+.0f}% hit"
    if held >= TIME_STOP_S:
        return f"time stop {TIME_STOP_S:.0f}s ({(y['c'] / entry - 1) * 100:+.1f}%)"
    return ""


def analyze(cs: List[Dict[str, float]], holding: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Prediction on the last candle + the same rule replayed over the chart (BP tags)."""
    cs = [c for c in cs or [] if c.get("c") and c.get("o")]
    if len(cs) < MIN_CANDLES:
        return {"ok": False, "call": "WAIT", "why": f"learning the coin ({len(cs)}/{MIN_CANDLES} candles)",
                "prob": None, "thr": MODEL["thr"], "base": MODEL["base"], "replay": None}
    rows = features(cs)
    probs = {}
    last_contrib = None
    for i, f in rows:
        p, contrib = _score(f)
        probs[i] = p
        if i == len(cs) - 1:
            last_contrib = contrib
    i = len(cs) - 1
    p = probs.get(i, 0.0)
    thr = MODEL["thr"]
    # replay: one trade at a time
    marks, trades, pos = [], [], None
    for k in range(MIN_CANDLES - 1, len(cs)):
        if pos is None:
            if probs.get(k, 0.0) >= thr:
                pos = {"i": k, "entry": cs[k]["c"]}
                marks.append({"t": cs[k]["t"], "side": "buy", "px": cs[k]["c"], "label": "BP"})
            continue
        why = _exit(cs, k, pos["entry"], (cs[k]["t"] - cs[pos["i"]]["t"]) / 1000)
        if why:
            e = pos["entry"]
            px = (min(cs[k]["o"], e * (1 + STOP_PCT / 100)) if why.startswith("stop")
                  else e * (1 + TARGET_PCT / 100) if why.startswith("target") else cs[k]["c"])
            pct = (px / e - 1) * 100 - FEE_PCT
            trades.append(pct)
            marks.append({"t": cs[k]["t"], "side": "sell", "px": px, "label": f"{pct:+.0f}%", "why": why})
            pos = None
    names = list(LABELS)
    order = sorted(range(len(last_contrib or [])), key=lambda k: -abs(last_contrib[k]))
    pros = [LABELS[names[k]][0 if (last_contrib[k] > 0) == (MODEL["w"][k] > 0) else 1]
            for k in order if last_contrib[k] > 0][:3]
    cons = [LABELS[names[k]][0 if (last_contrib[k] > 0) == (MODEL["w"][k] > 0) else 1]
            for k in order if last_contrib[k] < 0][:3]
    px = cs[i]["c"]
    entry = float(holding.get("entry") or 0) if holding else 0.0
    if entry <= 0:
        entry = px
    if holding:
        why = _exit(cs, i, entry, float(holding.get("held") or 0))
        call, text = ("SELL", why) if why else ("HOLD", f"in the trade — target {TARGET_PCT:+.0f}%, stop {STOP_PCT:+.0f}%, {TIME_STOP_S:.0f}s max")
    elif p >= thr:
        call, text = "BUY", f"{p * 100:.0f}% chance of +{PRED_PCT:.0f}% before {STOP_PCT:.0f}% (normal {MODEL['base'] * 100:.0f}%) — sells at +{TARGET_PCT:.0f}%"
    else:
        call, text = "WAIT", f"{p * 100:.1f}% chance — buys at {thr * 100:.0f}%+"
    wins = sum(1 for x in trades if x > 0)
    return {
        "ok": True, "call": call, "why": text, "prob": round(p, 4), "thr": thr, "base": MODEL["base"],
        "edge": round(p / MODEL["base"], 2) if MODEL["base"] else None,
        "pros": pros, "cons": cons, "px": px, "entry": entry,
        "target": entry * (1 + TARGET_PCT / 100), "stop": entry * (1 + STOP_PCT / 100),
        "targetPct": TARGET_PCT, "predPct": PRED_PCT, "stopPct": STOP_PCT, "timeStopS": TIME_STOP_S, "holding": bool(holding),
        "pnlPct": round((px / entry - 1) * 100, 2) if holding else None,
        "probs": [{"t": cs[k]["t"], "p": round(v, 4)} for k, v in sorted(probs.items())][-180:],
        "replay": {"marks": marks, "trades": len(trades), "wins": wins, "sumPct": round(sum(trades), 1),
                   "avgPct": round(sum(trades) / len(trades), 1) if trades else None, "open": pos is not None},
    }
