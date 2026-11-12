"""Botpredict: the bot's own prediction for a meme coin's next minute, learned from data.

What it predicts: the chance that, buying at this 1s candle's close, the price reaches +25% before it falls 5%,
within 60 seconds. A logistic model on 22 chart features (returns over 1-30 candles, distance to VWAP / EMA 9 /
EMA 21, RSI, volume surge, distance from the 30s high / low, candle body and wicks, red / green streaks,
volatility, share of volume on green candles).

How it was made (2026-11-13): trained on 43 pump coins, then tested on 52 different coins recorded later
(never seen in training). Buying only the model's top 2% most confident moments: training coins +1.8% per
trade, unseen coins +1.3% per trade (24 trades), and +3.6% per trade on the unseen coins even when entering one
candle late; ~30-40% of trades win, the target is 5x the stop. Final weights are fit on all 95 coins
(19,604 moments, 4.7% of them hit +25% first). Small samples: an edge, not a promise.
For comparison, the Pro Trader dip setup lost on the same unseen coins (-1.7% per trade).

Pure math, no I/O: the engine, the page and the tests all see the same prediction.
"""

import math
from typing import Any, Dict, List, Optional

TARGET_PCT = 25.0
STOP_PCT = -5.0
TIME_STOP_S = 60.0
FEE_PCT = 2.0
MIN_CANDLES = 31

MODEL = {
    "mu": [-0.000869, -0.002429, -0.00381, -0.006905, -0.014198, -0.060685, -0.005816, -0.013449, -0.007633, 0.488149, 0.683688, 0.515303, -0.134789, 0.132653, -0.000869, 0.019383, 0.024917, 0.180898, 0.195436, 0.023656, 0.267441, 0.479988],
    "sd": [0.051584, 0.091384, 0.116942, 0.15739, 0.283716, 0.358341, 0.071436, 0.132059, 0.075634, 0.174418, 0.485934, 0.499766, 0.241597, 0.208043, 0.051584, 0.098918, 0.115353, 0.257835, 0.267922, 0.036494, 0.324335, 0.289616],
    "w": [-0.016727, 0.006342, -0.105316, -0.182635, 0.098756, 0.213225, 0.132238, 0.037152, -0.060029, 0.130844, -0.047482, 0.0134, -0.16254, 0.053702, -0.016727, 0.116854, 0.120712, 0.057636, 0.162397, 0.192244, 0.155523, -0.155123],
    "b": -3.166801,
    "thr": 0.145906,      # top 2% of the model's scores
    "base": 0.0474,      # how often +25% came first at a random moment
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
        call, text = "BUY", f"{p * 100:.0f}% chance of +{TARGET_PCT:.0f}% before {STOP_PCT:.0f}% (normal {MODEL['base'] * 100:.0f}%)"
    else:
        call, text = "WAIT", f"{p * 100:.1f}% chance — buys at {thr * 100:.0f}%+"
    wins = sum(1 for x in trades if x > 0)
    return {
        "ok": True, "call": call, "why": text, "prob": round(p, 4), "thr": thr, "base": MODEL["base"],
        "edge": round(p / MODEL["base"], 2) if MODEL["base"] else None,
        "pros": pros, "cons": cons, "px": px, "entry": entry,
        "target": entry * (1 + TARGET_PCT / 100), "stop": entry * (1 + STOP_PCT / 100),
        "targetPct": TARGET_PCT, "stopPct": STOP_PCT, "timeStopS": TIME_STOP_S, "holding": bool(holding),
        "pnlPct": round((px / entry - 1) * 100, 2) if holding else None,
        "probs": [{"t": cs[k]["t"], "p": round(v, 4)} for k, v in sorted(probs.items())][-180:],
        "replay": {"marks": marks, "trades": len(trades), "wins": wins, "sumPct": round(sum(trades), 1),
                   "avgPct": round(sum(trades) / len(trades), 1) if trades else None, "open": pos is not None},
    }
