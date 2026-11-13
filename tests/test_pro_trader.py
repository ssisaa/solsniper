"""Pro Trader: VWAP dip with volume -> BUY; target +25% / stop -5% / 60s time stop; Mayhem2 strategy pro_trader."""

import time

from src.signal import pro_trader as P
from src.signal import mayhem2 as M


def _c(t, o, c, v=1.0, h=None, l=None):
    return {"t": t * 1000, "o": o, "c": c, "h": h if h is not None else max(o, c), "l": l if l is not None else min(o, c), "v": v}


def _flush():
    """30 calm candles near 1.0 (heavy volume sets VWAP there), then a fall to 0.80 and a green bounce on volume."""
    cs = [_c(i, 1.0, 1.0 + (0.002 if i % 2 else -0.002), v=10) for i in range(30)]
    px = 1.0
    for i in range(30, 40):
        nx = px * 0.977
        cs.append(_c(i, px, nx, v=1))
        px = nx
    cs.append(_c(40, px, px * 1.01, v=150))     # green, volume surge
    return cs


def test_dip_with_volume_is_a_buy():
    a = P.analyze(_flush())
    assert a["ok"] and a["call"] == "BUY", (a["why"], a["checks"])
    assert all(x["ok"] for x in a["checks"])
    assert a["targetPct"] == 40.0 and a["stopPct"] == -5.0 and a["rr"] == 8.0 and a["timeStopS"] == 120.0
    assert a["target"] > a["entry"] > a["stop"]


def test_calm_uptrend_is_not_a_buy():
    cs = [_c(i, 1 + i * 0.001, 1 + (i + 1) * 0.001, v=5) for i in range(60)]
    a = P.analyze(cs)
    assert a["call"] in ("WAIT", "AVOID") and not all(x["ok"] for x in a["checks"])


def test_too_few_candles_waits():
    assert P.analyze(_flush()[:10])["call"] == "WAIT"


def test_exits_stop_target_time():
    cs = [_c(0, 1.0, 1.0)]
    assert P.exit_reason([_c(0, 1.0, 0.94, l=0.94)], 0, 1.0, 5).startswith("stop")
    assert P.exit_reason([_c(0, 1.0, 1.41, h=1.41)], 0, 1.0, 5).startswith("target")
    assert P.exit_reason([_c(0, 1.0, 1.26, h=1.26)], 0, 1.0, 5) == ""          # +26% no longer sells: let it run
    assert P.exit_reason(cs, 0, 1.0, 121).startswith("time stop")
    assert P.exit_reason(cs, 0, 1.0, 61) == ""
    assert P.exit_reason(cs, 0, 1.0, 5) == ""


def test_holding_read_sells_on_target():
    cs = _flush()
    cs.append(_c(41, cs[-1]["c"], cs[-1]["c"] * 1.45, v=20))
    a = P.analyze(cs, holding={"entry": cs[-2]["c"], "held": 3})
    assert a["call"] == "SELL" and "target" in a["why"] and a["holding"]


def test_replay_marks_the_buy():
    cs = _flush()
    r = P.replay(cs, P.indicators(cs))
    assert r["marks"] and r["marks"][0]["side"] == "buy" and r["open"]


def test_engine_pro_exit_uses_fees():
    assert M.Mayhem2._pro_exit(38.5, 5).startswith("Pro Trader target")
    assert M.Mayhem2._pro_exit(23.5, 5) == ""
    assert M.Mayhem2._pro_exit(-7.2, 5).startswith("Pro Trader stop")
    assert M.Mayhem2._pro_exit(-3.0, 5) == ""
    assert "time stop" in M.Mayhem2._pro_exit(1.0, 121)


def test_pro_context_flow_counts_last_15s():
    e = M.Mayhem2()
    e.pin = {"mint": "X", "ts": time.time()}
    now = time.time()
    for side, age in (("buy", 1), ("buy", 2), ("sell", 3), ("buy", 30)):
        e.signals.appendleft({"side": side, "seen": now - age})
    assert e.pro_context("X")["flow"] == {"buy": 2, "sell": 1}


def test_strategy_listed_and_ignores_trade_signals(monkeypatch):
    from src.config.runtime import RuntimeConfig
    c = RuntimeConfig()
    c.update_from({"mayhem2_strategy": "pro_trader"})
    assert c.mayhem2_strategy == "pro_trader" and "pro_trader" in M.SHADOWS
    e = M.Mayhem2()
    e.pin = {"mint": "X", "ts": time.time()}
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "pro_trader")
    assert e._on_signal({"side": "buy", "seen": time.time(), "sol": 1}).startswith("ignored")


# ---------- Candles rule "pro" ----------

def _cm_engine(tmp_path, monkeypatch):
    from src.signal import candle_momo as CM
    from src.signal import candle_core as C
    from src.signal.candle_store import Store
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "p.sqlite"), str(tmp_path / "p.jsonl"))
    e.cfg.rule = "pro"
    routed = []
    monkeypatch.setattr(e, "_route", lambda sig: routed.append(sig) or "intent")
    f = CM.Feed("MINT", 400)
    f.status = "OK"
    f.using = "pump_rest"
    rows = [C.Candle(t=c["t"], open=c["o"], high=c["h"], low=c["l"], close=c["c"], volume=c["v"], final=True)
            for c in _flush()]
    f.books["pump_rest"].merge(rows[:-1])
    e.feeds["MINT"] = f
    return e, f, rows, routed


def test_candles_pro_rule_buys_on_the_call(tmp_path, monkeypatch):
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e._evaluate(f, time.time())                       # first read: history, no signal
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert len(routed) == 1 and routed[0]["side"] == "BUY" and routed[0]["pro"] and routed[0]["rule"] == "pro"
    assert e.pro_px["MINT"] == routed[0]["pro_px"] > 0


def test_candles_pro_rule_sells_at_target(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    f.books["pump_rest"].merge(rows[-1:])
    entry = rows[-1].close
    e.positions["MINT"] = {"mint": "MINT", "entry_ts": time.time() - 5, "entry_sol": 0.005, "lots": [{"n": 1}]}
    e.pro_px["MINT"] = entry
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge([C.Candle(t=rows[-1].t + 1000, open=entry, high=entry * 1.45, low=entry,
                                         close=entry * 1.45, volume=5, final=True)])
    e._evaluate(f, time.time())
    assert routed and routed[-1]["side"] == "SELL" and "target" in routed[-1]["pro_why"]


def test_candles_pro_hint_and_rule_choice():
    from src.signal import candle_core as C
    from src.signal import candle_momo as CM
    assert "pro" in C.RULES
    e = CM.CandleMomo()
    e.cfg.rule = "pro"
    h = e._hint("X")
    assert h.get("pro") and "VWAP" in h["entry"] and h["minProfit"] == 40.0
