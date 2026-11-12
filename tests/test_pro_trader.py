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
    assert a["targetPct"] == 25.0 and a["stopPct"] == -5.0 and a["rr"] == 5.0
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
    assert P.exit_reason([_c(0, 1.0, 1.26, h=1.26)], 0, 1.0, 5).startswith("target")
    assert P.exit_reason(cs, 0, 1.0, 61).startswith("time stop")
    assert P.exit_reason(cs, 0, 1.0, 5) == ""


def test_holding_read_sells_on_target():
    cs = _flush()
    cs.append(_c(41, cs[-1]["c"], cs[-1]["c"] * 1.3, v=20))
    a = P.analyze(cs, holding={"entry": cs[-2]["c"], "held": 3})
    assert a["call"] == "SELL" and "target" in a["why"] and a["holding"]


def test_replay_marks_the_buy():
    cs = _flush()
    r = P.replay(cs, P.indicators(cs))
    assert r["marks"] and r["marks"][0]["side"] == "buy" and r["open"]


def test_engine_pro_exit_uses_fees():
    assert M.Mayhem2._pro_exit(23.5, 5).startswith("Pro Trader target")
    assert M.Mayhem2._pro_exit(-7.2, 5).startswith("Pro Trader stop")
    assert M.Mayhem2._pro_exit(-3.0, 5) == ""
    assert "time stop" in M.Mayhem2._pro_exit(1.0, 61)


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
