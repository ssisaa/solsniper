"""Botpredict: the bot's learned prediction (+25% before -5% in 60s); Mayhem2 strategy botpredict."""

import time

from src.signal import botpredict as BP
from src.signal import mayhem2 as M


def _c(t, o, c, v=1.0, h=None, l=None):
    return {"t": t * 1000, "o": o, "c": c, "h": h if h is not None else max(o, c), "l": l if l is not None else min(o, c), "v": v}


def _chart(n=60):
    cs, px = [], 1.0
    for i in range(n):
        nx = px * (1.03 if i % 3 == 0 else 0.985)
        cs.append(_c(i, px, nx, v=1 + i % 5))
        px = nx
    return cs


def test_model_shape_matches_features():
    assert len(BP.MODEL["w"]) == len(BP.NAMES) == len(BP.MODEL["mu"]) == len(BP.MODEL["sd"]) == len(BP.LABELS)
    assert 0 < BP.MODEL["thr"] < 1 and 0 < BP.MODEL["base"] < BP.MODEL["thr"]


def test_needs_history():
    a = BP.analyze(_chart(20))
    assert not a["ok"] and a["call"] == "WAIT"


def test_prediction_fields_and_reasons():
    a = BP.analyze(_chart())
    assert a["ok"] and 0 <= a["prob"] <= 1 and a["call"] in ("BUY", "WAIT")
    assert (a["call"] == "BUY") == (a["prob"] >= a["thr"])
    assert a["target"] > a["entry"] > a["stop"] and a["replay"] is not None
    assert len(a["probs"]) > 0 and isinstance(a["pros"], list) and isinstance(a["cons"], list)


def test_buy_when_score_over_threshold(monkeypatch):
    monkeypatch.setitem(BP.MODEL, "thr", 0.0)
    assert BP.analyze(_chart())["call"] == "BUY"


def test_holding_sells_at_stop():
    cs = _chart()
    entry = cs[-1]["c"] * 1.2
    a = BP.analyze(cs, holding={"entry": entry, "held": 5})
    assert a["call"] == "SELL" and "stop" in a["why"]


def test_strategy_listed_and_shares_exits():
    from src.config.runtime import RuntimeConfig
    c = RuntimeConfig()
    c.update_from({"mayhem2_strategy": "botpredict"})
    assert c.mayhem2_strategy == "botpredict" and "botpredict" in M.SHADOWS
    assert BP.TARGET_PCT == 25.0 and BP.STOP_PCT == -5.0 and BP.TIME_STOP_S == 60.0
    assert "botpredict" in M.CHART_STRATS


def test_holding_context_for_botpredict(monkeypatch):
    e = M.Mayhem2()
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "botpredict")
    e.shadow["botpredict"]["pos"] = {"mint": "X", "pro_px": 1.5, "at": time.time() - 3}
    h = e.pro_context("X")["holding"]
    assert h["entry"] == 1.5 and 2 < h["held"] < 5
