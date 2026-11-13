"""AutoTrade-Candles with Candles in PAPER: test a source without real SOL."""
import time
from types import SimpleNamespace

from src.signal import candle_momo as CM


def _eng(monkeypatch, src="opencopy", rule="inverse", mode="paper"):
    e = CM.CandleMomo()
    e.cfg.rule, e.cfg.mode, e.cfg.support_enabled = rule, mode, True
    e.cfg.avoid_mayhem = False
    monkeypatch.setattr(e, "support_source", lambda: src)
    pos = SimpleNamespace(mint="Cx" + "y" * 40, status="OPEN", source=src, strategy=src, tokens=1e9,
                          entry_sol=0.005, entry_time=time.time(), symbol="CX", _candle_support=True)
    monkeypatch.setattr(e, "_support_held", lambda s: [pos])
    started = []
    monkeypatch.setattr(e, "_start", lambda sig: started.append(sig) or "intent")
    return e, pos, started


def test_paper_support_plays_lot1_on_paper_and_leaves_the_live_trade(monkeypatch):
    e, pos, started = _eng(monkeypatch)
    e._pick([])
    assert pos.mint in e.feeds
    assert len(started) == 1 and started[0]["mirror"] and started[0]["side"] == "BUY"
    assert pos.mint not in e.positions                  # the live trade is not taken over
    assert pos._candle_support is False                 # the source's own rules sell it
    assert e.supports(pos) is False                     # and are not held back
    e._pick([])
    assert len(started) == 1                            # once per coin


def test_live_support_still_takes_over(monkeypatch):
    e, pos, started = _eng(monkeypatch, mode="live")
    e._pick([])
    assert pos.mint in e.positions and e.positions[pos.mint]["support"] == "opencopy"
    assert not started


def test_paper_support_owns_entries_and_mirror_passes_routing(monkeypatch):
    e, pos, started = _eng(monkeypatch, src="dayscalp")
    assert e.pro_hold_owns_entries("dayscalp") and e.paper_support()
    e.cfg.mode = "live"
    assert not e.pro_hold_owns_entries("dayscalp")     # live + inverse: Dayscalp buys lot 1 itself
    e.cfg.mode = "paper"
    for m in ("Z" * 44, "W" * 44):
        e.feeds[m] = CM.Feed(m, 10)
        e.feeds[m].status = "OK"
    sig = {"id": "x1", "mint": "Z" * 44, "side": "BUY", "mirror": True, "_counted": True, "deltaPct": 0.0}
    assert e._route(sig) == "intent"
    sig2 = {"id": "x2", "mint": "W" * 44, "side": "BUY", "_counted": True, "deltaPct": 0.0}
    assert e._route(sig2) != "intent"                 # a plain candle BUY still waits for the source's lot 1


def test_paper_pro_hold_does_not_need_auto_trade(monkeypatch):
    e, pos, started = _eng(monkeypatch, src="dayscalp", rule="pro_hold")
    from src.config import runtime as R
    monkeypatch.setattr(R, "get_config", lambda: SimpleNamespace(trade_enabled=False))
    e.feeds["Q" * 44] = CM.Feed("Q" * 44, 10)
    e.feeds["Q" * 44].status = "OK"
    sig = {"id": "p1", "mint": "Q" * 44, "side": "BUY", "pro": True, "_counted": True, "deltaPct": 0.0}
    assert e._route(sig) == "intent"
