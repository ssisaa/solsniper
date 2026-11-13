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
    e, pos, started = _eng(monkeypatch, src="launch")       # Launch buys lot 1 itself: Paper plays it on paper
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


# ---------- AutoTrade-Candles feed-only sources: CallOuts / OpenCopy / MayhemMode offer tokens, Candles buys ----------

def _fo(monkeypatch, src="callout", rule="inverse", mode="live"):
    e = CM.CandleMomo()
    e.cfg.rule, e.cfg.mode, e.cfg.support_enabled, e.cfg.avoid_mayhem = rule, mode, True, False
    monkeypatch.setattr(e, "support_source", lambda: src)
    monkeypatch.setattr(e, "_support_held", lambda s: [])
    return e


def test_feeds_only_sources(monkeypatch):
    for src in ("callout", "opencopy", "mayhem"):
        e = _fo(monkeypatch, src)
        assert e.feeds_only(src) and e.pro_hold_owns_entries(src)
    e = _fo(monkeypatch, "dayscalp")
    assert not e.feeds_only("dayscalp")
    e = _fo(monkeypatch, "callout")
    e.cfg.support_enabled = False
    assert not e.feeds_only("callout")               # AutoTrade-Candles off: the source's own rules


def test_offered_token_is_fed_and_candles_buys_lot1(monkeypatch):
    e = _fo(monkeypatch, "callout", mode="paper")       # no bot running here: Live would answer NOT_READY
    monkeypatch.setattr(e, "_start", lambda sig: "intent")
    m = "Off" + "z" * 41
    assert e.offer("callout", m, "OFF") is True
    assert e.offer("callout", m, "OFF") is False      # once
    e._pick([])
    assert m in e.feeds and e.feeds[m].origin == "callout"
    e.feeds[m].status = "OK"
    sig = {"id": "o1", "mint": m, "side": "BUY", "_counted": True, "deltaPct": -1.0}
    from src.config import runtime as R
    monkeypatch.setattr(R, "get_config", lambda: SimpleNamespace(trade_enabled=True))
    assert e._route(sig) == "intent"                  # no SUPPORT_NO_ENTRY: Candles buys lot 1 by its rule


def test_offer_expires_and_newest_first(monkeypatch):
    e = _fo(monkeypatch, "opencopy")
    old, new = "Old" + "a" * 41, "New" + "b" * 41
    e.offer("opencopy", old)
    e.offers[old]["at"] = e.offers[old]["seen"] = time.time() - 10
    e.offer("opencopy", new)
    e._pick([])
    assert set(e.feeds) == {new}                      # Max mints 1: the newest offer
    e.offers[new]["seen"] = time.time() - 999
    e.offers[old]["seen"] = time.time() - 999
    e._pick([])
    assert not e.offers and not e.feeds
