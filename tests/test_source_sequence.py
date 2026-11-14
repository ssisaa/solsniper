"""Candles page Source Sequence: Launch -> Dayscalp -> CallOuts -> OpenCopy -> MayhemMode -> Mayhem board -> Mixed."""
import asyncio
import time

from src.signal import candle_momo as CM


def _eng(monkeypatch):
    e = CM.CandleMomo()
    e.cfg.source_seq = True
    switched = []
    filt = {"v": "launch"}
    monkeypatch.setattr(e, "_seq_apply_source", lambda key: (switched.append(key), filt.update(v=key)))
    monkeypatch.setattr(CM.CandleMomo, "live_filter", staticmethod(lambda: filt["v"]))
    monkeypatch.setattr(e, "update", lambda patch: e.cfg.__dict__.update(patch) or {})
    return e, switched


def _tick(e, n=1):
    async def go():
        t = asyncio.ensure_future(e._seq_loop())
        await asyncio.sleep(1.05 * n)
        t.cancel()
    asyncio.run(go())


def test_steps_in_order_and_board_steps(monkeypatch):
    e, switched = _eng(monkeypatch)
    order = []
    for i in range(len(CM.SEQ_STEPS)):
        e._seq_switch(i)
        order.append(CM.SEQ_STEPS[i][0])
        if CM.SEQ_STEPS[i][0].startswith("board:"):
            assert e.seq_board == CM.SEQ_STEPS[i][0][6:] and e.support_source() == ""
            assert e.feeds_only("mayhem")              # MayhemMode stays the source and only offers
        else:
            assert e.seq_board == "" and e.support_source() == CM.SEQ_STEPS[i][0]
    assert switched == ["launch", "mayhem"]
    assert e.cfg.support_enabled
    assert e.follow() == "mixed"


def test_moves_on_after_60s_without_a_trade(monkeypatch):
    e, switched = _eng(monkeypatch)
    _tick(e)
    assert e.seq["i"] == 0 and switched == ["launch"]
    e.seq["at"] = time.time() - CM.SEQ_STEP_S - 1
    _tick(e)
    assert e.seq["i"] == 1 and switched[-1] == "mayhem"


def test_waits_for_an_open_trade_then_moves_on(monkeypatch):
    e, switched = _eng(monkeypatch)
    _tick(e)
    e.positions["X"] = {"mint": "X"}
    e.seq["at"] = time.time() - 999
    _tick(e)
    assert e.seq["i"] == 0 and "trade open" in e.seq["state"]     # never switches mid-trade
    e.positions.clear()                                          # sold (or Max hold)
    _tick(e)
    assert e.seq["i"] == 1


def test_trade_closed_early_moves_on_at_once(monkeypatch):
    e, switched = _eng(monkeypatch)
    _tick(e)
    e.positions["X"] = {"mint": "X"}
    _tick(e)
    e.positions.clear()
    _tick(e)
    assert e.seq["i"] == 1                                      # well inside the 60s: next step at once


def test_off_stops_and_keeps_the_source(monkeypatch):
    e, switched = _eng(monkeypatch)
    e._seq_switch(2)
    assert e.seq_board == "mayhem_board"
    e.cfg.source_seq = False
    _tick(e)
    assert e.seq_board == "" and e.seq["state"] == "off" and e.seq_status()["on"] is False
