"""Mayhem2 inv_ladder: SELL (red) buys lot 1; then leave 3 reds and buy on the 4th (reds 1, 5, 9), only below the last
buy, max 3 lots; BUY (green) sells all lots, always."""

import time

import pytest

from src.signal import mayhem2 as M
from tests.test_mayhem2_ladder import _Mgr, _pos, MINT


@pytest.fixture
def eng(monkeypatch):
    e = M.Mayhem2()
    e.manager = _Mgr()
    e.pin = {"mint": MINT, "symbol": "INV", "ts": time.time() - 5}
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "inv_ladder")
    e.buys = []
    monkeypatch.setattr(e, "_try_buy", lambda pin, seen=0.0, add=False: e.buys.append(add) or ("LADDER BUY sent" if add else "BUY sent"))
    e.sold = []
    monkeypatch.setattr(e, "_sell", lambda pos, why: e.sold.append(why))
    monkeypatch.setattr(e, "note", lambda *a, **k: None)
    return e


def sig(side):
    return {"side": side, "seen": time.time(), "sol": 0.1}


def test_red_opens_lot_one(eng):
    assert eng._on_signal(sig("sell")) == "BUY sent" and eng.buys == [False]


def test_leave_three_reds_buy_on_fourth_below_last(eng):
    p = _pos(_m2_spot=9_000.0)                  # below the last buy (10_000)
    eng.manager.positions[p.id] = p
    for _ in range(3):                          # reds 2-4: leave
        assert "leave" in eng._on_signal(sig("sell"))
    assert eng._on_signal(sig("sell")) == "LADDER BUY sent"
    assert eng.buys == [True] and p._m2_reds == 0


def test_third_red_above_last_buy_does_not_buy(eng):
    p = _pos(_m2_spot=11_000.0)
    eng.manager.positions[p.id] = p
    for _ in range(4):
        out = eng._on_signal(sig("sell"))
    assert "vs last buy" in out and eng.buys == []


def test_cap_three_lots(eng):
    p = _pos(_m2_spot=9_000.0, _m2_buys=3)
    eng.manager.positions[p.id] = p
    for _ in range(4):
        out = eng._on_signal(sig("sell"))
    assert "ladder full" in out and eng.buys == []


@pytest.mark.parametrize("pct", [-30.0, 0.5, 12.0])
def test_green_always_sells_all(eng, pct):
    p = _pos(_m2_pct=pct, _m2_buys=2)
    eng.manager.positions[p.id] = p
    assert eng._on_signal(sig("buy")) == "SELL sent (green)"
    assert "green" in eng.sold[0] and "2 lot" in eng.sold[0]


def test_registered():
    assert "inv_ladder" in M.SHADOWS and "inv_ladder" in M.SHADOW_RULES
    from src.config.runtime import RuntimeConfig  # noqa: F401  (validation list includes it)


def test_paper_twin_follows_same_rule(monkeypatch):
    e = M.Mayhem2()
    e.pin = {"mint": MINT, "symbol": "INV", "ts": time.time() - 5}
    monkeypatch.setattr(e, "active", lambda: True)
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "inv_ladder")
    monkeypatch.setattr(e, "_paper_note", lambda *a, **k: None)
    calls = []

    def enter(name, mint, sym, add=False):
        calls.append(("enter", name, add))
        e.shadow[name]["busy"] = False
        if not add:
            e.shadow[name]["pos"] = {"mint": mint, "symbol": sym, "tokens": 1.0, "cost": 0.005, "at": time.time(),
                                     "pct": -8.0, "peak": None, "buys": 1, "last_px": 10.0, "spot": 9.0}

    def exit_(name, why):
        calls.append(("exit", name, why))
    monkeypatch.setattr(e, "_shadow_enter", lambda *a, **k: enter(*a, **k))
    monkeypatch.setattr(e, "_shadow_exit", lambda *a, **k: exit_(*a, **k))
    monkeypatch.setattr(M.asyncio, "ensure_future", lambda x: None)
    assert e._shadow_signal("sell", time.time()) == "BUY lot 1 (red)"
    for _ in range(3):
        assert "leave" in e._shadow_signal("sell", time.time())
    assert e._shadow_signal("sell", time.time()).startswith("LADDER BUY 2/3")
    assert e._shadow_signal("buy", time.time()) == "SELL all (green)"
    assert ("exit", "inv_ladder") == tuple(c[:2] for c in calls if c[0] == "exit" and c[1] == "inv_ladder")[0]


def test_paused_kick_does_not_leave_sell_busy_stuck(monkeypatch):
    import types as _t
    from src.risk import position_manager as PM
    import src.execution.fast_lane as FL
    mgr = PM.PositionManager.__new__(PM.PositionManager)
    mgr._sell_cooldown = {}
    mgr.positions = {}
    pos = _t.SimpleNamespace(mint=MINT, status="OPEN", _sell_busy=False, symbol="X", entry_price=1.0)
    monkeypatch.setattr(FL, "fire_sell", lambda *a, **k: False)          # another sell is already going out
    assert mgr.kick_mayhem_paused(pos, "low_sol_reserves") is False
    assert pos._sell_busy is False


def test_mayhem2_ignores_circuit_breaker(monkeypatch):
    e = M.Mayhem2()
    e.manager = _Mgr()
    e.manager.paused_until = time.time() + 2700           # breaker on for 45 min
    e.pin = {"mint": MINT, "symbol": "X", "ts": time.time()}
    monkeypatch.setattr(e, "dry", lambda: False)
    monkeypatch.setattr(e, "trading", lambda: True)
    monkeypatch.setattr(e, "_other_open", lambda m: None)
    monkeypatch.setattr(M.asyncio, "ensure_future", lambda c: c.close())
    assert e._try_buy(dict(e.pin)) == "BUY sent"
