"""Mayhem2 Mixed feed: New (under 3 min), then Movers, then Graduated, youngest first in each; every wallet's
trades on the pin are the signals; the Mayhem_Mode feed is unchanged."""

import asyncio
import time

import pytest

from src.signal import mayhem2 as M


def _row(mint, age, pg="pump", **kw):
    return dict({"m": mint, "t": mint[:4], "n": mint, "age": age, "pg": pg}, **kw)


@pytest.fixture
def eng(monkeypatch):
    e = M.Mayhem2()
    e.used = {}
    monkeypatch.setattr(M.Mayhem2, "feed", lambda self: "mixed")
    monkeypatch.setattr(e, "note", lambda *a, **k: None)
    now = time.time()
    e.mixed_rows = {
        "new": [_row("N2pump", 20), _row("N1pump", 5), _row("Noldpump", 400), _row("Nmetx", 3, pg="meteora_dbc")],
        "movers": [_row("M2pump", 900), _row("N1pump", 5), _row("M1pump", 300)],
        "graduated": [_row("G2pump", 2000, gd=1), _row("G1pump", 180, gd=1)],
    }
    e.mixed_at = {g: now for g in M.MIXED_GROUPS}
    return e


def test_order_new_then_movers_then_graduated_youngest_first(eng):
    got = [(r["m"], r["_grp"]) for r in eng.candidates()]
    assert got == [("N1pump", "new"), ("N2pump", "new"),
                   ("M1pump", "movers"), ("M2pump", "movers"),
                   ("G1pump", "graduated"), ("G2pump", "graduated")]


def test_paused_mayhem_coin_skipped(eng):
    eng.mixed_rows["new"].append(_row("Npausedpump", 1, ms="paused"))
    assert "Npausedpump" not in [r["m"] for r in eng.candidates()]


def test_brain_pins_first_candidate_without_mayhem_mode_holding(eng, monkeypatch):
    monkeypatch.setattr(M.Mayhem2, "active", lambda self: True)
    monkeypatch.setattr(M.Mayhem2, "_exit_check", lambda self: None)
    monkeypatch.setattr(M.Mayhem2, "_open_pos", lambda self, mint="": None)

    async def run():
        task = asyncio.ensure_future(eng._brain_loop())
        await asyncio.sleep(0.25)
        task.cancel()
    asyncio.run(run())
    assert eng.pin and eng.pin["mint"] == "N1pump" and eng.pin["grp"] == "new"


def test_used_coin_skipped_next_in_sequence(eng, monkeypatch):
    for m in ("N1pump", "N2pump"):
        eng.used[m] = {"ts": time.time(), "why": "traded"}
    monkeypatch.setattr(M.Mayhem2, "active", lambda self: True)
    monkeypatch.setattr(M.Mayhem2, "_exit_check", lambda self: None)
    monkeypatch.setattr(M.Mayhem2, "_open_pos", lambda self, mint="": None)

    async def run():
        task = asyncio.ensure_future(eng._brain_loop())
        await asyncio.sleep(0.25)
        task.cancel()
    asyncio.run(run())
    assert eng.pin["mint"] == "M1pump"


def test_any_wallet_trade_is_a_signal_on_stream(eng):
    class L:
        def __init__(self):
            self.calls = []

        def call_soon_threadsafe(self, fn, *a):
            self.calls.append(a)
    eng.loop = L()
    eng.pin = {"mint": "N1pump", "ts": time.time()}
    eng.on_stream({"mint": "N1pump", "wallet": "someoneElse", "signature": "s1", "kind": "sell", "ts": time.time()})
    assert len(eng.loop.calls) == 1


def test_mayhem_feed_unchanged_ignores_other_wallets(monkeypatch):
    e = M.Mayhem2()
    monkeypatch.setattr(M.Mayhem2, "feed", lambda self: "mayhem")
    e.loop = type("L", (), {"calls": [], "call_soon_threadsafe": lambda self, *a: self.calls.append(a)})()
    e.pin = {"mint": "X", "ts": time.time()}
    e.on_stream({"mint": "X", "wallet": "someoneElse", "signature": "s1", "kind": "sell", "ts": time.time()})
    assert e.loop.calls == []


def test_feed_setting_validated():
    from src.config.runtime import RuntimeConfig
    c = RuntimeConfig()
    assert c.mayhem2_feed == "mayhem"
    c.update_from({"mayhem2_feed": "MIXED"})
    assert c.mayhem2_feed == "mixed"
    c.update_from({"mayhem2_feed": "junk"})
    assert c.mayhem2_feed == "mixed"


def test_graduated_coin_priced_by_jupiter(eng, monkeypatch):
    from src.execution import pump_curve as PC

    async def curve(mint):
        return None, "graduated"

    async def fees():
        return "", "", 125
    monkeypatch.setattr(PC, "_curve", curve)
    monkeypatch.setattr(PC, "_fees", fees)

    class Q:
        out_amount = 1_000_000_000          # 1000 tokens (6 dp) for Max position

    class Jup:
        async def quote_sol_to_token(self, mint, sol, slippage_bps=0):
            return Q()
    eng.trader = type("T", (), {"jup": Jup()})()
    monkeypatch.setattr("src.config.runtime.get_config", lambda: type("C", (), {"max_position_sol": 0.005})())
    c, bps = asyncio.run(eng._curve_now("G1pump"))
    assert c["src"] == "jupiter" and bps == 125
    value = PC._sol_for_tokens(1_000_000_000, c["virtual_token"], c["virtual_sol"], 0) / 1e9
    assert abs(value - 0.005) < 1e-6


def test_pins_take_turns_new_movers_graduated(eng):
    got = []
    for _ in range(5):
        row = eng._mixed_pick()
        got.append((row["m"], row["_grp"]))
        eng.used[row["m"]] = {"ts": time.time(), "why": "traded"}
    assert got == [("N1pump", "new"), ("M1pump", "movers"), ("G1pump", "graduated"),
                   ("N2pump", "new"), ("M2pump", "movers")]


def test_empty_group_passes_turn(eng):
    eng.mixed_rows["movers"] = []
    eng.mixed_turn = 1                       # movers' turn, but movers is empty
    assert eng._mixed_pick()["_grp"] == "graduated" and eng.mixed_turn == 0
