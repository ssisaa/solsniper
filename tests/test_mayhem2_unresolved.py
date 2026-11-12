"""Mayhem2: a sent buy whose confirmation was missed is followed, never re-bought, and adopted if it lands."""
import asyncio
import time
import types

import pytest

from src.signal import mayhem2 as M2
from src.execution import tx_route


class Mgr:
    def __init__(self):
        self.positions = {}
        self.paused_until = 0

    async def open_position(self, mint, mode, entry_sol, entry_price, tokens, source, signature=""):
        p = types.SimpleNamespace(id=f"p{len(self.positions)}", mint=mint, status="OPEN", source=source, strategy=source,
                                  entry_sol=entry_sol, tokens=tokens, symbol="", name="", _sell_busy=False)
        self.positions[p.id] = p
        return p

    def set_price(self, *a):
        pass

    def _push_remote(self):
        pass


class Trader:
    def __init__(self, err=""):
        self._last_tx_err = ""
        self.err = err

    async def _confirm_signature(self, sig, **kw):
        self._last_tx_err = self.err
        return False


def _engine(monkeypatch, holdings, trader=None):
    e = M2.Mayhem2()
    e.manager = Mgr()
    e.trader = trader or Trader()
    e.loop = asyncio.get_event_loop()
    monkeypatch.setattr(M2.Mayhem2, "trading", lambda self: True)
    monkeypatch.setattr(M2.Mayhem2, "dry", lambda self: False)
    import src.execution.signer as signer
    import src.execution.wallet_tokens as wt
    monkeypatch.setattr(signer, "pubkey_from_env", lambda: "PK")

    async def holding(pk, mint, **kw):
        return {"raw": holdings.get(mint, 0)}
    monkeypatch.setattr(wt, "get_token_holding", holding)
    import src.observability.ledger as led
    monkeypatch.setattr(led, "get_ledger", lambda: types.SimpleNamespace(record_open=lambda **k: None))
    return e


def test_unconfirmed_buy_lands_and_is_adopted(monkeypatch):
    holdings = {}
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    e = _engine(monkeypatch, holdings)
    pin = {"mint": "MINT", "symbol": "DICK", "ts": time.time()}
    e.pin = pin
    tx_route.m2_buy_note("MINT", "SIG", 0.005)

    async def go():
        e.unresolved = {"mint": "MINT", "sig": "SIG", "sol": 0.005, "pin": pin, "until": time.time() + 5, "seen": time.time()}
        task = asyncio.ensure_future(e._resolve())
        await asyncio.sleep(0.1)
        assert e._try_buy(pin).startswith("not bought: previous buy not confirmed")   # no second buy
        holdings["MINT"] = 9_000_000                                                    # it landed
        await asyncio.wait_for(task, 4)
        await asyncio.sleep(0.05)
    loop.run_until_complete(go())
    pos = e._open_pos("MINT")
    assert pos is not None and pos.source == "mayhem2" and pos.strategy == "mayhem2" and pos._m2_tokens == 9_000_000
    assert e.unresolved is None
    loop.close()


def test_unconfirmed_buy_failed_on_chain_clears(monkeypatch):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    e = _engine(monkeypatch, {}, Trader(err="{'InstructionError': [4, {'Custom': 6042}]}"))
    pin = {"mint": "MINT", "symbol": "X", "ts": time.time()}
    e.pin = pin
    e.unresolved = {"mint": "MINT", "sig": "SIG", "sol": 0.005, "pin": pin, "until": time.time() + 5, "seen": time.time()}
    loop.run_until_complete(asyncio.wait_for(e._resolve(), 4))
    assert e.unresolved is None and e._open_pos("MINT") is None
    assert pin["buys_off_until"] > time.time()            # the usual short wait after a failed buy
    assert any("failed on chain" in n["m"] for n in e.notes)
    loop.close()


def test_registry():
    tx_route.m2_buy_note("A", "S", 0.004)
    assert tx_route.m2_buy_get("A")["sig"] == "S"
    assert tx_route.m2_buy_get("A", within=-1) is None
    assert tx_route.m2_buy_get("nope") is None
