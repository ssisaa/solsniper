"""Chain-read fills and per-round-trip ledger rows.

Fixtures are the real 2026-11-10 Mayhem2 trade on 7wYYHLzT…pump (getTransaction, jsonParsed):
buy 4eke3dKH…, pump sell that failed with 6003 (5DjrVUQK…), Lightning sell 1wJjBvDr….
"""

import asyncio
import json
import time
from pathlib import Path

import pytest

from src.execution import tx_route
from src.observability import ledger as ledger_mod

FIX = Path(__file__).parent / "fixtures"
MINT = "7wYYHLzTxqWnDUzjK18inywSC94WsZ31zMi86iHSpump"


def _tx(name):
    return json.loads((FIX / name).read_text())["result"]


def _owner(res):
    k = res["transaction"]["message"]["accountKeys"][0]
    return k if isinstance(k, str) else k["pubkey"]


# ---------- parse_fill against the chain ----------

def test_buy_fill_from_chain():
    res = _tx("tx_7wYY_buy.json")
    f = tx_route.parse_fill(res, _owner(res), MINT)
    assert f["err"] is None
    assert f["sol_delta"] == pytest.approx(-0.01153884)
    assert f["fee_sol"] == pytest.approx(0.000025)
    assert f["rent_opened_sol"] == pytest.approx(0.00151384)
    assert f["swap_sol"] == pytest.approx(-0.01)      # SOL into the swap, no fee, no rent
    assert f["tokens"] == 466170036386


def test_failed_sell_costs_only_fee():
    res = _tx("tx_7wYY_failed_sell.json")
    f = tx_route.parse_fill(res, _owner(res), MINT)
    assert f["err"] is not None
    assert f["sol_delta"] == pytest.approx(-0.000155)
    assert f["tokens"] == 0


def test_sell_fill_from_chain():
    res = _tx("tx_7wYY_sell.json")
    f = tx_route.parse_fill(res, _owner(res), MINT)
    assert f["err"] is None
    assert f["sol_delta"] == pytest.approx(0.00424316)
    assert f["rent_closed_sol"] == pytest.approx(0.00151384)
    assert f["fee_sol"] == pytest.approx(0.000005)
    assert f["swap_sol"] == pytest.approx(0.00273432)  # gross sale, before the 0.000005 fee
    assert f["tokens"] == -466170036386


def test_round_trip_pnl_matches_wallet():
    buy, bad, sell = (_tx(n) for n in ("tx_7wYY_buy.json", "tx_7wYY_failed_sell.json", "tx_7wYY_sell.json"))
    owner = _owner(buy)
    b, x, s = (tx_route.parse_fill(r, owner, MINT) for r in (buy, bad, sell))
    wallet = b["sol_delta"] + x["sol_delta"] + s["sol_delta"]
    booked = (s["swap_sol"] - s["fee_sol"] + x["sol_delta"]) - (-b["swap_sol"] + b["fee_sol"])
    # Rent opened on the buy came back in the sell tx, so the wallet and the book agree.
    assert wallet == pytest.approx(-0.00745068)
    assert booked == pytest.approx(wallet)


# ---------- run_swap result gets the chain numbers ----------

def _fake_chain(monkeypatch):
    fills = {
        "BUY": tx_route.parse_fill(_tx("tx_7wYY_buy.json"), "", MINT),
        "BAD": tx_route.parse_fill(_tx("tx_7wYY_failed_sell.json"), "", MINT),
        "SELL": tx_route.parse_fill(_tx("tx_7wYY_sell.json"), "", MINT),
    }

    async def fake(sig, mint, owner="", tries=10, delay=0.3):
        return fills.get(sig)

    monkeypatch.setattr(tx_route, "chain_fill", fake)


def test_apply_chain_fill_sell_replaces_quote(monkeypatch):
    _fake_chain(monkeypatch)
    got = {"ok": True, "sig": "SELL", "route": "lightning", "tokens": 466170036386, "sol": 0.0}
    asyncio.run(tx_route._apply_chain_fill(got, "sell", MINT, ["BAD"]))
    assert got["measured"] is True
    assert got["sol"] == pytest.approx(0.00273432)
    assert got["fee_sol"] == pytest.approx(0.000005)
    assert got["failed_fee_sol"] == pytest.approx(0.000155)
    assert got["tokens"] == 466170036386


def test_apply_chain_fill_buy_replaces_min_tokens(monkeypatch):
    _fake_chain(monkeypatch)
    got = {"ok": True, "sig": "BUY", "route": "pump", "tokens": 332330743896, "sol": 0.01}
    asyncio.run(tx_route._apply_chain_fill(got, "buy", MINT, []))
    assert got["tokens"] == 466170036386
    assert got["sol"] == pytest.approx(0.01)
    assert got["rent_sol"] == pytest.approx(0.00151384)


def test_apply_chain_fill_unreadable_keeps_quote(monkeypatch):
    _fake_chain(monkeypatch)
    got = {"ok": True, "sig": "NOPE", "route": "pump", "tokens": 5, "sol": 0.004945}
    asyncio.run(tx_route._apply_chain_fill(got, "sell", MINT, []))
    assert "measured" not in got and got["sol"] == 0.004945


# ---------- ledger: repeated round trips on one mint ----------

class _Fill:
    def __init__(self, side, sig, mint=MINT, ts=0.0, sol=0.0, tokens=0.0, status="confirmed"):
        self.side, self.signature, self.mint, self.ts = side, sig, mint, ts
        self.sol_amount, self.token_amount, self.status = sol, tokens, status


class _Hist:
    def __init__(self, fills):
        self.fills = fills

    def _fills(self):
        return list(self.fills)

    def confirmed_exit(self, mint, since_ts, held_tokens=0.0):
        return None


@pytest.fixture
def led(tmp_path, monkeypatch):
    monkeypatch.setattr(ledger_mod.TradeLedger, "_push_remote", lambda self, e: None)
    monkeypatch.setattr(ledger_mod.TradeLedger, "_load_remote", lambda self: None)
    import src.observability.email_alert as email
    monkeypatch.setattr(email, "notify_open", lambda *a, **k: None, raising=False)
    monkeypatch.setattr(email, "notify_close", lambda *a, **k: None, raising=False)
    hist = _Hist([])
    import src.observability.exec_history as eh
    monkeypatch.setattr(eh, "get_exec_history", lambda: hist)
    lg = ledger_mod.TradeLedger(tmp_path / "ledger.jsonl")
    lg._hist = hist
    return lg


def _open(lg, n, sig, tokens, ts):
    e = lg.record_open(
        id=f"m2-{n}", mint=MINT, symbol="LUV", mode="live", token_amount=tokens, entry_sol=0.01,
        entry_fee_sol=0.000025, entry_tip_sol=0.0, price_sol_per_token=0.0, signature=sig, source="mayhem2",
    )
    e.ts = ts
    return e


def _close(lg, n, sig, tokens, exit_sol, ts):
    return lg.record_close(
        id=f"pos{n}:0", mint=MINT, symbol="LUV", mode="live", token_amount=tokens, entry_sol=0.01,
        entry_fee_sol=0.000025, entry_tip_sol=0.0, exit_sol=exit_sol, exit_fee_sol=0.000005,
        hold_seconds=5, reason="mayhem2_sell", signature=sig, source="mayhem2", ts=ts, notify=False,
    )


def test_three_round_trips_stay_three(led):
    t = time.time() - 60
    for n in range(3):
        _open(led, n, f"B{n}", 1000.0, t + n * 10)
        led._hist.fills.append(_Fill("SELL", f"S{n}", ts=t + n * 10 + 5, sol=0.012, tokens=1000.0))
        # Same token amount and reason on every trip: only the signature tells them apart.
        assert _close(led, n, f"S{n}", 1000.0, 0.012, t + n * 10 + 5) is not None
    closes = [e for e in led.entries if e.side == "CLOSE"]
    opens = [e for e in led.entries if e.side == "OPEN"]
    assert [c.signature for c in closes] == ["S0", "S1", "S2"]
    assert len(opens) == 3
    # Same close twice is still a duplicate.
    again = _close(led, 2, "S2", 1000.0, 0.012, t + 25)
    assert again is closes[-1] and len([e for e in led.entries if e.side == "CLOSE"]) == 3


def test_rebuy_before_previous_close_is_booked(led):
    t = time.time() - 60
    _open(led, 0, "B0", 1000.0, t)
    # Re-buy lands before trip 0's close row is written (close task is still running).
    _open(led, 1, "B1", 2000.0, t + 6)
    _close(led, 0, "S0", 1000.0, 0.012, t + 5)
    assert [e.signature for e in led.entries if e.side == "OPEN"] == ["B0", "B1"]
    # Same buy written twice is still skipped.
    before = len(led.entries)
    led.record_open(
        id="dup", mint=MINT, symbol="LUV", mode="live", token_amount=2000.0, entry_sol=0.01,
        entry_fee_sol=0.0, entry_tip_sol=0.0, price_sol_per_token=0.0, signature="B1", source="mayhem2",
    )
    assert len(led.entries) == before


def test_repair_keeps_previous_trip_when_coin_rebought(led):
    t = time.time() - 60
    _open(led, 0, "B0", 1000.0, t)
    _close(led, 0, "S0", 1000.0, 0.012, t + 5)
    led._hist.fills.append(_Fill("SELL", "S0", ts=t + 5, sol=0.012, tokens=1000.0))
    _open(led, 1, "B1", 2000.0, t + 8)
    # The wallet holds the coin again (trip 1). Trip 0's close is a confirmed sale.
    led.repair_book({MINT})
    assert [e.signature for e in led.entries if e.side == "CLOSE"] == ["S0"]
    assert [e.signature for e in led.entries if e.side == "OPEN"] == ["B0", "B1"]


def test_repair_drops_unconfirmed_close_while_held_and_sold_out_trips_kept(led):
    t = time.time() - 60
    for n in range(2):
        _open(led, n, f"B{n}", 1000.0 + n, t + n * 10)
        _close(led, n, f"S{n}", 1000.0 + n, 0.012, t + n * 10 + 5)
    led._hist.fills.append(_Fill("SELL", "S0", ts=t + 5, sol=0.012, tokens=1000.0))
    # S1 never showed in the wallet history and the coin is still held: it was not a sale.
    led.repair_book({MINT})
    assert [e.signature for e in led.entries if e.side == "CLOSE"] == ["S0"]
    # Coin gone: every trip keeps its own open and close.
    _close(led, 1, "S1b", 1001.0, 0.011, t + 16)
    led.repair_book(set())
    assert [e.signature for e in led.entries if e.side == "CLOSE"] == ["S0", "S1b"]
    assert len([e for e in led.entries if e.side == "OPEN"]) == 2


def test_reconcile_does_not_merge_next_trip(led, monkeypatch):
    t = float(int(time.time()) - 120)
    _open(led, 0, "B0", 1000.0, t + 1.5)
    _close(led, 0, "S0", 1000.0, 0.02, t + 6.5)
    _open(led, 1, "B1", 3000.0, t + 9.5)
    _close(led, 1, "S1", 3000.0, 0.02, t + 15.5)
    rows = [
        {"signature": "B0", "side": "BUY", "mint": MINT, "ts": t, "sol_amount": 0.010025, "token_amount": 1000.0},
        {"signature": "S0", "side": "SELL", "mint": MINT, "ts": t + 5, "sol_amount": 0.013327, "token_amount": 1000.0},
        # Next trip's buy is 2s after trip 0's close: inside the old +3s / +210s windows.
        {"signature": "B1", "side": "BUY", "mint": MINT, "ts": t + 8, "sol_amount": 0.010025, "token_amount": 3000.0},
        {"signature": "S1", "side": "SELL", "mint": MINT, "ts": t + 14, "sol_amount": 0.002729, "token_amount": 3000.0},
    ]
    from src.config import runtime
    monkeypatch.setattr(runtime, "get_config", lambda: type("C", (), {"max_position_sol": 0.01})())
    led.reconcile_wallet_sells(rows)
    closes = [e for e in led.entries if e.side == "CLOSE"]
    assert [round(c.exit_sol, 6) for c in closes] == [0.013327, 0.002729]
    assert all(round(c.entry_sol, 4) == 0.01 for c in closes)
