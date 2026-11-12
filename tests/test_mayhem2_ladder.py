"""Mayhem2 ladder: BUY buys; SELL sells only in profit; a BUY below the last buy buys again (max 3)."""

import asyncio
import time
import types

import pytest

from src.signal import mayhem2 as M

MINT = "LadderMint1111111111111111111111111111111pump"


class _Mgr:
    def __init__(self):
        self.positions = {}
        self.kicked = []
        self.paused_until = 0.0

    def set_price(self, mint, px):
        pass

    def _push_remote(self):
        pass

    def kick_now(self, pos, reason):
        self.kicked.append(reason)
        return True


def _pos(**kw):
    base = dict(
        id="p1", mint=MINT, status="OPEN", source="mayhem2", strategy="mayhem2", symbol="LAD",
        entry_sol=0.01, tokens=1_000_000.0, _m2_tokens=1_000_000.0, fees_sol=0.000025,
        entry_time=time.time(), peak_since_pct=0.0, since_buy_pct=0.0, _m2_pct=0.0,
        _m2_buys=1, _m2_last_px=10_000.0, _m2_spot=10_000.0, entry_price_usd=1.0, _sell_busy=False,
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.fixture
def eng(monkeypatch):
    e = M.Mayhem2()
    e.manager = _Mgr()
    e.pin = {"mint": MINT, "symbol": "LAD", "ts": time.time() - 5}
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "ladder")
    e.buys = []
    monkeypatch.setattr(e, "_try_buy", lambda pin, seen=0.0, add=False: e.buys.append(add) or ("LADDER BUY sent" if add else "BUY sent"))
    e.sold = []
    monkeypatch.setattr(e, "_sell", lambda pos, why: e.sold.append(why))
    monkeypatch.setattr(e, "note", lambda *a, **k: None)
    return e


def _hold(eng, **kw):
    p = _pos(**kw)
    eng.manager.positions[p.id] = p
    return p


def sig(side):
    return {"side": side, "seen": time.time(), "sol": 0.1}


def test_buy_when_flat(eng):
    assert eng._on_signal(sig("buy")) == "BUY sent"
    assert eng.buys == [False]


def test_sell_in_profit_sells(eng):
    _hold(eng, _m2_pct=4.0)
    assert "SELL sent" in eng._on_signal(sig("sell"))
    assert len(eng.sold) == 1


def test_sell_at_loss_holds(eng):
    _hold(eng, _m2_pct=-12.0)
    out = eng._on_signal(sig("sell"))
    assert out.startswith("holding")
    assert eng.sold == [] and MINT not in eng.wait_profit


def test_buy_below_last_buy_adds(eng):
    _hold(eng, _m2_spot=9_000.0, _m2_last_px=10_000.0, _m2_pct=-11.0)
    assert eng._on_signal(sig("buy")) == "LADDER BUY sent"
    assert eng.buys == [True]


def test_buy_above_last_buy_ignored(eng):
    _hold(eng, _m2_spot=10_500.0, _m2_last_px=10_000.0)
    assert "ladder buys only below" in eng._on_signal(sig("buy"))
    assert eng.buys == []


def test_fourth_buy_refused(eng):
    _hold(eng, _m2_buys=3, _m2_spot=5_000.0, _m2_last_px=10_000.0)
    assert "ladder full" in eng._on_signal(sig("buy"))
    assert eng.buys == []


def test_other_strategy_still_ignores_buy_while_holding(eng, monkeypatch):
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "copy")
    _hold(eng, _m2_spot=5_000.0)
    assert eng._on_signal(sig("buy")) == "ignored: holding"


def test_ladder_has_no_stop_or_max_hold(eng):
    _hold(eng, _m2_pct=-60.0, entry_time=time.time() - 120, peak_since_pct=12.0)
    eng._exit_check()
    assert eng.sold == []


def test_ladder_take_profit_and_profit_trail(eng):
    p = _hold(eng, _m2_pct=21.0)
    eng._exit_check()
    assert eng.sold and "take profit" in eng.sold[-1]
    eng.sold.clear()
    p._m2_pct, p.peak_since_pct = 7.0, 15.0           # half of +15% gone, still above +3%: trail sells
    eng._exit_check()
    assert eng.sold and "trail" in eng.sold[-1]


def test_copy_keeps_stop(eng, monkeypatch):
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "copy")
    _hold(eng, _m2_pct=-20.0)
    eng._exit_check()
    assert eng.sold and "stop" in eng.sold[-1]


def test_ladder_merge_sums_the_trade(eng, monkeypatch):
    p = _hold(eng, entry_price_usd=2.0, peak_since_pct=9.0)
    added = []

    class _L:
        def add_to_open(self, *a):
            added.append(a)

    import src.observability.ledger as L
    monkeypatch.setattr(L, "get_ledger", lambda: _L())
    eng.fill_px = lambda fill: 1.0
    fill = types.SimpleNamespace(sol_in=0.01, tokens_out=2_000_000.0, fees_sol=0.000025, signature="B2")
    n = eng._ladder_merge(fill, eng.pin)
    assert n == 2 and p._m2_buys == 2
    assert p.entry_sol == pytest.approx(0.02)
    assert p.tokens == pytest.approx(3_000_000.0) and p._m2_tokens == pytest.approx(3_000_000.0)
    assert p.entry_price_usd == pytest.approx((2.0 * 1e6 + 1.0 * 2e6) / 3e6)
    assert p._m2_last_px == pytest.approx(0.01 * 1e9 / 2e6)
    assert p.peak_since_pct == 0.0
    assert added == [(MINT, 0.01, 0.000025, 2_000_000.0, "B2")]


def test_ledger_ladder_one_row_and_wallet_reconcile(tmp_path, monkeypatch):
    from src.observability import ledger as L
    monkeypatch.setattr(L.TradeLedger, "_push_remote", lambda self, e: None)
    monkeypatch.setattr(L.TradeLedger, "_load_remote", lambda self: None)
    import src.observability.email_alert as email
    monkeypatch.setattr(email, "notify_open", lambda *a, **k: None, raising=False)
    from src.config import runtime
    monkeypatch.setattr(runtime, "get_config", lambda: type("C", (), {"max_position_sol": 0.01})())
    lg = L.TradeLedger(tmp_path / "ledger.jsonl")
    t = float(int(time.time()) - 200)
    op = lg.record_open(id="o", mint=MINT, symbol="LAD", mode="live", token_amount=1e6, entry_sol=0.01,
                        entry_fee_sol=0.000025, entry_tip_sol=0.0, price_sol_per_token=0.0, signature="B1", source="mayhem2")
    op.ts = t + 1
    lg.add_to_open(MINT, 0.01, 0.000025, 2e6, "B2")
    lg.add_to_open(MINT, 0.01, 0.000025, 2e6, "B2")        # same buy twice: counted once
    lg.add_to_open(MINT, 0.01, 0.000025, 3e6, "B3")
    assert len([e for e in lg.entries if e.side == "OPEN"]) == 1
    assert op.entry_sol == pytest.approx(0.03) and op.token_amount == pytest.approx(6e6)
    assert op.add_sigs == ["B2", "B3"]
    lg.record_close(id="c", mint=MINT, symbol="LAD", mode="live", token_amount=6e6, entry_sol=0.03,
                    entry_fee_sol=0.000075, entry_tip_sol=0.0, exit_sol=0.035, exit_fee_sol=0.000005,
                    hold_seconds=40, reason="mayhem2_sell", signature="S1", source="mayhem2", ts=t + 60, notify=False)
    rows = [
        {"signature": "B1", "side": "BUY", "mint": MINT, "ts": t, "sol_amount": 0.010025, "token_amount": 1e6},
        {"signature": "B2", "side": "BUY", "mint": MINT, "ts": t + 20, "sol_amount": 0.010025, "token_amount": 2e6},
        {"signature": "B3", "side": "BUY", "mint": MINT, "ts": t + 40, "sol_amount": 0.010025, "token_amount": 3e6},
        {"signature": "S1", "side": "SELL", "mint": MINT, "ts": t + 59, "sol_amount": 0.035, "token_amount": 6e6},
    ]
    lg.reconcile_wallet_sells(rows)
    close = [e for e in lg.entries if e.side == "CLOSE"][0]
    # All three buys are this trade's cost, even the two after the first sell window.
    # (SOL above 3 x Max position is booked as fee, the ledger's existing spill rule.)
    assert close.entry_sol == pytest.approx(0.03, abs=1e-6)
    assert close.cost_basis_sol == pytest.approx(0.030075, abs=1e-6)
    assert close.exit_sol == pytest.approx(0.035)


# ---------- page follows the selected strategy (paper view when the bot is not trading it) ----------

def _paper_engine(monkeypatch, chosen="ladder"):
    e = M.Mayhem2()
    e.manager = _Mgr()
    e.pin = {"mint": MINT, "symbol": "LAD", "ts": time.time() - 5}
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: chosen)
    monkeypatch.setattr(M.Mayhem2, "active", lambda self: True)
    monkeypatch.setattr(M.Mayhem2, "dry", lambda self: False)
    monkeypatch.setattr(M.Mayhem2, "trading", lambda self: False)
    e.entered, e.exited, e.notes_seen = [], [], []

    async def enter(name, mint, sym, add=False):
        e.entered.append((name, add))
        e.shadow[name]["busy"] = False

    async def exit_(name, why):
        e.exited.append(name)

    monkeypatch.setattr(e, "_shadow_enter", enter)
    monkeypatch.setattr(e, "_shadow_exit", exit_)
    monkeypatch.setattr(e, "note", lambda level, m, mint="": e.notes_seen.append(m))
    return e


def test_plan_is_the_selected_strategys_decision(monkeypatch):
    e = _paper_engine(monkeypatch)

    async def go():
        flat = e._shadow_signal("buy", time.time(), 0.1)
        await asyncio.sleep(0)
        e.shadow["ladder"]["pos"] = {"mint": MINT, "symbol": "LAD", "tokens": 1, "cost": 0.01, "at": time.time(),
                                     "pct": -9.0, "peak": None, "buys": 1, "last_px": 100.0, "spot": 90.0}
        hold = e._shadow_signal("sell", time.time(), 0.1)
        add = e._shadow_signal("buy", time.time(), 0.1)
        await asyncio.sleep(0)
        e.shadow["ladder"]["pos"]["pct"] = 4.0
        sell = e._shadow_signal("sell", time.time(), 0.1)
        await asyncio.sleep(0)
        return flat, hold, add, sell

    flat, hold, add, sell = asyncio.run(go())
    assert flat == "BUY"
    assert hold.startswith("holding") and any("PAPER ladder" in m and "holding" in m for m in e.notes_seen)
    assert add == "LADDER BUY 2/3"
    assert sell.startswith("SELL at +4.0%")
    assert ("ladder", True) in e.entered and "ladder" in e.exited


def test_snapshot_shows_paper_marks_when_not_trading(monkeypatch):
    e = _paper_engine(monkeypatch)
    monkeypatch.setattr(M.Mayhem2, "_paper_stats", lambda self: {})
    e.shadow["ladder"]["marks"].append({"ts": time.time(), "mint": MINT, "side": "buy", "label": "B1"})
    e.shadow["copy"]["marks"].append({"ts": time.time(), "mint": MINT, "side": "sell", "label": "S"})
    e.marks.append({"ts": time.time(), "mint": MINT, "side": "buy", "label": "LIVE"})
    snap = e.snapshot()
    assert snap["view"] == "paper"
    assert [m["label"] for m in snap["marks"]] == ["B1"]
    monkeypatch.setattr(M.Mayhem2, "dry", lambda self: True)
    snap = e.snapshot()
    assert snap["view"] == "dry" and [m["label"] for m in snap["marks"]] == ["LIVE"]


# ---------- one trade per coin, then the next new coin ----------

def test_own_trade_closed_moves_on_and_is_remembered(monkeypatch, tmp_path):
    monkeypatch.setattr(M, "_used_path", lambda: tmp_path / "used.json")
    e = M.Mayhem2()
    e.manager = _Mgr()
    e.pin = {"mint": MINT, "symbol": "LAD", "ts": time.time() - 5}
    monkeypatch.setattr(M.Mayhem2, "dry", lambda self: True)
    monkeypatch.setattr(e, "note", lambda *a, **k: None)
    assert e._trade_done(e.pin) == ""                 # nothing bought yet: stay
    e.pin["traded"] = True
    p = _hold(e)
    assert e._trade_done(e.pin) == ""                 # still holding: stay
    p.status = "CLOSED"
    assert e._trade_done(e.pin) == "its trade closed"
    e._unpin("its trade closed (gain or loss) — one trade per coin, next new coin", "mayhem2_sell")
    assert e.pin is None and MINT in e.used
    # A restart keeps the coin off-limits.
    again = M.Mayhem2()
    assert MINT in again.used


def test_no_second_buy_on_a_traded_coin(monkeypatch):
    e = M.Mayhem2()
    e.pin = {"mint": MINT, "symbol": "LAD", "ts": time.time(), "traded": True}
    assert "one trade per coin" in e._try_buy(e.pin)


def test_paper_strategy_trades_a_coin_once(monkeypatch):
    e = _paper_engine(monkeypatch, chosen="copy")
    e.shadow["copy"]["done"][MINT] = time.time()
    async def go():
        out = e._shadow_signal("buy", time.time(), 0.1)     # other paper strategies still buy
        await asyncio.sleep(0)
        return out

    assert "one trade per coin" in asyncio.run(go())
    assert e._trade_done(e.pin) == "its paper copy trade closed"
