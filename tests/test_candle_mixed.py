"""Candles page Mixed feed: New -> Movers -> Graduated, youngest first, pump mints only."""
import time

from src.signal import candle_momo as CM


def _rows():
    R = lambda g, m, age, **k: dict({"m": m, "t": m[:4].upper(), "_grp": g, "_age": age, "c": "solana"}, **k)
    return [
        R("new", "AAAApump", 50), R("new", "BBBBpump", 20), R("new", "OLDDpump", 900),
        R("new", "notpumpcoin", 1),
        R("movers", "MOV1pump", 4000), R("movers", "MOV2pump", 600),
        R("graduated", "GRA1pump", 9000), R("graduated", "PAUSpump", 10, ms="paused"),
    ]


def _eng():
    e = CM.CandleMomo()
    e.cfg.auto_follow = "mixed"
    e.cfg.rule = "inverse"
    e.cfg.max_mints = 1
    e.cfg.reuse_coin = False
    return e


def test_candidates_pump_only_youngest_first_per_board():
    e = _eng()
    c = [r["m"] for r in e._mixed_candidates(_rows())]
    assert c == ["BBBBpump", "AAAApump", "MOV2pump", "MOV1pump", "GRA1pump"]


def test_turns_new_movers_graduated():
    e = _eng()
    rows = _rows()
    order = []
    for _ in range(4):
        e._pick(rows)
        (m,) = list(e.feeds)
        order.append(m)
        e.traded[m] = time.time() + 0.001          # its trade completed: next turn
    assert order == ["BBBBpump", "MOV2pump", "GRA1pump", "AAAApump"]
    assert e.feeds and next(iter(e.feeds.values())).origin.startswith("mixed-")


def test_watched_coin_stays_while_held_and_until_idle():
    e = _eng()
    rows = _rows()
    e._pick(rows)
    assert list(e.feeds) == ["BBBBpump"]
    e.positions["BBBBpump"] = {"mint": "BBBBpump"}
    e._mx_since["BBBBpump"] = time.time() - 999
    e._pick(rows)
    assert list(e.feeds) == ["BBBBpump"]               # held: never dropped
    e.positions.clear()
    e._pick(rows)
    assert list(e.feeds) == ["MOV2pump"]               # idle too long while flat: next board's turn


def test_pro_hold_one_coin_and_waits_for_open_trade():
    e = _eng()
    e.cfg.rule = "pro_hold"
    e.cfg.max_mints = 3
    e._pick(_rows())
    assert len(e.feeds) == 1


def test_config_accepts_mixed():
    from src.signal import candle_core as C
    assert "mixed" in C._CHOICES["auto_follow"]
    assert C.Config().mixed_idle_s == 60.0


def test_stream_candles_use_trade_price_when_price_sol_is_zero():
    import asyncio
    st = CM.StreamProvider()
    st.wanted = {"G"}
    base = int(time.time()) - 5
    for dt, px in ((0, 1e-7), (1, 1.2e-7), (2, 0.9e-7)):
        st.on_trade({"mint": "G", "price_sol": 0.0, "trade_px": px, "ts": base + dt, "sol": 0.01})
    rows = asyncio.run(st.fetch("G", None))
    assert [round(r.close * 1e8) for r in rows] == [10, 12, 9]


def test_avoid_mayhem_skips_mayhem_rows_and_blocks_buys():
    e = _eng()
    rows = _rows() + [{"m": "MAYHpump", "t": "M", "_grp": "new", "_age": 1, "c": "solana", "ms": "active"}]
    assert "MAYHpump" not in [r["m"] for r in e._mixed_candidates(rows)]
    e.cfg.avoid_mayhem = False
    assert "MAYHpump" in [r["m"] for r in e._mixed_candidates(rows)]
    e.cfg.avoid_mayhem = True
    e.cfg.inflow_window_s = 0
    e.cfg.max_below_peak_pct = 0
    e.feeds["X"] = CM.Feed("X", 10)
    e.mayhem_of["X"] = True
    assert e._entry_filters("X")[0] == "MAYHEM_COIN"
    e.mayhem_of["X"] = False
    assert e._entry_filters("X")[0] == ""
    e.mayhem_of.pop("X")
    e.feeds["X"].board_state = "active"
    assert e._entry_filters("X")[0] == "MAYHEM_COIN"


def test_mayhem_board_feed_is_not_skipped():
    e = _eng()
    e.cfg.avoid_mayhem = True
    e.cfg.inflow_window_s = 0
    e.cfg.max_below_peak_pct = 0
    f = CM.Feed("MB", 10)
    f.origin, f.board_state = "auto", "active"
    e.feeds["MB"] = f
    e.cfg.auto_follow = "mayhem_board"
    assert e._entry_filters("MB")[0] == ""            # Mayhem board: its Mayhem coins are traded
    e.cfg.auto_follow = "mixed"
    assert e._entry_filters("MB")[0] == "MAYHEM_COIN"


def test_mayhem_board_pro_hold_feeds_one_coin_at_a_time():
    e = CM.CandleMomo()
    e.cfg.auto_follow, e.cfg.rule, e.cfg.max_mints, e.cfg.reuse_coin = "mayhem_board", "pro_hold", 3, False
    rows = [{"m": f"M{i}pump", "t": f"M{i}", "ms": "active", "age": 10 + i} for i in range(3)]
    e._pick(rows)
    assert len(e.feeds) == 1
    held = next(iter(e.feeds))
    e.positions[held] = {"mint": held}
    e._pick(rows[1:])
    assert set(e.feeds) == {held}                     # trade open: no new coin fed


def test_candle_sell_uses_wide_slippage():
    from src.execution.tx_route import sell_slippage_bps
    assert sell_slippage_bps("candle_sell") == 9500
    assert sell_slippage_bps("caller_closed") == 3000


def test_pro_hold_profit_sell_is_always_floored():
    e = CM.CandleMomo()
    e.cfg.sell_only_in_profit = False
    assert e._floored({"candleTs": 1, "pro_hold_sell": True})
    assert not e._floored({"candleTs": 1})
    assert not e._floored({"candleTs": 1, "pro_hold_sell": True, "manual": True})


def _paper_sell_engine(tmp_path, monkeypatch, value_ratio):
    import asyncio
    from src.signal.candle_store import Store
    from src.execution import pump_curve as PC
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "s.sqlite"), str(tmp_path / "s.jsonl"))
    e.cfg.fill_lag_ms = 0
    e.cfg.sell_only_in_profit = True
    e.positions["PS"] = {"mint": "PS", "symbol": "PS", "mode": "paper", "tokens": 1_000_000.0, "entry_sol": 0.005,
                         "entry_ts": time.time() - 10, "entry_signal": "x", "costs_sol": 0.0, "lots": [{"n": 1}]}
    curve = {"virtual_token": 1e15, "virtual_sol": 3e10}

    async def fake_curve(mint):
        return dict(curve), 100, ""
    monkeypatch.setattr(e, "_curve", fake_curve)

    async def fake_tx(*a, **k):
        return "", 0, ""
    monkeypatch.setattr(PC, "pump_sell_tx", fake_tx)
    monkeypatch.setattr(PC, "_sol_for_tokens", lambda raw, vt, vs, bps: int(0.005 * value_ratio * 1e9))
    return e, asyncio


def test_paper_sell_runs_and_books_in_profit(tmp_path, monkeypatch):
    e, asyncio = _paper_sell_engine(tmp_path, monkeypatch, 1.5)
    sig = {"id": "s1", "mint": "PS", "side": "SELL", "candleTs": 1, "ts": 1, "open": 1.0, "prevClose": 1.0, "close": 1.0, "ref": 1.0, "deltaPct": 0.0, "rule": "pro_hold", "provider": "stream", "feedLagMs": 0, "seen": time.time(), "pro_hold_sell": True,
           "symbol": "PS"}
    asyncio.run(e._paper_sell(sig, "take profit"))
    assert "PS" not in e.positions                                # sold, no NameError


def test_paper_profit_sell_below_breakeven_is_held(tmp_path, monkeypatch):
    e, asyncio = _paper_sell_engine(tmp_path, monkeypatch, 0.5)
    sig = {"id": "s2", "mint": "PS", "side": "SELL", "candleTs": 1, "ts": 1, "open": 1.0, "prevClose": 1.0, "close": 1.0, "ref": 1.0, "deltaPct": 0.0, "rule": "pro_hold", "provider": "stream", "feedLagMs": 0, "seen": time.time(), "pro_hold_sell": True,
           "symbol": "PS"}
    asyncio.run(e._paper_sell(sig, "take profit"))
    assert "PS" in e.positions                                    # slippage would make a loss: not sold


def test_candle_send_skips_the_separate_simulation():
    import asyncio
    from src.execution.trader import Trader
    calls = []

    class T:
        async def _simulate_first(self, b64, t):
            calls.append("sim")
            return ""

        async def _rpc_send_with_err(self, *a, **k):
            calls.append("send")
            return "SIG", None
    t = T()
    asyncio.run(Trader._send_keep_fee(t, "b64", fast=True, presim=False))
    assert "sim" not in calls and "send" in calls
    calls.clear()
    asyncio.run(Trader._send_keep_fee(t, "b64", fast=True))
    assert calls[0] == "sim"


def test_trade_source_label_and_store(tmp_path):
    from src.signal.candle_store import Store
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "t.sqlite"), str(tmp_path / "t.jsonl"))
    e.store.src_hook = e._src_label
    for origin, want in (("auto", "Candles (Mayhem board)"), ("mixed-movers", "Candles (Mixed: Movers)"),
                         ("dayscalp", "Dayscalp"), ("manual", "Candles (pinned)")):
        m = "S" + origin
        e.feeds[m] = CM.Feed(m, 10)
        e.feeds[m].origin = origin
        assert e._src_label(m) == want
    p = {"mint": "Sauto", "symbol": "S", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": 1.0,
         "entry_signal": "x"}
    e.store.open_position(p)
    assert p["src"] == "Candles (Mayhem board)"
    e.store.close_position("Sauto", {"mint": "Sauto", "symbol": "S", "mode": "paper", "entry_ts": 1, "exit_ts": 2,
                                     "entry_sol": 0.005, "exit_sol": 0.006, "pnl_sol": 0.001, "pct": 20.0})
    assert e.store.trades(1)[0]["src"] == "Candles (Mayhem board)"
