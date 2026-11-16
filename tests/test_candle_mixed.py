"""Candles page Mixed feed: New -> Movers (Graduated removed for now), youngest first, pump mints only."""
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
    assert c == ["BBBBpump", "AAAApump", "MOV2pump", "MOV1pump"]            # the Graduated board is not used


def test_turns_new_movers_no_graduated():
    e = _eng()
    rows = _rows()
    order = []
    for _ in range(4):
        e._pick(rows)
        (m,) = list(e.feeds)
        order.append(m)
        e.traded[m] = time.time() + 0.001          # its trade completed: next turn
    assert order == ["BBBBpump", "MOV2pump", "AAAApump", "MOV1pump"]
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


def test_is_paused_marks_coins_pump_paused():
    e = CM.CandleMomo()
    f = CM.Feed("PZ", 10)
    e.feeds["PZ"] = f
    assert not e.is_paused("PZ")
    f.board_state = "paused"
    assert e.is_paused("PZ")
    e.paused_mints["OLD"] = time.time()
    assert e.is_paused("OLD")


def test_paused_coin_is_held_not_sold_and_does_not_block_feeds():
    e = CM.CandleMomo()
    e.cfg.auto_follow, e.cfg.rule, e.cfg.max_mints, e.cfg.reuse_coin = "mayhem_board", "pro_hold", 1, False
    pos = {"mint": "PZ", "symbol": "PZ", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01, "entry_ts": time.time()}
    e.positions["PZ"] = pos
    e._pause_hold("PZ", pos, "paused")
    assert pos["paused_at"] and not pos.get("closing") and e.is_paused("PZ")
    assert e.active_positions() == {}
    rows = [{"m": "NEXTpump", "t": "N", "ms": "active", "age": 10}]
    e._pick(rows)
    assert set(e.feeds) == {"NEXTpump"}                   # the next coin comes; the paused one is only watched
    assert e.paused_view()[0]["mint"] == "PZ"


def test_paused_coin_resumes_the_trade():
    e = CM.CandleMomo()
    pos = {"mint": "PZ", "symbol": "PZ", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01, "entry_ts": time.time() - 500}
    e.positions["PZ"] = pos
    e._pause_hold("PZ", pos, "paused")
    e._pause_resume("PZ", pos)
    assert "paused_at" not in pos and pos["resumed_at"] and not e.is_paused("PZ")
    assert "PZ" in e.active_positions()


def test_paused_coin_gone_to_dust_is_left(monkeypatch):
    e = CM.CandleMomo()
    pos = {"mint": "PZ", "symbol": "PZ", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01, "entry_ts": time.time()}
    e.positions["PZ"] = pos
    e._pause_hold("PZ", pos, "paused")
    exits = []

    async def fx(mint, why):
        exits.append(why)
    monkeypatch.setattr(e, "_forced_exit", fx)
    import asyncio

    async def go():
        e._pause_leave("PZ", pos, 0.00001)
        await asyncio.sleep(0)
    asyncio.run(go())
    assert exits and "dust" in exits[0] and "paused_at" not in pos


def test_a_buy_in_flight_counts_as_the_open_trade():
    e = CM.CandleMomo()
    e.cfg.auto_follow, e.cfg.rule, e.cfg.max_mints, e.cfg.reuse_coin = "mayhem_board", "pro_hold", 1, False
    rows = [{"m": f"F{i}pump", "t": f"F{i}", "ms": "active", "age": 10 + i} for i in range(3)]
    e._pick(rows)
    first = next(iter(e.feeds))
    e.busy[first] = "sig-1"                          # its buy is being sent: not booked yet
    e._pick(rows[1:])
    assert set(e.feeds) == {first}                   # no second coin while the first buy is in flight


def test_manual_exit_sells_a_paused_coin(monkeypatch):
    e = CM.CandleMomo()
    pos = {"mint": "PX", "symbol": "PX", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01, "entry_ts": time.time()}
    e.positions["PX"] = pos
    e._pause_hold("PX", pos, "paused")
    started = []
    monkeypatch.setattr(e, "_start", lambda sig: started.append(sig) or "intent")
    out = e.manual("exit", "PX")
    assert out["ok"] and started and started[0]["side"] == "SELL" and started[0]["manual"]


def test_paper_lot_price_is_the_real_fill(tmp_path, monkeypatch):
    import asyncio
    from src.signal.candle_store import Store
    from src.execution import pump_curve as PC
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "f.sqlite"), str(tmp_path / "f.jsonl"))
    e.cfg.fill_lag_ms, e.cfg.max_slippage_bps = 0, 10_000
    calls = {"n": 0}

    async def fake_curve(mint):
        calls["n"] += 1
        return {"virtual_token": 1e15, "virtual_sol": 3e10}, 100, ""
    monkeypatch.setattr(e, "_curve", fake_curve)

    async def fake_tx(*a, **k):
        return "", 0, ""
    monkeypatch.setattr(PC, "pump_buy_tx", fake_tx)
    seq = iter([1000.0, 250.0])                        # expected at send, then got: price moved 4x before the fill
    monkeypatch.setattr(PC, "_tokens_for_sol", lambda *a: next(seq))
    sig = {"id": "b1", "mint": "LP", "side": "BUY", "seen": time.time(), "close": 1.0, "provider": "stream",
           "symbol": "LP", "ts": 1, "candleTs": 1, "open": 1.0, "prevClose": 1.0, "ref": 1.0, "deltaPct": 0.0,
           "rule": "pro_hold", "feedLagMs": 0}
    asyncio.run(e._paper_buy(sig))
    assert round(e.positions["LP"]["lots"][0]["px"], 6) == 4.0


def test_pinned_coin_not_tradable_is_not_refed_and_paused_coins_are_not_fed():
    e = CM.CandleMomo()
    e.cfg.auto_follow, e.cfg.rule = "none", "pro_hold"
    e.cfg.mints = ["NOTPUMPmint1234567890"]
    e.unusable["NOTPUMPmint1234567890"] = "graduated"
    e.positions["PZpump"] = {"mint": "PZpump", "paused_at": time.time()}
    e._pick([])
    assert "NOTPUMPmint1234567890" not in e.feeds and "PZpump" not in e.feeds
    assert any("not a pump.fun" in n["msg"] for n in e.notes)


def test_paused_state_survives_a_restart(tmp_path):
    from src.signal.candle_store import Store
    st = Store(str(tmp_path / "r.sqlite"), str(tmp_path / "r.jsonl"))
    e = CM.CandleMomo()
    e.store = st
    pos = {"mint": "RZ", "symbol": "RZ", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01, "entry_ts": time.time() - 900,
           "entry_signal": "x"}
    e.positions["RZ"] = pos
    st.open_position(pos)
    e._pause_hold("RZ", pos, "paused")
    restored = {p["mint"]: p for p in Store(str(tmp_path / "r.sqlite"), str(tmp_path / "r.jsonl")).positions()}
    assert restored["RZ"]["paused_at"] and restored["RZ"]["pause_state"] == "paused"
    e._pause_resume("RZ", pos)
    restored = {p["mint"]: p for p in Store(str(tmp_path / "r.sqlite"), str(tmp_path / "r.jsonl")).positions()}
    assert not restored["RZ"]["paused_at"]


def test_pinned_pump_coin_takes_the_feed_slot_when_flat():
    e = CM.CandleMomo()
    e.cfg.auto_follow, e.cfg.rule = "mayhem_board", "pro_hold"
    e.cfg.mints = ["PINNEDxpump"]
    rows = [{"m": "BOARDxpump", "t": "B", "ms": "active", "age": 5}]
    e._pick(rows)
    assert set(e.feeds) == {"PINNEDxpump"}
    e.positions["BOARDxpump"] = {"mint": "BOARDxpump"}
    e.feeds["BOARDxpump"] = CM.Feed("BOARDxpump", 10)
    e._pick(rows)
    assert set(e.feeds) == {"BOARDxpump"}            # a trade is open: the pinned coin waits


def test_paper_trades_a_graduated_coin_with_jupiter(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from src.signal.candle_store import Store
    from src.execution import pump_curve as PC
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "g.sqlite"), str(tmp_path / "g.jsonl"))
    e.cfg.fill_lag_ms, e.cfg.max_slippage_bps, e.cfg.sell_only_in_profit = 0, 10_000, False

    async def no_curve(mint):
        return None, 0, "graduated"
    monkeypatch.setattr(e, "_curve", no_curve)

    async def fake_tx(*a, **k):
        return "", 0, ""
    monkeypatch.setattr(PC, "pump_buy_tx", fake_tx)
    monkeypatch.setattr(PC, "pump_sell_tx", fake_tx)

    class J:
        async def quote_sol_to_token(self, mint, sol, slippage_bps=0):
            return SimpleNamespace(out_amount=1_000_000)
        async def quote_token_to_sol(self, mint, raw, slippage_bps=0):
            return SimpleNamespace(out_amount=6_000_000)          # 0.006 SOL back
    monkeypatch.setattr(e, "_jup", lambda: J())
    base = {"ts": 1, "candleTs": 1, "open": 1.0, "prevClose": 1.0, "ref": 1.0, "deltaPct": 0.0, "rule": "pro_hold",
            "provider": "pump_rest", "feedLagMs": 0, "symbol": "GR", "close": 1.0}
    asyncio.run(e._paper_buy(dict(base, id="gb", mint="GRAD", side="BUY", seen=time.time())))
    assert "GRAD" in e.positions and e.positions["GRAD"]["tokens"] == 1_000_000
    assert "GRAD" not in e.unusable
    asyncio.run(e._paper_sell(dict(base, id="gs", mint="GRAD", side="SELL", seen=time.time()), "test sell"))
    assert "GRAD" not in e.positions
    t = e.store.trades(1)[0]
    assert round(t["exit_sol"], 4) == 0.006


def test_paper_buys_a_non_sol_quoted_coin_with_jupiter(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from src.signal.candle_store import Store
    from src.execution import pump_curve as PC
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "q.sqlite"), str(tmp_path / "q.jsonl"))
    e.cfg.fill_lag_ms, e.cfg.max_slippage_bps = 0, 10_000
    e.cfg.buy_jupiter = True          # "Jupiter may buy too"; off (default) drops the coin as NOT_TRADABLE

    async def no_curve(mint):
        return None, 0, "not_sol_quote"
    monkeypatch.setattr(e, "_curve", no_curve)

    async def fake_tx(*a, **k):
        return "", 0, ""
    monkeypatch.setattr(PC, "pump_buy_tx", fake_tx)

    class J:
        async def quote_sol_to_token(self, mint, sol, slippage_bps=0):
            return SimpleNamespace(out_amount=5_000)
    monkeypatch.setattr(e, "_jup", lambda: J())
    sig = {"id": "nq", "mint": "NQxpump", "side": "BUY", "seen": time.time(), "close": 1.0, "provider": "stream",
           "symbol": "NQ", "ts": 1, "candleTs": 1, "open": 1.0, "prevClose": 1.0, "ref": 1.0, "deltaPct": 0.0,
           "rule": "pro_hold", "feedLagMs": 0}
    asyncio.run(e._paper_buy(sig))
    assert "NQxpump" in e.positions and "NQxpump" not in e.unusable


def test_untradable_offer_is_dropped(monkeypatch):
    e = CM.CandleMomo()
    e.cfg.support_enabled, e.cfg.rule = True, "pro_hold"
    monkeypatch.setattr(CM.CandleMomo, "live_filter", staticmethod(lambda: "opencopy"))
    monkeypatch.setattr(e, "support_source", lambda: "opencopy")
    monkeypatch.setattr(e, "_support_held", lambda s: [])
    import src.observability.dashboard as D
    monkeypatch.setattr(D, "openhold_recent_coins", lambda window_s=900.0: [])
    e.offer("opencopy", "BADxpump")
    e.unusable["BADxpump"] = "no route"
    e._pick([])
    assert "BADxpump" not in e.offers and "BADxpump" not in e.feeds


def test_non_sol_quoted_coin_tries_lightning_before_jupiter(monkeypatch):
    import asyncio
    from src.execution import tx_route as TR
    tried = []

    async def fake_one(trader, name, side, mint, *a, **k):
        tried.append(name)
        return {"status": "skip", "err": "lightning_400:refused" if name == "lightning" else "NO_ROUTES_FOUND"}
    monkeypatch.setattr(TR, "_one", fake_one)
    monkeypatch.setattr(TR, "_non_sol_quote", lambda m: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    monkeypatch.setattr(TR, "ensure_lightning_warm", lambda: None)
    monkeypatch.setattr(TR, "lightning_held", lambda m: "")
    monkeypatch.setattr(TR, "jupiter_buy_off", lambda side, reason: False)      # "Jupiter may buy too"
    out = asyncio.run(TR.run_swap(None, "buy", "Qx" + "a" * 38 + "pump", sol=0.005, reason="candle"))
    assert tried[:2] == ["lightning", "jupiter"] and not out["ok"]


def test_candle_buy_never_uses_jupiter_when_switch_on_but_sell_does(monkeypatch):
    """2026-11-15 (user): buys Lightning / Local only; Jupiter is a one-way fallback for sells."""
    import asyncio
    from src.execution import tx_route as TR
    tried = []

    async def fake_one(trader, name, side, mint, *a, **k):
        tried.append((side, name))
        return {"status": "skip", "err": 'lightning_400:{"errors":["Pool account not found"]}' if name == "lightning" else "NO_ROUTES_FOUND"}
    monkeypatch.setattr(TR, "_one", fake_one)
    monkeypatch.setattr(TR, "_non_sol_quote", lambda m: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    monkeypatch.setattr(TR, "ensure_lightning_warm", lambda: None)
    monkeypatch.setattr(TR, "lightning_held", lambda m: "")
    monkeypatch.setattr(TR, "jupiter_buy_off", lambda side, reason: side == "buy" and reason.startswith("candle"))
    mint = "Qy" + "a" * 38 + "pump"
    out = asyncio.run(TR.run_swap(None, "buy", mint, sol=0.005, reason="candle"))
    assert tried == [("buy", "lightning")] and not out["ok"]
    assert out["err"].startswith("no_route")                # Candles drops the coin and takes the next one
    tried.clear()
    asyncio.run(TR.run_swap(None, "sell", mint, raw=1000, reason="candle_sell"))
    assert ("sell", "jupiter") in tried


def test_jupiter_buy_off_follows_the_candles_switch(monkeypatch):
    from src.execution import tx_route as TR
    from src.signal import candle_momo as CM
    eng = CM.get_engine()
    monkeypatch.setattr(eng.cfg, "buy_jupiter", False, raising=False)
    assert TR.jupiter_buy_off("buy", "candle") and TR.jupiter_buy_off("buy", "candle_add")
    assert not TR.jupiter_buy_off("sell", "candle_sell") and not TR.jupiter_buy_off("buy", "dayscalp")
    monkeypatch.setattr(eng.cfg, "buy_jupiter", True, raising=False)
    assert not TR.jupiter_buy_off("buy", "candle")


def test_completed_coin_is_sold_not_held_as_paused(monkeypatch):
    """pump state "completed" (Mayhem run over) is not a pause: the held coin is sold off (user 2026-11-15)."""
    import asyncio
    e = CM.CandleMomo()
    sold = []

    async def fake_exit(mint, why):
        sold.append((mint, why))
    monkeypatch.setattr(e, "_forced_exit", fake_exit)

    async def run():
        pos = {"mint": "DONEpump", "symbol": "DONE", "mode": "paper", "tokens": 1000, "entry_sol": 0.005,
               "paused_at": time.time() - 60, "pause_state": "paused"}
        e.positions["DONEpump"] = pos
        e._completed_sell("DONEpump", pos)
        e._completed_sell("DONEpump", pos)          # already closing: one sell only
        await asyncio.sleep(0)
        return pos
    pos = asyncio.run(run())
    assert sold and sold[0][0] == "DONEpump" and "completed" in sold[0][1].lower() and len(sold) == 1
    assert pos["closing"] and pos["pause_state"] == "completed"


def test_paused_coin_moving_again_is_sold_from_the_watch_not_fed(monkeypatch):
    """G6S / PPAWS 2026-11-15: paused coins came back as open trades next to the new one. Now they stay on the
    Paused Coins watch and are sold there on profit or at Max hold; the feed stays the newest coin."""
    import asyncio
    e = CM.CandleMomo()
    sold = []

    async def fake_exit(mint, why):
        sold.append(why)
    monkeypatch.setattr(e, "_forced_exit", fake_exit)
    monkeypatch.setattr(e, "green_target", lambda: (10.0, "Min profit"))
    e.cfg.max_hold_s = 100
    assert e._resume_off_feed()

    async def run():
        pos = {"mint": "PAUS", "paused_at": time.time() - 20, "tokens": 1, "entry_sol": 0.005,
               "upct": -30.0, "value_at": time.time()}
        e.positions["PAUS"] = pos
        e._moving_exit("PAUS", pos)
        assert not sold and pos["paused_at"] and "PAUS" not in e.active_positions()     # in a loss: held, not fed
        pos["upct"] = 12.0
        e._moving_exit("PAUS", pos)
        await asyncio.sleep(0)
        assert len(sold) == 1 and "take profit" in sold[0]
        pos.pop("closing")
        pos["upct"], pos["moving_at"] = -30.0, time.time() - 101
        e._moving_exit("PAUS", pos)
        await asyncio.sleep(0)
        assert len(sold) == 2 and "max hold" in sold[1]
    asyncio.run(run())


def test_completed_dust_coin_is_left_not_retried(monkeypatch):
    """CHIMP 2026-11-16: a completed coin worth 0.00008 SOL could not be sold; 124 tries held the slot 16 minutes."""
    e = CM.CandleMomo()
    left = []
    monkeypatch.setattr(e, "_pause_leave", lambda m, p, v: left.append((m, v)))
    pos = {"mint": "DUSTpump", "mode": "paper", "tokens": 1, "entry_sol": 0.005, "value_sol": 0.00008}
    e.positions["DUSTpump"] = pos
    e._completed_sell("DUSTpump", pos)
    assert left == [("DUSTpump", 0.00008)] and not pos.get("closing")


def test_pause_recorder_writes_one_line_per_read(tmp_path, monkeypatch):
    import json
    (tmp_path / "logs").mkdir()
    e = CM.CandleMomo()
    e._watch_path = str(tmp_path / "logs" / "pause_watch.jsonl")
    pos = {"mint": "RECpump", "symbol": "REC", "mode": "paper", "entry_ts": time.time() - 4, "entry_sol": 0.005,
           "value_sol": 0.0046, "upct": -8.0}
    e._watch_rec("RECpump", pos, {"state": "active", "stats": {"buyCount": 3}})
    e._watch_rec("RECpump", pos, {"state": "paused"}, "paused")
    e._watch_rec("RECpump", pos, "not a dict")                      # never raises
    rows = [json.loads(l) for l in open(tmp_path / "logs" / "pause_watch.jsonl")]
    assert len(rows) == 3 and rows[0]["state"]["stats"]["buyCount"] == 3 and rows[1]["event"] == "paused"
    assert 3 <= rows[0]["held_s"] <= 6 and rows[2]["state"] is None


def test_a_trade_on_a_held_coin_runs_the_rules_at_once(monkeypatch):
    """Instant take-profit: the 0.5 s wait between polls ends on a trade of the held coin."""
    import asyncio
    from src.signal import candle_core as C
    e = CM.CandleMomo()
    f = CM.Feed("HOLDpump", 50)
    f.using = "stream"
    e.feeds["HOLDpump"] = f
    e.positions["HOLDpump"] = {"mint": "HOLDpump", "tokens": 1, "entry_sol": 0.005}
    calls = []
    monkeypatch.setattr(e, "_evaluate", lambda feed, seen: calls.append((time.time(), e._instant)))

    async def fetch(mint, cfg):
        return [C.Candle(t=1000, open=1.0, high=1.2, low=1.0, close=1.2, volume=1, final=False)]
    monkeypatch.setattr(e.providers["stream"], "fetch", fetch)

    async def run():
        e.loop = asyncio.get_running_loop()
        t0 = time.time()
        task = asyncio.ensure_future(e._idle("HOLDpump", f, 0.5))
        await asyncio.sleep(0.05)
        e._on_stream_trade({"mint": "HOLDpump", "trade_px": 1.2, "ts": time.time()})
        await asyncio.sleep(0.1)
        assert len(calls) == 1 and calls[0][0] - t0 < 0.3 and calls[0][1] is True      # evaluated on the trade
        e._on_stream_trade({"mint": "OTHERpump", "trade_px": 1.0})                     # not held: nothing
        await task
        assert len(calls) == 1 and not e._instant
        # flat coin: a plain wait, no evaluation
        e.positions.clear()
        await e._idle("HOLDpump", f, 0.05)
        assert len(calls) == 1
    asyncio.run(run())


def test_two_losses_in_a_row_pause_new_trades_then_take_the_newest(monkeypatch):
    """User 2026-11-16, common to every source / feed / rule: 2 losing trades in a row -> no new trade for 30 s; flat,
    nothing is watched meanwhile, so the pick after the pause is the newest coin then."""
    e = CM.CandleMomo()
    assert e.cfg.loss_pause_n == 2 and e.cfg.loss_pause_s == 30.0
    e._note_result(-0.001, "A")
    assert e.loss_pause_left() == 0                      # one loss: nothing yet
    e._note_result(+0.002, "B")                          # a win resets the count
    e._note_result(-0.001, "C")
    assert e.loss_pause_left() == 0
    e._note_result(-0.001, "D")                          # second loss in a row
    assert 29 < e.loss_pause_left() <= 30 and e.loss_streak == 0
    # no new trade while paused; a manual Entry still goes through the gate
    f = CM.Feed("NEWpump", 10)
    f.status = "OK"
    e.feeds["NEWpump"] = f
    monkeypatch.setattr(e, "_start", lambda sig: "intent")
    sig = {"id": "s1", "mint": "NEWpump", "side": "BUY", "pro": True, "_counted": True, "deltaPct": 0.0}
    assert e._route(dict(sig)) == "LOSS_PAUSE"
    # flat and paused: the watched coin is dropped and nothing is picked
    picked = []
    monkeypatch.setattr(e, "_pick_feeds", lambda rows: picked.append(1))
    e._pick([])
    assert not e.feeds and not picked
    # after the pause the normal pick runs again (the newest coin then)
    e.loss_pause_until = time.time() - 1
    monkeypatch.setattr(e, "_single_feed", lambda: None)
    monkeypatch.setattr(e, "_name_feeds", lambda: None)
    e._pick([])
    assert picked == [1]
    # off switch
    e.cfg.loss_pause_n = 0
    e._note_result(-1, "X"); e._note_result(-1, "Y"); e._note_result(-1, "Z")
    assert e.loss_pause_left() == 0
