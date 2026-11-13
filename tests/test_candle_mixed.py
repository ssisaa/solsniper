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
