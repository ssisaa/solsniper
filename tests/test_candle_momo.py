"""Candle-momentum trader: rule matrix, parsing, dedupe, sizing, replay, chaos, restart."""
import asyncio
import json
import os
import time

import pytest

from src.signal import candle_core as C
from src.signal.candle_store import Store

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "candles_mayhem_20261110.json")


def K(t, o, c, final=True):
    return C.Candle(t=t, open=o, high=max(o, c), low=min(o, c), close=c, volume=1.0, final=final)


# ---------------------------------------------------------------- rule matrix
@pytest.mark.parametrize("rule,prev,cur,delta,side", [
    ("own_candle", None, K(1000, 1.0, 1.01), 0.5, "BUY"),
    ("own_candle", None, K(1000, 1.0, 0.99), 0.5, "SELL"),
    ("own_candle", None, K(1000, 1.0, 1.004), 0.5, ""),          # below threshold
    ("own_candle", None, K(1000, 1.0, 1.005), 0.5, "BUY"),       # exactly at threshold
    ("own_candle", None, K(1000, 1.0, 1.0), 0.0, ""),            # flat never signals
    ("own_candle", None, K(1000, 1.0, 1.5, final=False), 0.5, ""),  # live candle: not yet
    ("inverse", None, K(1000, 1.0, 1.01), 0.5, "SELL"),
    ("inverse", None, K(1000, 1.0, 0.99), 0.5, "BUY"),
    ("literal", K(0, 1.0, 1.0), K(1000, 1.01, 1.01), 0.5, "BUY"),
    ("literal", K(0, 1.0, 1.0), K(1000, 0.99, 0.99), 0.5, "SELL"),
    ("literal", K(0, 1.0, 1.0), K(1000, 1.0, 2.0), 0.5, ""),    # open == prev close (pump's feed)
    ("literal", None, K(1000, 1.0, 2.0), 0.5, ""),
])
def test_rule_matrix(rule, prev, cur, delta, side):
    assert C.evaluate(rule, prev, cur, delta)[0] == side


def test_delta_value():
    side, d, ref = C.evaluate("own_candle", None, K(1000, 2.0, 2.05), 0.5)
    assert side == "BUY" and d == pytest.approx(2.5) and ref == 2.0


# ---------------------------------------------------------------- parsing
def test_parse_pump_body():
    body = {"candles": [
        {"openTimeMs": 1794310402000, "open": "0.0000023", "high": "0.0000025", "low": "0.0000023",
         "close": "0.0000025", "volumeUsd": "23.0", "final": True},
        {"openTimeMs": 1794310403000, "open": "0.0000025", "close": "0.0000020", "final": False},
        {"openTimeMs": "x", "open": "1", "close": "1"},
        {"openTimeMs": 1794310404000, "open": "0", "close": "1"},
        {"openTimeMs": 1794310405000, "open": "nan", "close": "1"},
        "junk",
    ]}
    rows = C.parse_pump(body)
    assert [r.t for r in rows] == [1794310402000, 1794310403000]
    assert rows[0].volume == 23.0 and rows[0].final and not rows[1].final
    assert rows[1].high == 0.0000025 and rows[1].low == 0.0000020   # missing high/low filled


def test_parse_seconds_and_empty():
    assert C.parse_pump({"candles": [{"t": 1794310402, "open": 1, "close": 2}]})[0].t == 1794310402000
    assert C.parse_pump({}) == [] and C.parse_pump(None) == [] and C.parse_pump({"candles": None}) == []


# ---------------------------------------------------------------- buffer: dupes / order / gaps
def test_book_duplicates_out_of_order_and_live_update():
    b = C.CandleBook(5)
    assert b.merge([K(1000, 1, 1.1), K(2000, 1.1, 1.2, final=False)]) == 2
    assert b.merge([K(2000, 1.1, 1.3, final=True)]) == 0      # live candle replaced by its final
    assert b.rows[-1].close == 1.3 and b.rows[-1].final
    b.merge([K(2000, 1.1, 9.9)])                                # a different 'final' for the same t
    assert b.rows[-1].close == 1.3 and b.duplicates == 1       # first final kept
    b.merge([K(500, 1, 1)])                                     # stale response (older than newest)
    assert b.out_of_order == 1
    for t in range(3000, 9000, 1000):
        b.merge([K(t, 1, 1)])
    assert len(b.rows) == 5 and b.rows[-1].t == 8000
    b.merge([K(1000, 1, 1)])                                    # older than the window: dropped
    assert b.rows[0].t == 4000


def test_signal_id_deterministic():
    a = C.signal_id("M", 1000, "own_candle")
    assert a == C.signal_id("M", 1000, "own_candle")
    assert a != C.signal_id("M", 2000, "own_candle") and a != C.signal_id("M", 1000, "inverse")


# ---------------------------------------------------------------- config schema / sizing
def test_config_validation():
    cfg, errs = C.validate({"rule": "own_candle", "min_delta_pct": "1.5", "poll_ms": 50, "mode": "yolo",
                            "mints": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAApump bad", "nope": 1, "kill": "on"})
    assert cfg.min_delta_pct == 1.5 and cfg.kill is True
    assert cfg.poll_ms == 500 and cfg.mode == "paper" and cfg.mints == []
    assert any("poll_ms" in e for e in errs) and any("mode" in e for e in errs)
    assert any("not a mint" in e for e in errs) and any("unknown field nope" in e for e in errs)
    cfg2, errs2 = C.validate({"buy_size_sol": 0.5, "max_trade_sol": 0.1})
    assert cfg2.buy_size_sol == 0.1 and errs2                  # size capped by max per trade


def test_sell_ratio_bounds():
    assert C.validate({"sell_ratio": 0.25})[0].sell_ratio == 0.25
    cfg, errs = C.validate({"sell_ratio": 1.5})
    assert cfg.sell_ratio == 1.0 and errs


# ---------------------------------------------------------------- replay (golden)
def _fixture():
    with open(FIX) as fh:
        data = json.load(fh)
    return {m: [K(t, o, c) for t, o, c in rows] for m, rows in data.items()}


def test_replay_golden():
    data = _fixture()
    got = {}
    for rule in ("own_candle", "inverse", "literal"):
        sigs = [s for m, rows in sorted(data.items()) for s in C.replay(rows, rule, 0.5, m)]
        got[rule] = (sum(s["action"] == "enter" for s in sigs), sum(s["action"] == "exit" for s in sigs), len(sigs))
    # Golden values recorded 2026-11-10 from 30 Mayhem coins (1,174 final candles).
    assert got["literal"] == (0, 0, 0)          # pump's feed: open == previous close, always
    assert got == GOLDEN, got


def test_replay_chunking_is_deterministic():
    rows = next(iter(_fixture().values()))
    one = C.replay(rows, "own_candle", 0.5, "m", chunk=1)
    assert one == C.replay(rows, "own_candle", 0.5, "m", chunk=1)
    # A whole history read at once is history, not signals.
    assert C.replay(rows, "own_candle", 0.5, "m", chunk=len(rows)) == []


GOLDEN = {"own_candle": (269, 264, 1069), "inverse": (286, 261, 1069), "literal": (0, 0, 0)}


# ---------------------------------------------------------------- store: dedupe, restart, exactly once
def test_store_claim_exactly_once_and_restart(tmp_path):
    db, au = str(tmp_path / "c.sqlite"), str(tmp_path / "a.jsonl")
    s = Store(db, au)
    assert s.claim("sig-1") is True and s.claim("sig-1") is False
    s.open_position({"mint": "M", "symbol": "X", "mode": "paper", "tokens": 10.0, "entry_sol": 0.01,
                     "entry_ts": time.time(), "entry_signal": "sig-1", "costs_sol": 0.0002})
    s.audit("signal", signalId="sig-1")
    s2 = Store(db, au)                                     # restart
    assert s2.claimed("sig-1") and s2.claim("sig-1") is False
    assert [p["mint"] for p in s2.positions()] == ["M"]
    s2.close_position("M", {"mint": "M", "entry_sol": 0.01, "exit_sol": 0.011, "pnl_sol": 0.0009, "pct": 9.0,
                            "costs_sol": 0.0004, "gross_pnl_sol": 0.0013, "exit_ts": time.time(), "paused_exit": 0})
    assert s2.positions() == [] and s2.totals()["trades"] == 1 and s2.totals()["wins"] == 1
    with open(au) as fh:
        assert json.loads(fh.readline())["signalId"] == "sig-1"


# ---------------------------------------------------------------- engine routing (no network)
@pytest.fixture
def engine(tmp_path, monkeypatch):
    from src.signal import candle_momo as M
    e = M.CandleMomo()
    e.store = Store(str(tmp_path / "e.sqlite"), str(tmp_path / "e.jsonl"))
    started = []
    monkeypatch.setattr(e, "_start", lambda sig: started.append(sig) or "intent")
    f = M.Feed("MINT", 10)
    f.status = "OK"
    e.feeds["MINT"] = f
    e._started = started
    e.cfg.inflow_window_s = 0              # mechanics tests: the entry filters have their own test
    e.cfg.max_below_peak_pct = 0
    return e


def _sig(side, t=1000, mint="MINT"):
    return {"id": C.signal_id(mint, t, "own_candle"), "mint": mint, "ts": 0, "side": side, "open": 1.0,
            "prevClose": 1.0, "close": 1.01, "deltaPct": 1.0, "candleTs": t, "rule": "own_candle",
            "provider": "pump_rest", "feedLagMs": 900, "seen": time.time()}


def test_route_matrix(engine):
    assert engine._route(_sig("SELL")) == "NO_POSITION"
    assert engine._route(_sig("BUY", 2000)) == "intent"
    engine.positions["MINT"] = {"tokens": 1.0, "entry_sol": 0.01}
    assert engine._route(_sig("BUY", 3000)) == "ALREADY_LONG"
    assert engine._route(_sig("SELL", 4000)) == "intent"


def test_route_blocks(engine):
    engine.feeds["MINT"].status = "DEGRADED"
    assert engine._route(_sig("BUY", 1)) == "FEED_DEGRADED"
    engine.feeds["MINT"].status = "STALE"
    assert engine._route(_sig("BUY", 2)) == "STALE_FEED"
    engine.feeds["MINT"].status = "OK"
    engine.cfg.entries_paused = True
    assert engine._route(_sig("BUY", 3)) == "ENTRIES_PAUSED"
    engine.cfg.entries_paused = False
    engine.last_trade_at["MINT"] = time.time()
    assert engine._route(_sig("BUY", 4)) == "COOLDOWN"
    engine.last_trade_at.clear()
    engine.cfg.blacklist = ["MINT"]
    assert engine._route(_sig("BUY", 5)) == "BLACKLIST"
    engine.cfg.blacklist = []
    engine.cfg.mode = "live"
    assert engine._route(_sig("BUY", 6)) == "NOT_READY"          # live needs the bot's trader / manager
    engine.cfg.mode = "paper"
    engine.cfg.kill = True
    t0 = time.time()
    assert engine._route(_sig("BUY", 7)) == "KILLED" and time.time() - t0 < 1.0
    engine.cfg.kill = False
    engine.store.claim(_sig("BUY", 8)["id"])
    assert engine._route(_sig("BUY", 8)) == "DUPLICATE"
    assert engine._started == []


def test_daily_loss_breaker(engine):
    engine.store.close_position("X", {"mint": "X", "mode": "paper", "entry_sol": 0.1, "exit_sol": 0.0, "pnl_sol": -0.1,
                                      "pct": -100, "costs_sol": 0, "gross_pnl_sol": -0.1, "exit_ts": time.time(),
                                      "paused_exit": 0})
    assert engine._route(_sig("BUY", 9)) == "intent"                # paper is never blocked by losses
    engine.store.close_position("Y", {"mint": "Y", "mode": "live", "entry_sol": 0.1, "exit_sol": 0.0, "pnl_sol": -0.1,
                                      "pct": -100, "costs_sol": 0, "gross_pnl_sol": -0.1, "exit_ts": time.time(),
                                      "paused_exit": 0})
    engine.cfg.mode = "live"
    engine.trader = engine.manager = object()
    assert engine._route(_sig("BUY", 10)) == "DAILY_LOSS"             # live losses block live buys


def test_coalesce_in_flight(engine):
    engine.busy["MINT"] = "x"
    engine.positions["MINT"] = {"tokens": 1.0, "entry_sol": 0.01}
    assert engine._route(_sig("SELL", 10)) == "queued"
    assert engine._route(_sig("SELL", 11)) == "queued"           # the older queued one is dropped
    assert engine.pending["MINT"]["candleTs"] == 11 and engine.m["skips"].get("COALESCED") == 1


# ---------------------------------------------------------------- chaos: feed outage, malformed, stale
def test_feed_outage_degrades_and_recovers(engine, monkeypatch):
    from src.signal import candle_momo as M
    e = engine
    e.cfg.fallback = ""
    calls = {"n": 0}

    class Flaky(M.CandleProvider):
        name = "pump_rest"

        async def fetch(self, mint, cfg):
            calls["n"] += 1
            if 2 <= calls["n"] <= 5:
                raise ValueError("HTTP 500")
            if calls["n"] == 6:
                raise json.JSONDecodeError("bad", "x", 0)
            now = int(time.time()) * 1000
            return [K(now - 3000, 1, 1), K(now - 2000, 1, 1.0)]

    e.providers["pump_rest"] = Flaky()
    e.cfg.poll_ms = 200
    statuses = []

    async def go():
        for _ in range(8):
            await e._poll_one("MINT")
            statuses.append(e.feeds["MINT"].status)
    asyncio.run(go())
    assert "DEGRADED" in statuses and statuses[-1] == "OK"
    assert e.m["poll_errors"] == 5
    assert e._route(_sig("BUY", 99)) == "intent"


def test_fallback_provider_takes_over(engine):
    from src.signal import candle_momo as M
    e = engine

    class Down(M.CandleProvider):
        name = "pump_rest"

        async def fetch(self, mint, cfg):
            raise ValueError("HTTP 429")

    e.providers["pump_rest"] = Down()
    st = e.providers["stream"]
    st.wanted = {"MINT"}
    base = int(time.time()) - 3                       # whole seconds: exactly two 1s candles
    for dt, px in ((0.1, 1.0), (0.5, 1.02), (1.2, 1.02), (1.7, 1.0)):
        st.on_trade({"mint": "MINT", "price_sol": px, "ts": base + dt, "sol": 0.01})
    e.cfg.poll_ms = 200

    async def go():
        for _ in range(4):
            await e._poll_one("MINT")
    asyncio.run(go())
    f = e.feeds["MINT"]
    assert f.using == "stream" and f.status in ("FALLBACK", "STALE")
    assert len(f.books["stream"].rows) == 2


def test_rate_limit_counts_intents_only(engine):
    engine.cfg.max_signals_per_min = 2
    for t in range(20, 40):                       # SELLs with nothing to close do not count
        engine._route(_sig("SELL", t))
    engine.sig_times.extend([time.time(), time.time()])
    assert engine._route(_sig("BUY", 50)) == "RATE_LIMIT"
    engine.sig_times.clear()
    assert engine._route(_sig("BUY", 51)) == "intent"



# ---------------------------------------------------------------- live path (fake trader / manager)
import types as _types


class _Mgr:
    def __init__(self):
        self.positions = {}
        self.paused_until = 0
        self.kicks = 0

    def set_price(self, *a):
        pass

    def _push_remote(self):
        pass

    def kick_now(self, pos, reason):
        assert reason == "candle_sell"
        self.kicks += 1
        pos._sol_out = 0.0062
        pos._exit_fee = 0.00001
        pos.status = "CLOSED"
        return True


class _Trader:
    def __init__(self, mgr, ok=True, err=""):
        self.mgr, self.ok, self.err, self.calls = mgr, ok, err, []

    async def buy(self, mint, size_multiplier, source, ref_price):
        self.calls.append(source)
        if not self.ok:
            return _types.SimpleNamespace(ok=False, error=self.err)
        p = _types.SimpleNamespace(id="p1", mint=mint, status="OPEN", source=source, strategy=source, symbol="",
                                   fees_sol=0.00002, entry_sol=0.005, tokens=1e9, _sell_busy=False, name="")
        self.mgr.positions["p1"] = p
        return _types.SimpleNamespace(ok=True, sol_in=0.005, tokens_out=1e9, fees_sol=0.00002, signature="SIG")


@pytest.fixture
def live(tmp_path, monkeypatch):
    from src.signal import candle_momo as M
    import src.config.runtime as rt
    import src.observability.ledger as led
    e = M.CandleMomo()
    e.store = Store(str(tmp_path / "l.sqlite"), str(tmp_path / "l.jsonl"))
    e.manager = _Mgr()
    e.trader = _Trader(e.manager)
    f = M.Feed("MINT", 10)
    f.status = "OK"
    e.feeds["MINT"] = f
    e.cfg.mode = "live"
    e.cfg.sell_only_in_profit = False
    e.cfg.inflow_window_s = 0
    e.cfg.max_below_peak_pct = 0          # these tests check the sell mechanics, not the profit gate
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(trade_enabled=True, max_position_sol=0.005))
    monkeypatch.setattr(led, "get_ledger", lambda: _types.SimpleNamespace(record_open=lambda **k: None))
    return e


def test_live_gates(live, monkeypatch):
    import src.config.runtime as rt
    assert live.live_block() == ""
    live.manager.paused_until = time.time() + 60
    assert live.live_block() == ""                               # no circuit breaker for Candles
    live.manager.paused_until = 0
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(trade_enabled=False))
    assert live.live_block() == ""                               # Candles Live does not need Auto Trade

    async def go():
        r = live._route(_sig("BUY", 70))
        while live.busy:
            await asyncio.sleep(0.01)
        return r
    assert asyncio.run(go()) == "intent" and live.trader.calls == ["candle"]


def test_live_round_trip(live):
    async def go():
        s = _sig("BUY", 80)
        assert live._route(s) == "intent"
        while live.busy:
            await asyncio.sleep(0.01)
        assert live.trader.calls == ["candle"]
        pos = live.manager.positions["p1"]
        assert pos.source == "candle" and pos.strategy == "candle" and pos.stop_pct is None
        assert live.positions["MINT"]["mode"] == "live"
        live.last_trade_at.clear()
        assert live._route(_sig("SELL", 81)) == "intent"
        while live.busy:
            await asyncio.sleep(0.01)
    asyncio.run(go())
    assert live.manager.kicks == 1 and "MINT" not in live.positions
    t = live.store.trades(1)[0]
    assert t["mode"] == "live" and t["exit_sol"] == pytest.approx(0.0062)
    assert t["pnl_sol"] == pytest.approx(0.0062 - 0.005 - 0.00002 - 0.00001)


def test_live_buy_failure_is_recorded(live):
    live.trader = _Trader(live.manager, ok=False, err="Transaction simulation failed: 6042")

    async def go():
        live._route(_sig("BUY", 90))
        while live.busy:
            await asyncio.sleep(0.01)
    asyncio.run(go())
    assert "MINT" not in live.positions and live.m["exec_failed"] == 1


def test_max_hold_forces_exit(live):
    live.cfg.max_hold_s = 1
    live.positions["MINT"] = {"mint": "MINT", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01,
                              "entry_ts": time.time() - 5, "entry_signal": "x"}
    called = []

    async def fake_exit(mint, why):
        called.append(why)
    live._forced_exit = fake_exit
    live.feeds.clear()

    async def go():
        t = asyncio.ensure_future(live._value_loop())
        await asyncio.sleep(2.3)
        t.cancel()
    asyncio.run(go())
    assert called and called[0].startswith("max hold")


def test_live_untradable_coin_is_dropped(live):
    live.trader = _Trader(live.manager, ok=False, err="not_sol_quote")

    async def go():
        live._route(_sig("BUY", 95))
        while live.busy:
            await asyncio.sleep(0.01)
    asyncio.run(go())
    assert "MINT" in live.unusable and "MINT" not in live.feeds and live.m["skips"].get("NOT_TRADABLE") == 1


def test_forced_live_sell_failure_does_not_crash(live):
    class Stuck(_Mgr):
        def kick_now(self, pos, reason):
            return True                     # sell started but never lands
    live.manager = Stuck()
    p = _types.SimpleNamespace(id="p9", mint="MINT", status="OPEN", source="candle", strategy="candle", _sell_busy=False)
    live.manager.positions["p9"] = p
    live.positions["MINT"] = {"mint": "MINT", "mode": "live", "tokens": 1.0, "entry_sol": 0.005,
                              "entry_ts": time.time() - 100, "entry_signal": "x", "symbol": "S"}
    import src.signal.candle_momo as M
    orig = time.time
    t0 = orig()
    M.time = _types.SimpleNamespace(time=lambda: orig() + (31 if orig() - t0 > 0.3 else 0), sleep=time.sleep)
    try:
        asyncio.run(live._forced_exit("MINT", "max hold 60s"))
    finally:
        M.time = time
    assert "MINT" in live.positions and live.m["exec_failed"] == 1


# ---------------------------------------------------------------- inverse ladder
def _ladder_engine(engine):
    engine.cfg.ladder_count = "in_a_row"           # these tests cover the in-a-row mode
    engine.cfg.rule = "inverse"
    engine.cfg.ladder_enabled = True
    engine.cfg.max_ladder_buys = 4
    engine.cfg.min_red_count_to_ladder = 2
    engine.cfg.ladder_max_total_sol = 0.03
    engine.cfg.buy_size_sol = 0.005
    return engine


def test_ladder_state_machine(engine):
    e = _ladder_engine(engine)                      # reds_per_lot 3: lots on reds 1, 4, 7, 10
    e.cfg.max_ladder_buys = 3
    started = e._started
    assert e._route(_sig("BUY", 100)) == "intent"                 # red 1 (flat): lot 1
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0, "lots": [{"n": 1}]}
    assert e._route(_sig("BUY", 101)) == "LADDER_WAIT"            # red 2
    assert e._route(_sig("BUY", 102)) == "LADDER_WAIT"            # red 3
    assert e._route(_sig("BUY", 103)) == "intent"                 # red 4: lot 2
    assert started[-1]["add"] and started[-1]["lot_no"] == 2
    e.positions["MINT"]["lots"].append({"n": 2})
    e.positions["MINT"]["entry_sol"] = 0.01
    assert e._route(_sig("BUY", 104)) == "LADDER_WAIT"            # red 5
    assert e._route(_sig("BUY", 105)) == "LADDER_WAIT"            # red 6
    assert e._route(_sig("BUY", 106)) == "intent"                 # red 7: lot 3
    e.positions["MINT"]["lots"].append({"n": 3})
    e.positions["MINT"]["entry_sol"] = 0.015
    for t in (107, 108, 109):
        assert e._route(_sig("BUY", t)) in ("LADDER_WAIT", "LADDER_MAX")
    assert e._route(_sig("BUY", 109 + 1)) == "LADDER_MAX"          # red 11: 3 lots used, no more buys
    # green resets the run; red 1 of a new run while holding waits
    assert e._route(_sig("SELL", 120)) == "intent"
    assert e.streak["MINT"] == ["SELL", 1]
    e.positions["MINT"]["lots"] = [{"n": 1}]
    e.positions["MINT"]["entry_sol"] = 0.005
    assert e._route(_sig("BUY", 121)) == "LADDER_WAIT"


def test_ladder_cap_and_martingale(engine):
    e = _ladder_engine(engine)
    e.cfg.ladder_size_mode = "martingale"
    e.cfg.ladder_max_total_sol = 0.012
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.01, "tokens": 1.0, "lots": [{"n": 1}, {"n": 2}]}
    e.streak["MINT"] = ["BUY", 6]                                 # next red is red 7: a lot red
    # lot 3 in martingale = 2x = 0.01 -> 0.02 total > cap 0.012
    assert e._route(_sig("BUY", 110)) == "LADDER_CAP"


def test_green_confirm(engine):
    e = _ladder_engine(engine)
    e.cfg.sell_on_green_confirm = 2
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0, "lots": [{"n": 1}]}
    assert e._route(_sig("SELL", 120)) == "GREEN_CONFIRM"
    assert e._route(_sig("SELL", 121)) == "intent"


def test_ladder_only_for_inverse(engine):
    engine.cfg.rule = "own_candle"
    engine.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0}
    engine.streak["MINT"] = ["BUY", 5]
    assert engine._route(_sig("BUY", 130)) == "ALREADY_LONG"


def test_store_keeps_lots(tmp_path):
    s = Store(str(tmp_path / "x.sqlite"), str(tmp_path / "x.jsonl"))
    s.open_position({"mint": "M", "mode": "paper", "tokens": 2.0, "entry_sol": 0.01, "entry_ts": 1.0,
                     "entry_signal": "a", "lots": [{"n": 1, "sol": 0.005}, {"n": 2, "sol": 0.005}]})
    assert [l["n"] for l in Store(str(tmp_path / "x.sqlite"), str(tmp_path / "x.jsonl")).positions()[0]["lots"]] == [1, 2]


def test_live_ladder_add_merges_and_sells_together(live, monkeypatch):
    import src.execution.tx_route as TR
    import src.execution.wallet_tokens as WT
    import src.execution.signer as SG
    live.cfg.rule = "inverse"
    live.cfg.ladder_count = "in_a_row"
    live.cfg.ladder_max_total_sol = 0.02
    held = {"raw": 1e9}

    async def fake_swap(trader, side, mint, sol=0.0, raw=0, reason="", full=True):
        assert side == "buy" and reason == "candle_add"
        held["raw"] = 2.1e9
        return {"ok": True, "sig": "S2", "tokens": 1.1e9, "sol": sol}

    async def holding(pk, mint, **kw):
        return {"raw": held["raw"]}

    async def bal(fast=False):
        return 0.05
    monkeypatch.setattr(TR, "run_swap", fake_swap)
    monkeypatch.setattr(WT, "get_token_holding", holding)
    monkeypatch.setattr(SG, "pubkey_from_env", lambda: "PK")
    live.trader.get_sol_balance = bal
    added = []
    import src.observability.ledger as led
    monkeypatch.setattr(led, "get_ledger", lambda: _types.SimpleNamespace(
        record_open=lambda **k: None,
        add_to_open=lambda mint, sol, fee_sol, tokens, signature='': added.append((mint, sol, fee_sol, tokens))))

    async def go():
        live._route(_sig("BUY", 200))                       # red 1 -> lot 1 (trader.buy)
        while live.busy:
            await asyncio.sleep(0.01)
        live._route(_sig("BUY", 201))                       # red 2 -> wait
        live._route(_sig("BUY", 202))                       # red 3 -> wait
        live._route(_sig("BUY", 204))                       # red 4 -> lot 2 (run_swap)
        while live.busy:
            await asyncio.sleep(0.01)
        rec = live.positions["MINT"]
        assert len(rec["lots"]) == 2 and rec["entry_sol"] == pytest.approx(0.01)
        pos = live.manager.positions["p1"]
        assert pos.tokens == pytest.approx(2.1e9) and pos.entry_sol == pytest.approx(0.01)
        live._route(_sig("SELL", 203))                      # green -> one sell for both lots
        while live.busy:
            await asyncio.sleep(0.01)
    asyncio.run(go())
    assert added and added[0][1] == pytest.approx(0.005)
    assert added[0][2] < 0.001 and added[0][3] > 1e6       # fee is a fee, tokens are tokens (were swapped)
    t = live.store.trades(1)[0]
    assert t["lots"] == 2 and t["entry_sol"] == pytest.approx(0.01) and live.manager.kicks == 1


def test_sell_only_in_profit(engine, monkeypatch):
    e = _ladder_engine(engine)
    e.cfg.sell_only_in_profit = True
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.01, "tokens": 1e9, "lots": [{"n": 1}, {"n": 2}]}
    vals = {"v": 0.006}

    async def fake_curve(mint):
        return {"virtual_token": 1, "virtual_sol": 1}, 100, ""
    import src.execution.pump_curve as PC
    monkeypatch.setattr(e, "_curve", fake_curve)
    monkeypatch.setattr(PC, "_sol_for_tokens", lambda t, vt, vs, b: int(vals["v"] * 1e9))
    s1 = _sig("SELL", 300)
    assert asyncio.run(e._in_profit(s1)) is False and s1["reason"].startswith("NOT_IN_PROFIT")
    vals["v"] = 0.0102                                   # +2%: below the 5% buffer -> hold
    assert asyncio.run(e._in_profit(_sig("SELL", 301))) is False
    vals["v"] = 0.0108                                   # +8% -> sell
    assert asyncio.run(e._in_profit(_sig("SELL", 302))) is True


def test_stream_candles_open_at_previous_close():
    from src.signal import candle_momo as M
    st = M.StreamProvider()
    st.wanted = {"M"}
    base = int(time.time()) - 5
    for dt, px in ((0.2, 1.0), (1.3, 1.2), (2.4, 0.9)):          # one trade per second
        st.on_trade({"mint": "M", "price_sol": px, "ts": base + dt, "sol": 0.01})
    rows = asyncio.run(st.fetch("M", C.Config()))
    assert [(r.open, r.close) for r in rows] == [(1.0, 1.0), (1.0, 1.2), (1.2, 0.9)]
    assert C.evaluate("own_candle", None, rows[1], 0.5)[0] == "BUY"    # +20% now visible
    assert C.evaluate("inverse", None, rows[2], 0.5)[0] == "BUY"       # red -25% -> inverse buys


def test_pause_guard_blocks_buy_on_low_reserves(engine, monkeypatch):
    e = engine
    e._start = type(e)._start.__get__(e)                  # real start -> _run
    e.cfg.min_reserve_sol = 0.15

    async def low_curve(mint):
        return {"virtual_token": 1, "virtual_sol": 1, "real_sol": int(0.05e9)}, 100, ""
    monkeypatch.setattr(e, "_curve", low_curve)
    bought = []

    async def fake_buy(sig):
        bought.append(sig)
    monkeypatch.setattr(e, "_paper_buy", fake_buy)

    async def go():
        e._route(_sig("BUY", 400))
        while e.busy:
            await asyncio.sleep(0.01)
    asyncio.run(go())
    assert bought == [] and e.m["skips"].get("LOW_RESERVES") == 1


def test_pause_guard_sells_when_reserves_drain(engine, monkeypatch):
    e = engine
    e.cfg.exit_reserve_sol = 0.08
    e.feeds.clear()
    e.positions["MINT"] = {"mint": "MINT", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005,
                           "entry_ts": time.time(), "entry_signal": "x"}

    async def drained(mint):
        return {"virtual_token": 1, "virtual_sol": 1, "real_sol": int(0.03e9)}, 100, ""
    monkeypatch.setattr(e, "_curve", drained)
    exits = []

    async def fake_exit(mint, why):
        exits.append(why)
    e._forced_exit = fake_exit

    async def go():
        t = asyncio.ensure_future(e._value_loop())
        await asyncio.sleep(1.3)
        t.cancel()
    asyncio.run(go())
    assert exits and exits[0].startswith("reserves draining")


def test_no_sell_without_fresh_price(engine, monkeypatch):
    e = engine
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.01, "tokens": 1e9, "upct": 50.0}

    async def no_curve(mint):
        return None, 100, "rpc"
    monkeypatch.setattr(e, "_curve", no_curve)
    s1 = _sig("SELL", 410)
    assert asyncio.run(e._in_profit(s1)) is False and "price unknown" in s1["reason"]


def test_crash_candle_does_not_open(engine):
    e = _ladder_engine(engine)
    e.cfg.max_entry_red_pct = 15
    s = _sig("BUY", 500); s["deltaPct"] = -40.0
    assert e._route(s) == "CRASH_CANDLE"
    s2 = _sig("BUY", 501); s2["deltaPct"] = -8.0
    assert e._route(s2) == "intent"
    # ladder lots are not filtered: a held trade still adds on its lot reds
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0, "lots": [{"n": 1}]}
    e.streak["MINT"] = ["BUY", 3]
    s3 = _sig("BUY", 502); s3["deltaPct"] = -40.0
    assert e._route(s3) == "intent"


def test_floored_live_sell_refused_keeps_coins(live):
    import src.execution.tx_route as TR
    live.cfg.sell_only_in_profit = True
    seen_floor = []

    class Refuse(_Mgr):
        def kick_now(self, pos, reason):
            seen_floor.append(TR._SELL_FLOOR.get(pos.mint))
            return True                                    # sent, but below the floor it never lands
    live.manager = Refuse()
    p = _types.SimpleNamespace(id="p1", mint="MINT", status="OPEN", source="candle", strategy="candle", _sell_busy=False)
    live.manager.positions["p1"] = p
    live.positions["MINT"] = {"mint": "MINT", "mode": "live", "tokens": 1.0, "entry_sol": 0.005,
                              "entry_ts": time.time(), "entry_signal": "x", "symbol": "S"}
    s = _sig("SELL", 600)
    asyncio.run(live._live_sell(s, "SELL signal"))
    assert seen_floor and seen_floor[0] >= int(0.005e9)              # break-even written into the sell
    assert "MINT" not in TR._SELL_FLOOR                               # and cleared afterwards
    assert "MINT" in live.positions and s["reason"].startswith("BELOW_BREAKEVEN")


def test_forced_exit_has_no_floor(live):
    import src.execution.tx_route as TR
    seen = []

    class M(_Mgr):
        def kick_now(self, pos, reason):
            seen.append(TR._SELL_FLOOR.get(pos.mint))
            pos._sol_out = 0.001
            pos.status = "CLOSED"
            return True
    live.manager = M()
    p = _types.SimpleNamespace(id="p1", mint="MINT", status="OPEN", source="candle", strategy="candle", _sell_busy=False,
                               fees_sol=0.0)
    live.manager.positions["p1"] = p
    live.positions["MINT"] = {"mint": "MINT", "mode": "live", "tokens": 1.0, "entry_sol": 0.005,
                              "entry_ts": time.time(), "entry_signal": "x", "symbol": "S"}
    asyncio.run(live._forced_exit("MINT", "reserves draining (0.05 SOL left)"))
    assert seen == [None] and "MINT" not in live.positions


def test_pump_sell_tx_floor(monkeypatch):
    from src.execution import pump_curve as PC

    async def curve(m):
        return {"virtual_token": 1_000_000, "virtual_sol": 1_000, "mayhem": False}, ""

    async def fees():
        return "F", "B", 100
    monkeypatch.setattr(PC, "_curve", curve)
    monkeypatch.setattr(PC, "_fees", fees)
    signed, _m, err = asyncio.run(PC.pump_sell_tx("M", 1000, min_sol_out=10**9))
    assert signed == "" and err == "below_floor"


def _board(m, age, ms="active"):
    return {"m": m, "t": m[:4], "age": age, "ms": ms}


def test_reuse_coin_on_keeps_coin_until_paused(engine):
    e = engine
    e.feeds.clear()
    e.cfg.max_mints = 1
    A = "A" * 40 + "pump"; B = "B" * 40 + "pump"
    e._pick([_board(A, 10)])
    assert list(e.feeds) == [A]
    e.traded[A] = time.time()
    e._pick([_board(A, 400), _board(B, 5)])          # A old and traded: still kept (reuse on)
    assert list(e.feeds) == [A]
    e._pick([_board(A, 410, "paused"), _board(B, 6)])
    assert list(e.feeds) == [B]


def test_reuse_coin_off_moves_on_after_one_trade(engine):
    e = engine
    e.feeds.clear()
    e.cfg.max_mints = 1
    e.cfg.reuse_coin = False
    A = "A" * 40 + "pump"; B = "B" * 40 + "pump"
    e._pick([_board(A, 10), _board(B, 20)])
    assert list(e.feeds) == [A]
    e.traded[A] = time.time()                       # one round trip closed on A
    e._pick([_board(A, 12), _board(B, 22)])
    assert list(e.feeds) == [B]


def test_manual_entry_and_exit(engine):
    e = _ladder_engine(engine)
    assert e.manual("exit", "MINT")["ok"] is False                      # nothing held
    r = e.manual("entry", "MINT")
    assert r["ok"] and e._started[-1]["side"] == "BUY" and e._started[-1]["manual"] and not e._started[-1].get("add")
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0, "lots": [{"n": 1}]}
    r = e.manual("entry", "MINT")                                        # adds a lot, any red count
    assert r["ok"] and e._started[-1]["add"] and e._started[-1]["lot_no"] == 2
    e.positions["MINT"]["entry_sol"] = 0.03                               # total cap 0.03 reached
    assert "Total cap" in e.manual("entry", "MINT")["error"]
    assert e.manual("exit", "MINT")["ok"] and e._started[-1]["side"] == "SELL"
    e.cfg.kill = True
    assert e.manual("entry", "MINT")["ok"] is False


def test_manual_exit_skips_profit_and_floor(engine):
    e = engine
    s = {"candleTs": 1, "manual": True}
    e.cfg.sell_only_in_profit = True
    assert e._floored(s) is False and e._floored({"candleTs": 1}) is True


def test_reset_clears_data_keeps_settings(engine, tmp_path):
    e = engine
    e.cfg.min_profit_pct = 15
    e.store.close_position("X", {"mint": "X", "mode": "paper", "entry_sol": 0.01, "exit_sol": 0.02, "pnl_sol": 0.01,
                                 "pct": 100, "costs_sol": 0, "gross_pnl_sol": 0.01, "exit_ts": time.time(), "paused_exit": 0})
    e.store.audit("signal", signalId="a")
    e.positions["P"] = {"mode": "paper", "entry_sol": 0.005}
    e.m["executed"] = 5
    r = e.reset()
    assert r["ok"] and e.store.totals()["trades"] == 0 and not e.positions and e.m["executed"] == 0
    assert e.cfg.min_profit_pct == 15
    assert r["archived"] and (tmp_path / r["archived"]).exists()


def test_reset_refused_with_live_trade(engine):
    engine.positions["L"] = {"mode": "live", "entry_sol": 0.005}
    assert engine.reset()["ok"] is False


def test_manual_from_another_thread_runs_on_engine_loop(engine, monkeypatch):
    import threading
    e = _ladder_engine(engine)
    e._start = type(e)._start.__get__(e)
    ran = []

    async def fake_run(sig):
        ran.append(sig["side"])
        e.busy.pop(sig["mint"], None)
    monkeypatch.setattr(e, "_run", fake_run)
    out = {}

    async def main():
        e.loop = asyncio.get_running_loop()
        t = threading.Thread(target=lambda: out.update(e.call_threadsafe(e.manual, "entry", "MINT")))
        t.start()
        while t.is_alive():
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
    asyncio.run(main())
    assert out.get("ok") and ran == ["BUY"] and "MINT" not in e.busy


def test_start_off_loop_never_marks_busy(engine):
    e = engine
    e._start = type(e)._start.__get__(e)
    s = _sig("BUY", 700)
    assert e._start(s) == "NOT_READY" and "MINT" not in e.busy


def test_entry_filters_inflow_and_peak(engine):
    e = engine
    f = e.feeds["MINT"]
    f.using = "pump_rest"
    now = time.time()
    e.cfg.inflow_window_s = 5
    e.cfg.max_below_peak_pct = 85
    assert e._entry_filters("MINT")[0] == "SOL_OUTFLOW"                   # no history yet
    f.reserves.extend([(now - 6, 0.50), (now - 1, 0.40)])
    assert e._entry_filters("MINT")[0] == "SOL_OUTFLOW"                   # draining
    f.reserves.append((now, 0.55))
    assert e._entry_filters("MINT") == ("", "")                          # flowing in
    f.books["pump_rest"].merge([K(1000, 1.0, 1.0), K(2000, 1.0, 0.1)])
    f.books["pump_rest"].rows[0] = C.Candle(t=1000, open=1.0, high=1.0, low=1.0, close=1.0, volume=1, final=True)
    assert e._entry_filters("MINT")[0] == "FAR_BELOW_PEAK"               # 90% below its high


def test_closed_elsewhere_uses_real_exit(live, monkeypatch):
    from datetime import datetime, timezone
    live.positions["MINT"] = {"mint": "MINT", "mode": "live", "tokens": 1.0, "entry_sol": 0.005,
                              "entry_ts": time.time() - 30, "entry_signal": "x", "symbol": "S"}
    live.manager.closed = [_types.SimpleNamespace(mint="MINT", exit_sol=0.0057, fees_sol=0.00003,
                                                  closed_at=datetime.now(timezone.utc).isoformat())]
    live._live_closed("MINT", None, "no longer open in the bot")
    t = live.store.trades(1)[0]
    assert t["exit_sol"] == pytest.approx(0.0057) and t["pnl_sol"] == pytest.approx(0.0057 - 0.005 - 0.00003)


def test_value_loop_does_not_steal_a_closing_trade(live):
    live.started_at = time.time() - 200
    live.feeds.clear()
    live.positions["MINT"] = {"mint": "MINT", "mode": "live", "tokens": 1.0, "entry_sol": 0.005,
                              "entry_ts": time.time(), "entry_signal": "x", "symbol": "S", "closing": True}

    async def go():
        t = asyncio.ensure_future(live._value_loop())
        await asyncio.sleep(1.3)
        t.cancel()
    asyncio.run(go())
    assert "MINT" in live.positions                      # left to the sell in progress


def test_sell_landed_anyway(monkeypatch):
    import src.execution.tx_route as TR
    import src.execution.wallet_tokens as WT
    import src.execution.signer as SG

    class T:
        _last_tx_err = ""
        async def _confirm_signature(self, sig, **kw):
            self._last_tx_err = "confirm_unreachable"
            return False
    monkeypatch.setattr(SG, "pubkey_from_env", lambda: "PK")

    async def empty(pk, mint, **kw):
        return {"raw": 0}

    async def full(pk, mint, **kw):
        return {"raw": 5}
    monkeypatch.setattr(WT, "get_token_holding", empty)
    assert asyncio.run(TR._sell_landed_anyway(T(), "S", "M")) is True
    monkeypatch.setattr(WT, "get_token_holding", full)
    assert asyncio.run(TR._sell_landed_anyway(T(), "S", "M")) is False

    class Failed(T):
        async def _confirm_signature(self, sig, **kw):
            self._last_tx_err = "{'InstructionError': [3, {'Custom': 6003}]}"
            return False
    monkeypatch.setattr(WT, "get_token_holding", empty)
    assert asyncio.run(TR._sell_landed_anyway(Failed(), "S", "M")) is False


def test_downside_ladder_counts_reds_since_last_lot(engine):
    e = _ladder_engine(engine)
    e.cfg.ladder_count = "since_last_lot"
    e.cfg.reds_per_lot = 3
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0,
                           "lots": [{"n": 1, "px": 1.0, "src": "pump_rest"}]}
    e.reds_since["MINT"] = 0

    def red(t, close):
        s = _sig("BUY", t); s["close"] = close; return s
    def green(t):
        return _sig("SELL", t)
    e.positions["MINT"]["entry_sol"] = 0.005
    assert e._route(red(1, 0.9)) == "LADDER_WAIT"          # red 1 since lot
    e.cfg.sell_only_in_profit = True
    e.cfg.sell_on_green_confirm = 5                        # keep greens from selling in this test
    assert e._route(green(2)) == "GREEN_CONFIRM"            # a green does NOT reset the count
    assert e._route(red(3, 0.8)) == "LADDER_WAIT"           # red 2
    assert e._route(red(4, 0.7)) == "intent"                # red 3 since the last lot, below its price: add
    e.positions["MINT"]["lots"].append({"n": 2, "px": 0.7, "src": "pump_rest"})
    e.positions["MINT"]["entry_sol"] = 0.01
    for t in (5, 6):
        assert e._route(red(t, 0.75)) == "LADDER_WAIT"
    assert e._route(red(7, 0.75)) == "LADDER_ABOVE"         # 3 reds, but above lot 2's 0.7: no averaging up


def test_in_a_row_mode_still_available(engine):
    e = _ladder_engine(engine)
    e.cfg.ladder_count = "in_a_row"
    e.positions["MINT"] = {"mode": "paper", "entry_sol": 0.005, "tokens": 1.0, "lots": [{"n": 1}]}
    e.streak["MINT"] = ["BUY", 3]
    assert e._route(_sig("BUY", 50)) == "intent"            # red 4 in a row


# ---------------------------------------------------------------- support mode (AutoTrade-Candles)
def _support_engine(engine, monkeypatch, src="opencopy"):
    import src.config.runtime as rt
    e = engine
    e.manager = _Mgr()
    e.trader = _Trader(e.manager)
    held = _types.SimpleNamespace(id="oc1", mint="OCMINT", status="OPEN", source=src, strategy=src, symbol="OC",
                                  entry_sol=0.005, tokens=1e9, entry_time=time.time() - 10, fees_sol=0.00002,
                                  _sell_busy=False)
    e.manager.positions["oc1"] = held
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode=src, trade_enabled=True,
                                                                        max_position_sol=0.005))
    return e, held


def test_support_on_adopts_source_coins_and_never_opens(engine, monkeypatch, tmp_path):
    import src.signal.candle_momo as M
    monkeypatch.setattr(M, "CONFIG_PATH", str(tmp_path / "candle.yaml"))
    e, held = _support_engine(engine, monkeypatch)
    e.positions["P"] = {"mint": "P", "mode": "paper", "entry_sol": 0.005, "tokens": 1, "entry_ts": 1, "entry_signal": "x"}
    r = e.set_support(True)
    assert r["ok"] and e.cfg.support_enabled and e.cfg.mode == "live" and "P" not in e.positions
    assert e.support_source() == "opencopy"
    e._pick([])
    assert list(e.feeds) == ["OCMINT"] and e.positions["OCMINT"]["support"] == "opencopy"
    assert held.source == "opencopy"                                  # its own rules stay
    s = _sig("BUY", 900, mint="OTHER")
    assert e._route(s) == "SUPPORT_NO_ENTRY"                          # Candles never buys lot 1 here
    r = e.set_support(False)                                          # Revert
    assert r["ok"] and not e.cfg.support_enabled and e.cfg.mode == "paper" and "OCMINT" not in e.positions
    assert held.status == "OPEN"                                      # the source keeps its coin


def test_support_inactive_when_source_differs(engine, monkeypatch):
    import src.config.runtime as rt
    engine.cfg.support_enabled = True
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="mayhem2"))
    assert engine.support_source() == ""


def test_store_keeps_support_mark(tmp_path):
    s = Store(str(tmp_path / "s.sqlite"), str(tmp_path / "s.jsonl"))
    s.open_position({"mint": "M", "mode": "live", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": 1.0,
                     "entry_signal": "a", "support": "callout"})
    s.open_position({"mint": "N", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": 1.0, "entry_signal": "b"})
    rows = {r["mint"]: r for r in Store(str(tmp_path / "s.sqlite"), str(tmp_path / "s.jsonl")).positions()}
    assert rows["M"]["support"] == "callout" and "support" not in rows["N"]


def test_support_on_clears_page_data(tmp_path):
    from src.signal.candle_momo import CandleMomo
    from src.signal.candle_store import Store
    e = CandleMomo()
    e.store = Store(str(tmp_path / "c.sqlite"), str(tmp_path / "a.jsonl"))
    e.save_config = lambda cfg: None
    e.load_config = lambda force=False: None
    e.recent.appendleft({"id": "x", "mint": "M", "side": "BUY"})
    e.notes.appendleft({"msg": "old"})
    e.m["signals_buy"] = 9
    e.positions["P"] = {"mint": "P", "mode": "paper", "tokens": 1, "entry_sol": 0.005, "entry_ts": 1, "entry_signal": "s"}
    e.store.close_position("Q", {"mint": "Q", "mode": "paper", "pnl_sol": 0.001, "exit_ts": 2})
    out = e.set_support(True)
    assert out.get("support") is True
    assert not e.positions and e.m["signals_buy"] == 0
    assert all(s.get("id") != "x" for s in e.recent)
    assert e.store.trades() == []
    assert all("old" != n.get("msg") for n in e.notes)


def test_ladder_ledger_call_matches_real_signature():
    import inspect
    from src.observability.ledger import TradeLedger
    params = list(inspect.signature(TradeLedger.add_to_open).parameters)
    assert params[:5] == ["self", "mint", "sol", "fee_sol", "tokens"]
    assert list(TradeLedger.__dict__).count("add_to_open") == 1


def test_source_sell_is_booked_with_pnl(live):
    from datetime import datetime, timezone
    live.positions["MINT"] = {"mint": "MINT", "mode": "live", "tokens": 1.0, "entry_sol": 0.0101,
                              "entry_ts": time.time() - 90, "entry_signal": "x", "symbol": "S", "support": "opencopy"}
    live.manager.positions = {}
    live.manager.closed = [_types.SimpleNamespace(mint="MINT", exit_sol=0.0112, fees_sol=0.00003, exit_reason="fast_pace_sell",
                                                  closed_at=datetime.now(timezone.utc).isoformat())]

    async def go():
        t = asyncio.ensure_future(live._value_loop())
        await asyncio.sleep(1.3)
        t.cancel()
    asyncio.run(go())
    t = live.store.trades(1)[0]
    assert t["exit_sol"] == pytest.approx(0.0112) and t["pnl_sol"] > 0
    assert "opencopy's own rule" in t["why"] and "FAST PACE" in t["why"]


def test_last_lot_shrinks_to_fit_total_cap():
    from src.signal.candle_core import Config, ladder_decision_down
    cfg = Config(reds_per_lot=3, max_ladder_buys=5, ladder_max_total_sol=0.015)
    what, size = ladder_decision_down(3, 2, 0.0101, 0.005, cfg)      # OpenCopy lot 1 was 0.0051
    assert what == "add" and size == pytest.approx(0.0049, abs=1e-6)
    assert ladder_decision_down(3, 3, 0.0148, 0.005, cfg)[0] == "cap"  # 0.0002 left: no dust lot


def test_support_turns_off_source_exits_and_revert_restores():
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    p = _types.SimpleNamespace(mint="M", status="OPEN", source="opencopy", strategy="opencopy", tokens=1.0,
                               entry_sol=0.005, entry_time=time.time(), fees_sol=0.0, symbol="S")
    e.manager = _types.SimpleNamespace(positions={"1": p})
    e.store = None
    e._support_sync("opencopy")
    assert p._candle_support is True and e.positions["M"]["support"] == "opencopy"
    e.cfg.support_enabled = True
    e.update = lambda d: {"ok": True}
    e.set_support(False)
    assert p._candle_support is False


def test_source_sell_gate_profit_only(monkeypatch):
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.store = None
    pos = _types.SimpleNamespace(mint="M", tokens=1e9, entry_sol=0.01, symbol="S", source="opencopy")
    e.positions["M"] = {"mint": "M", "entry_sol": 0.01, "tokens": 1e9, "support": "opencopy", "symbol": "S"}

    async def val(v):
        async def f(mint, tokens):
            return v, None, "curve"
        return f
    e._value = asyncio.run(val(0.0088))                       # -12%: held back
    assert asyncio.run(e.source_sell_ok(pos, "caller_closed")) is False
    e._value = asyncio.run(val(0.0102))                       # +1.6%: in profit, allowed
    assert asyncio.run(e.source_sell_ok(pos, "fast_pace_sell")) is True
    e._value = asyncio.run(val(None))                         # no price: held back
    assert asyncio.run(e.source_sell_ok(pos, "fast_pace_sell")) is False


def test_guard_covers_coin_before_adoption():
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.support_source = lambda: "opencopy"
    fresh = _types.SimpleNamespace(mint="M", source="opencopy", strategy="opencopy")   # bought 1s ago, not adopted
    other = _types.SimpleNamespace(mint="N", source="mayhem2", strategy="mayhem2")
    assert e.supports(fresh) is True and e.supports(other) is False
    e.support_source = lambda: ""
    assert e.supports(fresh) is False


def test_launch_is_a_support_source(monkeypatch):
    import src.config.runtime as rt
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.cfg.support_enabled = True
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="launch"))
    assert e.support_source() == "launch"
    pos = _types.SimpleNamespace(mint="M", source="launch", strategy="launch")
    assert e.supports(pos) is True
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="mayhem2"))
    assert e.support_source() == ""


def test_live_card_shows_whole_candle_trade():
    from src.risk.position_manager import _candle_overlay
    from src.signal.candle_momo import get_engine
    e = get_engine()
    e.positions["NEKO"] = {"mint": "NEKO", "mode": "live", "entry_sol": 0.015, "value_sol": 0.01371, "upct": -8.74,
                           "upnl_sol": -0.00131, "value_at": time.time(), "lots": [{}, {}, {}]}
    try:
        row = {"mint": "NEKO", "entry_sol": 0.0051, "pnl_pct": -33.43, "sol_paid_pct": -33.43, "pnl_sol": -0.0017,
               "price_usd": 4.15e-6, "pnl_usd": -0.1446}
        out = _candle_overlay(row)
        assert out["entry_sol"] == 0.015 and out["pnl_pct"] == -8.74 and out["sol_paid_pct"] == -8.74
        assert out["pnl_sol"] == -0.00131 and out["candle_lots"] == 3
        assert out["entry_usd"] > row["price_usd"] and out["pnl_usd"] < 0
        assert _candle_overlay({"mint": "OTHER", "pnl_pct": 1.0}) == {"mint": "OTHER", "pnl_pct": 1.0}
    finally:
        e.positions.pop("NEKO", None)


def test_support_on_refused_for_other_source(monkeypatch):
    import src.config.runtime as rt
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="opencopy"))
    out = e.set_support(True, "launch")
    assert out["ok"] is False and "Select Source to Launch" in out["error"]
    assert e.cfg.support_enabled is False


def test_decoded_page_pump_only(monkeypatch):
    import src.observability.dashboard as D
    sent = []

    async def pub(payload):
        sent.append(payload)
    monkeypatch.setattr(D.bus, "publish", pub)
    monkeypatch.setattr(D, "_queue_ath", lambda m: None)
    monkeypatch.setattr(D, "_m5_cache", {"X": (time.time(), None, None, None, None)})
    asyncio.run(D.publish_decoded(mint="HqJRxKECi7Rc63pgBT2YxEBY37aKPxGMJ92SxQYub2pQ", verdict="BUYING"))
    assert sent == []
    from src.main import is_pump_mint
    assert is_pump_mint("6suUabcpump") and is_pump_mint("pumpAbc123") and not is_pump_mint("HqJR2pQ")


def test_mayhem_support_takes_only_young_opens(monkeypatch):
    import src.config.runtime as rt
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.cfg.support_enabled = True
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="mayhem"))
    young = _types.SimpleNamespace(mint="Y", status="OPEN", source="mayhem", strategy="mayhem", _open_age=12.0,
                                   tokens=1.0, entry_sol=0.005, entry_time=time.time(), fees_sol=0.0, symbol="Y")
    old = _types.SimpleNamespace(mint="O", status="OPEN", source="mayhem", strategy="mayhem", _open_age=45.0,
                                 tokens=1.0, entry_sol=0.005, entry_time=time.time(), fees_sol=0.0, symbol="O")
    clicked = _types.SimpleNamespace(mint="C", status="OPEN", source="mayhem", strategy="mayhem", _open_age=None,
                                     tokens=1.0, entry_sol=0.005, entry_time=time.time(), fees_sol=0.0, symbol="C")
    e.manager = _types.SimpleNamespace(positions={"1": young, "2": old, "3": clicked})
    e.store = None
    assert e.support_source() == "mayhem"
    e._support_sync("mayhem")
    assert set(e.positions) == {"Y"}
    assert e.supports(young) and not e.supports(old) and not e.supports(clicked)


def test_dayscalp_is_a_support_source(monkeypatch):
    import src.config.runtime as rt
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.cfg.support_enabled = True
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="dayscalp"))
    assert e.support_source() == "dayscalp"
    assert e.supports(_types.SimpleNamespace(mint="M", source="dayscalp", strategy="dayscalp"))
    out = CandleMomo().set_support(True, "launch")
    assert out["ok"] is False and "Launch" in out["error"]


def test_mayhem_grab_feeds_and_buys_youngest(monkeypatch):
    import src.config.runtime as rt
    import src.observability.dashboard as D
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.store = None
    e.cfg.support_enabled = True
    e.manager = _types.SimpleNamespace(positions={})
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="mayhem", trade_enabled=True))
    monkeypatch.setattr(D, "mayhem_young_opens", lambda max_age: [
        {"coinMint": "YOUNGpump", "symbol": "YNG", "age": 4.0}, {"coinMint": "OLDERpump", "symbol": "OLD", "age": 20.0}])
    orders = []
    monkeypatch.setattr(D, "queue_openhold_order", lambda *a, **k: orders.append((a, k)))
    e._pick([])
    assert "YOUNGpump" in e.feeds and "OLDERpump" not in e.feeds
    assert len(orders) == 1 and orders[0][0][1] == "YOUNGpump" and orders[0][1]["candle"] is True
    e._pick([])                                    # one coin at a time: no second buy
    assert len(orders) == 1
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="mayhem", trade_enabled=False))
    e2 = CandleMomo(); e2.store = None; e2.cfg.support_enabled = True
    e2.manager = _types.SimpleNamespace(positions={})
    e2._pick([])
    assert len(orders) == 1 and "YOUNGpump" in e2.feeds   # Auto Trade off: shown in the feeds, not bought


def test_green_target_uses_fast_pace_when_lower(monkeypatch):
    import src.config.runtime as rt
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.cfg.min_profit_pct = 15.0
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(fast_pace_sell=True, fast_pace_margin_pct=10.0))
    assert e.green_target() == (10.0, "Fast Pace")
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(fast_pace_sell=False, fast_pace_margin_pct=10.0))
    assert e.green_target() == (15.0, "Min profit")
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(fast_pace_sell=True, fast_pace_margin_pct=20.0))
    assert e.green_target() == (15.0, "Min profit")


def test_dayscalp_candle_pick_takes_first_scan_token(monkeypatch):
    import src.signal.dayscalp as DS
    from src.signal.candle_momo import get_engine
    now = time.time()
    rows = [
        {"mint": "T" * 40 + "pump", "symbol": "TRADED", "change_m5_pct": 0.0, "volume_m5_usd": 0, "created_ts": now - 5},
        {"mint": "F" * 40 + "pump", "symbol": "FIRST", "change_m5_pct": 0.0, "volume_m5_usd": 0, "created_ts": now - 8},
        {"mint": "S" * 40 + "pump", "symbol": "SECOND", "change_m5_pct": 20.0, "volume_m5_usd": 9000, "created_ts": now - 600},
    ]

    async def ranked(client, pages=1):
        return rows

    async def shown(*a, **k):
        return None
    monkeypatch.setattr(DS, "fetch_pumpfun_ranked_by_m5", ranked)
    monkeypatch.setattr(DS, "_show_decoded", shown)
    monkeypatch.setattr(DS, "_candle_supports_dayscalp", lambda: True)
    monkeypatch.setattr(DS, "_apply_live_band", lambda c: c)
    eng = get_engine()
    eng.traded["T" * 40 + "pump"] = now                     # already traded by Candles: skipped
    try:
        top = asyncio.run(DS.select_top1_dayscalp(DS.DayScalpConfig()))
    finally:
        eng.traded.pop("T" * 40 + "pump", None)
        DS.LAST_CANDLE_PICK.update(mint="", ts=0.0)
    assert top.symbol == "FIRST" and top.quick_entry is True


def test_dayscalp_pick_is_fed_even_without_buy(monkeypatch):
    import src.config.runtime as rt
    import src.signal.dayscalp as DS
    from src.signal.candle_momo import CandleMomo
    e = CandleMomo()
    e.store = None
    e.cfg.support_enabled = True
    e.manager = _types.SimpleNamespace(positions={})
    monkeypatch.setattr(rt, "get_config", lambda: _types.SimpleNamespace(filter_mode="dayscalp", trade_enabled=False))
    DS.LAST_CANDLE_PICK.update(mint="YENUpump", symbol="yenu", ts=time.time())
    e._pick([])
    assert "YENUpump" in e.feeds and e.feeds["YENUpump"].symbol == "yenu"
    DS.LAST_CANDLE_PICK.update(mint="", ts=0.0)
