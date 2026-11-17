"""Pro Trader: VWAP dip with volume -> BUY; target +25% / stop -5% / 60s time stop; Mayhem2 strategy pro_trader."""

import time

from src.signal import pro_trader as P
from src.signal import mayhem2 as M


def _c(t, o, c, v=1.0, h=None, l=None):
    return {"t": t * 1000, "o": o, "c": c, "h": h if h is not None else max(o, c), "l": l if l is not None else min(o, c), "v": v}


def _flush():
    """30 calm candles near 1.0 (heavy volume sets VWAP there), then a fall to 0.80 and a green bounce on volume."""
    cs = [_c(i, 1.0, 1.0 + (0.002 if i % 2 else -0.002), v=10) for i in range(30)]
    px = 1.0
    for i in range(30, 40):
        nx = px * 0.977
        cs.append(_c(i, px, nx, v=1))
        px = nx
    cs.append(_c(40, px, px * 1.01, v=150))     # green, volume surge
    return cs


def test_dip_with_volume_is_a_buy():
    a = P.analyze(_flush())
    assert a["ok"] and a["call"] == "BUY", (a["why"], a["checks"])
    assert all(x["ok"] for x in a["checks"])
    assert a["targetPct"] == 40.0 and a["stopPct"] == -5.0 and a["rr"] == 8.0 and a["timeStopS"] == 120.0
    assert a["target"] > a["entry"] > a["stop"]


def test_calm_uptrend_is_not_a_buy():
    cs = [_c(i, 1 + i * 0.001, 1 + (i + 1) * 0.001, v=5) for i in range(60)]
    a = P.analyze(cs)
    assert a["call"] in ("WAIT", "AVOID") and not all(x["ok"] for x in a["checks"])


def test_too_few_candles_waits():
    assert P.analyze(_flush()[:10])["call"] == "WAIT"


def test_exits_stop_target_time():
    cs = [_c(0, 1.0, 1.0)]
    assert P.exit_reason([_c(0, 1.0, 0.94, l=0.94)], 0, 1.0, 5).startswith("stop")
    assert P.exit_reason([_c(0, 1.0, 1.41, h=1.41)], 0, 1.0, 5).startswith("target")
    assert P.exit_reason([_c(0, 1.0, 1.26, h=1.26)], 0, 1.0, 5) == ""          # +26% no longer sells: let it run
    assert P.exit_reason(cs, 0, 1.0, 121).startswith("time stop")
    assert P.exit_reason(cs, 0, 1.0, 61) == ""
    assert P.exit_reason(cs, 0, 1.0, 5) == ""


def test_holding_read_sells_on_target():
    cs = _flush()
    cs.append(_c(41, cs[-1]["c"], cs[-1]["c"] * 1.45, v=20))
    a = P.analyze(cs, holding={"entry": cs[-2]["c"], "held": 3})
    assert a["call"] == "SELL" and "target" in a["why"] and a["holding"]


def test_replay_marks_the_buy():
    cs = _flush()
    r = P.replay(cs, P.indicators(cs))
    assert r["marks"] and r["marks"][0]["side"] == "buy" and r["open"]


def test_engine_pro_exit_uses_fees():
    assert M.Mayhem2._pro_exit(38.5, 5).startswith("Pro Trader target")
    assert M.Mayhem2._pro_exit(23.5, 5) == ""
    assert M.Mayhem2._pro_exit(-7.2, 5).startswith("Pro Trader stop")
    assert M.Mayhem2._pro_exit(-3.0, 5) == ""
    assert "time stop" in M.Mayhem2._pro_exit(1.0, 121)


def test_pro_context_flow_counts_last_15s():
    e = M.Mayhem2()
    e.pin = {"mint": "X", "ts": time.time()}
    now = time.time()
    for side, age in (("buy", 1), ("buy", 2), ("sell", 3), ("buy", 30)):
        e.signals.appendleft({"side": side, "seen": now - age})
    assert e.pro_context("X")["flow"] == {"buy": 2, "sell": 1}


def test_strategy_listed_and_ignores_trade_signals(monkeypatch):
    from src.config.runtime import RuntimeConfig
    c = RuntimeConfig()
    c.update_from({"mayhem2_strategy": "pro_trader"})
    assert c.mayhem2_strategy == "pro_trader" and "pro_trader" in M.SHADOWS
    e = M.Mayhem2()
    e.pin = {"mint": "X", "ts": time.time()}
    monkeypatch.setattr(M.Mayhem2, "strategy", lambda self: "pro_trader")
    assert e._on_signal({"side": "buy", "seen": time.time(), "sol": 1}).startswith("ignored")


# ---------- Candles rule "pro" ----------

def _cm_engine(tmp_path, monkeypatch):
    from src.signal import candle_momo as CM
    from src.signal import candle_core as C
    from src.signal.candle_store import Store
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "p.sqlite"), str(tmp_path / "p.jsonl"))
    e.cfg.rule = "pro"
    routed = []
    monkeypatch.setattr(e, "_route", lambda sig: routed.append(sig) or "intent")
    f = CM.Feed("MINT", 400)
    f.status = "OK"
    f.using = "pump_rest"
    rows = [C.Candle(t=c["t"], open=c["o"], high=c["h"], low=c["l"], close=c["c"], volume=c["v"], final=True)
            for c in _flush()]
    f.books["pump_rest"].merge(rows[:-1])
    e.feeds["MINT"] = f
    return e, f, rows, routed


def test_candles_pro_rule_buys_on_the_call(tmp_path, monkeypatch):
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e._evaluate(f, time.time())                       # first read: history, no signal
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert len(routed) == 1 and routed[0]["side"] == "BUY" and routed[0]["pro"] and routed[0]["rule"] == "pro"
    assert e.pro_px["MINT"] == routed[0]["pro_px"] > 0


def test_candles_pro_rule_sells_at_target(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    f.books["pump_rest"].merge(rows[-1:])
    entry = rows[-1].close
    e.positions["MINT"] = {"mint": "MINT", "entry_ts": time.time() - 5, "entry_sol": 0.005, "lots": [{"n": 1}]}
    e.pro_px["MINT"] = entry
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge([C.Candle(t=rows[-1].t + 1000, open=entry, high=entry * 1.45, low=entry,
                                         close=entry * 1.45, volume=5, final=True)])
    e._evaluate(f, time.time())
    assert routed and routed[-1]["side"] == "SELL" and "target" in routed[-1]["pro_why"]


def test_candles_pro_hint_and_rule_choice():
    from src.signal import candle_core as C
    from src.signal import candle_momo as CM
    assert "pro" in C.RULES
    e = CM.CandleMomo()
    e.cfg.rule = "pro"
    h = e._hint("X")
    assert h.get("pro") and "VWAP" in h["entry"] and h["minProfit"] == 40.0


# ---------- Candles rule "pro_hold": buy on the chart BUY, sell only in profit, else at Max hold ----------

def test_pro_hold_buys_on_the_call_but_never_sells_on_the_chart_plan(tmp_path, monkeypatch):
    from src.signal import botpredict as BPm
    monkeypatch.setitem(BPm.MODEL, "thr", 0.0)
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule = "pro_hold"
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert routed and routed[-1]["side"] == "BUY"
    entry = rows[-1].close
    e.positions["MINT"] = {"mint": "MINT", "entry_ts": time.time() - 5, "entry_sol": 0.005, "lots": [{"n": 1}]}
    f.books["pump_rest"].merge([C.Candle(t=rows[-1].t + 1000, open=entry, high=entry, low=entry * 0.5,
                                         close=entry * 0.5, volume=5, final=True)])     # -50%: the chart plan would stop out
    n = len(routed)
    e._evaluate(f, time.time())
    assert len(routed) == n                      # no sell in a loss
    assert e.pro_context("MINT")["holding"] is None


def test_pro_hold_value_loop_arms_but_does_not_sell_at_once(tmp_path, monkeypatch):
    import asyncio as _a
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule = "pro_hold"
    e.cfg.max_hold_s = 150
    monkeypatch.setattr(e, "green_target", lambda: (10.0, "Min profit"))
    e.positions["MINT"] = {"mint": "MINT", "mode": "paper", "tokens": 1000.0, "entry_sol": 0.005,
                           "entry_ts": time.time() - 5, "symbol": "S", "lots": [{"n": 1}]}

    async def val(mint, tokens):
        return 0.0062, None, "curve"             # +24% before network fees
    monkeypatch.setattr(e, "_value", val)
    sold = []

    async def fx(mint, why):
        sold.append(why)
    monkeypatch.setattr(e, "_forced_exit", fx)

    async def go():
        t = _a.ensure_future(e._value_loop())
        await _a.sleep(1.3)
        t.cancel()
    _a.run(go())
    assert not sold                              # rides the green run; the chart SELL signal takes the profit
    assert e.positions["MINT"]["pro_armed"] and e.positions["MINT"]["pro_peak"] > 10


def _held_pro_hold(tmp_path, monkeypatch, upct):
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule = "pro_hold"
    monkeypatch.setattr(e, "green_target", lambda: (15.0, "Min profit"))
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e.positions["MINT"] = {"mint": "MINT", "mode": "paper", "tokens": 1000.0, "entry_sol": 0.005,
                           "entry_ts": time.time() - 5, "symbol": "S", "lots": [{"n": 1}],
                           "upct": upct, "value_at": time.time(), "pro_peak": upct}
    e._evaluate(f, time.time())
    return e, f, rows, routed


def _next(f, rows, k, red):
    from src.signal import candle_core as C
    px = rows[-1].close
    o, c = (px * 1.1, px) if red else (px, px * 1.1)
    f.books["pump_rest"].merge([C.Candle(t=rows[-1].t + 1000 * k, open=o, high=max(o, c), low=min(o, c),
                                         close=c, volume=5, final=True)])


def test_pro_hold_sell_signal_on_red_candle_in_profit(tmp_path, monkeypatch):
    e, f, rows, routed = _held_pro_hold(tmp_path, monkeypatch, 22.0)
    n = len(routed)
    _next(f, rows, 1, red=False)                 # green: keep riding
    e._evaluate(f, time.time())
    assert not [r for r in routed[n:] if r["side"] == "SELL"]
    _next(f, rows, 2, red=True)                  # red while +22% >= 15%: SELL signal
    e._evaluate(f, time.time())
    sells = [r for r in routed[n:] if r["side"] == "SELL"]
    assert sells and sells[0]["pro_hold_sell"] and "take profit" in sells[0]["pro_why"]


def test_pro_hold_no_sell_signal_below_target(tmp_path, monkeypatch):
    e, f, rows, routed = _held_pro_hold(tmp_path, monkeypatch, 8.0)
    n = len(routed)
    _next(f, rows, 1, red=True)                  # red but only +8%: hold (never sells on the loss side)
    e._evaluate(f, time.time())
    assert not [r for r in routed[n:] if r["side"] == "SELL"]


def test_pro_hold_no_sell_signal_on_stale_value(tmp_path, monkeypatch):
    e, f, rows, routed = _held_pro_hold(tmp_path, monkeypatch, 30.0)
    e.positions["MINT"]["value_at"] = time.time() - 10
    n = len(routed)
    _next(f, rows, 1, red=True)
    e._evaluate(f, time.time())
    assert not [r for r in routed[n:] if r["side"] == "SELL"]


def test_pro_hold_hint_and_choice():
    from src.signal import candle_core as C
    from src.signal import candle_momo as CM
    assert "pro_hold" in C.RULES
    e = CM.CandleMomo()
    e.cfg.rule = "pro_hold"
    e.positions["X"] = {"mint": "X", "upct": -4.0, "lots": [{"n": 1}]}
    h = e._hint("X")
    assert h["profitOnly"] and "Max hold" in h["exit"] and not h["ready"]


def test_pro_hold_new_signal_buys_again_once(tmp_path, monkeypatch):
    from src.signal import botpredict as BPm
    monkeypatch.setitem(BPm.MODEL, "thr", 0.0)
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule = "pro_hold"
    e._evaluate(f, time.time())
    e.positions["MINT"] = {"mint": "MINT", "entry_ts": time.time() - 5, "entry_sol": 0.005, "lots": [{"n": 1}], "mode": "paper"}
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())                       # a fresh BUY signal while holding -> buys again
    assert routed and routed[-1]["side"] == "BUY"
    n = len(routed)
    last = rows[-1]
    f.books["pump_rest"].merge([C.Candle(t=last.t + 1000, open=last.close, high=last.close * 1.01, low=last.close,
                                         close=last.close * 1.01, volume=200, final=True)])
    e._evaluate(f, time.time())                       # the same signal still on: no second buy
    assert len(routed) == n or routed[-1]["side"] != "BUY"


def test_pro_hold_add_respects_total_cap(tmp_path, monkeypatch):
    from src.signal import candle_momo as CM
    from src.signal.candle_store import Store
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "c.sqlite"), str(tmp_path / "c.jsonl"))
    e.cfg.rule = "pro_hold"
    e.cfg.ladder_max_total_sol = 0.015
    f = CM.Feed("MINT", 10)
    f.status = "OK"
    e.feeds["MINT"] = f
    started = []
    monkeypatch.setattr(e, "_start", lambda sig: started.append(sig) or "intent")
    monkeypatch.setattr(e, "_lot_base", lambda mode: 0.005)
    e.positions["MINT"] = {"mint": "MINT", "entry_sol": 0.010, "lots": [{"n": 1}, {"n": 2}], "mode": "paper"}
    def sig(i):
        return {"id": i, "mint": "MINT", "side": "BUY", "pro": True, "symbol": "S", "provider": "pump_rest",
                "ts": int(time.time() * 1000), "open": 1.0, "prevClose": 1.0, "close": 1.0, "deltaPct": 0.0,
                "candleTs": 0, "rule": "pro", "feedLagMs": 0}
    assert e._route(sig("a")) == "intent" and started[-1]["add"] and started[-1]["lot_no"] == 3
    e.positions["MINT"]["entry_sol"] = 0.015
    assert e._route(sig("b")) != "intent"


def _support_engine(tmp_path, monkeypatch, rule, trade_on):
    from src.signal import candle_momo as CM
    from src.signal.candle_store import Store
    import src.config.runtime as rt
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "s.sqlite"), str(tmp_path / "s.jsonl"))
    e.cfg.rule = rule
    e.cfg.mode = "live"
    e.cfg.inflow_window_s = 0
    e.cfg.max_below_peak_pct = 0
    f = CM.Feed("MINT", 10)
    f.status = "OK"
    e.feeds["MINT"] = f
    monkeypatch.setattr(e, "support_source", lambda: "dayscalp")
    monkeypatch.setattr(e, "live_block", lambda: "")
    started = []
    monkeypatch.setattr(e, "_start", lambda sig: started.append(sig) or "intent")
    monkeypatch.setattr(rt, "get_config", lambda: type("C", (), {"trade_enabled": trade_on})())
    sig = {"id": "x", "mint": "MINT", "side": "BUY", "pro": True, "symbol": "S", "provider": "pump_rest",
           "ts": int(time.time() * 1000), "open": 1.0, "prevClose": 1.0, "close": 1.0, "deltaPct": 0.0,
           "candleTs": 0, "rule": "pro", "feedLagMs": 0}
    return e, sig


def test_pro_hold_buys_lot_one_in_support_mode(tmp_path, monkeypatch):
    e, sig = _support_engine(tmp_path, monkeypatch, "pro_hold", True)
    assert e._route(sig) == "intent"


def test_pro_hold_support_watch_only_with_auto_trade_off(tmp_path, monkeypatch):
    e, sig = _support_engine(tmp_path, monkeypatch, "pro_hold", False)
    assert e._route(sig) == "AUTO_TRADE_OFF"


def test_other_rules_still_leave_lot_one_to_the_source(tmp_path, monkeypatch):
    e, sig = _support_engine(tmp_path, monkeypatch, "pro", True)
    assert e._route(sig) == "SUPPORT_NO_ENTRY"


def test_dayscalp_knows_the_candles_rule(monkeypatch):
    from src.signal import dayscalp as DS
    from src.signal import candle_momo as CM
    eng = CM.get_engine()
    monkeypatch.setattr(eng.cfg, "rule", "pro_hold")
    assert DS._candle_rule() == "pro_hold"


def test_pro_hold_watches_the_scan_tokens_it_is_given(monkeypatch):
    from src.signal import candle_momo as CM
    from src.signal import dayscalp as DS
    e = CM.CandleMomo()
    e.cfg.mode = "live"
    e.cfg.rule = "pro_hold"
    monkeypatch.setattr(e, "support_source", lambda: "dayscalp")
    monkeypatch.setattr(e, "_support_sync", lambda src: None)
    monkeypatch.setattr(e, "_support_held", lambda src: [])
    now = time.time()
    picks = [{"mint": f"M{i}" + "x" * 40, "symbol": f"S{i}", "ts": now} for i in range(3)]
    monkeypatch.setattr(DS, "LAST_CANDLE_PICKS", picks)
    monkeypatch.setitem(DS.LAST_CANDLE_PICK, "mint", picks[0]["mint"])
    monkeypatch.setitem(DS.LAST_CANDLE_PICK, "ts", now)
    e._pick([])
    assert set(e.feeds) == {picks[0]["mint"]}     # pro_hold: one coin at a time
    e.cfg.rule = "inverse"                       # other rules: only the single top pick
    e.feeds.clear()
    e._pick([])
    assert set(e.feeds) == {picks[0]["mint"]}


def test_pro_hold_entry_text_names_the_rule():
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.rule = "pro_hold"
    assert e._hint("X")["entry"].startswith("pro_hold:")


def test_pro_hold_buys_on_botpredict_not_pro_trader(tmp_path, monkeypatch):
    from src.signal import botpredict as BPm
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule = "pro_hold"
    e.cfg.pro_hold_pro_buy = False                        # the ▲ BUY entry switched off: Botpredict only
    monkeypatch.setitem(BPm.MODEL, "thr", 1.01)          # Botpredict never buys
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])                 # a Pro Trader BUY setup
    e._evaluate(f, time.time())
    assert not routed
    monkeypatch.setitem(BPm.MODEL, "thr", 0.0)            # Botpredict always buys
    from src.signal import candle_core as C
    last = rows[-1]
    f.books["pump_rest"].merge([C.Candle(t=last.t + 1000, open=last.close, high=last.close, low=last.close,
                                         close=last.close, volume=1, final=True)])
    f.pro_was_buy = False
    e._evaluate(f, time.time())
    assert routed and routed[-1]["side"] == "BUY" and "chance" in routed[-1]["pro_why"]


def test_pro_hold_one_coin_at_a_time(monkeypatch):
    from src.signal import candle_momo as CM
    from src.signal import dayscalp as DS
    assert DS.PRO_HOLD_WATCH == 1
    e = CM.CandleMomo()
    e.cfg.rule = "pro_hold"
    monkeypatch.setattr(e, "support_source", lambda: "dayscalp")
    monkeypatch.setattr(e, "_support_sync", lambda src: None)
    monkeypatch.setattr(e, "_support_held", lambda src: [])
    held = "H" + "x" * 43
    e.positions[held] = {"mint": held, "symbol": "HELD", "lots": [{"n": 1}]}
    nxt = {"mint": "N" + "x" * 43, "symbol": "NEXT", "ts": time.time()}
    monkeypatch.setattr(DS, "LAST_CANDLE_PICKS", [nxt])
    e._pick([])
    assert set(e.feeds) == {held}                 # trading: only the held coin
    e.positions.clear()
    e._pick([])
    assert set(e.feeds) == {nxt["mint"]}          # trade complete: the next scan token


def test_pro_hold_one_feed_with_mayhem_source(monkeypatch):
    from src.signal import candle_momo as CM
    import src.observability.dashboard as DB
    e = CM.CandleMomo()
    e.cfg.rule = "pro_hold"
    monkeypatch.setattr(e, "support_source", lambda: "mayhem")
    monkeypatch.setattr(e, "_support_sync", lambda src: None)
    monkeypatch.setattr(e, "_support_held", lambda src: [])
    a, b, c = ("A" + "x" * 43), ("B" + "x" * 43), ("C" + "x" * 43)
    e.positions[a] = {"mint": a, "entry_ts": 100.0, "lots": [{"n": 1}], "support": "mayhem"}
    e.positions[b] = {"mint": b, "entry_ts": 200.0, "lots": [{"n": 1}], "support": "mayhem"}
    monkeypatch.setattr(DB, "mayhem_state_of", lambda m: "paused" if m == a else "active")
    monkeypatch.setattr(DB, "mayhem_young_opens", lambda age: [{"coinMint": c, "age": 3}])
    e._pick([])
    assert set(e.feeds) == {b}                   # one feed: the tradable held coin (a is paused)
    e.positions.clear()
    e.feeds.clear()
    e._pick([])
    assert set(e.feeds) == {c}                   # flat: the youngest Mayhem_Mode open, watched (not bought)
    assert e.pro_hold_owns_entries("mayhem") and not e.pro_hold_owns_entries("dayscalp")


# ---------- pro_hold extra entries: a new coin's first candle, and the green ▲ BUY (Pro Trader) ----------

def _ph_first_engine(tmp_path, monkeypatch, age):
    from src.signal import candle_momo as CM
    from src.signal import candle_core as C
    from src.signal.candle_store import Store
    e = CM.CandleMomo()
    e.store = Store(str(tmp_path / "f.sqlite"), str(tmp_path / "f.jsonl"))
    e.cfg.rule = "pro_hold"
    routed = []
    monkeypatch.setattr(e, "_route", lambda sig: routed.append(sig) or "intent")
    f = CM.Feed("NEW", 400)
    f.status, f.using, f.age = "OK", "pump_rest", age
    e.mayhem_of["NEW"] = False
    t = int(time.time()) * 1000 - 3000
    f.books["pump_rest"].merge([C.Candle(t=t, open=1.0, high=1.2, low=1.0, close=1.1, volume=3, final=True)])
    e.feeds["NEW"] = f
    return e, f, routed


def test_pro_hold_buys_a_new_coin_on_its_first_candle(tmp_path, monkeypatch):
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 4)
    e._evaluate(f, time.time())
    assert len(routed) == 1 and routed[0]["side"] == "BUY" and routed[0]["first"]
    e._evaluate(f, time.time())
    assert len(routed) == 1                          # once per coin


def test_pro_hold_first_candle_only_for_young_coins_and_switch(tmp_path, monkeypatch):
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 500)
    e._evaluate(f, time.time())
    assert not routed
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 4)
    e.cfg.pro_hold_first = False
    e._evaluate(f, time.time())
    assert not routed


def test_pro_hold_first_candle_waits_for_the_mayhem_check(tmp_path, monkeypatch):
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 4)
    e.mayhem_of.pop("NEW")
    e._evaluate(f, time.time())
    assert not routed and not getattr(f, "first_done", False)
    e.mayhem_of["NEW"] = False
    e._evaluate(f, time.time())
    assert routed and routed[0]["first"]


def test_first_candle_buy_skips_history_checks(tmp_path, monkeypatch):
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.inflow_window_s = 5
    e.cfg.max_below_peak_pct = 0
    e.feeds["X"] = CM.Feed("X", 10)
    e.mayhem_of["X"] = False
    assert e._entry_filters("X")[0] == "SOL_OUTFLOW"
    assert e._entry_filters("X", first=True)[0] == ""


def test_pro_hold_buys_on_pro_trader_buy(tmp_path, monkeypatch):
    from src.signal import botpredict as BPm
    monkeypatch.setitem(BPm.MODEL, "thr", 2.0)       # Botpredict never calls BUY here
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule = "pro_hold"
    e.cfg.pro_hold_first = False
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert routed and routed[-1]["side"] == "BUY" and "Pro Trader" in routed[-1]["pro_why"]
    e2, f2, rows2, routed2 = _cm_engine(tmp_path, monkeypatch)
    e2.cfg.rule = "pro_hold"
    e2.cfg.pro_hold_first = False
    e2.cfg.pro_hold_pro_buy = False
    e2._evaluate(f2, time.time())
    f2.books["pump_rest"].merge(rows2[-1:])
    e2._evaluate(f2, time.time())
    assert not routed2


def test_support_feed_gets_the_scan_tokens_age(monkeypatch):
    from src.signal import candle_momo as CM
    from src.signal import dayscalp as DS
    e = CM.CandleMomo()
    e.cfg.mode = "live"
    e.cfg.rule = "pro_hold"
    monkeypatch.setattr(e, "support_source", lambda: "dayscalp")
    monkeypatch.setattr(e, "_support_sync", lambda src: None)
    monkeypatch.setattr(e, "_support_held", lambda src: [])
    now = time.time()
    picks = [{"mint": "N" + "x" * 40, "symbol": "N", "ts": now, "age": 6.0}]
    monkeypatch.setattr(DS, "LAST_CANDLE_PICKS", picks)
    monkeypatch.setitem(DS.LAST_CANDLE_PICK, "mint", picks[0]["mint"])
    monkeypatch.setitem(DS.LAST_CANDLE_PICK, "ts", now)
    e._pick([])
    f = e.feeds[picks[0]["mint"]]
    assert f.age is not None and 6.0 <= f.age < 8.0     # young: the first-candle buy applies


def test_pro_hold_first_buy_waits_for_the_first_green_candle(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 4)
    e.cfg.pro_hold_first_at = "green"
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t, open=1.1, high=1.1, low=1.0, close=1.0, volume=3, final=True)])   # red
    e._evaluate(f, time.time())
    assert not routed and not getattr(f, "first_done", False)
    book.merge([C.Candle(t=t + 1000, open=1.0, high=1.2, low=1.0, close=1.15, volume=3, final=True)])  # green
    e._evaluate(f, time.time())
    assert routed and routed[0]["first"] and "green" in routed[0]["pro_why"]


def test_skip_mayhem_does_not_block_the_mayhemmode_source(monkeypatch):
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.avoid_mayhem = True
    e.cfg.inflow_window_s = 0
    e.cfg.max_below_peak_pct = 0
    f = CM.Feed("MM", 10)
    f.board_state = "active"
    e.feeds["MM"] = f
    monkeypatch.setattr(e, "support_source", lambda: "mayhem")
    assert e._entry_filters("MM")[0] == ""
    monkeypatch.setattr(e, "support_source", lambda: "opencopy")
    assert e._entry_filters("MM")[0] == "MAYHEM_COIN"


def test_pro_hold_first_buy_fires_while_the_green_candle_is_forming(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 4)
    e.cfg.pro_hold_first_at = "green"
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t, open=1.1, high=1.1, low=1.0, close=1.0, volume=3, final=True)])   # red
    e._evaluate(f, time.time())
    assert not routed
    book.merge([C.Candle(t=t + 1000, open=1.0, high=1.03, low=1.0, close=1.03, volume=1, final=False)])
    e._evaluate(f, time.time())
    assert routed and routed[0]["first"] and "forming" in routed[0]["pro_why"]


def test_pro_hold_first_buy_at_the_start_of_the_chart(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 3)
    assert e.cfg.pro_hold_first_at == "start"
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t, open=1.0, high=1.0, low=1.0, close=1.0, volume=1, final=False)])   # flat, just begun
    e._evaluate(f, time.time())
    assert routed and routed[0]["first"] and "just started" in routed[0]["pro_why"]


def test_pro_hold_start_with_history_waits_for_green(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 30)
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t + k * 1000, open=1.0 - k * 0.01, high=1.0, low=0.9, close=0.99 - k * 0.01,
                         volume=1, final=True) for k in range(5)])                                # already moving down
    e._evaluate(f, time.time())
    assert not routed                                  # not "just started": waits for a green start


def test_pro_hold_first_buy_at_once_buys_any_coin_it_is_given(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 900)        # old coin, long history
    e.cfg.pro_hold_first_at = "now"
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.merge([C.Candle(t=t + k * 1000, open=1.0, high=1.0, low=0.9, close=0.95, volume=1, final=True)
                for k in range(1, 8)])
    e._evaluate(f, time.time())
    assert len(routed) == 1 and routed[0]["first"] and routed[0]["now_buy"] and "0.00%" in routed[0]["pro_why"]
    e._evaluate(f, time.time())
    assert len(routed) == 1                                             # once per coin


def test_at_once_buy_is_not_held_by_far_below_peak():
    from src.signal import candle_momo as CM
    from src.signal import candle_core as C
    e = CM.CandleMomo()
    e.cfg.inflow_window_s, e.cfg.max_below_peak_pct, e.cfg.avoid_mayhem = 0, 50, False
    f = CM.Feed("P", 10)
    f.using = "pump_rest"
    f.books["pump_rest"].merge([C.Candle(t=1000, open=10, high=10, low=1, close=1, volume=1, final=True)])
    e.feeds["P"] = f
    assert e._entry_filters("P")[0] == "FAR_BELOW_PEAK"
    assert e._entry_filters("P", first=True, now_buy=True)[0] == ""


def _flat_engine(tmp_path, monkeypatch, candles):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 3)
    e.cfg.pro_hold_first_at = "flat"
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t + k * 1000, open=o, high=max(o, c), low=min(o, c), close=c, volume=1, final=(k < len(candles) - 1))
                for k, (o, c) in enumerate(candles)])
    return e, f, routed


def test_flat_buys_on_the_price_line_with_no_candles(tmp_path, monkeypatch):
    e, f, routed = _flat_engine(tmp_path, monkeypatch, [(1.0, 1.0)])
    e._evaluate(f, time.time())
    assert routed and routed[0]["first"] and "no candles yet" in routed[0]["pro_why"]


def test_flat_waits_while_the_only_candle_has_moved(tmp_path, monkeypatch):
    e, f, routed = _flat_engine(tmp_path, monkeypatch, [(1.0, 1.05)])
    e._evaluate(f, time.time())
    assert not routed and not getattr(f, "first_done", False)


def test_flat_skips_a_chart_that_already_has_candles(tmp_path, monkeypatch):
    e, f, routed = _flat_engine(tmp_path, monkeypatch, [(1.0, 1.0), (1.0, 1.0)])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r.get("first")] and f.first_done


def test_pro_hold_takes_profit_as_the_forming_candle_turns_red(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 3)
    e.cfg.pro_hold_first = False
    monkeypatch.setattr(e, "green_target", lambda: (15.0, "Min profit"))
    e.positions["NEW"] = {"mint": "NEW", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": time.time() - 5,
                          "upct": 22.0, "value_at": time.time(), "pro_peak": 25.0, "lots": [{"n": 1}]}
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.merge([C.Candle(t=t + 1000, open=1.2, high=1.2, low=1.15, close=1.16, volume=1, final=False)])  # turning red
    e._evaluate(f, time.time())
    sells = [r for r in routed if r["side"] == "SELL"]
    assert len(sells) == 1 and "sold at once" in sells[0]["pro_why"] and sells[0]["pro_hold_sell"]
    e._evaluate(f, time.time())
    assert len([r for r in routed if r["side"] == "SELL"]) == 1      # once per candle


def _gar_engine(tmp_path, monkeypatch, candles):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 30)
    e.cfg.pro_hold_first_at = "gar"
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t + k * 1000, open=o, high=max(o, c), low=min(o, c), close=c, volume=1,
                         final=(k < len(candles) - 1)) for k, (o, c) in enumerate(candles)])
    return e, f, routed


def test_gar_buys_green_after_red_when_flat_and_down(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 2.0), (2.0, 1.5), (1.5, 1.6)])   # up, red, green starting
    f.first_done = True                                       # the start buy is tested on its own
    e._evaluate(f, time.time())
    assert len(routed) == 1 and routed[0]["side"] == "BUY" and "green after red" in routed[0]["pro_why"]
    e._evaluate(f, time.time())
    assert len(routed) == 1                                   # once per candle


def test_gar_adds_only_while_the_trade_is_in_a_loss(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 2.0), (2.0, 1.5), (1.5, 1.6)])
    e.positions["NEW"] = {"mint": "NEW", "mode": "paper", "upct": 12.0, "value_at": time.time(), "lots": [{"n": 1}],
                          "entry_sol": 0.005, "tokens": 1.0, "entry_ts": time.time()}
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]      # in profit: no buy
    e.positions["NEW"]["upct"] = -20.0
    f.gar_t = None
    e._evaluate(f, time.time())
    assert [r for r in routed if r["side"] == "BUY" and "lower the average" in r["pro_why"]]


def test_gar_needs_a_red_before(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 2.0), (2.0, 2.5), (2.5, 2.6)])  # green, green
    e._evaluate(f, time.time())
    assert not [r for r in routed if "green after red" in r.get("pro_why", "")]


def test_gar_starts_with_a_buy_at_the_start(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 1.0)])       # chart just started
    e._evaluate(f, time.time())
    assert len(routed) == 1 and "bought at the start" in routed[0]["pro_why"] and routed[0]["first"]


def test_gar_signal_skips_cooldown_and_entry_checks(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 2.0), (2.0, 1.5), (1.5, 1.6)])
    e._evaluate(f, time.time())
    sig = routed[-1]
    assert sig["gar"] and sig["first"] and sig["now_buy"]
    e.cfg.inflow_window_s, e.cfg.max_below_peak_pct = 5, 10
    assert e._entry_filters("NEW", first=True, now_buy=True)[0] == ""


def test_gar_always_buys_at_the_start_even_with_history(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 1.0)] * 6)        # older chart, flat, no red
    f.age = 500
    e._evaluate(f, time.time())
    assert len(routed) == 1 and "bought at the start" in routed[0]["pro_why"] and routed[0]["gar"]


def test_pro_hold_adds_stop_at_max_lots(tmp_path, monkeypatch):
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.rule, e.cfg.max_ladder_buys, e.cfg.ladder_max_total_sol, e.cfg.buy_size_sol = "pro_hold", 2, 1.0, 0.005
    e.feeds["H"] = CM.Feed("H", 10)
    e.feeds["H"].status = "OK"
    e.positions["H"] = {"mint": "H", "mode": "paper", "entry_sol": 0.01, "lots": [{"n": 1}, {"n": 2}]}
    sig = {"id": "a1", "mint": "H", "side": "BUY", "pro": True, "_counted": True, "deltaPct": 0.0}
    assert e._route(sig) == "LADDER_MAX"
    e.cfg.max_ladder_buys = 5
    assert e._route(dict(sig, id="a2")) != "LADDER_MAX"


def test_pro_hold_hint_names_the_limit():
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.rule, e.cfg.max_ladder_buys, e.cfg.ladder_max_total_sol, e.cfg.buy_size_sol = "pro_hold", 5, 0.015, 0.005
    e.positions["X"] = {"mint": "X", "mode": "paper", "entry_sol": 0.005, "lots": [{"n": 1}], "upct": -3.0}
    h = e._hint("X")
    assert "allows 3" in h["entry"] and "raise Total cap to 0.025" in h["entry"]


def test_pro_hold_sells_at_once_when_the_chart_shows_profit(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 3)
    e.cfg.pro_hold_first = False
    monkeypatch.setattr(e, "green_target", lambda: (10.0, "Min profit"))
    monkeypatch.setattr(e, "_network_fee_sol", lambda: 0.0)
    e.positions["NEW"] = {"mint": "NEW", "mode": "paper", "tokens": 1.0, "entry_sol": 0.01, "entry_ts": time.time(),
                          "lots": [{"n": 1, "sol": 0.005, "px": 1.0, "src": "pump_rest"},
                                   {"n": 2, "sol": 0.005, "px": 0.5, "src": "pump_rest"}]}
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.merge([C.Candle(t=t + 1000, open=0.8, high=0.9, low=0.8, close=0.9, volume=1, final=False)])  # green, still rising
    e._evaluate(f, time.time())
    sells = [r for r in routed if r["side"] == "SELL"]
    assert len(sells) == 1 and "sold at once" in sells[0]["pro_why"]        # avg 0.667 -> 0.9 = +35%: no red wait


def test_pro_hold_no_sell_below_target_on_the_chart(tmp_path, monkeypatch):
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 3)
    e.cfg.pro_hold_first = False
    monkeypatch.setattr(e, "green_target", lambda: (10.0, "Min profit"))
    monkeypatch.setattr(e, "_network_fee_sol", lambda: 0.0)
    e.positions["NEW"] = {"mint": "NEW", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": time.time(),
                          "lots": [{"n": 1, "sol": 0.005, "px": 1.0, "src": "pump_rest"}]}
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.merge([C.Candle(t=t + 1000, open=1.0, high=1.05, low=1.0, close=1.05, volume=1, final=False)])  # +3% after fees
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "SELL"]


def test_one_feed_at_a_time_for_every_feed():
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.auto_follow, e.cfg.rule, e.cfg.max_mints = "mayhem_board", "inverse", 3
    rows = [{"m": f"Z{i}pump", "t": f"Z{i}", "ms": "active", "age": 10 + i} for i in range(3)]
    e._pick(rows)
    assert len(e.feeds) == 1
    held = next(iter(e.feeds))
    e.positions[held] = {"mint": held}
    e._pick(rows)
    assert set(e.feeds) == {held}


def test_fast_upct_applies_the_price_impact():
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    f = CM.Feed("I", 10)
    f.using = "pump_rest"
    held = {"lots": [{"sol": 0.01, "px": 1.0, "src": "pump_rest"}]}
    e._network_fee_sol = lambda: 0.0
    assert round(e._fast_upct(held, f, 1.2)) == 18          # chart +20% - 2% fees
    held["impact"] = 0.85                                     # the real sale gets 15% less than the chart shows
    assert round(e._fast_upct(held, f, 1.2)) == 0


def test_fast_upct_has_no_pump_fee_for_jupiter_valued_coins():
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e._network_fee_sol = lambda: 0.0
    f = CM.Feed("G", 10)
    f.using = "pump_rest"
    held = {"lots": [{"sol": 0.01, "px": 1.0, "src": "pump_rest"}], "value_src": "jupiter"}
    assert round(e._fast_upct(held, f, 1.1)) == 10
    held["value_src"] = "curve"
    assert round(e._fast_upct(held, f, 1.1)) == 8


def test_pinned_coin_buys_only_on_the_pro_trader_buy(tmp_path, monkeypatch):
    from src.signal import botpredict as BPm
    from src.signal import pro_trader as PT
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule, e.cfg.pro_hold_first, e.cfg.pro_hold_first_at = "pro_hold", True, "now"
    e.cfg.mints = ["MINT"]
    f.origin = "manual"
    f.age = 3
    e.mayhem_of["MINT"] = False
    monkeypatch.setitem(BPm.MODEL, "thr", 0.0)               # Botpredict would always buy: must be ignored
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "WAIT"})
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]      # no first-candle / BP buy for a pinned coin
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "BUY", "px": 1.0, "why": "VWAP dip"})
    from src.signal import candle_core as C
    last = rows[-1]
    f.books["pump_rest"].merge([C.Candle(t=last.t + 1000, open=last.close, high=last.close, low=last.close,
                                         close=last.close, volume=1, final=True)])
    e._evaluate(f, time.time())
    buys = [r for r in routed if r["side"] == "BUY"]
    assert len(buys) == 1 and "pinned coin" in buys[0]["pro_why"]


def _pinned_held(tmp_path, monkeypatch):
    from src.signal import pro_trader as PT
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 3)
    e.cfg.mints = ["NEW"]
    e.cfg.pinned_min_profit_pct = 1.5
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "WAIT"})
    monkeypatch.setattr(e, "_network_fee_sol", lambda: 0.0)
    e.positions["NEW"] = {"mint": "NEW", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": time.time(),
                          "value_src": "jupiter", "lots": [{"n": 1, "sol": 0.005, "px": 1.0, "src": "pump_rest"}]}
    return e, f, routed


def _forming(f, k, o, c):
    from src.signal import candle_core as C
    t = f.books["pump_rest"].rows[-1].t + 1000 * k
    f.books["pump_rest"].merge([C.Candle(t=t, open=o, high=max(o, c), low=min(o, c), close=c, volume=1, final=False)])


def test_pinned_rides_greens_then_sells_on_first_red_in_profit(tmp_path, monkeypatch):
    e, f, routed = _pinned_held(tmp_path, monkeypatch)
    _forming(f, 1, 1.00, 1.03)                 # green, +3%: in profit, keep riding
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "SELL"]
    _forming(f, 2, 1.06, 1.05)                 # first red while still +5%: sell
    e._evaluate(f, time.time())
    sells = [r for r in routed if r["side"] == "SELL"]
    assert len(sells) == 1 and "pinned: green run ended" in sells[0]["pro_why"]


def test_pinned_never_sells_under_its_profit(tmp_path, monkeypatch):
    e, f, routed = _pinned_held(tmp_path, monkeypatch)
    _forming(f, 1, 1.02, 1.01)                 # red at +1% (< 1.5%): hold
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "SELL"]


def _garl(tmp_path, monkeypatch, candles, lots):
    """candles: (open, high, low, close); the last one is still forming."""
    from src.signal import candle_core as C
    e, f, routed = _ph_first_engine(tmp_path, monkeypatch, 30)
    e.cfg.pro_hold_first_at = "gar_low"
    e.cfg.lot_low_pct, e.cfg.lot_x_times = 0.0, 1.0      # Lotlow 0 % = the original rule: any lower low
    f.first_done = True
    book = f.books["pump_rest"]
    t = book.rows[-1].t
    book.rows.clear()
    book.merge([C.Candle(t=t + k * 1000, open=o, high=h, low=l, close=c, volume=1, final=(k < len(candles) - 1))
                for k, (o, h, l, c) in enumerate(candles)])
    for lot in lots:
        if "k" in lot:
            lot["lot_t"] = t + lot.pop("k") * 1000
        if "ts_k" in lot:
            lot["ts"] = (t + lot.pop("ts_k") * 1000) / 1000.0 + 0.5
    e.positions["NEW"] = {"mint": "NEW", "mode": "paper", "upct": -5.0, "value_at": time.time(), "lots": lots,
                          "entry_sol": 0.005, "tokens": 1.0, "entry_ts": time.time()}
    return e, f, routed


def test_gar_low_buys_lot2_only_below_lot1s_candle_low(tmp_path, monkeypatch):
    # L1 bought in candle 0 (low 1.9). Red low 1.5, green opens 1.5: both under 1.9 -> lot 2
    e, f, routed = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (2.0, 2.0, 1.5, 1.5), (1.5, 1.6, 1.5, 1.6)],
                         [{"n": 1, "k": 0}])
    e._evaluate(f, time.time())
    buys = [r for r in routed if r["side"] == "BUY"]
    assert len(buys) == 1 and "low below" in buys[0]["pro_why"] and buys[0]["low_ref"] == 1.5


def test_gar_low_skips_a_green_after_red_above_the_last_lots_low(tmp_path, monkeypatch):
    # L1's candle low 1.0; the red's low (1.4) and the green's open (1.5) are higher -> no lot
    e, f, routed = _garl(tmp_path, monkeypatch, [(1.0, 2.0, 1.0, 2.0), (2.0, 2.0, 1.4, 1.5), (1.5, 1.6, 1.5, 1.6)],
                         [{"n": 1, "k": 0}])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]


def test_gar_low_lot3_must_be_below_lot2(tmp_path, monkeypatch):
    # lot 2 was bought with red low / green open 1.5; this red's low 1.6 is above it -> no lot 3
    e, f, routed = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (2.0, 2.0, 1.6, 1.7), (1.7, 1.8, 1.7, 1.8)],
                         [{"n": 1, "k": 0}, {"n": 2, "low_ref": 1.5}])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]
    # red low under 1.5 but the green opens above it -> still no lot
    e, f, routed = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (1.7, 1.7, 1.4, 1.55), (1.55, 1.6, 1.55, 1.6)],
                         [{"n": 1, "k": 0}, {"n": 2, "low_ref": 1.5}])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]
    # both under 1.5 -> lot 3
    e, f, routed = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (1.6, 1.6, 1.3, 1.4), (1.4, 1.45, 1.4, 1.45)],
                         [{"n": 1, "k": 0}, {"n": 2, "low_ref": 1.5}])
    e._evaluate(f, time.time())
    assert len([r for r in routed if r["side"] == "BUY"]) == 1


def test_gar_low_starts_with_a_buy_and_plain_gar_is_unchanged(tmp_path, monkeypatch):
    e, f, routed = _gar_engine(tmp_path, monkeypatch, [(1.0, 1.0)])
    e.cfg.pro_hold_first_at = "gar_low"
    e._evaluate(f, time.time())
    assert len(routed) == 1 and "bought at the start" in routed[0]["pro_why"]
    # plain "gar": a green after a red above lot 1's low still adds while the trade is in a loss
    e, f, routed = _garl(tmp_path, monkeypatch, [(1.0, 2.0, 1.0, 2.0), (2.0, 2.0, 1.4, 1.5), (1.5, 1.6, 1.5, 1.6)],
                         [{"n": 1, "k": 0}])
    e.cfg.pro_hold_first_at = "gar"
    e._evaluate(f, time.time())
    assert [r for r in routed if r["side"] == "BUY" and "lower the average" in r["pro_why"]]


def test_gar_low_lot1_bought_on_another_feed_uses_this_charts_candle(tmp_path, monkeypatch):
    """SXSN 2026-11-15: lot 1 was bought on pump_rest (price 2.157e-06), the chart then ran on the stream (2.5e-08):
    every candle looked below lot 1, and lot 2 was bought ABOVE it. The low is now this chart's candle at the buy."""
    chart = [(2.55, 2.55, 2.55, 2.55), (2.55, 3.3, 2.55, 3.3), (3.3, 3.3, 2.46, 3.0), (3.0, 3.2, 3.0, 3.2)]
    lot1 = {"n": 1, "src": "stream", "px": 215.7, "lot_t": 1, "ts_k": 0}
    e, f, routed = _garl(tmp_path, monkeypatch, chart, [dict(lot1)])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]          # green opens at 3.0, above lot 1's low 2.55
    chart = [(2.55, 2.55, 2.55, 2.55), (2.55, 2.6, 2.3, 2.4), (2.4, 2.5, 2.4, 2.5)]
    e, f, routed = _garl(tmp_path, monkeypatch, chart, [dict(lot1)])
    e._evaluate(f, time.time())
    assert len([r for r in routed if r["side"] == "BUY"]) == 1    # red low 2.3 and green open 2.4 are under 2.55


def test_gar_low_equal_low_is_not_lower(tmp_path, monkeypatch):
    e, f, routed = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (2.0, 2.0, 1.9, 1.9), (1.9, 2.0, 1.9, 2.0)],
                         [{"n": 1, "k": 0}])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]


def test_lot_marker_sits_on_the_candle_the_order_was_sent_in():
    from src.signal import candle_momo as CM
    # sent 0.76 s into candle 387, filled at 388.3: the marker belongs to candle 387
    assert int(CM.CandleMomo._mark_ts({"ts": 1794751388.30, "lot_t": 1794751387000})) == 1794751387
    assert CM.CandleMomo._mark_ts({"ts": 1794751377.49, "lot_t": 1794751373000}) == 1794751377.49   # slow feed: fill time
    assert CM.CandleMomo._mark_ts({"ts": 5.0}) == 5.0                                                # old lots


def test_signal_only_buys_just_on_the_chart_buy(tmp_path, monkeypatch):
    """pro_hold first buy "signal" (user 2026-11-16): no buy at the start, no green-after-red, no ◆ BP — only ▲ BUY."""
    from src.signal import botpredict as BPm
    from src.signal import pro_trader as PT
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule, e.cfg.pro_hold_first, e.cfg.pro_hold_first_at = "pro_hold", True, "signal"
    f.age = 3
    e.mayhem_of["MINT"] = False
    monkeypatch.setitem(BPm.MODEL, "thr", 0.0)               # Botpredict would always buy: must be ignored
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "WAIT"})
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]      # a new coin, but no ▲ BUY yet: nothing bought
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "BUY", "px": 1.0, "why": "VWAP dip"})
    last = rows[-1]
    f.books["pump_rest"].merge([C.Candle(t=last.t + 1000, open=last.close, high=last.close, low=last.close,
                                         close=last.close, volume=1, final=True)])
    e._evaluate(f, time.time())
    buys = [r for r in routed if r["side"] == "BUY"]
    assert len(buys) == 1 and buys[0]["pro_why"].startswith("▲ BUY signal")
    # bought ON the signal: the SOL-outflow / far-below-high / Min reserve / cooldown checks do not hold it back
    assert buys[0]["first"] and buys[0]["now_buy"] and buys[0]["gar"]
    e.cfg.inflow_window_s, e.cfg.max_below_peak_pct = 5, 10
    assert e._entry_filters("MINT", first=True, now_buy=True)[0] == ""


def test_signal_on_a_candle_that_arrived_in_a_batch_still_buys(tmp_path, monkeypatch):
    """2026-11-16: three candles came at once; the ▲ BUY was on the middle one and only the newest was read."""
    from src.signal import pro_trader as PT
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule, e.cfg.pro_hold_first, e.cfg.pro_hold_first_at = "pro_hold", True, "signal"
    e.mayhem_of["MINT"] = False
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "WAIT"})
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    last = rows[-1]
    buy_t = last.t + 2000                                     # the ▲ BUY candle: the 2nd of the 3 new ones
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "BUY", "px": 1.0, "why": "VWAP dip"}
                        if cs[-1]["t"] == buy_t else {"call": "WAIT"})
    f.books["pump_rest"].merge([C.Candle(t=last.t + k * 1000, open=last.close, high=last.close, low=last.close,
                                         close=last.close, volume=1, final=True) for k in (1, 2, 3)])
    e._evaluate(f, time.time())
    buys = [r for r in routed if r["side"] == "BUY"]
    assert len(buys) == 1 and "arrived late" in buys[0]["pro_why"]
    # a ▲ BUY more than 5 s before the newest candle is too old
    from src.signal import candle_momo as CM
    assert CM.CandleMomo._late_buy(PT, [{"t": 0}, {"t": 1000}, {"t": 9000}], 0) is None


def test_bp_only_buys_lot1_on_bp_and_next_lots_only_25pct_lower(tmp_path, monkeypatch):
    """pro_hold first buy "bp" (user 2026-11-16): only ◆ BP BUY buys; the first is lot 1; a later ◆ BP BUY buys only
    when its candle closed at least 25% below the candle of the lot before — not on every tag."""
    from src.signal import botpredict as BPm
    from src.signal import pro_trader as PT
    from src.signal import candle_core as C
    from src.signal import candle_momo as CM
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule, e.cfg.pro_hold_first, e.cfg.pro_hold_first_at, e.cfg.pro_hold_pro_buy = "pro_hold", True, "bp", True
    f.age = 3
    e.mayhem_of["MINT"] = False
    monkeypatch.setattr(PT, "analyze", lambda cs, h, held: {"call": "BUY", "px": 1.0, "why": "VWAP dip"})  # ▲ BUY: ignored
    monkeypatch.setattr(BPm, "analyze", lambda cs, h=None: {"call": "WAIT"})
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]           # no first-candle buy, no ▲ BUY buy
    monkeypatch.setattr(BPm, "analyze", lambda cs, h=None: {"call": "BUY", "px": 1.0, "why": "43% chance"})
    t = [rows[-1].t]

    def candle(px):
        t[0] += 1000
        f.books["pump_rest"].merge([C.Candle(t=t[0], open=px, high=px, low=px, close=px, volume=1, final=True)])
        e._evaluate(f, time.time())
        return t[0]
    t1 = candle(1.0)
    buys = [r for r in routed if r["side"] == "BUY"]
    assert len(buys) == 1 and "lot 1" in buys[0]["pro_why"] and buys[0]["first"] and buys[0]["gar"]
    e.positions["MINT"] = {"mint": "MINT", "mode": "paper", "tokens": 1.0, "entry_sol": 0.005, "entry_ts": time.time(),
                           "lots": [{"n": 1, "lot_t": t1, "src": "pump_rest", "ts": time.time()}]}
    candle(0.9)                                                     # ◆ BP BUY still on, only 10% lower: no lot
    candle(0.76)                                                    # 24% lower: no lot
    assert len([r for r in routed if r["side"] == "BUY"]) == 1
    t2 = candle(0.74)                                               # 26% lower: lot 2
    buys = [r for r in routed if r["side"] == "BUY"]
    assert len(buys) == 2 and "below the last lot's candle" in buys[1]["pro_why"]
    e.positions["MINT"]["lots"].append({"n": 2, "lot_t": t2, "src": "pump_rest", "ts": time.time()})
    candle(0.60)                                                    # 19% under lot 2: no lot
    assert len([r for r in routed if r["side"] == "BUY"]) == 2
    candle(0.55)                                                    # 26% under lot 2: lot 3
    assert len([r for r in routed if r["side"] == "BUY"]) == 3
    assert CM.BP_LOWER_PCT == 25.0


def test_bp_only_no_second_lot1_while_buying_and_no_rebuy_when_reuse_is_off(tmp_path, monkeypatch):
    """WARM 2026-11-16: a ◆ BP BUY one second after lot 1's signal was queued and bought as lot 2 (no 25% check), and
    1 s after the profit sale the coin was bought again although Reuse same coin was off."""
    from src.signal import botpredict as BPm
    from src.signal import candle_core as C
    e, f, rows, routed = _cm_engine(tmp_path, monkeypatch)
    e.cfg.rule, e.cfg.pro_hold_first, e.cfg.pro_hold_first_at = "pro_hold", True, "bp"
    e.cfg.reuse_coin = False
    e.mayhem_of["MINT"] = False
    monkeypatch.setattr(BPm, "analyze", lambda cs, h=None: {"call": "WAIT"})
    e._evaluate(f, time.time())
    f.books["pump_rest"].merge(rows[-1:])
    e._evaluate(f, time.time())
    monkeypatch.setattr(BPm, "analyze", lambda cs, h=None: {"call": "BUY", "px": 1.0, "why": "60% chance"})
    t = [rows[-1].t]

    def candle(px=1.0):
        t[0] += 1000
        f.books["pump_rest"].merge([C.Candle(t=t[0], open=px, high=px, low=px, close=px, volume=1, final=True)])
        e._evaluate(f, time.time())
    e.busy["MINT"] = "sig-lot1"                          # lot 1 is still being sent
    candle()
    assert not [r for r in routed if r["side"] == "BUY"]
    e.busy.pop("MINT")
    e.traded["MINT"] = time.time()                       # its trade completed; Reuse same coin is off
    candle()
    assert not [r for r in routed if r["side"] == "BUY"]
    e.cfg.reuse_coin = True
    candle()
    assert len([r for r in routed if r["side"] == "BUY"]) == 1


def test_lot_need_pct_lotlow_and_xtimes():
    """Candles page Lotlow % / xtimes (user 2026-11-17): L2 33 %, L3 66 %, L4 132 % (never reached) with 33 / 2."""
    from src.signal import candle_momo as CM
    e = CM.CandleMomo()
    e.cfg.lot_low_pct, e.cfg.lot_x_times = 33.0, 2.0
    assert e.lot_need_pct({"lots": [{"n": 1}]}) == 33.0
    assert e.lot_need_pct({"lots": [{"n": 1}, {"n": 2}]}) == 66.0
    assert e.lot_need_pct({"lots": [{"n": 1}, {"n": 2}, {"n": 3}]}) == 132.0
    e.cfg.lot_low_pct, e.cfg.lot_x_times = 25.0, 1.0                 # the defaults: every lot 25 % lower
    assert e.lot_need_pct({"lots": [{"n": 1}, {"n": 2}]}) == 25.0


def test_gar_low_lot2_needs_lotlow_pct(tmp_path, monkeypatch):
    """Lotlow 25 %: L1's candle low 1.9; red low / green open 1.5 is only 21 % lower -> no lot 2; 1.4 (26 %) -> lot 2."""
    e, f, routed = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (2.0, 2.0, 1.5, 1.5), (1.5, 1.6, 1.5, 1.6)],
                         [{"n": 1, "k": 0}])
    e.cfg.lot_low_pct = 25.0
    n = len(routed)
    e._evaluate(f, time.time())
    assert len(routed) == n
    e2, f2, routed2 = _garl(tmp_path, monkeypatch, [(2.0, 2.1, 1.9, 2.0), (2.0, 2.0, 1.4, 1.4), (1.4, 1.5, 1.4, 1.5)],
                            [{"n": 1, "k": 0}])
    e2.cfg.lot_low_pct = 25.0
    n2 = len(routed2)
    e2._evaluate(f2, time.time())
    assert len(routed2) == n2 + 1 and "needs 25%" in routed2[-1]["pro_why"]


def test_gar_low_ignores_bp_and_pro_buy_signals(tmp_path, monkeypatch):
    """Cyber 2026-11-18: with "green after red – low below" chosen, ◆ BP and ▲ BUY added lots 15 % / 3 % lower,
    ignoring Lotlow 33 % / xtimes 2. Under gar_low only its own rule buys."""
    from src.signal import botpredict as BPm
    from src.signal import pro_trader as PT
    # all greens: no green-after-red, so only a ◆ BP / ▲ BUY signal could buy
    e, f, routed = _garl(tmp_path, monkeypatch, [(1.0, 1.1, 1.0, 1.05), (1.05, 1.1, 1.05, 1.08), (1.08, 1.1, 1.08, 1.09)],
                         [{"n": 1, "k": 0}])
    e.cfg.pro_hold_pro_buy = True
    monkeypatch.setattr(BPm, "analyze", lambda cs, h=None: {"call": "BUY", "px": 1.0, "why": "BP"})
    monkeypatch.setattr(PT, "analyze", lambda cs, a=None, b=None: {"call": "BUY", "px": 1.0, "why": "PT"})
    f.last_eval_t = None
    e._evaluate(f, time.time())
    e._evaluate(f, time.time())
    assert not [r for r in routed if r["side"] == "BUY"]
    e.cfg.pro_hold_first_at = "gar"                      # plain "gar" keeps the ◆ BP / ▲ BUY buys
    f.last_eval_t = 0
    e._evaluate(f, time.time())
    assert [r for r in routed if r["side"] == "BUY"]
