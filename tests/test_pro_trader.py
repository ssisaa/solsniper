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
