"""The CallOuts reader runs only when something reads it (it costs ~8 pump requests a second)."""
import time
from types import SimpleNamespace

import src.config.runtime as R
import src.observability.dashboard as D


def test_callout_reader_runs_only_for_its_sources_or_its_page(monkeypatch):
    monkeypatch.setattr(D, "_callout_page_at", 0.0)
    for mode, want in (("callout", True), ("opencopy", True), ("mayhem", False), ("launch", False), ("dayscalp", False)):
        monkeypatch.setattr(R, "get_config", lambda m=mode: SimpleNamespace(filter_mode=m))
        assert D.callout_reader_needed() is want, mode
    monkeypatch.setattr(R, "get_config", lambda: SimpleNamespace(filter_mode="mayhem"))
    monkeypatch.setattr(D, "_callout_page_at", time.time() - 5)      # the CallOuts page is open
    assert D.callout_reader_needed() is True
    monkeypatch.setattr(D, "_callout_page_at", time.time() - 60)     # closed a minute ago
    assert D.callout_reader_needed() is False


def test_pumpswap_event_is_decoded_for_the_subscribed_coin():
    """Graduated coins (2026-11-17): PumpSwap BuyEvent / SellEvent from the RPC logs feed the stream chart."""
    import base64
    import struct
    from src.data import caller_stream as S
    body = struct.pack("<q", 1) + struct.pack("<13Q", 254605_000000, 0, 0, 0, 10**12, 5 * 10**12, 1_346_600_000, *([0] * 6))
    raw = S._AMM_BUY + body + bytes(32) + bytes(range(32)) + bytes(64)
    logs = ["Program pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA invoke [1]", "Program data: " + base64.b64encode(raw).decode()]
    (t,) = S.decode_amm_logs(logs, "COINpump")
    assert t["mint"] == "COINpump" and t["txType"] == "buy"
    assert abs(t["solAmount"] - 1.3466) < 1e-9 and abs(t["tokenAmount"] - 254605) < 1e-6
    assert S.decode_amm_logs(logs, "") == []
    sell = S._AMM_SELL + body + bytes(128)
    assert S.decode_amm_logs(["Program data: " + base64.b64encode(sell).decode()], "C")[0]["txType"] == "sell"


def test_lot_on_the_other_chart_is_converted():
    """A lot bought on pump_rest still gets a live % when the chart runs on the stream (other units)."""
    import time as _t
    from src.signal import candle_momo as CM
    from src.signal import candle_core as C
    e = CM.CandleMomo()
    f = CM.Feed("M", e.cfg.count)
    f.using = "stream"
    t0 = (int(_t.time()) - 10) * 1000
    f.books["pump_rest"].merge([C.Candle(t=t0 + i * 1000, open=100.0, high=100.0, low=100.0, close=100.0, volume=1, final=True) for i in range(5)])
    f.books["stream"].merge([C.Candle(t=t0 + i * 1000, open=1.0, high=1.0, low=1.0, close=1.0, volume=1, final=True) for i in range(5)]
                            + [C.Candle(t=t0 + 9000, open=1.0, high=1.1, low=1.0, close=1.1, volume=1, final=False)])
    e.feeds["M"] = f
    e.positions["M"] = {"mint": "M", "mode": "paper", "entry_sol": 0.005, "value_src": "jupiter", "impact": 1.0,
                        "lots": [{"n": 1, "sol": 0.005, "px": 100.0, "src": "pump_rest"}]}
    v, _u, pct = e.live_reading("M")
    assert 9.0 < pct < 10.0
