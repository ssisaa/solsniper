"""Signed transactions are re-sent until they land (one send with maxRetries 0 was often dropped)."""
import asyncio
import time

import src.execution.trader as T


def test_rebroadcast_resends_until_landed(monkeypatch):
    import src.execution.rpc_pool as RP
    import src.execution.tx_route as TR
    sent = []
    monkeypatch.setattr(RP, "healthy_paid", lambda: ["https://a", "https://b", "https://c", "https://d"])
    monkeypatch.setattr(TR, "rpc_post", lambda url, body, timeout, x: (sent.append((url, body["params"][1])) or (200, "{}")))

    async def go():
        task = asyncio.ensure_future(T._rebroadcast("TX", "SIG1", every=0.05, seconds=5))
        await asyncio.sleep(0.18)
        T._LANDED.add("SIG1")                      # confirmed: re-broadcast stops
        return await asyncio.wait_for(task, 1)
    n = asyncio.run(go())
    assert 3 <= n <= 12 and {u for u, _ in sent} == {"https://a", "https://b", "https://c"}   # max 3 RPCs
    assert all(p["skipPreflight"] is True for _, p in sent)


def test_rebroadcast_stops_after_time(monkeypatch):
    import src.execution.rpc_pool as RP
    import src.execution.tx_route as TR
    monkeypatch.setattr(RP, "healthy_paid", lambda: ["https://a"])
    monkeypatch.setattr(TR, "rpc_post", lambda *a: (200, "{}"))
    t0 = time.time()
    asyncio.run(T._rebroadcast("TX", "SIG2", every=0.05, seconds=0.3))
    assert time.time() - t0 < 1.0
