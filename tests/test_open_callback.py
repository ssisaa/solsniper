"""A new trade fires the open callback once; the same coin opened again does not."""
import asyncio

from src.risk.position_manager import PositionManager, PositionMode


def test_open_callback_fires_once_per_new_trade(monkeypatch):
    mgr = PositionManager()
    monkeypatch.setattr(mgr, "_push_remote", lambda: None)
    seen = []
    mgr.add_open_callback(lambda pos: seen.append((pos.mint, pos.entry_sol, pos.fees_sol)))

    async def go():
        await mgr.open_position(mint="M1", mode=PositionMode.LIVE, entry_sol=0.005, tokens=1000.0,
                                source="mayhem2", fees_sol=0.000025, force=True)
        await mgr.open_position(mint="M1", mode=PositionMode.LIVE, entry_sol=0.005, tokens=1000.0,
                                source="mayhem2", fees_sol=0.000025, force=True)   # already open

    asyncio.run(go())
    assert seen == [("M1", 0.005, 0.000025)]
