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
