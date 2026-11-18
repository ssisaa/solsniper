"""Own DNS cache for pump reads (2026-11-18: "curl: (28) Resolving timed out after 1500 ms" on the Candles feed)."""
import socket
import time

import src.data.fast_get as FG


def _wait(host):
    for _ in range(100):
        if host not in FG._dns_busy:
            return
        time.sleep(0.01)


def test_resolved_ip_cached_and_kept_when_lookup_fails(monkeypatch):
    FG._dns.clear()
    FG._dns_busy.clear()
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("1.2.3.4", 443))])
    assert FG.resolved_ip("h.example") == ""          # first time: looked up in the background
    _wait("h.example")
    assert FG.resolved_ip("h.example") == "1.2.3.4"
    from curl_cffi import CurlOpt
    assert FG._resolve_opt("https://h.example/x?y=1") == {CurlOpt.RESOLVE: ["h.example:443:1.2.3.4"]}

    def boom(*a, **k):
        raise socket.gaierror("timed out")
    monkeypatch.setattr(socket, "getaddrinfo", boom)
    FG._dns["h.example"] = ("1.2.3.4", time.monotonic() - FG._DNS_FRESH_S - 1)   # stale: refreshed, lookup fails
    assert FG.resolved_ip("h.example") == "1.2.3.4"
    _wait("h.example")
    assert FG.resolved_ip("h.example") == "1.2.3.4"   # the last good address is kept
