"""Pump GETs with Chrome's TLS fingerprint.

Python's own HTTPS stack is what Cloudflare answers with 403 Just a moment.
Chrome's fingerprint is the one the browser uses for the same URL. The headers
are Chrome's client hints and sec-fetch-dest / sec-fetch-mode. sec-fetch-site
is not sent. same-site and same-origin are what Pump refuses.
"""

from __future__ import annotations

import http.client
import json
import logging
import threading
import time
import urllib.parse
from typing import Any

import httpx

log = logging.getLogger("sol-sniper.fastget")

# ---------- own DNS cache (2026-11-18): the WSL DNS relay sometimes takes > 1.5 s, and every failed read builds a new
# curl session that looks the host up again — the Candles chart showed "curl: (28) Resolving timed out after 1500 ms".
# Hosts are resolved here (system resolver, in a background thread after the first time), the last good address is
# kept when a lookup fails, and curl is handed the address (CURLOPT_RESOLVE) so a request never waits on DNS.
_DNS_FRESH_S = 300.0
_dns: dict[str, tuple[str, float]] = {}       # host -> (ip, resolved at)
_dns_busy: set[str] = set()
_dns_lock = threading.Lock()


def _dns_lookup(host: str) -> str:
    import socket
    try:
        infos = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
        ip = infos[0][4][0] if infos else ""
    except Exception:
        ip = ""
    with _dns_lock:
        if ip:
            _dns[host] = (ip, time.monotonic())
        _dns_busy.discard(host)
    return ip


def resolved_ip(host: str) -> str:
    """The cached address of host ('' = unknown yet: curl resolves it itself this once). A stale entry is refreshed
    in the background and still used meanwhile."""
    if not host:
        return ""
    with _dns_lock:
        ent = _dns.get(host)
        stale = ent is None or time.monotonic() - ent[1] >= _DNS_FRESH_S
        start = stale and host not in _dns_busy
        if start:
            _dns_busy.add(host)
    if start:
        threading.Thread(target=_dns_lookup, args=(host,), daemon=True, name="dns-cache").start()
    return ent[0] if ent else ""


# ---------- read counter (2026-11-19): pump answered 429 on every host for this connection; one log line a minute shows
# which hosts / lanes make the requests, so the loop that overruns pump's ~10 req/s can be found.
_reads: dict[tuple[str, str], int] = {}
_reads_at = time.monotonic()
_reads_lock = threading.Lock()


def _count(url: str, lane: str) -> None:
    global _reads_at
    host = (urllib.parse.urlsplit(url).hostname or "").replace(".pump.fun", "")
    now = time.monotonic()
    line = ""
    with _reads_lock:
        _reads[(host, lane)] = _reads.get((host, lane), 0) + 1
        span = now - _reads_at
        if span >= 60.0:
            by_host: dict[str, list] = {}
            for (h, ln), n in _reads.items():
                by_host.setdefault(h, []).append((n, ln))
            parts = []
            for h, rows in sorted(by_host.items(), key=lambda kv: -sum(n for n, _ in kv[1])):
                tot = sum(n for n, _ in rows)
                lanes = ", ".join(f"{ln} {n / span:.1f}" for n, ln in sorted(rows, reverse=True)[:6])
                parts.append(f"{h} {tot / span:.1f}/s ({lanes})")
            line = "PUMP reads per second, last minute: " + "; ".join(parts)
            _reads.clear()
            _reads_at = now
    if line:
        log.info(line)


def short_err(exc: BaseException) -> str:
    """What a page shows for a failed pump read: the status and, on a rate limit, that reads are paused —
    not just 'HTTPStatusError'."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = getattr(getattr(exc, "response", None), "status_code", 0) or 0
        if code == 429:
            host = urllib.parse.urlsplit(str(getattr(getattr(exc, "request", None), "url", "") or "")).hostname or ""
            left = int(rate_limit_left(host) or 0) if host else 0
            return "pump rate limit (HTTP 429)" + (f" — reads paused {left}s, retrying" if left else " — retrying")
        if code == 403:
            return "pump blocked this read (HTTP 403) — retrying shortly"
        return f"pump HTTP {code}"
    return type(exc).__name__


def _resolve_opt(url: str) -> dict:
    """curl_options pinning url's host to its cached address ({} when not known yet)."""
    try:
        from curl_cffi import CurlOpt
    except Exception:
        return {}
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    ip = resolved_ip(host)
    if not ip:
        return {}
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return {CurlOpt.RESOLVE: [f"{host}:{port}:{ip}"]}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://pump.fun",
    "Referer": "https://pump.fun/",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
}
_IMPERSONATE = "chrome152"

_local = threading.local()
_lock = threading.Lock()
_cache: dict[str, tuple[float, Any]] = {}
_lane_locals: dict[str, threading.local] = {}
_lane_cache: dict[str, dict[str, tuple[float, Any]]] = {}
_lane_lock = threading.Lock()


def _bag(lane: str) -> threading.local:
    if lane == "shared":
        return _local
    with _lane_lock:
        local = _lane_locals.get(lane)
        if local is None:
            local = threading.local()
            _lane_locals[lane] = local
        return local


def _conn(host: str, timeout: float, lane: str = "shared") -> http.client.HTTPSConnection:
    bag = getattr(_bag(lane), "conns", None)
    if bag is None:
        bag = {}
        _bag(lane).conns = bag
    conn = bag.get(host)
    if conn is not None:
        conn.timeout = timeout
        return conn
    # No custom SSL context. Pump 403s those and accepts this default.
    conn = http.client.HTTPSConnection(host, timeout=timeout)
    bag[host] = conn
    return conn


def _drop(host: str, lane: str = "shared") -> None:
    bag = getattr(_bag(lane), "conns", None) or {}
    conn = bag.pop(host, None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def _arm(conn: http.client.HTTPSConnection, timeout: float) -> None:
    """The socket keeps its old timeout. Setting conn.timeout does not change it.

    A dead profile-api socket was sitting for one to two minutes. This caps that read.
    """
    conn.timeout = timeout
    sock = getattr(conn, "sock", None)
    if sock is not None:
        try:
            sock.settimeout(timeout)
        except Exception:
            pass


_chrome_ok: bool | None = None
# A long-lived Chrome session is what starts 403ing. A restart works because
# it has no cookie and no old socket. Recycle before Cloudflare scores that jar.
_SESSION_LIFE = 90.0
_cool_until: dict[str, float] = {}
_cool_lock = threading.Lock()


def _host_of(url: str) -> str:
    return urllib.parse.urlsplit(url).hostname or ""


def _cool_key(host: str, lane: str) -> str:
    return f"{lane or 'shared'}|{host}"


def _cooling(url: str, lane: str = "") -> bool:
    host = _host_of(url)
    with _cool_lock:
        now = time.monotonic()
        return (now < float(_cool_until.get(_cool_key(host, lane)) or 0)
                or now < float(_rate_until.get(host) or 0))


# HTTP 429 = pump's rate limit for this connection (every lane, every page). Asking on at full speed keeps the
# limit on (2026-11-17: every board and page answered 429 for 20+ minutes). So 429s that keep coming pause ALL reads
# of that host: 5 s, 10, 20, 40, then 60 s at a time, until answers come back. A 429 now and then (a short overrun)
# pauses nothing: every good answer takes one 429 off the count.
_RATE_WINDOW = 180.0
_RATE_FREE = 2                   # this many 429s inside the window pause nothing
_RATE_MAX_S = 60.0
_rate_hits: dict[str, list[float]] = {}
_rate_until: dict[str, float] = {}
_rate_logged: dict[str, float] = {}


def _rate_limited(url: str) -> float:
    """Note one HTTP 429 from this host. Returns the pause (seconds) it started, 0 when none."""
    host = _host_of(url)
    if not host:
        return 0.0
    now = time.monotonic()
    with _cool_lock:
        hits = [t for t in _rate_hits.get(host, []) if now - t < _RATE_WINDOW]
        hits.append(now)
        _rate_hits[host] = hits[-40:]
        over = len(hits) - _RATE_FREE
        if over <= 0:
            return 0.0
        pause = min(_RATE_MAX_S, 5.0 * (2 ** min(over - 1, 6)))
        _rate_until[host] = max(float(_rate_until.get(host) or 0), now + pause)
        say = now - float(_rate_logged.get(host) or 0) > 30.0
        if say:
            _rate_logged[host] = now
    if say:
        log.warning("PUMP rate limit (HTTP 429) on %s — every read of it is paused %.0f s (%d in the last 3 min)",
                    host, pause, len(hits))
    return pause


def _rate_ok(url: str) -> None:
    """A good answer: one 429 less on the count (a short overrun never builds up to a pause)."""
    host = _host_of(url)
    with _cool_lock:
        hits = _rate_hits.get(host)
        if hits:
            hits.pop(0)


def rate_limit_left(host: str = "") -> float:
    """Seconds the pump hosts stay paused after HTTP 429s (0 = none); for the dashboard."""
    now = time.monotonic()
    with _cool_lock:
        vals = [u - now for h, u in _rate_until.items() if not host or h == host]
    return max([0.0] + vals)


def _restart_clients() -> None:
    """Drop every Chrome session. The next call is as clean as a process restart."""
    lanes = ["shared", "callout", "predict", "openholds", "mayhem", "fresh"]
    with _lane_lock:
        lanes.extend(_lane_locals.keys())
    for lane in dict.fromkeys(lanes):
        _close_chrome(lane)


def _mark_blocked(url: str, seconds: float = 20.0, lane: str = "") -> None:
    """A 403 pauses that lane only. Callout does not freeze Predict, and OpenHolds retries in 2s."""
    host = _host_of(url)
    if not host:
        return
    lane = lane or "shared"
    if lane in ("openholds", "mayhem", "mayhemstate") or lane.startswith("mayhem2"):
        seconds = min(float(seconds), 2.0)
    key = _cool_key(host, lane)
    with _cool_lock:
        _cool_until[key] = max(float(_cool_until.get(key) or 0), time.monotonic() + seconds)
    _close_chrome(lane)


def _blocked(url: str) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", url)
    left = rate_limit_left(_host_of(url))
    if left > 0:
        response = httpx.Response(429, request=request)
        return httpx.HTTPStatusError(f"HTTP 429 (pump rate limit: reads paused {left:.0f} s) for {url}",
                                     request=request, response=response)
    response = httpx.Response(403, request=request)
    return httpx.HTTPStatusError(
        f"HTTP 403 client={_IMPERSONATE} for {url}",
        request=request,
        response=response,
    )


def _send_headers() -> dict:
    """Chrome client hints only. sec-fetch-site is never sent, including same-site."""
    headers = dict(_HEADERS)
    for key in list(headers):
        if key.lower() == "sec-fetch-site":
            headers.pop(key, None)
    return headers


def _chrome(lane: str):
    """One Chrome session per lane. It is replaced before Cloudflare can stick a 403 on it."""
    global _chrome_ok
    if _chrome_ok is False:
        return None
    bag = _bag(lane)
    sess = getattr(bag, "session", None)
    born = float(getattr(bag, "born", 0) or 0)
    if sess is not None and time.monotonic() - born >= _SESSION_LIFE:
        _close_chrome(lane)
        sess = None
        bag = _bag(lane)
    if sess is not None:
        return sess
    try:
        from curl_cffi import requests as creq
    except ModuleNotFoundError:
        _chrome_ok = False
        log.warning("Pump client is Python's socket — curl_cffi is not installed, so Cloudflare answers 403")
        return None
    _chrome_ok = True
    try:
        sess = creq.Session(impersonate=_IMPERSONATE)
    except Exception:
        sess = creq.Session(impersonate="chrome146")
    bag.session = sess
    bag.born = time.monotonic()
    return sess


def _close_chrome(lane: str) -> None:
    bag = _bag(lane)
    sess = getattr(bag, "session", None)
    if sess is None:
        return
    bag.session = None
    try:
        sess.close()
    except Exception:
        pass


def _body(text: str) -> Any:
    if not text or not text.strip():
        return {}
    return json.loads(text)


def _plain(url: str, timeout: float, lane: str) -> Any:
    """Same headers, Python's own socket. Used only when the Chrome library is not installed."""
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    last_status = 0
    for attempt in (1, 2):
        conn = _conn(host, timeout, lane)
        try:
            _arm(conn, timeout)
            conn.request("GET", path, headers=_send_headers())
            _arm(conn, timeout)
            resp = conn.getresponse()
            raw = resp.read()
            last_status = resp.status
        except Exception:
            _drop(host, lane)
            if attempt == 1:
                continue
            raise
        if last_status == 200:
            _rate_ok(url)
            text = raw.decode("utf-8", "replace") if raw else "{}"
            return _body(text)
        if last_status == 429:
            _rate_limited(url)
            break
        _drop(host, lane)
        if last_status == 403 or attempt == 2:
            if last_status == 403:
                _mark_blocked(url, 20.0, lane)
            break
    request = httpx.Request("GET", url)
    response = httpx.Response(last_status or 403, request=request)
    raise httpx.HTTPStatusError(
        f"HTTP {last_status} client=plain for {url}",
        request=request,
        response=response,
    )


def _read(url: str, timeout: float, lane: str = "shared") -> Any:
    timeout = max(0.4, float(timeout or 1.0))
    if _cooling(url, lane):
        raise _blocked(url)
    _count(url, lane)
    sess = _chrome(lane)
    if sess is None:
        return _plain(url, timeout, lane)
    last_status = 0
    for attempt in (1, 2):
        if sess is None:
            sess = _chrome(lane)
        if sess is None:
            return _plain(url, timeout, lane)
        try:
            pin = _resolve_opt(url)
            if pin:
                sess.curl_options.update(pin)       # this session reuses the cached address (no DNS wait)
            resp = sess.get(
                url,
                headers=_send_headers(),
                timeout=timeout,
                default_headers=False,
                allow_redirects=True,
            )
            last_status = int(resp.status_code or 0)
        except ModuleNotFoundError:
            _close_chrome(lane)
            return _plain(url, timeout, lane)
        except Exception:
            _close_chrome(lane)
            if attempt == 1:
                sess = None
                continue
            raise
        if last_status == 200:
            _rate_ok(url)
            return _body(resp.text or "")
        if last_status == 429:
            _rate_limited(url)   # rate limit, not a block: keep the warm session; repeated 429s pause the host
            break
        text = resp.text or ""
        challenged = last_status == 403 and ("Just a moment" in text or text.lstrip().startswith("<!DOCTYPE"))
        _close_chrome(lane)
        sess = None
        if last_status != 403 or attempt == 2:
            if last_status == 403:
                _mark_blocked(url, 45.0 if challenged else 8.0, lane)
            break
        # One fresh session. A poisoned cookie 403s; the next socket is clean.
        # Hammering is still refused: a second 403 pauses the host.
    request = httpx.Request("GET", url)
    response = httpx.Response(last_status or 403, request=request)
    raise httpx.HTTPStatusError(
        f"HTTP {last_status} client={_IMPERSONATE} for {url}",
        request=request,
        response=response,
    )


def pump_get_fresh(url: str, timeout: float = 0.8) -> Any:
    """One new request. Chrome when that library is installed, otherwise the plain socket."""
    timeout = max(0.4, float(timeout or 0.8))
    if _cooling(url, "fresh"):
        raise _blocked(url)
    _count(url, "fresh")
    headers = _send_headers()
    headers["Cache-Control"] = "no-cache"
    if _chrome_ok is not False:
        try:
            from curl_cffi import requests as creq

            resp = creq.get(
                url,
                headers=headers,
                timeout=timeout,
                impersonate=_IMPERSONATE,
                default_headers=False,
                allow_redirects=True,
                curl_options=_resolve_opt(url),
            )
            status = int(resp.status_code or 0)
            if status == 200:
                _rate_ok(url)
                return _body(resp.text or "")
            if status == 429:
                _rate_limited(url)
            request = httpx.Request("GET", url)
            response = httpx.Response(status or 403, request=request)
            raise httpx.HTTPStatusError(
                f"HTTP {status} client={_IMPERSONATE} for {url}",
                request=request,
                response=response,
            )
        except ModuleNotFoundError:
            pass
    return _plain(url, timeout, "fresh")


def pump_get(url: str, timeout: float = 3.0, cache_s: float = 0.0) -> Any:
    """JSON GET. cache_s reuses a body that fresh. 0 always hits the network.

    The timeout is only the ceiling. A warm socket returns as soon as Pump answers.
    """
    return _lane_get("shared", url, timeout=timeout, cache_s=cache_s)


def callout_get(url: str, timeout: float = 1.2, cache_s: float = 0.0) -> Any:
    """Callouts only. Its socket is not the OpenHolds socket and not the Predict socket."""
    return _lane_get("callout", url, timeout=timeout, cache_s=cache_s)


def mayhem_get(url: str, timeout: float = 1.5, cache_s: float = 0.0) -> Any:
    """Mayhem_Mode's book only. Its own socket: a rate limit elsewhere never pauses it, it waits at
    most 2s after its own, and it never answers with an old cached body."""
    return _lane_get("mayhem", url, timeout=timeout, cache_s=cache_s)


def mayhem_state_get(url: str, timeout: float = 1.5) -> Any:
    """Mayhem coin state reads (active / paused). Own socket, so they never slow the Mayhem book."""
    return _lane_get("mayhemstate", url, timeout=timeout, cache_s=0.0)


def mayhem2_get(url: str, lane: str = "mayhem2", timeout: float = 1.0) -> Any:
    """Mayhem2 only. lane is mayhem2 (trades), mayhem2pos, mayhem2state or mayhem2board: each its own
    socket, at most 2s of cooling after its own rate limit, never an old cached body."""
    return _lane_get(lane if lane.startswith("mayhem2") else "mayhem2", url, timeout=timeout, cache_s=0.0)


def predict_get(url: str, timeout: float = 4.0, cache_s: float = 0.0) -> Any:
    """Predict only."""
    return _lane_get("predict", url, timeout=timeout, cache_s=cache_s)


def openholds_get(url: str, timeout: float = 1.2, cache_s: float = 0.0) -> Any:
    """OpenHolds only. Portfolio and coin price for a stored caller."""
    return _lane_get("openholds", url, timeout=timeout, cache_s=cache_s)


def _lane_get(lane: str, url: str, timeout: float = 3.0, cache_s: float = 0.0) -> Any:
    now = time.monotonic()
    with _lane_lock:
        cache = _lane_cache.setdefault(lane, {})
    if cache_s > 0:
        with _lane_lock:
            hit = cache.get(url)
        if hit and now - hit[0] < cache_s:
            return hit[1]
    if _cooling(url, lane):
        if lane in ("openholds", "mayhem") or lane.startswith("mayhem2"):
            raise _blocked(url)
        with _lane_lock:
            hit = cache.get(url)
        if hit and now - hit[0] < 60.0:
            return hit[1]
        raise _blocked(url)
    try:
        body = _read(url, max(0.35, float(timeout or 3.0)), lane)
    except Exception:
        if lane not in ("openholds", "mayhem") and not lane.startswith("mayhem2") and (cache_s > 0 or _cooling(url, lane)):
            with _lane_lock:
                hit = cache.get(url)
            if hit and now - hit[0] < 60.0:
                return hit[1]
        raise
    if isinstance(body, (dict, list)):
        with _lane_lock:
            cache[url] = (time.monotonic(), body)
            if len(cache) > 400:
                cutoff = time.monotonic() - 3.0
                for key, (ts, _) in list(cache.items()):
                    if ts < cutoff:
                        cache.pop(key, None)
    return body
