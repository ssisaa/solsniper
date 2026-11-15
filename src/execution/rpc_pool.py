"""Paid RPC chain with failover.

config/sniper.yaml:

    rpc:
      paid:            # tried in this order; a failing one is skipped for a while
        - https://...
      urls:            # public fallbacks, used only when every paid RPC is down
        - https://api.mainnet-beta.solana.com

primary_url() everywhere in the bot returns active_paid(): the first paid RPC that is not
cooling down. A paid call that fails (connect error, timeout, HTTP 401/403/429/5xx) marks that
RPC bad for COOL_S seconds and rpc_post() moves on to the next one, then to the public nodes.
A small health probe (getSlot every PROBE_S seconds per paid RPC) brings a recovered RPC back.

Settings page (2026-11-15): the user may pick ONE paid RPC as the primary. Then only that one is used as
paid and a failure goes straight to the public nodes (not to the other paid ones). No pick ("Automatic") =
the ordered failover above. RPCs can be added and removed there; the pick, the added URLs and the removed
ones live in logs/rpc_choice.json (never in Git: the URLs carry API keys; yaml is not rewritten).

Never log a full URL: the API key is in it. host() is the only thing that goes to logs.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

log = logging.getLogger("sol-sniper.rpc")

PUBLIC_DEFAULT = ["https://api.mainnet-beta.solana.com", "https://solana-rpc.publicnode.com"]
COOL_S = 30.0
COOL_429_S = 10.0  # rate limit: short pause, the window resets fast
PROBE_S = 20.0
BAD_HTTP = {401, 402, 403, 408, 429}

_lock = threading.Lock()
_cool: dict = {}
_no_method: set = set()  # (url, method) the plan does not allow, e.g. Chainstack free getTokenAccountsByOwner
_cache = {"at": 0.0, "paid": [], "public": [], "listed": [], "known": [], "selected": ""}
CHOICE_PATH = Path(__file__).resolve().parents[2] / "logs" / "rpc_choice.json"
_probe_started = False


def host(url: str) -> str:
    return str(url or "").split("//")[-1].split("/")[0].split("?")[0] or "?"


def rpc_id(url: str) -> str:
    """Short id for the dashboard: the URL itself (it carries the key) never goes to the browser."""
    return hashlib.sha1(str(url or "").encode()).hexdigest()[:12]


def _read_choice() -> Dict[str, Any]:
    try:
        d = json.loads(CHOICE_PATH.read_text() or "{}")
    except Exception:
        d = {}
    if not isinstance(d, dict):
        d = {}
    return {"selected": str(d.get("selected") or ""),
            "added": [str(u) for u in (d.get("added") or []) if str(u).startswith("http")],
            "removed": [str(u) for u in (d.get("removed") or []) if u]}


def _write_choice(d: Dict[str, Any]) -> None:
    CHOICE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHOICE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(d))
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    tmp.replace(CHOICE_PATH)
    _cache["at"] = 0.0


def _load() -> None:
    now = time.time()
    if now - _cache["at"] < 10.0 and (_cache["paid"] or _cache["public"]):
        return
    paid: List[str] = []
    public: List[str] = []
    choice = _read_choice()
    try:
        from src.config.file_config import load_config
        rpc = (load_config() or {}).get("rpc") or {}
        for u in list(rpc.get("paid") or []) + [rpc.get("primary") or ""]:
            u = str(u or "").strip()
            if u.startswith("http") and u not in paid:
                paid.append(u)
        for u in rpc.get("urls") or []:
            u = str(u or "").strip()
            if u.startswith("http") and u not in paid and u not in public:
                public.append(u)
    except Exception:
        log.debug("rpc config read failed", exc_info=True)
    for u in choice["added"]:
        if u not in paid:
            paid.append(u)
    known = list(paid)                                     # every paid URL, removed ones too: never a "public" node
    public = [u for u in public if u not in known]
    listed = [u for u in paid if u not in choice["removed"]]
    for u in PUBLIC_DEFAULT:
        if u not in public and u not in known:
            public.append(u)
    selected = choice["selected"] if choice["selected"] in listed else ""
    # a picked RPC is the only paid one in use: its failure falls back to the public nodes
    _cache.update(at=now, paid=[selected] if selected else listed, public=public, listed=listed,
                  known=known, selected=selected)


def paid_urls() -> List[str]:
    """The paid RPCs in use: the picked one alone, or (Automatic) all listed ones in order."""
    _load()
    return list(_cache["paid"])


def all_paid_urls() -> List[str]:
    """Every paid URL the bot knows (picked or not, removed ones too) — to keep them out of public lists."""
    _load()
    return list(_cache["known"])


def settings_view() -> Dict[str, Any]:
    """For the Settings page: id + host only, never a URL."""
    _load()
    now = time.time()
    rows = [{"id": rpc_id(u), "host": host(u), "ok": _ok_now(u, now)} for u in _cache["listed"]]
    active = active_paid()
    return {"ok": True, "rpcs": rows, "selected": rpc_id(_cache["selected"]) if _cache["selected"] else "",
            "active": host(active), "active_paid": active in _cache["paid"]}


def _by_id(rid: str) -> str:
    _load()
    return next((u for u in _cache["listed"] if rpc_id(u) == str(rid or "")), "")


def select_rpc(rid: str) -> Dict[str, Any]:
    """rid "" = Automatic (ordered failover through every listed paid RPC)."""
    url = _by_id(rid) if rid else ""
    if rid and not url:
        return {"ok": False, "error": "That RPC is not in the list"}
    d = _read_choice()
    d["selected"] = url
    _write_choice(d)
    log.warning("RPC primary set on the Settings page: %s", host(url) + " (failure → public nodes)" if url
                else "Automatic (failover through all paid RPCs)")
    return settings_view()


def add_rpc(url: str) -> Dict[str, Any]:
    url = str(url or "").strip()
    if not url.startswith("https://") or len(url) > 400 or any(c.isspace() for c in url) or "." not in host(url):
        return {"ok": False, "error": "Enter the full https:// RPC URL"}
    d = _read_choice()
    _load()
    if url in _cache["listed"]:
        return {"ok": False, "error": "That RPC is already in the list"}
    answers = False
    try:
        from src.execution.tx_route import sync_https, _rpc_url
        status, text = sync_https("POST", _rpc_url(url), json_body={"jsonrpc": "2.0", "id": 1, "method": "getSlot"}, timeout=4.0)
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        answers = status == 200 and bool((json.loads(text or "{}") or {}).get("result"))
    except Exception:
        answers = False
    d["removed"] = [u for u in d["removed"] if u != url]
    if url not in _cache["known"] and url not in d["added"]:
        d["added"].append(url)
    _write_choice(d)
    log.warning("RPC added on the Settings page: %s (%s)", host(url), "answers" if answers else "did not answer the test")
    out = settings_view()
    out["note"] = f"{host(url)} added — it answers." if answers else f"{host(url)} added, but it did not answer the test call."
    return out


def remove_rpc(rid: str) -> Dict[str, Any]:
    url = _by_id(rid)
    if not url:
        return {"ok": False, "error": "That RPC is not in the list"}
    d = _read_choice()
    if url in d["added"]:
        d["added"] = [u for u in d["added"] if u != url]
    else:
        d["removed"].append(url)          # from sniper.yaml: hidden and unused, the yaml file is not rewritten
    if d["selected"] == url:
        d["selected"] = ""
    _write_choice(d)
    log.warning("RPC removed on the Settings page: %s", host(url))
    return settings_view()


def public_urls() -> List[str]:
    _load()
    return list(_cache["public"])


def is_paid(url: str) -> bool:
    return str(url or "").strip() in paid_urls()


def _ok_now(url: str, now: float) -> bool:
    return _cool.get(url, 0.0) <= now


def healthy_paid() -> List[str]:
    now = time.time()
    return [u for u in paid_urls() if _ok_now(u, now)]


def active_paid() -> str:
    """First healthy paid RPC. All paid down: first healthy public node."""
    ensure_probe()
    now = time.time()
    for u in paid_urls():
        if _ok_now(u, now):
            return u
    for u in public_urls():
        if _ok_now(u, now):
            return u
    allp = paid_urls()
    return allp[0] if allp else PUBLIC_DEFAULT[0]


def plan_blocked(status: int, text: str) -> bool:
    """403/-32601 "requires plan upgrade" / method not allowed: this method only, the RPC is fine."""
    t = str(text or "")[:400].lower()
    return status in (200, 400, 401, 403) and any(
        k in t for k in ("plan upgrade", "upgrade your plan", "not available on", "method not allowed", "not supported on")
    )


def note_no_method(url: str, method: str) -> None:
    key = (str(url or ""), str(method or ""))
    if key not in _no_method:
        _no_method.add(key)
        log.info("RPC %s does not allow %s on this plan — that call goes to the next RPC", host(url), method)


def supports(url: str, method: str) -> bool:
    return (str(url or ""), str(method or "")) not in _no_method


def chain_after(url: str, method: str = "") -> List[str]:
    """Where a failed paid call goes next: the healthy paid RPCs after it, then the public nodes."""
    now = time.time()
    paid = paid_urls()
    rest = paid[paid.index(url) + 1:] if url in paid else paid
    out = [u for u in rest if u != url and _ok_now(u, now) and supports(u, method)]
    out += [u for u in public_urls() if _ok_now(u, now)]
    return out


def mark_bad(url: str, why: str, secs: float = COOL_S) -> None:
    url = str(url or "").strip()
    if not url:
        return
    with _lock:
        was_ok = _ok_now(url, time.time())
        _cool[url] = time.time() + secs
    if was_ok:
        nxt = active_paid()
        log.warning("RPC %s failed (%s) — skipping it %.0fs, now using %s", host(url), why, secs, host(nxt))


def mark_ok(url: str) -> None:
    url = str(url or "").strip()
    with _lock:
        was_bad = not _ok_now(url, time.time())
        _cool.pop(url, None)
    if was_bad and is_paid(url):
        log.info("RPC %s is back — using %s", host(url), host(active_paid()))


def bad_status(status: int) -> bool:
    return status in BAD_HTTP or status >= 500


def _probe_once() -> None:
    from src.execution.tx_route import sync_https, _rpc_url
    body = {"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": [{"commitment": "processed"}]}
    now = time.time()
    for url in paid_urls():
        if _ok_now(url, now):
            continue   # healthy: real calls prove it. Only a cooled-off RPC is probed back.
        try:
            status, text = sync_https("POST", _rpc_url(url), json_body=body, timeout=3.0)
            if isinstance(text, bytes):
                text = text.decode("utf-8", "replace")
            data = json.loads(text or "{}") if status == 200 else {}
            if status == 200 and isinstance(data, dict) and data.get("result"):
                mark_ok(url)
            else:
                mark_bad(url, f"health http {status}", PROBE_S + 5)
        except Exception as exc:
            mark_bad(url, f"health {type(exc).__name__}", PROBE_S + 5)


def ensure_probe() -> None:
    global _probe_started
    if _probe_started:
        return
    with _lock:
        if _probe_started:
            return
        _probe_started = True

    def _loop():
        while True:
            try:
                _probe_once()
            except Exception:
                log.debug("rpc probe failed", exc_info=True)
            time.sleep(PROBE_S)

    threading.Thread(target=_loop, name="rpc-health", daemon=True).start()
