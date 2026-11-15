"""Settings page paid-RPC picker: pick one (failure → public), add, remove. 2026-11-15."""
import json

from src.execution import rpc_pool as R

A, B, PUB = "https://a.paid.example/key1", "https://b.paid.example/key2", "https://pub.example"


def _setup(tmp_path, monkeypatch):
    from src.config import file_config
    monkeypatch.setattr(file_config, "load_config", lambda *a, **k: {"rpc": {"paid": [A, B], "urls": [PUB]}})
    monkeypatch.setattr(R, "CHOICE_PATH", tmp_path / "rpc_choice.json")
    monkeypatch.setattr(R, "ensure_probe", lambda: None)
    R._cache["at"] = 0.0
    R._cool.clear()


def test_automatic_keeps_ordered_failover(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    assert R.paid_urls() == [A, B] and R.active_paid() == A
    assert R.chain_after(A)[0] == B


def test_picked_rpc_is_the_only_paid_and_fails_over_to_public(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    out = R.select_rpc(R.rpc_id(B))
    assert out["ok"] and out["selected"] == R.rpc_id(B)
    assert R.paid_urls() == [B] and R.active_paid() == B
    assert A not in R.chain_after(B) and R.chain_after(B)[0] == PUB       # not the other paid RPC
    assert A not in R.public_urls() and A in R.all_paid_urls()            # an unpicked paid RPC is never "public"
    R.mark_bad(B, "test")
    assert R.active_paid() == PUB
    assert R.select_rpc("")["selected"] == "" and R.paid_urls() == [A, B]


def test_add_and_remove(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    from src.execution import tx_route
    monkeypatch.setattr(tx_route, "sync_https", lambda *a, **k: (200, '{"result": 5}'))
    new = "https://c.paid.example/key3"
    assert not R.add_rpc("ftp://nope")["ok"]
    out = R.add_rpc(new)
    assert out["ok"] and [r["host"] for r in out["rpcs"]] == ["a.paid.example", "b.paid.example", "c.paid.example"]
    assert "key3" not in json.dumps(out)                                  # no URL / key goes to the page
    assert not R.add_rpc(new)["ok"]                                       # already listed
    R.select_rpc(R.rpc_id(A))
    out = R.remove_rpc(R.rpc_id(A))                                       # a yaml RPC: hidden, yaml untouched
    assert out["selected"] == "" and R.paid_urls() == [B, new]
    assert A in R.all_paid_urls() and A not in R.public_urls()
    R.remove_rpc(R.rpc_id(new))
    assert R.paid_urls() == [B]
    assert R.add_rpc(A)["ok"] and A in R.paid_urls()                      # a removed one can be added back
