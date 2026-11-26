import json
import uuid
from datetime import datetime, timezone

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import ingest_rpc
from conftest import ROOT, FakeDb

NOW = datetime(2026, 11, 26, 10, 15, 0, tzinfo=timezone.utc)
SCHEMA = json.loads((ROOT / "contracts" / "ingest" / "v1.schema.json").read_text(encoding="utf-8"))


def items(n, start=1):
    return [{"anh_id": i, "fuel_type_id": 1, "condition": "available"} for i in range(start, start + n)]


def test_one_call_per_chunk_with_valid_payloads_and_unique_run_ids(capsys):
    db = FakeDb()
    summary = ingest_rpc.ingest(db, "ANH_SCRAPER V2", items(700), NOW, batch_size=300)
    assert [len(p["items"]) for p in db.rpc_payloads] == [300, 300, 100]
    assert all(c == ("rpc", "ingest_official_reports", "api") for c in db.calls)
    assert len({p["run_id"] for p in db.rpc_payloads}) == 3
    validator = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
    for p in db.rpc_payloads:
        assert not list(validator.iter_errors(p))
        assert p["source"] == "ANH_SCRAPER V2" and p["observed_at"] == "2026-11-26T10:15:00+00:00"
    assert summary["received"] == 700 and summary["inserted"] == 700 and summary["failed_chunks"] == 0
    assert "[INGEST] ANH_SCRAPER V2" in capsys.readouterr().out


def test_counters_unknown_and_invalid_are_aggregated(capsys):
    def handler(payload):
        return {"received": 2, "inserted": 1, "heartbeats": 1, "duplicates": 0, "duplicates_of_last": 0,
                "duplicates_in_batch": 0, "stale": 0, "superseded": 0, "unknown": 1, "invalid": 1,
                "unknown_stations": [{"anh_id": 2733, "reason": "not_found", "items": 4}],
                "invalid_items": [{"index": 0, "reason": "invalid_condition"}]}

    db = FakeDb(handler)
    s = ingest_rpc.ingest(db, "ANH_SCRAPER V2", items(4), NOW, batch_size=2)
    assert (s["received"], s["inserted"], s["heartbeats"], s["unknown"], s["invalid"]) == (4, 2, 2, 2, 2)
    assert s["unknown_stations"] == [{"anh_id": 2733, "reason": "not_found", "items": 8}]
    assert [i["chunk"] for i in s["invalid_items"]] == [1, 2]
    out = capsys.readouterr().out
    assert "::warning::" in out and "2733" in out and "inserted=2" in out


def test_retry_uses_same_run_id_and_failed_chunk_does_not_stop_others(capsys):
    seen = {}

    def handler(payload):
        rid = payload["run_id"]
        seen[rid] = seen.get(rid, 0) + 1
        first = payload["items"][0]["anh_id"]
        if first == 1 and seen[rid] == 1:
            raise RuntimeError("timeout simulado")  # se recupera en el reintento
        if first == 3:
            raise RuntimeError("falla permanente")  # el bloque 2 falla del todo
        return {"received": len(payload["items"]), "inserted": len(payload["items"])}

    db = FakeDb(handler)
    s = ingest_rpc.ingest(db, "ANH_SCRAPER V2", items(6), NOW, batch_size=2)
    assert s["chunks"] == 3 and s["failed_chunks"] == 1 and s["inserted"] == 4
    retried = [p for p in db.rpc_payloads if p["items"][0]["anh_id"] == 1]
    assert len(retried) == 2 and retried[0]["run_id"] == retried[1]["run_id"]
    assert "::warning::" in capsys.readouterr().out


def test_dry_run_never_calls_rpc():
    db = FakeDb()
    s = ingest_rpc.ingest(db, "ANH_SCRAPER V2", items(10), NOW, dry_run=True)
    assert db.calls == [] and s["chunks"] == 1 and s["inserted"] == 0


def test_observed_at_requires_timezone():
    with pytest.raises(ValueError):
        ingest_rpc.to_rfc3339(datetime(2026, 11, 26, 10, 0, 0))
    assert uuid.UUID(ingest_rpc.build_payload("X_Y", NOW, [])["run_id"])
