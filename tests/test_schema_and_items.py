import copy
import hashlib
import json
import uuid
from datetime import datetime, timezone

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import asdfghjkl2
import ingest_rpc
from conftest import ROOT, load_fixture

SCHEMA_PATH = ROOT / "contracts" / "ingest" / "v1.schema.json"
# sha256 de la copia de packages/contracts/src/ingest/v1.schema.json (monorepo de Surtiveo), con saltos LF.
# Si el contrato cambia, se copia el archivo nuevo y se actualiza este valor en el mismo PR.
SCHEMA_SHA256 = "073780336c6a7d70e8b62bdec2556628c8fe3901dfa8ab9aa04e3c785aadee5e"
NOW = datetime(2026, 11, 26, 10, 15, 0, tzinfo=timezone.utc)

SOURCES = {
    "ANH": "ANH_SCRAPER V2",
    "GENEX": "GENEX_OFFICIAL_WEB",
    "BIOPETROL": "BIOPETROL_OFFICIAL_WEB",
    "GASGROUP": "GASGROUP_OFFICIAL_WEB",
}


@pytest.fixture(scope="module")
def validator():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def errors(validator, payload):
    return [e.message for e in validator.iter_errors(payload)]


def test_schema_copy_is_unmodified():
    data = SCHEMA_PATH.read_bytes().replace(b"\r\n", b"\n")
    assert hashlib.sha256(data).hexdigest() == SCHEMA_SHA256


def test_sources_are_the_historic_texts():
    import main
    assert asdfghjkl2.SOURCE_NAME == SOURCES["ANH"] == main.ANH_SOURCE
    assert {s["id"]: s["source"] for s in main.SCRAPER_REGISTRY} == {
        k: v for k, v in SOURCES.items() if k != "ANH"}


def test_anh_items_match_golden_and_schema(validator):
    items, skipped = asdfghjkl2.build_anh_items(load_fixture("anh_raw.json"), NOW)
    assert items == load_fixture("anh_items_expected.json")
    assert skipped == {"sin_mapeo": 2, "sin_condicion": 1}
    payload = ingest_rpc.build_payload(SOURCES["ANH"], NOW, items)
    assert errors(validator, payload) == []


def test_anh_zero_liters_become_none_and_reported_at_is_sale_date():
    raw = [{"id": 1, "_api_producto": 1, "saldo_litros": 0, "saldo_estado": "alto",
            "fecha_ultima_venta": "2026-11-26T06:00:00"}]
    (item,), _ = asdfghjkl2.build_anh_items(raw, NOW)
    assert "available_liters" not in item
    assert item["reported_at"] == "2026-11-26T10:00:00+00:00"
    assert "liters_reported_at" not in item and "station_id" not in item


@pytest.mark.parametrize("key", ["GENEX", "BIOPETROL", "GASGROUP"])
def test_chain_items_match_golden_and_schema(validator, key):
    source = SOURCES[key]
    items, rejected = ingest_rpc.build_chain_items(load_fixture("chain_records.json")[source], source)
    assert rejected == 0
    assert items == load_fixture("chain_items_expected.json")[source]
    assert all("reported_at" not in i and "liters_reported_at" not in i and "anh_id" not in i for i in items)
    assert errors(validator, ingest_rpc.build_payload(source, NOW, items)) == []


def test_chain_items_reject_wrong_source_and_bad_values():
    src = "GENEX_OFFICIAL_WEB"
    recs = [
        {"station_id": 1, "fuel_type_id": 1, "official_condition": "available", "available_liters": 5.0, "source": "OTRO"},
        {"station_id": 1, "fuel_type_id": 1, "official_condition": "raro", "available_liters": 5.0, "source": src},
        {"station_id": None, "fuel_type_id": 1, "official_condition": "available", "source": src},
        {"station_id": 2, "fuel_type_id": 1, "official_condition": "available", "available_liters": float("nan"), "source": src},
    ]
    items, rejected = ingest_rpc.build_chain_items(recs, src)
    assert rejected == 3
    assert items == [{"station_id": 2, "fuel_type_id": 1, "condition": "available"}]


def base_payload():
    return ingest_rpc.build_payload(
        "ANH_SCRAPER V2", NOW, [{"anh_id": 1, "fuel_type_id": 1, "condition": "available"}],
        run_id=str(uuid.uuid4()))


def test_schema_rejects_bad_payloads(validator):
    assert errors(validator, base_payload()) == []
    cases = {}
    p = base_payload(); p["items"][0]["station_id"] = 5; cases["anh_id y station_id"] = p
    p = base_payload(); p["extra"] = 1; cases["clave extra en payload"] = p
    p = base_payload(); p["items"][0]["otro"] = 1; cases["clave extra en item"] = p
    p = base_payload(); p["items"] = [p["items"][0]] * 1001; cases["1001 items"] = p
    p = base_payload(); p["observed_at"] = "2026-11-26T10:15:00"; cases["observed_at sin zona"] = p
    p = base_payload(); p["items"][0]["available_liters"] = -1; cases["litros negativos"] = p
    p = base_payload(); p["source"] = "anh_scraper v2"; cases["source con formato inválido"] = p
    p = base_payload(); p["contract_version"] = 2; cases["version"] = p
    p = base_payload(); del p["items"][0]["anh_id"]; cases["sin referencia de estación"] = p
    p = base_payload(); p["items"][0]["condition"] = "raro"; cases["condition"] = p
    p = base_payload(); p["run_id"] = "no-uuid"; cases["run_id"] = p
    for name, payload in cases.items():
        assert errors(validator, copy.deepcopy(payload)), name


def test_chunking_respects_size_and_keeps_station_rows_together():
    items = []
    for anh_id in range(1, 251):
        for fuel in (1, 2, 3):
            items.append({"anh_id": anh_id, "fuel_type_id": fuel, "condition": "available"})
    items.reverse()
    chunks = ingest_rpc.chunk_items(items, 300)
    assert all(len(c) <= 300 for c in chunks)
    assert sum(len(c) for c in chunks) == len(items)
    seen = {}
    for n, c in enumerate(chunks):
        for i in c:
            assert seen.setdefault(i["anh_id"], n) == n  # un anh_id nunca queda en dos bloques
    assert len(chunks) == 3


def test_batch_size_config(monkeypatch):
    assert ingest_rpc.get_batch_size() == 300
    for raw, expected in (("50", 50), ("5000", 1000), ("0", 1), ("abc", 300), ("", 300)):
        monkeypatch.setenv("INGEST_BATCH_SIZE", raw)
        assert ingest_rpc.get_batch_size() == expected
