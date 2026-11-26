"""main.py con scrapers y ANH simulados y un cliente falso: ninguna prueba sale a la red."""
import json

import pytest

import asdfghjkl2
import main
from conftest import FakeDb, load_fixture
from switches import is_opted_in

CHAIN = load_fixture("chain_records.json")
ANH_RAW = load_fixture("anh_raw.json")


@pytest.fixture
def env(monkeypatch):
    """Simula los scrapers, la API de ANH y los cachés; registra las llamadas en fake.calls."""
    fake = FakeDb()

    def make_scrape(source):
        def scrape():
            return json.loads(json.dumps(CHAIN[source]))
        return scrape

    for entry in main.SCRAPER_REGISTRY:
        for attr in ("scrape", "extract_and_parse", "scrape_and_parse_genex"):
            if hasattr(entry["module"], attr):
                monkeypatch.setattr(entry["module"], attr, make_scrape(entry["source"]))

    async def fake_fetch():
        return json.loads(json.dumps(ANH_RAW))

    def fake_stations(db):
        fake.calls.append(("cache", "stations"))
        return {2390: {"id": 5001}, 2400: {"id": 5002}, 2502: {"id": 5003}, 2503: {"id": 796}}

    def fake_snapshots(db):
        fake.calls.append(("cache", "snapshots"))
        return {}

    monkeypatch.setattr(asdfghjkl2, "fetch_all_anh_telemetry", fake_fetch)
    monkeypatch.setattr(asdfghjkl2, "get_station_cache", fake_stations)
    monkeypatch.setattr(asdfghjkl2, "get_latest_official_cache", fake_snapshots)
    monkeypatch.setattr(main.ingestar_despachos_ypfb, "run_ypfb_ingestion",
                        lambda *a, **k: fake.calls.append(("ypfb",)))
    monkeypatch.setattr(main, "create_client", lambda *a, **k: fake)
    monkeypatch.setattr(main, "SUPABASE_URL", "http://supabase.invalid")
    monkeypatch.setattr(main, "SUPABASE_KEY", "ficticia")
    return fake


def test_switches_are_off_by_default(monkeypatch):
    for name in main.RPC_SWITCHES:
        assert is_opted_in(name) is False
    for raw in ("", "banana", "false", "off", "0"):
        monkeypatch.setenv("INGEST_RPC_ANH", raw)
        assert is_opted_in("INGEST_RPC_ANH") is False
    for raw in ("true", "ON", "1", " yes "):
        monkeypatch.setenv("INGEST_RPC_ANH", raw)
        assert is_opted_in("INGEST_RPC_ANH") is True


def test_all_off_uses_only_legacy_path(env):
    main.main()
    assert env.rpc_payloads == []
    assert ("cache", "stations") in env.calls and ("cache", "snapshots") in env.calls
    # un solo insert directo con cadenas y ANH de respaldo, como antes
    assert [t for t, _ in env.inserted] == ["station_official_reports"]
    rows = env.inserted[0][1]
    assert {r["source"] for r in rows} == {
        "GENEX_OFFICIAL_WEB", "BIOPETROL_OFFICIAL_WEB", "GASGROUP_OFFICIAL_WEB", "ANH_SCRAPER V2"}
    # cadena > ANH: la estación 796 (combustible 2) la cubre Genex y ANH no la repite
    assert not [r for r in rows if r["source"] == "ANH_SCRAPER V2"
                and r["station_id"] == 796 and r["fuel_type_id"] == 2]


def test_all_on_uses_only_rpc_chains_first_and_no_caches(env, monkeypatch):
    for name in main.RPC_SWITCHES:
        monkeypatch.setenv(name, "true")
    main.main()
    assert env.inserted == []
    assert ("cache", "stations") not in env.calls and ("cache", "snapshots") not in env.calls
    order = [p["source"] for p in env.rpc_payloads]
    assert order[-1] == "ANH_SCRAPER V2"
    assert set(order[:-1]) == {"GENEX_OFFICIAL_WEB", "BIOPETROL_OFFICIAL_WEB", "GASGROUP_OFFICIAL_WEB"}
    assert len(order) == len(set(order))  # una llamada por fuente (pocos items)
    assert all("anh_id" in i and "station_id" not in i for i in env.rpc_payloads[-1]["items"])
    for p in env.rpc_payloads[:-1]:
        assert all("station_id" in i and "reported_at" not in i for i in p["items"])


def test_mixed_chain_rpc_anh_legacy_keeps_chain_over_anh(env, monkeypatch):
    monkeypatch.setenv("INGEST_RPC_GENEX", "true")
    main.main()
    assert [p["source"] for p in env.rpc_payloads] == ["GENEX_OFFICIAL_WEB"]
    rows = env.inserted[0][1]
    sources = {r["source"] for r in rows}
    assert "GENEX_OFFICIAL_WEB" not in sources
    assert {"BIOPETROL_OFFICIAL_WEB", "GASGROUP_OFFICIAL_WEB", "ANH_SCRAPER V2"} <= sources
    assert not [r for r in rows if r["source"] == "ANH_SCRAPER V2"
                and r["station_id"] == 796 and r["fuel_type_id"] == 2]


def test_anh_rpc_with_chains_legacy_inserts_chains_before_anh(env, monkeypatch):
    monkeypatch.setenv("INGEST_RPC_ANH", "true")
    main.main()
    kinds = [c[0] for c in env.calls if c[0] in ("table", "rpc")]
    assert kinds == ["table", "rpc"]  # primero el insert de cadenas, después la RPC de ANH
    assert {r["source"] for r in env.inserted[0][1]} == {
        "GENEX_OFFICIAL_WEB", "BIOPETROL_OFFICIAL_WEB", "GASGROUP_OFFICIAL_WEB"}
    assert env.rpc_payloads[0]["source"] == "ANH_SCRAPER V2"
    assert ("cache", "stations") not in env.calls


def test_scrape_switch_off_skips_source_even_with_rpc_on(env, monkeypatch):
    monkeypatch.setenv("INGEST_RPC_GENEX", "true")
    monkeypatch.setenv("SCRAPE_GENEX", "off")
    main.main()
    assert env.rpc_payloads == []


def test_rpc_failure_does_not_break_the_run(env, monkeypatch, capsys):
    monkeypatch.setenv("INGEST_RPC_GENEX", "true")
    monkeypatch.setenv("INGEST_RPC_ANH", "true")

    def boom(payload):
        raise RuntimeError("PostgREST caído")

    env.rpc_handler = boom
    main.main()  # no lanza: el código de salida no cambia y el resto del pipeline sigue
    assert "::warning::" in capsys.readouterr().out
    assert ("ypfb",) in env.calls  # el resto del pipeline sigue
    assert env.inserted  # las cadenas legacy se insertaron antes de ANH


def test_dry_run_main_makes_no_rpc_calls(env, monkeypatch):
    for name in main.RPC_SWITCHES:
        monkeypatch.setenv(name, "true")
    monkeypatch.setenv("INGEST_DRY_RUN", "true")
    main.main()
    assert env.rpc_payloads == [] and env.inserted == []
