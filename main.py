"""ORQUESTADOR UNIFICADO DE SCRAPERS DE COMBUSTIBLE
===================================================
1. Ejecuta en paralelo los scrapers personalizados (Genex, Biopetrol, GasGroup, etc.).
2. Extrae la telemetría nacional de la ANH.
3. Guarda los reportes por una de dos vías, elegida POR FUENTE con un switch (SUR-79):
   - RPC (INGEST_RPC_<FUENTE>=true): api.ingest_official_reports mapea, deduplica e inserta en el
     servidor (contrato contracts/ingest/v1.schema.json). Una llamada por fuente, cadenas primero.
   - Legacy (por defecto): fusión en memoria cadena > ANH e insert directo en un solo lote.
   Los switches de la RPC están APAGADOS por defecto; los enciende el responsable (ver switches.py).
   El camino legacy se conserva como respaldo y se borrará en una tarea posterior.
4. Procesa el historial de despachos y vinculación RPC.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import os
import time
from dotenv import load_dotenv
from supabase import Client, create_client

# Importar módulos de scrapers
import asdfghjkl2
import bp
import genex
import gg
import ingestar_despachos_ypfb
import ingest_rpc
from switches import describe, describe_opt_in, is_enabled, is_opted_in

# Cargar variables de entorno
load_dotenv()

SUPABASE_URL = os.getenv("V3")
SUPABASE_KEY = os.getenv("V4")

# =============================================================================
# REGISTRO DE SCRAPERS PERSONALIZADOS (ESCALABLE)
# =============================================================================
# Para agregar un nuevo scraper en el futuro:
# 1. Importa el módulo (ej. import nuevo_surtidor)
# 2. Agrégalo a la lista SCRAPER_REGISTRY con su variable de switch (ver switches.py)
SCRAPER_REGISTRY = [
    {
        "id": "GENEX",
        "name": "Cadena Genex (Santa Cruz)",
        "module": genex,
        "switch": "SCRAPE_GENEX",
        "rpc_switch": "INGEST_RPC_GENEX",
        "source": "GENEX_OFFICIAL_WEB",
    },
    {
        "id": "BIOPETROL",
        "name": "Cadena Biopetrol",
        "module": bp,
        "switch": "SCRAPE_BIOPETROL",
        "rpc_switch": "INGEST_RPC_BIOPETROL",
        "source": "BIOPETROL_OFFICIAL_WEB",
    },
    {
        "id": "GASGROUP",
        "name": "Cadena GasGroup (Santa Cruz / Tarija / Cbba)",
        "module": gg,
        "switch": "SCRAPE_GASGROUP",
        "rpc_switch": "INGEST_RPC_GASGROUP",
        "source": "GASGROUP_OFFICIAL_WEB",
    },
]

ALL_SWITCHES = ["SCRAPE_ANH", *(s["switch"] for s in SCRAPER_REGISTRY), "SCRAPE_YPFB"]

# Switches de la vía RPC (SUR-79): apagados por defecto; un valor explícito (true/on/1) los enciende.
# El texto de source debe ser exactamente el histórico y el de private.data_sources.ingest_source.
ANH_RPC_SWITCH = "INGEST_RPC_ANH"
ANH_SOURCE = asdfghjkl2.SOURCE_NAME  # "ANH_SCRAPER V2"
RPC_SWITCHES = [ANH_RPC_SWITCH, *(s["rpc_switch"] for s in SCRAPER_REGISTRY)]


def run_custom_scraper(scraper_info: dict) -> tuple[str, list[dict], datetime]:
    """Ejecuta un scraper individual con manejo de errores aislado."""
    name = scraper_info["name"]
    module = scraper_info["module"]
    scraper_id = scraper_info["id"]

    print(f"  [RUN] Iniciando scraper personalizado: {name}...")
    start_t = time.time()
    try:
        if hasattr(module, "scrape"):
            records = module.scrape()
        elif hasattr(module, "extract_and_parse"):
            records = module.extract_and_parse()
        elif hasattr(module, "scrape_and_parse_genex"):
            records = module.scrape_and_parse_genex()
        else:
            raise AttributeError(f"El módulo {scraper_id} no implementa la función 'scrape()'")

        elapsed = time.time() - start_t
        print(f"  [OK] {name} finalizó en {elapsed:.2f}s -> {len(records)} reportes obtenidos.")
        return scraper_id, records, datetime.now(timezone.utc)
    except Exception as e:
        elapsed = time.time() - start_t
        print(f"  [ERROR] Scraper {name} falló tras {elapsed:.2f}s: {e}")
        print(f"  [FALLBACK] Las estaciones de {name} serán respaldadas automáticamente por ANH.")
        return scraper_id, [], datetime.now(timezone.utc)


def execute_all_custom_scrapers() -> dict[str, tuple[list[dict], datetime]]:
    """Ejecuta en paralelo los scrapers personalizados activos. Devuelve {id: (registros, observed_at)}."""
    print("\n[ETAPA 1/4] Ejecutando scrapers personalizados en paralelo...")
    results: dict[str, tuple[list[dict], datetime]] = {}

    enabled_scrapers = []
    for scraper in SCRAPER_REGISTRY:
        if is_enabled(scraper["switch"]):
            enabled_scrapers.append(scraper)
        else:
            print(f"  [SWITCH] {scraper['name']} desactivado ({scraper['switch']}=off). Se omite.")

    if not enabled_scrapers:
        return results

    with ThreadPoolExecutor(max_workers=len(enabled_scrapers)) as executor:
        futures = {
            executor.submit(run_custom_scraper, scraper): scraper["name"]
            for scraper in enabled_scrapers
        }
        for future in as_completed(futures):
            scraper_id, records, observed_at = future.result()
            results[scraper_id] = (records, observed_at)

    return results


def publish_chains(db: Client, chain_results: dict[str, tuple[list[dict], datetime]]):
    """
    Cada cadena va por su vía: RPC si su switch está encendido; si no, se acumula para el insert legacy.
    Devuelve (legacy_reports por (station_id, fuel_type_id), llaves de TODAS las cadenas).
    """
    legacy_reports: dict[tuple[int, int], dict] = {}
    chain_keys: set[tuple[int, int]] = set()
    dry_run = is_opted_in("INGEST_DRY_RUN")

    for scraper in SCRAPER_REGISTRY:
        if scraper["id"] not in chain_results:
            continue
        records, observed_at = chain_results[scraper["id"]]
        for rec in records:
            station_id, fuel_type_id = rec.get("station_id"), rec.get("fuel_type_id")
            if station_id is not None and fuel_type_id is not None:
                chain_keys.add((station_id, fuel_type_id))

        if is_opted_in(scraper["rpc_switch"]):
            items, rejected = ingest_rpc.build_chain_items(records, scraper["source"])
            if rejected:
                print(f"::warning::SUR-79 {scraper['name']}: {rejected} registros descartados (source o ids inválidos).")
            if not items:
                print(f"  [RPC] {scraper['name']}: sin items, no se llama a la RPC.")
                continue
            print(f"  [RPC] {scraper['name']}: enviando {len(items)} items a api.ingest_official_reports...")
            ingest_rpc.ingest(db, scraper["source"], items, observed_at, dry_run=dry_run)
        else:
            for rec in records:
                station_id, fuel_type_id = rec.get("station_id"), rec.get("fuel_type_id")
                if station_id is not None and fuel_type_id is not None:
                    legacy_reports[(station_id, fuel_type_id)] = rec

    return legacy_reports, chain_keys


def publish_anh_rpc(db: Client):
    """ANH por RPC: sin catálogo de estaciones ni snapshot; el servidor mapea y deduplica."""
    raw_anh_telemetry = asdfghjkl2.asyncio.run(asdfghjkl2.fetch_all_anh_telemetry())
    now_utc = datetime.now(timezone.utc)
    items, skipped = asdfghjkl2.build_anh_items(raw_anh_telemetry, now_utc)
    print(f"  -> Items ANH para la RPC: {len(items)} | omitidos sin mapeo: {skipped['sin_mapeo']}"
          f" | sin condición: {skipped['sin_condicion']}")
    if not items:
        print("  [RPC] ANH: sin items, no se llama a la RPC.")
        return
    ingest_rpc.ingest(db, ANH_SOURCE, items, now_utc, dry_run=is_opted_in("INGEST_DRY_RUN"))


def publish_legacy(db: Client, legacy_chain: dict[tuple[int, int], dict],
                   chain_keys: set[tuple[int, int]], with_anh: bool):
    """Camino legacy (respaldo): fusión en memoria cadena > ANH e insert directo en un solo lote."""
    anh_reports: list[dict] = []
    if with_anh:
        # Las cachés solo las usa el camino legacy de ANH
        stations_cache = asdfghjkl2.get_station_cache(db)
        latest_official_cache = asdfghjkl2.get_latest_official_cache(db)
        raw_anh_telemetry = asdfghjkl2.asyncio.run(asdfghjkl2.fetch_all_anh_telemetry())
        anh_reports = asdfghjkl2.generate_anh_reports(raw_anh_telemetry, stations_cache, latest_official_cache)
        print(f"  -> Reportes generados por ANH (macro nacional): {len(anh_reports)}")

    consolidated_reports: list[dict] = list(legacy_chain.values())
    anh_used_as_fallback = 0
    anh_overridden = 0
    for r_anh in anh_reports:
        key = (r_anh["station_id"], r_anh["fuel_type_id"])
        # Una cadena (por RPC o legacy) tiene el dato exacto: se descarta el de ANH
        if key in chain_keys:
            anh_overridden += 1
        else:
            consolidated_reports.append(r_anh)
            anh_used_as_fallback += 1

    print(f"  -> Reportes de cadenas por camino legacy: {len(legacy_chain)}")
    print(f"  -> Reportes ANH descartados por superposición: {anh_overridden}")
    print(f"  -> Reportes ANH utilizados como Fallback / Cobertura: {anh_used_as_fallback}")
    print(f"  -> TOTAL consolidado a insertar en la BD: {len(consolidated_reports)}")
    asdfghjkl2.batch_insert_reports(db, consolidated_reports)


def main():
    start_total = time.time()
    print("=" * 70)
    print("MOTOR DE INGESTIÓN UNIFICADO -", datetime.now(timezone.utc).isoformat())
    print("=" * 70)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Variables de entorno de Supabase faltantes (V3, V4).")
        exit(1)

    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"[SWITCHES] {describe(ALL_SWITCHES)}")
    print(f"[SWITCHES RPC] {describe_opt_in(RPC_SWITCHES)} | DRY_RUN={'ON' if is_opted_in('INGEST_DRY_RUN') else 'OFF'}")

    # 1. Cadenas primero (en paralelo); cada una por su vía (RPC o legacy)
    chain_results = execute_all_custom_scrapers()
    print("\n[ETAPA 2/4] Guardando reportes de las cadenas...")
    legacy_chain, chain_keys = publish_chains(db, chain_results)

    # 2. ANH después
    print("\n[ETAPA 3/4] Telemetría ANH y guardado de reportes...")
    anh_on = is_enabled("SCRAPE_ANH")
    if not anh_on:
        print("  [SWITCH] ANH desactivado (SCRAPE_ANH=off). Se omite.")
    if anh_on and is_opted_in(ANH_RPC_SWITCH):
        # Las cadenas que siguen en legacy se insertan antes de ANH (cadenas primero)
        if legacy_chain:
            publish_legacy(db, legacy_chain, chain_keys, with_anh=False)
        publish_anh_rpc(db)
    else:
        publish_legacy(db, legacy_chain, chain_keys, with_anh=anh_on)

    # 3. Ingestión oficial de despachos y nominaciones YPFB / Kyros (Nuevo Motor V3)
    print("\n[ETAPA 4/4] Sincronizando despachos y nominaciones oficiales YPFB...")
    if not is_enabled("SCRAPE_YPFB"):
        print("  [SWITCH] YPFB desactivado (SCRAPE_YPFB=off). No se consulta consulta.ypfb.gob.bo ni la API Kyros.")
    else:
        try:
            ingestar_despachos_ypfb.run_ypfb_ingestion(db, dias_atras=0)
        except Exception as e:
            print(f"  [ERROR] Falló la sincronización de despachos YPFB: {e}")

    total_time = time.time() - start_total
    print("\n" + "=" * 70)
    print(f"[FIN] Ingestión unificada completada con éxito en {total_time:.2f} segundos.")
    print("=" * 70)


if __name__ == "__main__":
    main()
