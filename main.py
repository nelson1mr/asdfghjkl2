"""ORQUESTADOR UNIFICADO DE SCRAPERS DE COMBUSTIBLE
===================================================
1. Ejecuta en paralelo los scrapers personalizados (Genex, Biopetrol, GasGroup, etc.).
2. Extrae la telemetría nacional de la ANH.
3. Resuelve conflictos a nivel (station_id, fuel_type_id):
   - Prioridad 1: Scraper personalizado (Litros exactos en tanque).
   - Prioridad 2: ANH como fallback automático si el scraper personalizado falla o no cubre la estación/producto.
4. Inserta un ÚNICO lote consolidado a Supabase (Elimina parpadeos y alertas duplicadas).
5. Procesa el historial de despachos y vinculación RPC.
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

# Cargar variables de entorno
load_dotenv()

SUPABASE_URL = os.getenv("V3")
SUPABASE_KEY = os.getenv("V4")

# =============================================================================
# REGISTRO DE SCRAPERS PERSONALIZADOS (ESCALABLE)
# =============================================================================
# Para agregar un nuevo scraper en el futuro:
# 1. Importa el módulo (ej. import nuevo_surtidor)
# 2. Agrégalo a la lista SCRAPER_REGISTRY
SCRAPER_REGISTRY = [
    {
        "id": "GENEX",
        "name": "Cadena Genex (Santa Cruz)",
        "module": genex,
    },
    {
        "id": "BIOPETROL",
        "name": "Cadena Biopetrol",
        "module": bp,
    },
    {
        "id": "GASGROUP",
        "name": "Cadena GasGroup (Santa Cruz / Tarija / Cbba)",
        "module": gg,
    },
]


def run_custom_scraper(scraper_info: dict) -> tuple[str, list[dict]]:
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
        return scraper_id, records
    except Exception as e:
        elapsed = time.time() - start_t
        print(f"  [ERROR] Scraper {name} falló tras {elapsed:.2f}s: {e}")
        print(f"  [FALLBACK] Las estaciones de {name} serán respaldadas automáticamente por ANH.")
        return scraper_id, []


def execute_all_custom_scrapers() -> dict[tuple[int, int], dict]:
    """Ejecuta todos los scrapers personalizados registrados en paralelo."""
    print("\n[ETAPA 1/4] Ejecutando scrapers personalizados en paralelo...")
    custom_reports: dict[tuple[int, int], dict] = {}
    stats: dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=len(SCRAPER_REGISTRY) or 1) as executor:
        futures = {
            executor.submit(run_custom_scraper, scraper): scraper["name"]
            for scraper in SCRAPER_REGISTRY
        }

        for future in as_completed(futures):
            scraper_id, records = future.result()
            stats[scraper_id] = len(records)
            for rec in records:
                station_id = rec.get("station_id")
                fuel_type_id = rec.get("fuel_type_id")
                if station_id is not None and fuel_type_id is not None:
                    # Guardar por clave única (station_id, fuel_type_id)
                    custom_reports[(station_id, fuel_type_id)] = rec

    print(f"  -> Total de combustibles con telemetría personalizada exacta: {len(custom_reports)}")
    return custom_reports


def main():
    start_total = time.time()
    print("=" * 70)
    print("MOTOR DE INGESTIÓN UNIFICADO -", datetime.now(timezone.utc).isoformat())
    print("=" * 70)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Variables de entorno de Supabase faltantes (V3, V4).")
        exit(1)

    db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    # 1. Cargar caché de estaciones y telemetría ANH
    print("\n[ETAPA 2/4] Consultando catálogo de estaciones y API ANH...")
    stations_cache = asdfghjkl2.get_station_cache(db)
    raw_anh_telemetry = asdfghjkl2.asyncio.run(asdfghjkl2.fetch_all_anh_telemetry())
    anh_reports = asdfghjkl2.generate_anh_reports(raw_anh_telemetry, stations_cache)
    print(f"  -> Reportes generados por ANH (macro nacional): {len(anh_reports)}")

    # 2. Ejecutar scrapers personalizados
    custom_reports = execute_all_custom_scrapers()

    # 3. Fusión y priorización inteligente (Custom > ANH Fallback)
    print("\n[ETAPA 3/4] Fusionando datos con motor de prioridad y fallback...")
    consolidated_reports: list[dict] = list(custom_reports.values())
    anh_used_as_fallback = 0
    anh_overridden = 0

    for r_anh in anh_reports:
        key = (r_anh["station_id"], r_anh["fuel_type_id"])
        if key in custom_reports:
            # Descartar ANH porque el scraper personalizado tiene el dato exacto
            anh_overridden += 1
        else:
            # Usar ANH como respaldo / cobertura general
            consolidated_reports.append(r_anh)
            anh_used_as_fallback += 1

    print(f"  -> Reportes Personalizados prioritarios (Litros exactos): {len(custom_reports)}")
    print(f"  -> Reportes ANH descartados por superposición: {anh_overridden}")
    print(f"  -> Reportes ANH utilizados como Fallback / Cobertura: {anh_used_as_fallback}")
    print(f"  -> TOTAL consolidado a insertar en la BD: {len(consolidated_reports)}")

    # 4. Inserción única en lote (Elimina parpadeos)
    print("\n[ETAPA 4/4] Guardando lote unificado y procesando despachos...")
    asdfghjkl2.batch_insert_reports(db, consolidated_reports)

    # 5. Gestión de despachos ANH y mapeo RPC
    if raw_anh_telemetry:
        asdfghjkl2.manage_dispatches(db, raw_anh_telemetry, stations_cache)

    total_time = time.time() - start_total
    print("\n" + "=" * 70)
    print(f"[FIN] Ingestión unificada completada con éxito en {total_time:.2f} segundos.")
    print("=" * 70)


if __name__ == "__main__":
    main()
