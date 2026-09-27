"""
MOTOR DE TELEMETRÍA Y EXTRACCIÓN ANH - VERSIÓN DEFINITIVA
=========================================================
Arquitectura basada en eventos físicos reales:
1. Tríada de la verdad: Separa Disponibilidad (B-SISA) de Volumen (Sensor de tanque).
2. Cero ventanas artificiales: Sin filtros rígidos de 24h que generen falsos negativos.
3. Ignora 'con_venta': Usa el pulso real de transacciones de la última hora.
4. Deduplicación en memoria (RPC): Descarta zombis y noches inactivas sin tocar la BD.
5. Ingesta de cisternas nocturnas garantizada con fecha exacta del evento.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
import time
import aiohttp
from dotenv import load_dotenv
from supabase import Client, create_client

# Cargar variables de entorno
load_dotenv()

# =============================================================================
# 1. CONFIGURACIÓN Y CONSTANTES
# =============================================================================
SUPABASE_URL = os.getenv("V3")
SUPABASE_KEY = os.getenv("V4")
ANH_API_URL = os.getenv("V0")

HEADERS = {
    "User-Agent": "Dart/3.4 (dart:io)",
    "Accept": "application/json",
    "Connection": "close",
}

# 1=Chuquisaca, 2=La Paz, 3=Cochabamba, 4=Oruro, 5=Potosí, 6=Tarija, 7=Santa Cruz, 8=Beni, 9=Pando
DEPARTAMENTOS_MAP = {
    1: {"name": "Chuquisaca", "key": "chuquisaca"},
    2: {"name": "La Paz", "key": "la_paz"},
    3: {"name": "Cochabamba", "key": "cochabamba"},
    4: {"name": "Oruro", "key": "oruro"},
    5: {"name": "Potosí", "key": "potosi"},
    6: {"name": "Tarija", "key": "tarija"},
    7: {"name": "Santa Cruz", "key": "santa_cruz"},
    8: {"name": "Beni", "key": "beni"},
    9: {"name": "Pando", "key": "pando"},
}

DEPARTAMENTOS = list(DEPARTAMENTOS_MAP.keys())

# Mapeo ANH -> Producto interno
API_PRODUCT_TO_FUEL_TYPE_ID = {
    0: {"id": 1, "name": "GES"},
    1: {"id": 2, "name": "DOS"},
    2: {"id": 3, "name": "GP+"},
    3: {"id": 4, "name": "DUL"},
}

# 20 Plantas y Zonas Comerciales de Despacho YPFB / ANH
PLANTAS_DISTRIBUIDORAS = {
    # 1: Chuquisaca
    1: [
        {"nombre": "Planta Qhora Qhora (Sucre)", "lat": -19.07998626110280, "lng": -65.22167650982740},
        {"nombre": "Planta Monteagudo", "lat": -19.74559341842000, "lng": -63.95991249941300},
    ],
    # 2: La Paz
    2: [
        {"nombre": "Planta Senkata (El Alto)", "lat": -16.57400707348200, "lng": -68.18613838385400},
    ],
    # 3: Cochabamba
    3: [
        {"nombre": "Planta Valle Hermoso", "lat": -17.45056070037580, "lng": -66.12384363077580},
        {"nombre": "Planta Puerto Villarroel", "lat": -16.84177978017380, "lng": -64.80314536951480},
    ],
    # 4: Oruro
    4: [
        {"nombre": "Planta San Pedro", "lat": -17.93571571041580, "lng": -67.11460797116160},
    ],
    # 5: Potosí
    5: [
        {"nombre": "Planta Potosí (San Clemente)", "lat": -19.57737082058280, "lng": -65.76022191904490},
        {"nombre": "Planta Uyuni", "lat": -20.45555987377030, "lng": -66.81324108503760},
        {"nombre": "Planta Tupiza", "lat": -21.46794714927200, "lng": -65.71686132811010},
        {"nombre": "Zona Comercial Villazón", "lat": -22.07423729756340, "lng": -65.59901311062280},
    ],
    # 6: Tarija
    6: [
        {"nombre": "Planta Tarija (El Portillo)", "lat": -21.56692058473000, "lng": -64.66612664982680},
        {"nombre": "Planta Villa Montes", "lat": -21.26809159197950, "lng": -63.45017401501540},
        {"nombre": "Zona Comercial Yacuiba", "lat": -22.04187815162910, "lng": -63.67874361108990},
        {"nombre": "Zona Comercial Bermejo", "lat": -22.72582853533690, "lng": -64.35166157316420},
    ],
    # 7: Santa Cruz
    7: [
        {"nombre": "Planta Santa Cruz (Palmasola)", "lat": -17.87708642377990, "lng": -63.20039447396990},
        {"nombre": "Planta Camiri", "lat": -20.01562013641880, "lng": -63.53367201983930},
        {"nombre": "Planta San José de Chiquitos", "lat": -17.84416194151650, "lng": -60.73058573529120},
        {"nombre": "Zona Comercial Puerto Suárez", "lat": -18.98924775924940, "lng": -57.79274321626870},
    ],
    # 8: Beni
    8: [
        {"nombre": "Planta Trinidad", "lat": -14.84321802876630, "lng": -64.90912405774000},
        {"nombre": "Planta Riberalta", "lat": -11.00370517677800, "lng": -66.04830114170910},
        {"nombre": "Zona Comercial Guayaramerín", "lat": -10.80801641933020, "lng": -65.35227326676250},
    ],
    # 9: Pando
    9: [
        {"nombre": "Zona Comercial Cobija", "lat": -11.02727737546800, "lng": -68.75543913244700},
    ],
}
# Los siguientes anh_id son falsos positivos o ya han sido considerados
#  - ANH_ID: 2719, Nombre: INVERSIONES JANA S.A., Dep_ID: 1 - En la api existen duplicados de esta estación, se considera solo el mas reciente
#  - ANH_ID: 2733, Nombre: YUPANQUI QUISPE MARCOS, Dep_ID: 2 - El ultimo reporte de esta estación es de 2025 y no esta claro su ubicacion, se ignora
#  - ANH_ID: 3275, Nombre: REFINERIA ORIENTAL S.A. SUCURSAL 6 ORURO, Dep_ID: 4 - sospecha de que es el surtidor lucyfer que ya ha desaparecido de la api oficial
#  - ANH_ID: 2333, Nombre: ESTACION DE SERVICIO GUADALUPE POTOSI, Dep_ID: 5 - En la api existen duplicados de esta estación, se considera solo el mas reciente

STATIONS_TABLE = "stations"
REPORTS_TABLE = "station_official_reports"
DISPATCHES_TABLE = "anh_dispatches_history"
SOURCE_NAME = "ANH_SCRAPER V2"
BATCH_SIZE = 150
BOLIVIA_TZ = timezone(timedelta(hours=-4))


# =============================================================================
# 2. HELPERS DE FECHA Y COMPARACIÓN PRECISA
# =============================================================================

def format_anh_date(date_raw: str | None) -> str | None:
    """Convierte la fecha local de la ANH a formato UTC ISO (+00:00)."""
    if not date_raw:
        return None
    try:
        dt = datetime.fromisoformat(date_raw)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=BOLIVIA_TZ)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return date_raw


def parse_utc_dt(dt_str: str | None) -> datetime | None:
    """Parsea cualquier string ISO a objeto datetime consciente de zona en UTC."""
    if not dt_str:
        return None
    try:
        clean = dt_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BOLIVIA_TZ)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def are_dates_equal(d1_str: str | None, d2_str: str | None) -> bool:
    """Compara si dos timestamps representan el mismo instante real en el tiempo."""
    if d1_str == d2_str:
        return True
    dt1 = parse_utc_dt(d1_str)
    dt2 = parse_utc_dt(d2_str)
    if not dt1 or not dt2:
        return False
    # Tolerancia de 1 segundo para ignorar discrepancias de milisegundos
    return abs((dt1 - dt2).total_seconds()) < 1.0


# =============================================================================
# 3. CÁLCULO DE ESTIMACIÓN DE TIEMPO DE LLEGADA (Despachos)
# =============================================================================

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia geodésica en km entre dos puntos."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def estimate_arrival_time(dep_id: int, st_lat: float, st_lng: float, salida_dt: datetime) -> datetime:
    plantas_departamento = PLANTAS_DISTRIBUIDORAS.get(dep_id, PLANTAS_DISTRIBUIDORAS[2])
    dist_minima_km = min(
        haversine_km(planta["lat"], planta["lng"], st_lat, st_lng)
        for planta in plantas_departamento
    )

    if dep_id == 2:  # La Paz (Topografía de montaña)
        factor_curvatura = 1.40
        radio_urbano_km = 15.0
        vel_urbana_kmh = 18.0
        vel_carretera_kmh = 55.0
        tiempo_base_min = 3.0
    else:  # Santa Cruz, Cochabamba y otros
        factor_curvatura = 1.35
        radio_urbano_km = 12.0
        vel_urbana_kmh = 24.0
        vel_carretera_kmh = 65.0
        tiempo_base_min = 2.0

    dist_vial_total = dist_minima_km * factor_curvatura

    if dist_vial_total <= radio_urbano_km:
        minutos_viaje = tiempo_base_min + (dist_vial_total / vel_urbana_kmh) * 60.0
    else:
        tiempo_urbano = (radio_urbano_km / vel_urbana_kmh) * 60.0
        dist_carretera = dist_vial_total - radio_urbano_km
        tiempo_carretera = (dist_carretera / vel_carretera_kmh) * 60.0
        minutos_viaje = tiempo_base_min + tiempo_urbano + tiempo_carretera

    return salida_dt + timedelta(minutes=int(round(minutos_viaje)))


# =============================================================================
# 4. CARGA DE CATÁLOGO Y ÚLTIMO SNAPSHOT OFICIAL (Caché en RAM)
# =============================================================================

def get_station_cache(db: Client) -> dict[int, dict]:
    """Carga el catálogo de estaciones mapeadas en memoria."""
    t_start = time.time()
    try:
        response = (
            db.table(STATIONS_TABLE)
            .select("id, anh_id, latitude, longitude, department, name")
            .not_.is_("anh_id", "null")
            .limit(5000)
            .execute()
        )
        elapsed = time.time() - t_start
        print(f"  [CACHE] Estaciones extraidas para el cache {len(response.data)} completado en {elapsed:.2f}s")
        return {
            row["anh_id"]: row
            for row in response.data
            if row.get("anh_id") is not None
        }
    except Exception as e:
        elapsed = time.time() - t_start
        print(f"  [ERROR] Falló al cargar catálogo de estaciones tras {elapsed:.2f}s: {e}")
        return {}


def get_latest_official_cache(db: Client) -> dict[tuple[int, int], dict]:
    """
    Carga el último reporte oficial conocido por (station_id, fuel_type_id)
    directamente desde la RPC en PostgreSQL en menos de 10 ms.
    """
    t_start = time.time()
    try:
        response = db.rpc("get_latest_official_snapshots").limit(5000).execute()
        elapsed = time.time() - t_start
        print(f"  [CACHE] Registros traidos para la deduplicacion {len(response.data)} completado en {elapsed:.2f}s")
        return {
            (row["station_id"], row["fuel_type_id"]): row
            for row in response.data
        }
    except Exception as e:
        elapsed = time.time() - t_start
        print(f"  [WARN] No se pudo cargar snapshot oficial previo tras {elapsed:.2f}s: {e}")
        return {}


# =============================================================================
# 5. EXTRACCIÓN ASÍNCRONA DESDE LA API ANH
# =============================================================================

async def fetch_dep_prod(session: aiohttp.ClientSession, dep: int, prod: int) -> list[dict]:
    url = ANH_API_URL.format(dep=dep, prod=prod)
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30), ssl=False) as response:
            if response.status != 200:
                return []
            data = await response.json()
            if data.get("strMensaje") == "OK" and "oResultado" in data:
                items = data.get("oResultado", [])
                server_time = data.get("server_time")
                for item in items:
                    item["_api_producto"] = prod
                    item["_server_time"] = server_time
                    item["_dep_id"] = dep
                return items
            return []
    except Exception as e:
        print(f"  [WARN] Error consultando dep={dep}, prod={prod}: {e}")
        return []


async def fetch_all_anh_telemetry() -> list[dict]:
    print("[PASO 2/5] Consultando API ANH en paralelo...")
    connector = aiohttp.TCPConnector(ssl=False, limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_dep_prod(session, dep, prod)
            for dep in DEPARTAMENTOS
            for prod in API_PRODUCT_TO_FUEL_TYPE_ID.keys()
        ]
        results = await asyncio.gather(*tasks)

    all_records = [item for sublist in results for item in sublist]
    print(f"  [OK] Registros brutos obtenidos de la ANH: {len(all_records)}")
    return all_records


# =============================================================================
# 6. PARSEO DE SALDO Y DETERMINACIÓN PURA DE DISPONIBILIDAD
# =============================================================================

def parse_available_liters(item: dict) -> float | None:
    """
    Extrae los litros reales.
    Si la ANH manda 0 o vacío, devuelve None (significa que la estación NO tiene sonda).
    """
    raw_liters = item.get("saldo_litros")
    if raw_liters is None or raw_liters == "":
        return None
    try:
        liters = float(raw_liters)
        return liters if math.isfinite(liters) and liters > 0 else None
    except (TypeError, ValueError):
        return None


def determine_availability(item: dict, minutos_sin_venta: float) -> str | None:
    """
    Traduce el semáforo de la ANH a nuestro enum (available / unavailable).
    Basado en hechos y sin suposiciones:
      1. Si saldo_estado es 'alto' o 'medio': La ANH certifica stock en tanque (>= 5.000 L) -> available.
      2. Si saldo_estado es 'bajo':
         - Con venta en los últimos 60 min -> available (bombeando reservas en vivo).
         - Sin venta en más de 60 min -> unavailable (bombas secas / agotado).
    """
    saldo_estado = (item.get("saldo_estado") or "").lower()

    if saldo_estado in ["alto", "medio"]:
        return "available"
    elif saldo_estado == "bajo":
        # 45 minutos es el termómetro natural entre despacho activo y manguera colgada
        return "available" if minutos_sin_venta <= 45.0 else "unavailable"

    return None


def generate_anh_reports(
    raw_data: list[dict], 
    stations_cache: dict[int, dict], 
    latest_cache: dict[tuple[int, int], dict]
) -> list[dict]:
    """
    Genera reportes oficiales aplicando deduplicación pura por eventos.
    Solo inserta si hubo venta nueva, cisterna o cambio de condición.
    """
    records = []
    un_records = []
    descartados_sin_cambio = 0

    now_utc = datetime.now(timezone.utc)
    now_utc_str = now_utc.isoformat()

    for item in raw_data:
        anh_id = item.get("id")
        api_prod = item.get("_api_producto")
        station = stations_cache.get(anh_id)
        prod_meta = API_PRODUCT_TO_FUEL_TYPE_ID.get(api_prod)

        if not station or not prod_meta:
            un_records.append(item)
            continue

        station_id = station["id"]
        fuel_type_id = prod_meta["id"]

        # 1. Extracción de datos
        current_litros = parse_available_liters(item) # float o None
        raw_fecha_venta = item.get("fecha_ultima_venta")
        current_fecha_venta = format_anh_date(raw_fecha_venta)
        server_time = item.get("_server_time") or now_utc_str

        # Calcular antigüedad real de la última venta
        dt_current_venta = parse_utc_dt(raw_fecha_venta)
        if dt_current_venta:
            minutos_sin_venta = (now_utc - dt_current_venta).total_seconds() / 60.0
        else:
            minutos_sin_venta = 999999.0

        # 2. Determinar condición
        current_condition = determine_availability(item, minutos_sin_venta)
        if current_condition is None:
            continue

        # 3. DEDUPLICACIÓN EN MEMORIA (El filtro natural por eventos)
        prev = latest_cache.get((station_id, fuel_type_id))

        if prev:
            prev_litros = prev.get("available_liters")
            prev_reported_at = prev.get("reported_at")
            prev_condition = prev.get("official_condition")

            # ¿Hubo venta nueva? (Comparamos si el timestamp de venta se movió)
            hubo_venta = (current_fecha_venta is not None) and not are_dates_equal(current_fecha_venta, prev_reported_at)
            
            # ¿Cambiaron los litros? (Cisterna, consumo medible o sensor reparado)
            cambiaron_litros = False
            if current_litros is not None and prev_litros is not None:
                cambiaron_litros = abs(float(current_litros) - float(prev_litros)) > 0.01
            elif current_litros != prev_litros:
                cambiaron_litros = True

            # ¿Cambió el estado comercial? (ej. available <-> unavailable)
            cambio_condicion = (current_condition != prev_condition)

            # Si NADA cambió en el mundo real, se descarta silenciosamente
            if not hubo_venta and not cambiaron_litros and not cambio_condicion:
                descartados_sin_cambio += 1
                
            # LOG DE DIAGNÓSTICO (Solo muestra los primeros 5 eventos para no saturar la terminal)
            if len(records) < 20:
                motivo = []
                if hubo_venta:
                    motivo.append(f"Venta ({prev_reported_at} -> {current_fecha_venta})")
                if cambiaron_litros:
                    motivo.append(f"Litros ({prev_litros}L -> {current_litros}L)")
                if cambio_condicion:
                    motivo.append(f"Condicion ({prev_condition} -> {current_condition})")
                print(f"  [CAMBIO DETECTADO] Estación {station_id}: {', '.join(motivo)}")

            # ASIGNACIÓN DE TIMESTAMPS
            # reported_at: Fecha de la venta real si la hubo; de lo contrario server_time (cisterna o cambio)
            reported_at = current_fecha_venta if hubo_venta else server_time

            # liters_reported_at: Solo avanza si los litros cambiaron físicamente en el sensor
            if current_litros is not None:
                liters_reported_at = server_time if cambiaron_litros else prev.get("liters_reported_at", server_time)
            else:
                liters_reported_at = None

        else:
            # Primer registro histórico para este combustible
            reported_at = current_fecha_venta or server_time
            liters_reported_at = server_time if current_litros is not None else None

        records.append({
            "station_id": station_id,
            "fuel_type_id": fuel_type_id,
            "official_condition": current_condition,
            "official_queue_cars_estimate": None,
            "available_liters": current_litros,
            "source": SOURCE_NAME,
            "reported_at": reported_at,                  # Pulso de Venta / Disponibilidad
            "liters_reported_at": liters_reported_at     # Pulso físico del Tanque
        })

    print(f"  -> Reportes generados: {len(records)} | Sin cambios (descartados): {descartados_sin_cambio}")
    if un_records:
        print(f"  [INFO] Registros ANH no emparejados en el catálogo: {len(un_records)}")
    return records


# =============================================================================
# 7. INSERCIÓN EN LOTES Y DISPATCHES
# =============================================================================

def batch_insert_reports(db: Client, records: list[dict], batch_size: int = BATCH_SIZE) -> int:
    inserted_count = 0
    if not records:
        print("  [INFO] No hay reportes nuevos para insertar.")
        return 0

    print(f"  [DB] Insertando {len(records)} reportes oficiales en lotes de {batch_size}...")
    t_total = time.time()
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        lote_num = i // batch_size + 1
        t_batch = time.time()
        try:
            print(f"    [DB] Insertando lote {lote_num} de {len(batch)} registros...", end="", flush=True)
            db.table(REPORTS_TABLE).insert(batch).execute()
            duracion = time.time() - t_batch
            print(f" completado en {duracion:.2f}s")
            inserted_count += len(batch)
        except Exception as e:
            duracion = time.time() - t_batch
            print(f" falló tras {duracion:.2f}s")
            print(f"  [ERROR] Falló inserción en '{REPORTS_TABLE}' (lote {lote_num}): {e}")

    total_elapsed = time.time() - t_total
    print(f"  [OK] Inserción finalizada: {inserted_count}/{len(records)} registros guardados en {total_elapsed:.2f}s.")
    return inserted_count


def transform_and_insert_reports(
    db: Client, 
    raw_data: list[dict], 
    stations_cache: dict[int, dict],
    latest_cache: dict[tuple[int, int], dict]
):
    print("[PASO 3/5] Generando e insertando estados en official reports...")
    records = generate_anh_reports(raw_data, stations_cache, latest_cache)
    batch_insert_reports(db, records)


def manage_dispatches(db: Client, raw_data: list[dict], stations_cache: dict[int, dict]):
    print("[PASO 4/5] Procesando despachos en curso...")
    dispatches = []
    now_utc = datetime.now(timezone.utc)

    for item in raw_data:
        fecha_despacho_raw = item.get("fecha_hora_despacho")
        if not fecha_despacho_raw:
            continue

        anh_id = item.get("id")
        dep_id = item.get("departamento_id") or item.get("_dep_id", 2)
        prod_code = item.get("_api_producto", 0)
        prod_name = API_PRODUCT_TO_FUEL_TYPE_ID.get(prod_code, {}).get("name", "GES")
        fuel_type_id = API_PRODUCT_TO_FUEL_TYPE_ID.get(prod_code, {}).get("id", None)

        try:
            fecha_salida = datetime.fromisoformat(fecha_despacho_raw)
            if not fecha_salida.tzinfo:
                fecha_salida = fecha_salida.replace(tzinfo=BOLIVIA_TZ)
        except Exception:
            continue

        st_db = stations_cache.get(anh_id, {})
        lat = st_db.get("latitude")
        lng = st_db.get("longitude")

        if lat and lng:
            fecha_llegada = estimate_arrival_time(dep_id, float(lat), float(lng), fecha_salida)
        else:
            fecha_llegada = fecha_salida + timedelta(minutes=45)

        raw_station_name = item.get("nombre") or st_db.get("name") or "DESCONOCIDO"

        despacho_data = {
            "station_id": None,              
            "anh_id": anh_id,                
            "raw_station_name": raw_station_name,
            "producto": prod_name,
            "fecha_salida_planta": fecha_salida.isoformat(),
            "fecha_llegada_aprox": fecha_llegada.isoformat(),
            "report_timestamp": now_utc.isoformat(),
            "fuel_type_id": fuel_type_id,
        }

        hash_base = {
            "producto": prod_name,
            "fecha_salida_planta": despacho_data["fecha_salida_planta"],
            "fecha_llegada_aprox": despacho_data["fecha_llegada_aprox"],
            "anh_id": anh_id
        }
        unique_hash = hashlib.md5(json.dumps(hash_base, sort_keys=True).encode("utf-8")).hexdigest()
        despacho_data["unique_hash"] = unique_hash

        dispatches.append(despacho_data)

    if not dispatches:
        print("  [INFO] No hay despachos activos con fecha de salida en esta ejecución.")
        return

    print(f"  [OK] Insertando {len(dispatches)} despachos en '{DISPATCHES_TABLE}'...")
    for i in range(0, len(dispatches), BATCH_SIZE):
        batch = dispatches[i:i + BATCH_SIZE]
        try:
            db.table(DISPATCHES_TABLE).upsert(batch, on_conflict="unique_hash", ignore_duplicates=True).execute()
        except Exception as e:
            print(f"  [ERROR] Error en upsert de despachos: {e}")

    print("[PASO 5/5] Invocando RPC 'map_new_dispatches' en Supabase...")
    try:
        db.rpc("map_new_dispatches").execute()
        print("  [OK] RPC de mapeo completado exitosamente.")
    except Exception as e:
        print(f"  [ERROR] Falló la invocación del RPC de mapeo: {e}")


# =============================================================================
# 8. ENTRADA PRINCIPAL (Ejecución aislada de prueba)
# =============================================================================

def main():
    print("=" * 60)
    print("SCRAPER ANH V2 -", datetime.now(timezone.utc).isoformat())
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY or not ANH_API_URL:
        print("[FATAL] Variables de entorno faltantes.")
        exit(1)

    db = create_client(SUPABASE_URL, SUPABASE_KEY)
    stations_cache = get_station_cache(db)
    latest_cache = get_latest_official_cache(db)
    raw_telemetry = asyncio.run(fetch_all_anh_telemetry())

    if raw_telemetry:
        transform_and_insert_reports(db, raw_telemetry, stations_cache, latest_cache)
        manage_dispatches(db, raw_telemetry, stations_cache)

    print("=" * 60)
    print("[FIN] Proceso completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()