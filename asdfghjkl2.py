import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
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

# Mapeo ANH -> Producto
API_PRODUCT_TO_FUEL_TYPE_ID = {
    0: {"id": 1, "name": "GES"},
    1: {"id": 2, "name": "DOS"},
    2: {"id": 3, "name": "GP+"},
    3: {"id": 4, "name": "DUL"},
}

# Lista completa de las 20 Plantas y Zonas Comerciales de Despacho YPFB / ANH
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

STATIONS_TABLE = "stations"
REPORTS_TABLE = "station_official_reports"
DISPATCHES_TABLE = "anh_dispatches_history"
SOURCE_NAME = "ANH_SCRAPER V2"
BATCH_SIZE = 500
BOLIVIA_TZ = timezone(timedelta(hours=-4))

# =============================================================================
# 2. CÁLCULO DE ESTIMACIÓN DE TIEMPO DE LLEGADA
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
    """
    Calcula la hora estimada de llegada combinando:
    1. Fase Urbana (primeros 12-15 km): Velocidad baja por tráfico, rotondas y salida de planta.
    2. Fase Carretera (distancia restante): Velocidad de crucero interprovincial.
    """
    plantas_departamento = PLANTAS_DISTRIBUIDORAS.get(dep_id, PLANTAS_DISTRIBUIDORAS[2])

    # 1. Distancia lineal a la planta más cercana
    dist_minima_km = min(
        haversine_km(planta["lat"], planta["lng"], st_lat, st_lng)
        for planta in plantas_departamento
    )

    # 2. Configuración de parámetros según geografía del departamento
    if dep_id == 2:  # La Paz (Topografía de montaña / Descenso Autopista / Altiplano)
        factor_curvatura = 1.40
        radio_urbano_km = 15.0      # Km de salida urbana lenta
        vel_urbana_kmh = 18.0       # Tráfico Ceja / Senkata
        vel_carretera_kmh = 55.0    # Carretera Altiplano / Rutas montaña (Copacabana, etc.)
        tiempo_base_min = 3.0       # Maniobra salida
    else:            # Santa Cruz, Cochabamba, Oruro, Tarija, Beni, Pando, Chuquisaca, Potosí
        factor_curvatura = 1.35
        radio_urbano_km = 12.0      # Km de salida urbana
        vel_urbana_kmh = 24.0       # Tráfico avenidas / anillos
        vel_carretera_kmh = 65.0    # Carreteras troncales y dobles vías
        tiempo_base_min = 2.0       # Maniobra salida

    # 3. Estimación de distancia vial real
    dist_vial_total = dist_minima_km * factor_curvatura

    # 4. Cálculo segmentado del tiempo
    if dist_vial_total <= radio_urbano_km:
        # Caso A: El viaje es 100% dentro del radio urbano
        minutos_viaje = tiempo_base_min + (dist_vial_total / vel_urbana_kmh) * 60.0
    else:
        # Caso B: Tramo urbano lento + Tramo interprovincial en carretera
        tiempo_urbano = (radio_urbano_km / vel_urbana_kmh) * 60.0
        dist_carretera = dist_vial_total - radio_urbano_km
        tiempo_carretera = (dist_carretera / vel_carretera_kmh) * 60.0
        minutos_viaje = tiempo_base_min + tiempo_urbano + tiempo_carretera

    return salida_dt + timedelta(minutes=int(round(minutos_viaje)))


# =============================================================================
# 3. CARGA DE MAPA DE ESTACIONES
# =============================================================================
def get_station_cache(db: Client) -> dict[int, dict]:
    print("[PASO 1/5] Cargando catálogo de estaciones desde Supabase...")
    try:
        response = (
            db.table(STATIONS_TABLE)
            .select("id, anh_id, latitude, longitude, department, name")
            .not_.is_("anh_id", "null")
            .limit(5000)
            .execute()
        )
        return {
            row["anh_id"]: row
            for row in response.data
            if row.get("anh_id") is not None
        }
    except Exception as e:
        print(f"  [ERROR] Falló al cargar estaciones: {e}")
        return {}


# =============================================================================
# 4. EXTRACCIÓN ASÍNCRONA DESDE LA API ANH
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
    print(f"  [OK] Estados obtenidas: {len(all_records)}")
    return all_records


# =============================================================================
# 5. REPORTES DE DISPONIBILIDAD
# =============================================================================
def determine_availability(item: dict) -> str | None:
    saldo_estado = (item.get("saldo_estado") or "").lower()
    fecha_venta_raw = item.get("fecha_ultima_venta")

    if not fecha_venta_raw:
        return None

    try:
        now_bolivia = datetime.now(BOLIVIA_TZ)
        fecha_venta = datetime.fromisoformat(fecha_venta_raw)
        if not fecha_venta.tzinfo:
            fecha_venta = fecha_venta.replace(tzinfo=BOLIVIA_TZ)
        minutos_sin_venta = (now_bolivia - fecha_venta).total_seconds() / 60.0
    except Exception:
        return None

    if saldo_estado in ["alto", "medio"]:
        return "available" if minutos_sin_venta <= 720.0 else None
    elif saldo_estado == "bajo":
        return "available" if minutos_sin_venta <= 45.0 else "unavailable"

    return None


def transform_and_insert_reports(db: Client, raw_data: list[dict], stations_cache: dict[int, dict]):
    print("[PASO 3/5] Guardando estados en official reports...")
    records = []
    un_records = []

    for item in raw_data:
        anh_id = item.get("id")
        api_prod = item.get("_api_producto")
        station = stations_cache.get(anh_id)
        prod_meta = API_PRODUCT_TO_FUEL_TYPE_ID.get(api_prod)

        if not station or not prod_meta:
            un_records.append(item)
            continue

        official_condition = determine_availability(item)
        if official_condition is None:
            continue

        records.append({
            "station_id": station["id"],
            "fuel_type_id": prod_meta["id"],
            "official_condition": official_condition,
            "official_queue_cars_estimate": None,
            "available_liters": None,
            "source": SOURCE_NAME,
        })

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            db.table(REPORTS_TABLE).insert(batch).execute()
        except Exception as e:
            print(f"  [ERROR] Falló inserción en '{REPORTS_TABLE}': {e}")

    if un_records:
        print(f"  [INFO] Registros sin procesar: {len(un_records)}")
        #listar las estaciones no encontradas
        for item in un_records:
            anh_id = item.get("id")
            raw_station_name = item.get("nombre") or "DESCONOCIDO"
            coord = f"({item.get('lat')}, {item.get('lng')})" or "(sin coordenadas)"
            prod_name = API_PRODUCT_TO_FUEL_TYPE_ID.get(item.get("_api_producto"), {}).get("name", "GES")
            ultima_venta = item.get("fecha_ultima_venta") or "N/A"
            print(f"    - ANH_ID: {anh_id}, {prod_name}, {ultima_venta}, {raw_station_name}, {coord}")

# =============================================================================
# 6. GESTIÓN DE HISTORIAL DE DESPACHOS Y MAPEO RPC
# =============================================================================
def manage_dispatches(db: Client, raw_data: list[dict], stations_cache: dict[int, dict]):
    print("[PASO 4/5] Procesando despachos en curso...")
    dispatches = []
    un_records_dispatches = []
    now_utc = datetime.now(timezone.utc)

    for item in raw_data:
        fecha_despacho_raw = item.get("fecha_hora_despacho")
        if not fecha_despacho_raw:
            continue  # Si es null, no hay despacho activo

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

        # Coordenadas para estimar llegada
        st_db = stations_cache.get(anh_id, {})
        lat = st_db.get("latitude")
        lng = st_db.get("longitude")

        if lat and lng:
            fecha_llegada = estimate_arrival_time(dep_id, float(lat), float(lng), fecha_salida)
        else:
            fecha_llegada = fecha_salida + timedelta(minutes=45)

        raw_station_name = item.get("nombre") or st_db.get("name") or "DESCONOCIDO"

        # Estructura del registro
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

        # Generación del hash único idéntico
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

    # Disparar RPC de vinculación map_new_dispatches
    print("[PASO 5/5] Invocando RPC 'map_new_dispatches' en Supabase...")
    try:
        db.rpc("map_new_dispatches").execute()
        print("  [OK] RPC de mapeo completado exitosamente.")
    except Exception as e:
        print(f"  [ERROR] Falló la invocación del RPC de mapeo: {e}")


# 7. ENTRADA PRINCIPAL
def main():
    print("=" * 60)
    print("SCRAPER ANH V2 -", datetime.now(timezone.utc).isoformat())
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY or not ANH_API_URL:
        print("[FATAL] Variables de entorno faltantes.")
        exit(1)

    db = create_client(SUPABASE_URL, SUPABASE_KEY)
    stations_cache = get_station_cache(db)
    raw_telemetry = asyncio.run(fetch_all_anh_telemetry())

    if raw_telemetry:
        transform_and_insert_reports(db, raw_telemetry, stations_cache)
        manage_dispatches(db, raw_telemetry, stations_cache)

    print("=" * 60)
    print("[FIN] Proceso completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()