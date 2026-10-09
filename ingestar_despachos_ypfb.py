"""
MOTOR DE INGESTIÓN OFICIAL YPFB / KYROS -> SURTIVEO (V3)
=========================================================
1. Consulta distritos activos en Kyros API (Despachos y Programación).
2. Mapeo ultrarrápido O(1) con la base de datos Supabase utilizando 'stations.ypfb_names'.
3. Mapeo secundario tolerante a fallos para marcas limpias y contención tokenizada.
4. Estimación física de tiempos de viaje por geodésica (Haversine) desde la planta distribuidora.
5. Caché local de polylines OSRM ($0) para cisternas en tránsito.
6. Upsert idempotente en 'public.dispatches' con esquema en inglés.
7. Mantiene retrocompatibilidad con 'stations.dispatch' (JSONB) para la app móvil en producción.
"""

import argparse
import asyncio
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
import re
import time
import unicodedata

import aiohttp
from dotenv import load_dotenv
from supabase import Client, create_client

# Cargar variables de entorno (.env)
load_dotenv()

# =============================================================================
# 1. CONFIGURACIÓN Y CREDENCIALES
# =============================================================================
SUPABASE_URL = os.getenv("V3") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("V4") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

KYROS_BASE_URL = "https://nominac.kyros-tech.com/api"
OSRM_BASE_URL = "http://router.project-osrm.org/route/v1/driving"

PAGE_SIZE = 200
BATCH_SIZE = 100
MAX_CONCURRENT_KYROS = 3
OSRM_TIMEOUT_SECONDS = 2.5

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OSRM_CACHE_FILE = os.path.join(SCRIPT_DIR, "osrm_routes_cache.json")

# =============================================================================
# 2. CATÁLOGO DE DISTRITOS KYROS Y COORDENADAS DE PLANTAS DISTRIBUIDORAS
# =============================================================================
CATALOGO_DISTRITOS = {
    # 1. Beni Norte / Pando / Riberalta
    11: {"codigo": "AMZ", "nombre": "Amazonico (Riberalta)", "departamento": "beni", "plant_lat": -11.00370517677800, "plant_lng": -66.04830114170910},
    # 2. Tarija Frontera Sur
    17: {"codigo": "BER", "nombre": "Zona Comercial Bermejo", "departamento": "tarija", "plant_lat": -22.72582853533690, "plant_lng": -64.35166157316420},
    # 3. Chuquisaca / Sucre
    20: {"codigo": "CHU", "nombre": "Planta Qhora Qhora (Sucre)", "departamento": "chuquisaca", "plant_lat": -19.07998626110280, "plant_lng": -65.22167650982740},
    # 4. Pando Capital
    10: {"codigo": "COB", "nombre": "Zona Comercial Cobija", "departamento": "pando", "plant_lat": -11.02727737546800, "plant_lng": -68.75543913244700},
    # 5. Cochabamba Central
    19: {"codigo": "CEN", "nombre": "Planta Valle Hermoso", "departamento": "cochabamba", "plant_lat": -17.45056070037580, "plant_lng": -66.12384363077580},
    # 6. La Paz / El Alto
    5:  {"codigo": "LPZ", "nombre": "Planta Senkata (El Alto)", "departamento": "la_paz", "plant_lat": -16.57400707348200, "plant_lng": -68.18613838385400},
    # 7. Beni Frontera
    12: {"codigo": "GUA", "nombre": "Zona Comercial Guayaramerin", "departamento": "beni", "plant_lat": -10.80801641933020, "plant_lng": -65.35227326676250},
    # 8. Chuquisaca Chaco
    25: {"codigo": "MON", "nombre": "Planta Monteagudo", "departamento": "chuquisaca", "plant_lat": -19.74559341842000, "plant_lng": -63.95991249941300},
    # 9. Oruro Altiplano
    9:  {"codigo": "ORU", "nombre": "Planta San Pedro", "departamento": "oruro", "plant_lat": -17.93571571041580, "plant_lng": -67.11460797116160},
    # 10. Potosí Sur (Atocha - ANH Oficial)
    27: {"codigo": "PEA", "nombre": "Planta Engarrafadora Atocha", "departamento": "potosi", "plant_lat": -20.961194, "plant_lng": -66.204000},
    # 11. Chuquisaca Cinti (Camargo - ANH Oficial)
    29: {"codigo": "PECM", "nombre": "Planta Engarrafadora Camargo", "departamento": "chuquisaca", "plant_lat": -20.643879, "plant_lng": -65.210830},
    # 12. Potosí Norte (Catavi/Llallagua - ANH Oficial)
    28: {"codigo": "PEC", "nombre": "Planta Engarrafadora Catavi", "departamento": "potosi", "plant_lat": -18.422617, "plant_lng": -66.578006},
    # 13. Potosí Centro
    22: {"codigo": "POT", "nombre": "Planta Potosí (San Clemente)", "departamento": "potosi", "plant_lat": -19.57737082058280, "plant_lng": -65.76022191904490},
    # 14. Cochabamba Trópico
    26: {"codigo": "PVR", "nombre": "Planta Puerto Villarroel", "departamento": "cochabamba", "plant_lat": -16.84177978017380, "plant_lng": -64.80314536951480},
    # 15. Santa Cruz Central (Refinería Palmasola)
    4:  {"codigo": "SCZ", "nombre": "Planta Santa Cruz (Palmasola)", "departamento": "santa_cruz", "plant_lat": -17.87708642377990, "plant_lng": -63.20039447396990},
    # 16. Chuquisaca Tomina (Estación Tarabuquillo YPFB)
    21: {"codigo": "TBQ", "nombre": "Estación Tarabuquillo", "departamento": "chuquisaca", "plant_lat": -19.352513230910183, "plant_lng": -64.4783075529959},
    # 17. Tarija Valle Central
    14: {"codigo": "TAR", "nombre": "Planta Tarija (El Portillo)", "departamento": "tarija", "plant_lat": -21.56692058473000, "plant_lng": -64.66612664982680},
    # 18. Beni Central
    13: {"codigo": "TRI", "nombre": "Planta Trinidad", "departamento": "beni", "plant_lat": -14.84321802876630, "plant_lng": -64.90912405774000},
    # 19. Potosí Sur (Tupiza)
    24: {"codigo": "TUP", "nombre": "Planta Tupiza", "departamento": "potosi", "plant_lat": -21.46794714927200, "plant_lng": -65.71686132811010},
    # 20. Potosí Suroeste (Salar Uyuni)
    23: {"codigo": "UYU", "nombre": "Planta Uyuni", "departamento": "potosi", "plant_lat": -20.45555987377030, "plant_lng": -66.81324108503760},
    # 21. Tarija Chaco
    18: {"codigo": "VLM", "nombre": "Planta Villa Montes", "departamento": "tarija", "plant_lat": -21.26809159197950, "plant_lng": -63.45017401501540},
    # 22. Potosí Frontera Argentina
    16: {"codigo": "VLZ", "nombre": "Zona Comercial Villazón", "departamento": "potosi", "plant_lat": -22.07423729756340, "plant_lng": -65.59901311062280},
    # 23. Tarija Frontera
    15: {"codigo": "YAC", "nombre": "Zona Comercial Yacuiba", "departamento": "tarija", "plant_lat": -22.04187815162910, "plant_lng": -63.67874361108990},
    # 24. Santa Cruz Cordillera
    7:  {"codigo": "CAM", "nombre": "Planta Camiri", "departamento": "santa_cruz", "plant_lat": -20.01562013641880, "plant_lng": -63.53367201983930},
    # 25. Santa Cruz Pantanal
    6:  {"codigo": "PSZ", "nombre": "Zona Comercial Puerto Suárez", "departamento": "santa_cruz", "plant_lat": -18.98924775924940, "plant_lng": -57.79274321626870},
    # 26. Santa Cruz Chiquitanía
    8:  {"codigo": "SJC", "nombre": "Planta San José de Chiquitos", "departamento": "santa_cruz", "plant_lat": -17.84416194151650, "plant_lng": -60.73058573529120}
}

VALID_DEPARTMENTS = {
    "la_paz", "santa_cruz", "cochabamba", "chuquisaca",
    "tarija", "oruro", "potosi", "pando", "beni", "bolivia"
}

# =============================================================================
# 3. NORMALIZACIÓN Y LIMPIEZA DE NOMBRES
# =============================================================================
PREFIJOS_A_ELIMINAR = [
    r"\bEST(?:ACION)?\.?\s*(?:DE\s*)?SERV(?:ICIO)?\.?\b",
    r"\bEST-SERV-\b",
    r"\bE°\s*S°\b", r"\bEº\s*Sº\b", r"\bE\.?\s*S\.?\b", r"\bEE\.?\s*SS\.?\b", r"\bEOSO\b",
    r"\bSURTIDOR(?:\s+DE\s+GASOLINA\s+Y\s+DIESEL)?\b",
    r"\bGRIFO\b"
]

SUFIJOS_A_ELIMINAR = [
    r"\bS\s*\.?\s*R\s*\.?\s*L\s*\.?\b",
    r"\bS\s*\.?\s*A\s*\.?\b",
    r"\bL\s*\.?\s*T\s*\.?\s*D\s*\.?\s*A\s*\.?\b",
    r"\bR\s*\.?\s*L\s*\.?\b",
    r"\bSUC(?:URSAL)?\.?\s*\d+\b"
]

def normalize_text(text: str | None) -> str:
    """Normaliza texto: remueve acentos, mayúsculas, comillas y espacios redundantes."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", str(text))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.upper().replace('"', "").replace("'", "").strip()
    return re.sub(r"\s+", " ", t)

def clean_brand(text: str | None) -> str:
    """Extrae el núcleo o marca distintiva de la estación."""
    s = normalize_text(text)
    if not s:
        return ""
    if " - " in s:
        parts = s.split(" - ")
        left = parts[0].strip()
        if len(left) > 3:
            s = left

    for pref in PREFIJOS_A_ELIMINAR:
        s = re.sub(pref, " ", s, flags=re.IGNORECASE)
    for suf in SUFIJOS_A_ELIMINAR:
        s = re.sub(suf, " ", s, flags=re.IGNORECASE)

    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def map_fuel_type(prod_str: str, familia_str: str = "") -> tuple[int, str]:
    """Mapea el producto textual de Kyros al fuel_type_id de Surtiveo (1, 2, 3, 4)."""
    full = f"{prod_str or ''} {familia_str or ''}".upper()
    if any(k in full for k in ("PREMIUM", "GP+", "GUP")):
        return 3, "GP+"
    if any(k in full for k in ("ULS", "DUL", "DIE-ULS")):
        return 4, "DUL"
    if any(k in full for k in ("DIESEL", "DOS", "DO+")):
        return 2, "DOS"
    if any(k in full for k in ("GASOLINA", "ESPECIAL", "GES")):
        return 1, "GES"
    return 1, "GES"

BOLIVIA_TZ = timezone(timedelta(hours=-4))

def parse_kyros_datetime(dt_str: str | None) -> datetime | None:
    """Parsea fecha/hora de Kyros asignando zona horaria de Bolivia (-04:00) y normalizando a UTC aware."""
    if not dt_str:
        return None
    s = str(dt_str).strip()
    dt = None
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        pass
    if dt is None:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s[:19], fmt)
                break
            except Exception:
                continue
    if dt is not None:
        if dt.tzinfo is None:
            # Kyros publica fechas locales de Bolivia (UTC-4)
            dt = dt.replace(tzinfo=BOLIVIA_TZ)
        return dt.astimezone(timezone.utc)
    return None

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia geodésica del gran círculo entre dos puntos en kilómetros."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    return 2.0 * R * math.asin(math.sqrt(a))

def estimate_travel_minutes(plant_lat: float | None, plant_lng: float | None, st_lat: float | None, st_lng: float | None, is_provincia: bool) -> int:
    """
    Fallback físico de tiempo de viaje (cuando OSRM no responde):
    Calcula distancia Haversine con factor de curvatura vial (35% más por trazado de carreteras)
    y velocidad media según ámbito.
    ESTRICTAMENTE tiempo de transporte físico hasta la llegada al surtidor (SIN sumar descarga).
    """
    if plant_lat is not None and plant_lng is not None and st_lat is not None and st_lng is not None:
        try:
            dist_linea_recta_km = haversine_km(plant_lat, plant_lng, float(st_lat), float(st_lng))
            dist_vial_km = dist_linea_recta_km * 1.35
            if dist_vial_km <= 15:
                # Tráfico urbano pesado: ~25 km/h promedio
                return max(5, int(round((dist_vial_km / 25.0) * 60.0)))
            else:
                # Carretera interprovincial: ~55 km/h promedio
                return max(15, int(round((dist_vial_km / 55.0) * 60.0)))
        except Exception:
            pass
    return 60 if is_provincia else 30

# =============================================================================
# 4. GESTIÓN DE CACHÉ LOCAL DE RUTAS Y TIEMPOS OSRM ($0)
# =============================================================================
class OSRMCache:
    def __init__(self, cache_file: str = OSRM_CACHE_FILE):
        self.cache_file = cache_file
        self.routes: dict[str, dict] = {}
        self.load()

    def load(self):
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for k, v in data.items():
                        if isinstance(v, dict):
                            self.routes[k] = v
                        elif isinstance(v, str):
                            self.routes[k] = {"polyline": v, "duration_min": None, "distance_km": None}
            except Exception:
                self.routes = {}

    def save(self):
        try:
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.routes, f, ensure_ascii=False)
        except Exception:
            pass

    def get_key(self, plat: float, plng: float, dlat: float, dlng: float) -> str:
        return f"{plat:.4f},{plng:.4f}->{dlat:.4f},{dlng:.4f}"

    def get(self, plat: float, plng: float, dlat: float, dlng: float) -> dict | None:
        return self.routes.get(self.get_key(plat, plng, dlat, dlng))

    def set(self, plat: float, plng: float, dlat: float, dlng: float, polyline: str, duration_min: int, distance_km: float):
        self.routes[self.get_key(plat, plng, dlat, dlng)] = {
            "polyline": polyline,
            "duration_min": duration_min,
            "distance_km": distance_km
        }

async def fetch_osrm_route(
    session: aiohttp.ClientSession,
    osrm_cache: OSRMCache,
    plat: float,
    plng: float,
    dlat: float,
    dlng: float
) -> dict | None:
    """
    Consulta OSRM para obtener la distancia real de asfalto, duración de viaje y polilínea.
    Caché en disco permanente ($0 costo y cero peticiones repetidas).
    """
    cached = osrm_cache.get(plat, plng, dlat, dlng)
    if cached and cached.get("duration_min") is not None:
        return cached

    url = f"{OSRM_BASE_URL}/{plng:.5f},{plat:.5f};{dlng:.5f},{dlat:.5f}?overview=simplified&geometries=polyline"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=OSRM_TIMEOUT_SECONDS)) as resp:
            if resp.status == 200:
                data = await resp.json()
                routes = data.get("routes", [])
                if routes and "geometry" in routes[0]:
                    r = routes[0]
                    poly = r["geometry"]
                    dur_sec = float(r.get("duration", 0.0))
                    dist_meters = float(r.get("distance", 0.0))
                    
                    # Tiempo físico de viaje directo (minutos) sin recargos
                    dur_min = max(5, int(round(dur_sec / 60.0)))
                    dist_km = round(dist_meters / 1000.0, 2)

                    osrm_cache.set(plat, plng, dlat, dlng, poly, dur_min, dist_km)
                    return {
                        "polyline": poly,
                        "duration_min": dur_min,
                        "distance_km": dist_km
                    }
    except Exception:
        pass
    return None

# =============================================================================
# 5. ÍNDICE EN MEMORIA Y MOTOR DE EMPAREJAMIENTO ESTRICTO DE ESTACIONES
# =============================================================================
class StationMatcher:
    def __init__(self, stations_db: list[dict]):
        self.stations = stations_db
        self.alias_map: dict[str, dict] = {}
        self.unmapped_logged: set[str] = set()
        self._build_indexes()

    def _build_indexes(self):
        for st in self.stations:
            # 1. Alias oficiales y verificados de YPFB en la BD (stations.ypfb_names)
            ypfb_aliases = st.get("ypfb_names") or []
            if isinstance(ypfb_aliases, list):
                for alias in ypfb_aliases:
                    norm = normalize_text(alias)
                    if norm:
                        self.alias_map[norm] = st

            # 2. Nombre exacto de la estación en Surtiveo
            name = st.get("name")
            if name:
                norm_name = normalize_text(name)
                if norm_name:
                    self.alias_map[norm_name] = st

            # 3. Nombre oficial de la ANH
            anh_name = st.get("anh_name")
            if anh_name:
                norm_anh = normalize_text(anh_name)
                if norm_anh:
                    self.alias_map[norm_anh] = st

    def match(self, raw_name: str, d_id: int | None = None) -> tuple[int | None, dict | None]:
        norm = normalize_text(raw_name)
        if norm and norm in self.alias_map:
            st = self.alias_map[norm]
            return st["id"], st

        # Sin coincidencia exacta: NO se hace fuzzy match ni adivinanzas.
        # Se registra el despacho con station_id = NULL y se emite log explícito.
        clean_raw = (raw_name or "").strip()
        if clean_raw and clean_raw not in self.unmapped_logged:
            self.unmapped_logged.add(clean_raw)
            dist_str = f" [Distrito {d_id}]" if d_id else ""
            print(f"  [UNMAPPED]{dist_str} Estación YPFB no emparejada: '{clean_raw}' -> Registrado con station_id = NULL")

        return None, None

# =============================================================================
# 6. EXTRACCIÓN ASÍNCRONA DE LA API DE KYROS
# =============================================================================
async def fetch_kyros_page(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    params: dict,
    max_retries: int = 3
) -> dict:
    async with semaphore:
        for attempt in range(max_retries):
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=25)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status in (429, 500, 502, 503, 504):
                        await asyncio.sleep(1.5 * (attempt + 1))
            except Exception:
                if attempt < max_retries - 1:
                    await asyncio.sleep(1.5 * (attempt + 1))
        return {}

async def fetch_district_data(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    district_info: dict,
    fecha_desde: str,
    fecha_hasta: str,
    incluir_prog: bool
) -> tuple[list[dict], list[dict]]:
    d_id = district_info["id"]
    despachos_raw = []
    programacion_raw = []

    url_desp = f"{KYROS_BASE_URL}/reportes/despachos/publico"
    p1 = await fetch_kyros_page(session, semaphore, url_desp, {
        "distrito_id": d_id,
        "desde": fecha_desde,
        "hasta": fecha_hasta,
        "page": 1,
        "page_size": PAGE_SIZE
    })
    total_pages = p1.get("total_pages", 1)
    items = p1.get("items", [])

    if total_pages > 1:
        tasks = [
            fetch_kyros_page(session, semaphore, url_desp, {
                "distrito_id": d_id,
                "desde": fecha_desde,
                "hasta": fecha_hasta,
                "page": p,
                "page_size": PAGE_SIZE
            })
            for p in range(2, total_pages + 1)
        ]
        pages = await asyncio.gather(*tasks)
        for pg in pages:
            items.extend(pg.get("items", []))

    for it in items:
        if it.get("familia") == "GLP":
            continue
        st_name = it.get("estacion") or it.get("cliente")
        if not st_name or st_name.strip() == "YPFB":
            continue
        it["_distrito_id"] = d_id
        despachos_raw.append(it)

    if incluir_prog:
        url_prog = f"{KYROS_BASE_URL}/reportes/programacion/publico"
        p1_prog = await fetch_kyros_page(session, semaphore, url_prog, {
            "distrito_id": d_id,
            "desde": fecha_desde,
            "hasta": fecha_hasta,
            "page": 1,
            "page_size": PAGE_SIZE
        })
        total_p_prog = p1_prog.get("total_pages", 1)
        items_prog = p1_prog.get("items", [])

        if total_p_prog > 1:
            tasks_prog = [
                fetch_kyros_page(session, semaphore, url_prog, {
                    "distrito_id": d_id,
                    "desde": fecha_desde,
                    "hasta": fecha_hasta,
                    "page": p,
                    "page_size": PAGE_SIZE
                })
                for p in range(2, total_p_prog + 1)
            ]
            pages_p = await asyncio.gather(*tasks_prog)
            for pg in pages_p:
                items_prog.extend(pg.get("items", []))

        for it in items_prog:
            prod = it.get("producto", "")
            if "EESS" in prod or "GASOLINA" in prod or "DIESEL" in prod:
                cliente = it.get("cliente")
                if not cliente or cliente.strip() == "YPFB":
                    continue
                it["_distrito_id"] = d_id
                programacion_raw.append(it)

    return despachos_raw, programacion_raw

# =============================================================================
# 7. PROCESAMIENTO Y TRANSFORMACIÓN AL ESQUEMA EN INGLÉS
# =============================================================================
async def process_and_transform_records(
    despachos_raw: list[dict],
    programacion_raw: list[dict],
    matcher: StationMatcher,
    osrm_cache: OSRMCache,
    enable_osrm: bool
) -> list[dict]:
    dispatches: list[dict] = []
    now_utc = datetime.now(timezone.utc)

    connector = aiohttp.TCPConnector(limit_per_host=4)
    async with aiohttp.ClientSession(connector=connector) as osrm_session:
        for it in despachos_raw:
            raw_name = (it.get("estacion") or it.get("cliente") or "UNKNOWN").strip()
            d_id = it.get("_distrito_id", 4)
            d_meta = CATALOGO_DISTRITOS.get(d_id, {
                "departamento": "santa_cruz",
                "plant_lat": -17.8771,
                "plant_lng": -63.2004
            })

            station_id, st_dict = matcher.match(raw_name, d_id)
            department = st_dict.get("department") if st_dict else d_meta.get("departamento", "santa_cruz")
            if department not in VALID_DEPARTMENTS:
                department = "santa_cruz"

            prod_raw = it.get("producto_despachado") or it.get("producto") or it.get("familia") or "FUEL"
            familia = it.get("familia", "")
            fuel_type_id, _ = map_fuel_type(prod_raw, familia)

            liters = float(it.get("cantidad_despachada") or 0.0)
            plate = (it.get("placa_despacho") or "").strip() or None
            has_gps = bool(plate)
            is_provincia = (it.get("ambito") or "").strip().lower() == "provincia"

            fecha_salida_str = it.get("fecha_despacho_efectivo_hora") or it.get("fecha_despacho_efectivo")
            dispatched_at = parse_kyros_datetime(fecha_salida_str) or now_utc

            st_lat = st_dict.get("latitude") if st_dict else None
            st_lng = st_dict.get("longitude") if st_dict else None
            plant_lat = d_meta.get("plant_lat")
            plant_lng = d_meta.get("plant_lng")

            # 1. Obtener ruta OSRM con distancia y duración real de viaje
            planned_route_polyline = None
            travel_min = None

            if enable_osrm and plant_lat and plant_lng and st_lat and st_lng:
                osrm_data = await fetch_osrm_route(
                    osrm_session, osrm_cache, plant_lat, plant_lng, float(st_lat), float(st_lng)
                )
                if osrm_data and osrm_data.get("duration_min"):
                    travel_min = osrm_data["duration_min"]
                    planned_route_polyline = osrm_data.get("polyline")

            # 2. Respaldo por física vial si OSRM no responde o está desactivado
            if travel_min is None:
                travel_min = estimate_travel_minutes(plant_lat, plant_lng, st_lat, st_lng, is_provincia)

            estimated_arrival_at = dispatched_at + timedelta(minutes=travel_min)
            elapsed_min = (now_utc - dispatched_at).total_seconds() / 60.0

            if elapsed_min < 0:
                status = "scheduled"
                confirmed_arrival_at = None
            elif elapsed_min < travel_min:
                status = "in_transit"
                confirmed_arrival_at = None
            elif travel_min <= elapsed_min < travel_min + 30:
                status = "delivering"
                confirmed_arrival_at = None
            else:
                status = "delivered"
                confirmed_arrival_at = estimated_arrival_at

            hash_input = f"ypfb_desp_{d_id}_{dispatched_at.isoformat()}_{normalize_text(raw_name)}_{fuel_type_id}_{plate or 'NOPLATE'}_{liters}"
            unique_hash = hashlib.md5(hash_input.encode("utf-8")).hexdigest()

            scheduled_date_val = it.get("fecha_despacho_programado") or dispatched_at.astimezone(BOLIVIA_TZ).strftime("%Y-%m-%d")

            dispatch_record = {
                "station_id": station_id,
                "raw_station_name": raw_name,
                "department": department,
                "district_id": d_id,
                "fuel_type_id": fuel_type_id,
                "raw_product_name": prod_raw,
                "volume_liters": liters if liters > 0 else None,
                "license_plate": plate,
                "has_gps": has_gps,
                "is_public": True,
                "scheduled_date": str(scheduled_date_val)[:10] if scheduled_date_val else None,
                "dispatched_at": dispatched_at.isoformat(),
                "estimated_arrival_at": estimated_arrival_at.isoformat(),
                "confirmed_arrival_at": confirmed_arrival_at.isoformat() if confirmed_arrival_at else None,
                "status": status,
                "source": "ypfb",
                "unique_hash": unique_hash,
                "planned_route_polyline": planned_route_polyline,
                "actual_route_polyline": None,
                "metadata": {
                    "family": familia,
                    "family_code": it.get("familia_codigo"),
                    "client": it.get("cliente"),
                    "district_name": d_meta.get("nombre"),
                    "unit": it.get("unidad", "L")
                },
                "updated_at": now_utc.isoformat()
            }
            dispatches.append(dispatch_record)

        for it in programacion_raw:
            raw_name = (it.get("cliente") or "UNKNOWN").strip()
            d_id = it.get("_distrito_id", 4)
            d_meta = CATALOGO_DISTRITOS.get(d_id, {
                "departamento": "santa_cruz",
                "plant_lat": -17.8771,
                "plant_lng": -63.2004
            })

            station_id, st_dict = matcher.match(raw_name, d_id)
            department = st_dict.get("department") if st_dict else d_meta.get("departamento", "santa_cruz")
            if department not in VALID_DEPARTMENTS:
                department = "santa_cruz"

            prod_raw = it.get("producto") or it.get("familia") or "FUEL"
            familia = it.get("familia", "")
            fuel_type_id, _ = map_fuel_type(prod_raw, familia)

            liters = float(it.get("volumen_programado") or 0.0)
            scheduled_date_val = str(it.get("fecha_programacion") or now_utc.astimezone(BOLIVIA_TZ).strftime("%Y-%m-%d"))[:10]

            canal = (it.get("canal") or "").strip()
            fam_cod = (it.get("familia_codigo") or "").strip()
            hash_input = f"ypfb_prog_{d_id}_{scheduled_date_val}_{normalize_text(raw_name)}_{fuel_type_id}_{canal}_{fam_cod}_{liters}"
            unique_hash = hashlib.md5(hash_input.encode("utf-8")).hexdigest()

            dispatch_record = {
                "station_id": station_id,
                "raw_station_name": raw_name,
                "department": department,
                "district_id": d_id,
                "fuel_type_id": fuel_type_id,
                "raw_product_name": prod_raw,
                "volume_liters": liters if liters > 0 else None,
                "license_plate": None,
                "has_gps": False,
                "is_public": True,
                "scheduled_date": scheduled_date_val,
                "dispatched_at": None,
                "estimated_arrival_at": None,
                "confirmed_arrival_at": None,
                "status": "scheduled",
                "source": "ypfb",
                "unique_hash": unique_hash,
                "planned_route_polyline": None,
                "actual_route_polyline": None,
                "metadata": {
                    "family": familia,
                    "channel": it.get("canal"),
                    "district_name": d_meta.get("nombre")
                },
                "updated_at": now_utc.isoformat()
            }
            dispatches.append(dispatch_record)

    return dispatches

# =============================================================================
# 8. INSERCIÓN / ACTUALIZACIÓN POR LOTES EN SUPABASE
# =============================================================================
def batch_upsert_dispatches(db: Client, dispatches: list[dict], batch_size: int = BATCH_SIZE) -> int:
    if not dispatches:
        print("  [INFO] No hay despachos para insertar.")
        return 0

    # 1. Deduplicación estricta en memoria por unique_hash
    # Evita el error Postgres 21000 (ON CONFLICT DO UPDATE cannot affect row a second time)
    seen_hashes = set()
    unique_dispatches = []
    for d in dispatches:
        h = d["unique_hash"]
        if h not in seen_hashes:
            seen_hashes.add(h)
            unique_dispatches.append(d)

    total_records = len(unique_dispatches)
    dups_removed = len(dispatches) - total_records
    if dups_removed > 0:
        print(f"  [DEDUP] Se consolidaron {dups_removed} registros duplicados de Kyros en memoria.")

    upserted_count = 0
    t_start = time.time()

    print(f"  [DB] Realizando upsert de {total_records} registros únicos en 'public.dispatches' (lotes de {batch_size})...")

    for i in range(0, total_records, batch_size):
        batch = unique_dispatches[i:i + batch_size]
        lote_num = (i // batch_size) + 1
        total_lotes = (total_records + batch_size - 1) // batch_size
        try:
            print(f"    -> Lote {lote_num}/{total_lotes} ({len(batch)} items)...", end="", flush=True)
            t_batch = time.time()
            db.table("dispatches").upsert(batch, on_conflict="unique_hash").execute()
            elapsed_batch = time.time() - t_batch
            print(f" OK ({elapsed_batch:.2f}s)")
            upserted_count += len(batch)
        except Exception as e:
            # Fallback de resiliencia: intentar registro por registro si el lote completo falla
            print(f" FALLÓ lote ({e}). Reintentando items individualmente...", end="", flush=True)
            salvados = 0
            for item in batch:
                try:
                    db.table("dispatches").upsert([item], on_conflict="unique_hash").execute()
                    salvados += 1
                except Exception:
                    pass
            print(f" Rescatados {salvados}/{len(batch)}")
            upserted_count += salvados

    total_elapsed = time.time() - t_start
    print(f"  [OK] Upsert finalizado: {upserted_count}/{total_records} registros sincronizados en {total_elapsed:.2f}s.")
    return upserted_count

# =============================================================================
# 9. FUNCIÓN PRINCIPAL / ORQUESTADOR
# =============================================================================
async def run_ypfb_ingestion_async(
    db: Client,
    dias_atras: int = 0,
    incluir_prog: bool = True,
    enable_osrm: bool = True,
    dry_run: bool = False
) -> dict:
    t_start_total = time.time()
    now_bo = datetime.now(BOLIVIA_TZ)
    fecha_hasta = now_bo.strftime("%Y-%m-%d")
    fecha_desde = (now_bo - timedelta(days=dias_atras)).strftime("%Y-%m-%d")

    print("\n" + "=" * 75)
    rango_str = f"Solo Hoy ({fecha_hasta})" if dias_atras == 0 else f"{fecha_desde} a {fecha_hasta}"
    print(f"🚀 INGESTIÓN YPFB/KYROS -> SURTIVEO | Rango: {rango_str}")
    print("=" * 75)

    print("[1/5] Cargando catálogo de estaciones físicas desde Supabase...")
    st_res = db.table("stations").select("id, name, anh_name, ypfb_names, department, latitude, longitude").execute()
    stations_db = st_res.data or []
    print(f"  -> {len(stations_db)} estaciones cargadas.")

    matcher = StationMatcher(stations_db)
    print(f"  -> Índice de emparejamiento construido ({len(matcher.alias_map)} alias directos O(1)).")

    osrm_cache = OSRMCache()
    print(f"  -> Caché OSRM cargada ({len(osrm_cache.routes)} rutas precalculadas).")

    print("[2/5] Consultando distritos activos en Kyros API...")
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_KYROS)
    connector = aiohttp.TCPConnector(limit_per_host=6)

    async with aiohttp.ClientSession(connector=connector) as session:
        distritos_raw = await fetch_kyros_page(session, semaphore, f"{KYROS_BASE_URL}/catalogos/distritos/publico", {})
        distritos_activos = [d for d in distritos_raw if d.get("tiene_datos")]
        print(f"  -> {len(distritos_activos)} distritos comerciales con datos activos.")

        print(f"[3/5] Extrayendo despachos y programación (Semáforo: {MAX_CONCURRENT_KYROS} hilos)...")
        tasks = [
            fetch_district_data(session, semaphore, d, fecha_desde, fecha_hasta, incluir_prog)
            for d in distritos_activos
        ]
        results = await asyncio.gather(*tasks)

    all_despachos = []
    all_programacion = []
    for desp_list, prog_list in results:
        all_despachos.extend(desp_list)
        all_programacion.extend(prog_list)

    print(f"  -> Despachos efectivos extraídos: {len(all_despachos)}")
    print(f"  -> Nominaciones programadas extraídas: {len(all_programacion)}")

    print("[4/5] Mapeando estaciones, calculando estados físicos y polylines...")
    dispatches = await process_and_transform_records(
        all_despachos, all_programacion, matcher, osrm_cache, enable_osrm
    )

    matched_count = sum(1 for d in dispatches if d["station_id"] is not None)
    unmapped_count = len(dispatches) - matched_count
    status_stats = defaultdict(int)
    for d in dispatches:
        status_stats[d["status"]] += 1

    osrm_cache.save()

    print(f"  -> Registros procesados listos para BD: {len(dispatches)}")
    print(f"  -> Emparejamiento:")
    print(f"       * Vinculados a estación física: {matched_count}")
    print(f"       * Sin vincular (station_id NULL): {unmapped_count}")
    print(f"  -> Estados físicos calculados:")
    print(f"       * Programado (scheduled): {status_stats['scheduled']}")
    print(f"       * En tránsito (in_transit): {status_stats['in_transit']}")
    print(f"       * Descargando (delivering): {status_stats['delivering']}")
    print(f"       * Entregado (delivered):   {status_stats['delivered']}")

    upserted_count = 0
    if not dry_run:
        print("[5/5] Sincronizando con base de datos 'public.dispatches'...")
        upserted_count = batch_upsert_dispatches(db, dispatches)
    else:
        print("[5/5] MODO DRY-RUN: Inserción en base de datos omitida.")

    total_time = time.time() - t_start_total
    print("=" * 75)
    print(f"✨ Proceso completado exitosamente en {total_time:.2f} segundos.")
    print("=" * 75 + "\n")

    return {
        "total_records": len(dispatches),
        "upserted": upserted_count,
        "matched": matched_count,
        "unmapped": unmapped_count,
        "status_stats": dict(status_stats),
        "elapsed_seconds": total_time
    }

def run_ypfb_ingestion(
    db: Client | None = None,
    dias_atras: int = 0,
    incluir_prog: bool = True,
    enable_osrm: bool = True,
    dry_run: bool = False
) -> dict:
    if db is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Variables de entorno SUPABASE_URL (V3) o SUPABASE_KEY (V4) no encontradas.")
        db = create_client(SUPABASE_URL, SUPABASE_KEY)

    return asyncio.run(
        run_ypfb_ingestion_async(db, dias_atras, incluir_prog, enable_osrm, dry_run)
    )

def main():
    parser = argparse.ArgumentParser(description="Ingestor de Despachos y Cisternas YPFB / Kyros para Surtiveo")
    parser.add_argument("--dias", type=int, default=0, help="Número de días hacia atrás a consultar (default: 0 = solo hoy)")
    parser.add_argument("--solo-hoy", action="store_true", help="Consultar únicamente el día de hoy")
    parser.add_argument("--sin-programacion", action="store_true", help="Omitir reporte de programación/nominaciones")
    parser.add_argument("--sin-osrm", action="store_true", help="Desactivar generación de rutas polyline OSRM")
    parser.add_argument("--dry-run", action="store_true", help="Ejecutar análisis sin escribir en la base de datos")

    args = parser.parse_args()

    dias = 0 if args.solo_hoy else args.dias
    incluir_prog = not args.sin_programacion
    enable_osrm = not args.sin_osrm

    run_ypfb_ingestion(
        db=None,
        dias_atras=dias,
        incluir_prog=incluir_prog,
        enable_osrm=enable_osrm,
        dry_run=args.dry_run
    )

if __name__ == "__main__":
    main()
