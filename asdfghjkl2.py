import asyncio
from datetime import datetime, timezone
import os
import aiohttp
from dotenv import load_dotenv
from supabase import Client, create_client

# Cargar variables de entorno locales (.env) o desde GitHub Actions Secrets
load_dotenv()

# =============================================================================
# 1. CONFIGURACIÓN Y CONSTANTES
# =============================================================================
SUPABASE_URL = os.getenv("V3")
# Se requiere la service_role key para tener permisos de inserción directa
SUPABASE_KEY = os.getenv("V4")

# Endpoint de la API v2 , configurado mediante .env o el entorno.
ANH_API_URL = os.getenv("V0")

# Headers necesarios para simular el cliente móvil oficial
HEADERS = {
    "User-Agent": "Dart/3.4 (dart:io)",
    "Accept": "application/json",
    "Connection": "close",
}

# Departamentos de Bolivia: 1=Chuquisaca, 2=La Paz, 3=Cochabamba, 4=Oruro,
# 5=Potosí, 6=Tarija, 7=Santa Cruz, 8=Beni, 9=Pando
DEPARTAMENTOS = range(1, 10)

# Mapeo del código de producto que usa la ANH a los 'fuel_type_id' en nuestra BD:
# ANH 0 -> Gasolina Especial
# ANH 1 -> Diésel Oil
# ANH 2 -> Gasolina Premium
# ANH 3 -> Diésel ULS
API_PRODUCT_TO_FUEL_TYPE_ID = {
    0: 1,
    1: 2,
    2: 3,
    3: 4,
}

# Tablas de Supabase
STATIONS_TABLE = "stations"
REPORTS_TABLE = "station_official_reports"
SOURCE_NAME = "ANH_SCRAPER V2"

# Tamaño de bloque para inserciones masivas (evita superar el límite de payload de PostgREST)
BATCH_SIZE = 500


# =============================================================================
# 2. CARGA DEL MAPA DE ESTACIONES EN MEMORIA
# =============================================================================
def get_station_id_mapping(db: Client) -> dict[int, int]:
  """Descarga la lista de estaciones registradas en nuestra base de datos

  y construye un diccionario de traducción: { anh_id: id_interno }.
  """
  print("[PASO 1/4] Obteniendo mapa de traducción de estaciones desde Supabase...")
  try:
    # Traemos hasta 5.000 estaciones para cubrir todo el territorio nacional
    response = (
        db.table(STATIONS_TABLE)
        .select("id, anh_id")
        .not_.is_("anh_id", "null")
        .limit(5000)
        .execute()
    )

    mapping = {
        row["anh_id"]: row["id"]
        for row in response.data
        if row.get("anh_id") is not None
    }
    print(
        f"  [OK] Mapeo cargado exitosamente: {len(mapping)} estaciones listas."
    )
    return mapping
  except Exception as e:
    print(
        "  [ERROR] Falló la conexión con Supabase al cargar 'stations':"
        f" {e}"
    )
    return {}


# =============================================================================
# 3. EXTRACCIÓN ASÍNCRONA DE LA API ANH
# =============================================================================
async def fetch_dep_prod(
    session: aiohttp.ClientSession, dep: int, prod: int
) -> list[dict]:
  """Realiza una petición HTTP GET para una combinación específica de

  departamento y tipo de combustible.
  """
  url = ANH_API_URL.format(dep=dep, prod=prod)
  try:
    # ssl=False es necesario porque algunos certificados intermedios del gobierno no son reconocidos por defecto
    async with session.get(
        url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30), ssl=False
    ) as response:
      if response.status != 200:
        return []

      data = await response.json()
      if data.get("strMensaje") == "OK" and "oResultado" in data:
        items = data.get("oResultado", [])
        # Guardamos el código de producto y la hora del servidor en cada registro
        server_time = data.get("server_time")
        for item in items:
          item["_api_producto"] = prod
          item["_server_time"] = server_time
        return items
      return []
  except Exception as e:
    print(f"  [WARN] Error consultando dep={dep}, prod={prod}: {e}")
    return []


async def fetch_all_anh_telemetry() -> list[dict]:
  """Dispara concurrentemente las 36 consultas a la ANH (9 departamentos x 4 productos)."""
  print("[PASO 2/4] Consultando la API de la ANH en paralelo...")
  connector = aiohttp.TCPConnector(ssl=False, limit=20)

  async with aiohttp.ClientSession(connector=connector) as session:
    tasks = [
        fetch_dep_prod(session, dep, prod)
        for dep in DEPARTAMENTOS
        for prod in API_PRODUCT_TO_FUEL_TYPE_ID.keys()
    ]
    results = await asyncio.gather(*tasks)

  # Aplanar la lista de listas en una sola lista de estaciones
  all_records = [item for sublist in results for item in sublist]
  print(
      f"  [OK] Total de telemetrías recibidas de la ANH: {len(all_records)}"
  )
  return all_records


# =============================================================================
# 4. ALGORITMO DE CLASIFICACIÓN DE DISPONIBILIDAD
# =============================================================================
def determine_availability(item: dict) -> str:
  """Determina si el estado es 'available', 'unavailable' o 'unknown'.

  - 'available': Venta activa confirmada en tiempo real o tanque con stock hoy.
  - 'unavailable': Certeza de agotado (> 90 min inactivo con saldo bajo) o meses
  sin operar.
  - 'unknown': Incertidumbre (sensores desactualizados en provincias, zona gris
  de 45-90 min o datos nulos).
  """
  saldo_estado = (item.get("saldo_estado") or "bajo").lower()
  fecha_venta_raw = item.get("fecha_ultima_venta")
  server_time_raw = item.get("_server_time")

  # CASO 1: Si no hay fecha de venta registrada en la API -> 'unknown'
  if not fecha_venta_raw:
    return "unknown"

  # 1. Obtener fecha de referencia
  if server_time_raw:
    try:
      ref_time = datetime.fromisoformat(server_time_raw)
    except Exception:
      ref_time = datetime.now(timezone.utc)
  else:
    ref_time = datetime.now(timezone.utc)

  # 2. Calcular minutos transcurridos
  minutos_sin_venta = 999999.0
  try:
    fecha_venta = datetime.fromisoformat(fecha_venta_raw)
    if ref_time.tzinfo and not fecha_venta.tzinfo:
      fecha_venta = fecha_venta.replace(tzinfo=ref_time.tzinfo)
    minutos_sin_venta = (ref_time - fecha_venta).total_seconds() / 60.0
  except Exception:
    return "unknown"

  # =========================================================================
  # 3. REGLAS DE DECISIÓN CON 'UNKNOWN'
  # =========================================================================

  # RAMA A: Tanques que reportan nivel ALTO o MEDIO
  if saldo_estado in ["alto", "medio"]:
    # Vendió en las últimas 12 horas -> Stock garantizado
    if minutos_sin_venta <= 720.0:
      return "available"
    # Dice tener stock pero no vende hace más de 12 horas (telemetría en duda)
    return "unknown"

  # RAMA B: Tanques con nivel BAJO (El 90% de los surtidores)
  elif saldo_estado == "bajo":
    # 0 a 45 minutos: Despacho activo en curso
    if minutos_sin_venta <= 45.0:
      return "available"

    # 45 a 90 minutos: Zona gris (cambio de turno o se está terminando)
    # elif minutos_sin_venta <= 90.0:
    #   return "unknown"

    # Más de 90 minutos con saldo bajo: Tanque 100% agotado
    else:
      return "unavailable"

  return "unknown"


# =============================================================================
# 5. TRANSFORMACIÓN Y NORMALIZACIÓN DE DATOS
# =============================================================================
def transform_to_reports_schema(
    raw_data: list[dict], anh_map: dict[int, int]
) -> list[dict]:
  """Transforma los datos en bruto al esquema exacto de la tabla

  'station_official_reports'.
  """
  print("[PASO 3/4] Transformando y normalizando datos con la lógica de negocio...")
  now_utc = datetime.now(timezone.utc).isoformat()
  records_to_insert = []
  skipped_count = 0

  for item in raw_data:
    anh_id = item.get("id")
    api_prod = item.get("_api_producto")

    # Obtener el ID interno de nuestra BD a partir del ID de la ANH
    internal_station_id = anh_map.get(anh_id)
    fuel_type_id = API_PRODUCT_TO_FUEL_TYPE_ID.get(api_prod)

    # Si la estación no está en nuestro catálogo o el combustible no existe, omitir
    if not internal_station_id or not fuel_type_id:
      skipped_count += 1
      continue

    # Determinar condición calculada
    condition = determine_availability(item)

    # Fecha del reporte (usar fecha del sensor si es válida, sino la actual)
    # reported_at = item.get("updated_at") or item.get("fecha_ultima_venta")
    # if reported_at:
    #   try:
    #     reported_at = datetime.fromisoformat(reported_at).isoformat()
    #   except ValueError:
    #     reported_at = now_utc
    # else:
    reported_at = now_utc

    records_to_insert.append({
        "station_id": internal_station_id,  # FK hacia nuestra tabla 'stations'
        "fuel_type_id": fuel_type_id,
        "reported_at": reported_at,
        "official_condition": condition,  # 'available' | 'unavailable'
        "official_queue_cars_estimate": None,
        "available_liters": None,  # La API v2 no reporta litros numéricos
        "source": SOURCE_NAME,
    })

  print(
      f"  [OK] Registros estructurados: {len(records_to_insert)} (Omitidos por"
      f" falta de match: {skipped_count})"
  )
  return records_to_insert


# =============================================================================
# 6. INSERCIÓN EN SUPABASE
# =============================================================================
def insert_reports_batch(db: Client, records: list[dict]):
  """Inserta los registros en 'station_official_reports' divididos en lotes

  para no saturar la API ni exceder los límites de payload.
  """
  if not records:
    print("  [WARN] No hay registros para insertar.")
    return

  total = len(records)
  total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
  print(
      f"[PASO 4/4] Insertando {total} registros en '{REPORTS_TABLE}' en"
      f" {total_batches} lotes..."
  )

  for i in range(0, total, BATCH_SIZE):
    batch = records[i : i + BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    try:
      db.table(REPORTS_TABLE).insert(batch).execute()
      print(f"  [OK] Lote {batch_num}/{total_batches} insertado ({len(batch)} filas)")
    except Exception as e:
      print(f"  [ERROR] Falló la inserción del lote {batch_num}: {e}")


# =============================================================================
# 7. FUNCIÓN PRINCIPAL (ENTRYPOINT)
# =============================================================================
def main():
  print("=" * 60)
  print(
      "INICIANDO SCRAPER DE COMBUSTIBLE ANH V2 -",
      datetime.now(timezone.utc).isoformat(),
  )
  print("=" * 60)

  if not SUPABASE_URL or not SUPABASE_KEY or not ANH_API_URL:
    print(
        "[FATAL] Faltan variables de entorno: V3, V4 o V0."
    )
    exit(1)

  # Inicializar cliente de Supabase
  db = create_client(SUPABASE_URL, SUPABASE_KEY)

  # 1. Cargar el mapa de traducción { anh_id -> id_interno }
  anh_map = get_station_id_mapping(db)
  if not anh_map:
    print(
        "[FATAL] El mapa de estaciones está vacío. Revisa que tu tabla"
        " 'stations' tenga la columna 'anh_id' poblada."
    )
    exit(1)

  # 2. Descargar datos de la ANH
  raw_telemetry = asyncio.run(fetch_all_anh_telemetry())

  if raw_telemetry:
    # 3. Transformar y calcular 'available' / 'unavailable'
    reports = transform_to_reports_schema(raw_telemetry, anh_map)

    # 4. Insertar en Supabase para disparar triggers/vistas de la app
    insert_reports_batch(db, reports)

  print("=" * 60)
  print("[FIN] Proceso completado exitosamente.")
  print("=" * 60)


if __name__ == "__main__":
  main()