"""SCRAPER OFICIAL - CADENA GENEX (SANTA CRUZ) - V2

==================================================
1. Mapea TODAS las estaciones de la web (incluso las que hoy solo tienen GNV).
2. Filtra a nivel de producto (ignora 'GAS', procesa cualquier líquido).
3. Sistema de Alerta: Detecta y reporta estaciones nuevas no emparejadas.
"""

import os
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
from supabase import Client, create_client

load_dotenv()

# =============================================================================
# 1. CONFIGURACIÓN Y MAPEOS
# =============================================================================
SUPABASE_URL = os.getenv("V3")
SUPABASE_KEY = os.getenv("V4")
GENEX_URL = os.getenv("gx") or ""

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
        " like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# MAPEO COMPLETO DE LAS 18 ESTACIONES (Reemplazar números con IDs reales de tu BD)
GENEX_STATION_MAPPING = {
    "ARACATACA": 853,
    "GENEX CINTHIA": 1610,  # Hoy solo GNV, pero queda lista por si vende líquidos
    "GENEX GUARACACHI": 800,
    "GENEX I": 796,
    "GENEX II": 801,
    "GENEX III": 798,
    "GENEX IV": 797,
    "GENEX JALDIN": 1607,  # Hoy solo GNV
    "GENEX LA GAVIOTA": 1662,  # Hoy solo GNV
    "GENEX MUTUALISTA": 799,
    "GENEX TROMPILLO": 937,
    "GENEX V": 802,
    "GENEX VI": 1608,  # Hoy solo GNV
    "GENEX WARNES": 1609,  # Hoy solo GNV
    "JARAJORECHI": 1202,
    "PONTONS": 1722,  # Hoy solo GNV
    "TREBOL DE MAYO": 1626,  # Hoy solo GNV
    "VANGAS": 829,
}

# MAPEO DE PRODUCTOS (GNV no está aquí, por lo que se ignora de forma natural)
GENEX_PRODUCT_MAP = {
    "G. ESPECIAL+": 1,  # Gasolina Especial
    "DIESEL+": 2,  # Diésel Oil
    "G. PREMIUM+": 3,  # Gasolina Premium
}

QUEUE_MAP = {"No hay cola": 0,"Poca cola": 5, "Hay cola": 15, "Mucha cola": 35}

SOURCE_NAME = "GENEX_OFFICIAL_WEB"


# =============================================================================
# 2. EXTRACCIÓN, AUDITORÍA Y TRANSFORMACIÓN
# =============================================================================
def scrape_and_parse_genex() -> list[dict]:
  print("[LOG] Descargando datos de la cadena GENEX...")
  try:
    res = requests.get(GENEX_URL, headers=HEADERS, timeout=25)
    res.raise_for_status()
  except Exception as e:
    print(f"[ERROR] Falló la descarga de Genex: {e}")
    return []

  soup = BeautifulSoup(res.text, "html.parser")
  filas = soup.select("table.wcpt-table tr.wcpt-row")
  now_utc = datetime.now(timezone.utc).isoformat()

  records = []
  unmatched_stations = []

  for fila in filas:
    # 1. Extraer nombre y dirección del surtidor
    nombre_tag = fila.select_one(".station_name")
    address_tag = fila.select_one(".station_address")

    if not nombre_tag:
      continue

    nombre_estacion = nombre_tag.get_text(strip=True).upper()
    direccion = address_tag.get_text(strip=True) if address_tag else ""

    # 2. Verificar si está emparejada con nuestra base de datos
    internal_station_id = GENEX_STATION_MAPPING.get(nombre_estacion)

    if not internal_station_id:
      # ALERTA: Si Genex agrega un nuevo surtidor, lo detectamos aquí
      unmatched_stations.append(
          {"nombre": nombre_estacion, "direccion": direccion}
      )
      continue

    # 3. Recorrer los combustibles de la estación
    productos = fila.select(".product_wrapper")
    for prod in productos:
      prod_name_tag = prod.select_one(".product_name")
      if not prod_name_tag:
        continue

      nombre_prod = prod_name_tag.get_text(strip=True).upper()

      # FILTRO: Si es "GAS" (GNV) o no está en el mapa, se ignora
      fuel_type_id = GENEX_PRODUCT_MAP.get(nombre_prod)
      if not fuel_type_id:
        continue

      volumen_tag = prod.select_one(".product_volume")
      volumen_str = (
          volumen_tag.get_text(strip=True).upper() if volumen_tag else ""
      )
      cola_tag = prod.select_one(".product_queue_label")
      cola_str = cola_tag.get_text(strip=True) if cola_tag else ""

      # 4. Determinar condición, litros y cola
      litros = None 
      if volumen_str == "[AGOTADO]":
        condition = "unavailable"
        litros = 0.0
        queue_estimate = None
      else:
        # Extraer litros numéricos (ej. "28.992 litros" -> 28992.0)
        match = re.search(r"(\d+(?:\.\d+)?)", volumen_str.replace(",", "."))
        if match:
          num_limpio = match.group(1).replace(".", "")
          litros = float(num_limpio)
          condition = "available" if litros > 0 else "unavailable"
        else:
          condition = "available"

        queue_estimate = QUEUE_MAP.get(cola_str)

      records.append({
          "station_id": internal_station_id,
          "fuel_type_id": fuel_type_id,
          "reported_at": now_utc,
          "official_condition": condition,
          #"official_queue_cars_estimate": queue_estimate,
          "available_liters": litros,
          "source": SOURCE_NAME,
      })

  # =========================================================================
  # 5. REPORTE DE AUDITORÍA Y SALUD DE SINCRONIZACIÓN
  # =========================================================================
  if unmatched_stations:
    print(
        f"[GX ALERT] Se encontraron"
        f" {len(unmatched_stations)} estaciones NO emparejadas:"
    )
    for un in unmatched_stations:
      print(f"   -> Nombre: '{un['nombre']}' | Dirección: '{un['direccion']}'")
  else:
    print("[GX] Todas las estaciones de la web emparejadas.")

  print(f"[GX] Se generaron {len(records)} reportes de combustibles líquidos.")
  return records


# =============================================================================
# 3. INSERCIÓN EN SUPABASE
# =============================================================================
def main():
  if not SUPABASE_URL or not SUPABASE_KEY:
    print("[GX] Faltan variables de entorno de Supabase.")
    return

  db: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
  reports = scrape_and_parse_genex()

  if reports:
    try:
      db.table("station_official_reports").insert(reports).execute()
      print(f"[GX] {len(reports)} reportes insertados con éxito.")
    except Exception as e:
      print(f"[GX] Error insertando datos: {e}")


if __name__ == "__main__":
  main()