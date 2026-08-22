import os
import requests
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

URL = os.getenv("V3")
KEY = os.getenv("V4")
GG_BASE_URL = os.getenv("gg") or ""

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

DEPARTMENTS = ["santacruz", "tarija", "cochabamba"]

PRODUCT_MAPPING = {
    "GASOLINA ESPECIAL": 1,
    "GASOLINA ESPECIAL +": 1,
    "GASOLINA SUPER ETANOL": 3, #consideramos etanol como gasolina premium al tener mas octanaje que la especial
    "SUPER ETANOL 92": 3,
    "DIESEL": 2,
    "DIESEL OIL": 2,
    "DIESEL OIL +": 2,
    "GASOLINA PREMIUM": 3,
    "GASOLINA SUPER PREMIUM": 3,
    "GASOLINA PREMIUN +": 3,
    "GASOLINA PREMIUM +": 3,
    "DIESEL ULSD": 4,
    "DIESEL ULS": 4,
}

STATION_MAPPING = {
    "ORSA ALEMANA": 1036,
    "ORSA URUBO": 1037,
    "AGRUPA SRL": 778,
    "SOINTA SRL": 1237,
    "PORTILLO": 1154,
    "ESTAGAS SRL": 1176,
}

def fetch_department_data(department: str) -> dict | None:
    url = f"{GG_BASE_URL.rstrip('/')}/{department}"
    try:
        res = requests.get(url, headers=HEADERS, timeout=25)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        print(f"[GG ERROR] Error al consultar {department} ({url}): {e}")
        return None

def extract_and_parse() -> list[dict]:
    all_records = []
    unmatched_stations = []
    unmatched_products = set()

    for dept in DEPARTMENTS:
        print(f"[GG] Procesando departamento: {dept}...")
        data = fetch_department_data(dept)
        if not data or "estaciones" not in data:
            continue

        for est in data.get("estaciones", []):
            nombre = est.get("nombre", "").strip().upper()
            station_id = STATION_MAPPING.get(nombre)

            if not station_id:
                unmatched_stations.append({
                    "nombre": nombre,
                    "departamento": dept,
                })
                continue

            fuels_accumulator = {}

            for tanque in est.get("tanques", []):
                raw_prod = tanque.get("producto", "")
                prod_clean = " ".join(raw_prod.upper().split())

                if "GNV" in prod_clean or "GAS NATURAL" in prod_clean:
                    continue

                fuel_id = PRODUCT_MAPPING.get(prod_clean)
                if fuel_id is None:
                    unmatched_products.add(raw_prod)
                    continue

                litros = float(tanque.get("litros", 0.0) or 0.0)
                fuels_accumulator[fuel_id] = fuels_accumulator.get(fuel_id, 0.0) + litros

            for fuel_id, total_liters in fuels_accumulator.items():
                volumen = round(total_liters, 2)
                condition = "available" if volumen > 0 else "unavailable"

                all_records.append({
                    "station_id": station_id,
                    "fuel_type_id": fuel_id,
                    "official_condition": condition,
                    "available_liters": volumen,
                    "source": "GASGROUP_OFFICIAL_WEB",
                })

    if unmatched_stations:
        print(f"[GG ALERTA] Se encontraron {len(unmatched_stations)} estaciones NO mapeadas:")
        for un in unmatched_stations:
            print(f"   -> Nombre: '{un['nombre']}' | Dept: {un['departamento']}")
    else:
        print("[GG] Todas las estaciones de la API están mapeadas.")

    if unmatched_products:
        print(f"[GG ALERTA] Se encontraron productos NO reconocidos en PRODUCT_MAPPING:")
        for up in unmatched_products:
            print(f"   -> Producto: '{up}'")

    print(f"[GG] Total de reportes generados: {len(all_records)}")
    return all_records

def main():
    if not URL or not KEY:
        print("[GG] Faltan variables de entorno.")
        return

    db: Client = create_client(URL, KEY)
    reports = extract_and_parse()

    if reports:
        try:
            db.table("station_official_reports").insert(reports).execute()
            print(f"[GG] {len(reports)} reportes insertados con éxito.")
        except Exception as e:
            print(f"[GG] Error insertando datos: {e}")

if __name__ == "__main__":
    main()