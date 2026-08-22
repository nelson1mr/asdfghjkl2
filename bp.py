import os
import re
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import requests
from supabase import Client, create_client

load_dotenv()

URL = os.getenv("V3")
KEY = os.getenv("V4")
BP_URL = os.getenv("bp") or ""

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

# Configuración de productos a consultar (puedes agregar más en el futuro)
PRODUCTS = [
    {
        "bp_code": 134,
        "fuel_type_id": 1,
        "name": "Gasolina Especial",
        "url": f"{BP_URL}/134",
    },
    {
        "bp_code": 132,
        "fuel_type_id": 2,
        "name": "Diésel Oil",
        "url": f"{BP_URL}/132",
    },
]

# MAPEO DE ESTACIONES
STATION_MAPPING = {
    "BEREA":        {"id": 1053, "fuels": [1, 2]}, 
    "CABEZAS":      {"id": 1055, "fuels": [1, 2]}, 
    "CEDENO":       {"id": 881,  "fuels": [1, 2]}, 
    "CHACO":        {"id": 1220, "fuels": [1, 2]}, 
    "EQUIPETROL":   {"id": 1221, "fuels": [1, 2]},  
    "LA TECA":      {"id": 1241, "fuels": [1, 2]},  
    "LOPEZ":        {"id": 1103, "fuels": [1, 2]}, 
    "LUCYFER":      {"id": 1009, "fuels": [1, 2]},  
    "MONTEVERDE":   {"id": 1104, "fuels": [1, 2]},  
    "MONTECRISTO":  {"id": 1238, "fuels": [1, 2]},  
    "PARAGUA":      {"id": 1244, "fuels": [1, 2]},  
    "PARAPETI":     {"id": 1054, "fuels": [1, 2]}, 
    "SAAVEDRA":     {"id": 1247, "fuels": [1, 2]},  
    "VIRU VIRU":    {"id": 1246, "fuels": [1, 2]}, 

    "ALEMANA":      {"id": 1219, "fuels": [1]},     
    "BENI":         {"id": 1102, "fuels": [1]},     
    "GASCO":        {"id": 1183, "fuels": [1]},    
    "PIRAI":        {"id": 1052, "fuels": [1]},     
    "ROYAL":        {"id": 1245, "fuels": [1]},     
    "SUR CENTRAL":  {"id": 1101, "fuels": [1]},     

    "BELL GAS":     {"id": 1638, "fuels": []},      
}

def parse_product_page(prod_cfg: dict) -> tuple[list[dict], list[dict]]:
    url = prod_cfg["url"]
    fuel_id = prod_cfg["fuel_type_id"]
    fuel_name = prod_cfg["name"]

    try:
        res = requests.get(url, headers=HEADERS, timeout=25)
        res.raise_for_status()
    except Exception as e:
        print(f"[BP ERROR] Error al consultar {fuel_name} ({url}): {e}")
        return [], []

    soup = BeautifulSoup(res.text, "html.parser")
    cards = soup.find_all("div", class_="btn-bio-app")

    records = []
    unmatched = []
    found_stations_in_web = set()

    for card in cards:
        header = card.find("div", class_="bg-oscuro-1")
        if not header:
            continue
        nombre = header.get_text(strip=True).upper()

        volumen = 0.0
        match = re.search(r"([\d,]+(?:\.\d+)?)\s*Lts", card.get_text())
        if match:
            volumen_str = match.group(1).replace(",", "")
            try:
                volumen = float(volumen_str)
            except ValueError:
                volumen = 0.0

        if nombre not in STATION_MAPPING:
            unmatched.append({
                "nombre": nombre,
                "producto": fuel_name,
                "volumen": volumen,
            })
            continue

        st_info = STATION_MAPPING[nombre]
        found_stations_in_web.add(nombre)

        records.append({
            "station_id": st_info["id"],
            "fuel_type_id": fuel_id,
            "official_condition": "available" if volumen > 0 else "unavailable",
            "available_liters": volumen,
            "source": "BIOPETROL_OFFICIAL_WEB",
        })

    for name, st_info in STATION_MAPPING.items():
        if fuel_id in st_info.get("fuels", [1, 2]):
            if name not in found_stations_in_web:
                records.append({
                    "station_id": st_info["id"],
                    "fuel_type_id": fuel_id,
                    "official_condition": "unavailable",
                    "available_liters": 0.0,
                    "source": "BIOPETROL_OFFICIAL_WEB",
                })

    return records, unmatched


def extract_and_parse() -> list[dict]:
    all_records = []
    all_unmatched = []

    for prod in PRODUCTS:
        print(f"[BP] Procesando {prod['name']}...")
        recs, unmatched = parse_product_page(prod)
        all_records.extend(recs)
        all_unmatched.extend(unmatched)

    if all_unmatched:
        print(f"[BP ALERTA] Se encontraron {len(all_unmatched)} estaciones nuevas NO mapeadas:")
        for un in all_unmatched:
            print(f"   -> Nombre: '{un['nombre']}' | Producto: {un['producto']} | Litros: {un['volumen']}")
    else:
        print("[BP] Todas las estaciones encontradas en la web están mapeadas.")

    print(f"[BP] Total de registros generados: {len(all_records)}")
    return all_records

def main():
    if not URL or not KEY:
        print("[BP] Faltan variables de entorno.")
        return

    db: Client = create_client(URL, KEY)
    reports = extract_and_parse()

    if reports:
        try:
            db.table("station_official_reports").insert(reports).execute()
            print(f"[BP] {len(reports)} reportes insertados con éxito.")
        except Exception as e:
            print(f"[BP] Error insertando datos: {e}")


if __name__ == "__main__":
    main()