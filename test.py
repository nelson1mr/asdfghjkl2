"""
AUDITOR DE DUPLICADOS EN API ANH
Recorre los 9 departamentos y 4 combustibles en busca de:
1. IDs de estación que aparezcan más de una vez en el mismo departamento.
2. IDs de estación que aparezcan en múltiples departamentos.
3. El estado puntual de la estación 2470 (Automóvil Club Boliviano).
"""

import asyncio
from collections import defaultdict
import aiohttp

API_URL = "https://vsr11vpr08m22gb.anh.gob.bo:9443/WSMobile/v2/estaciones/9ADE86E5A083423EBE50C051F4DB9778?departamento={dep}&producto={prod}"

DEPARTAMENTOS = {
    1: "Chuquisaca",
    2: "La Paz",
    3: "Cochabamba",
    4: "Oruro",
    5: "Potosí",
    6: "Tarija",
    7: "Santa Cruz",
    8: "Beni",
    9: "Pando",
}

PRODUCTOS = {
    0: "GES (Gasolina Especial)",
    1: "DOS (Diésel Oíl)",
    2: "GP+ (Gasolina Premium Plus)",
    3: "DUL (Diésel Ultra)",
}

HEADERS = {
    "User-Agent": "Dart/3.4 (dart:io)",
    "Accept": "application/json",
    "Connection": "close",
}


async def fetch_endpoint(session: aiohttp.ClientSession, dep: int, prod: int):
    url = API_URL.format(dep=dep, prod=prod)
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=25), ssl=False) as resp:
            if resp.status == 200:
                data = await resp.json()
                items = data.get("oResultado") or []
                return dep, prod, items
            else:
                print(f"Dep {dep} Prod {prod}: HTTP {resp.status}")
    except Exception as e:
        print(f"Error consultando Dep {dep} Prod {prod}: {e}")
    return dep, prod, []


async def main():
    print("=" * 70)
    print("INICIANDO ESCANEO DE LA API ANH (9 Departamentos x 4 Productos)")
    print("=" * 70)

    connector = aiohttp.TCPConnector(ssl=False, limit=15)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_endpoint(session, dep, prod)
            for dep in DEPARTAMENTOS
            for prod in PRODUCTOS
        ]
        results = await asyncio.gather(*tasks)

    # Estructuras para agrupar
    # (id, prod) -> lista de apariciones
    by_station_product = defaultdict(list)
    # id -> departamentos donde aparece
    by_station_deps = defaultdict(set)
    total_registros = 0

    for dep, prod, items in results:
        total_registros += len(items)
        for item in items:
            st_id = item.get("id")
            if not st_id:
                continue

            by_station_deps[st_id].add(dep)
            by_station_product[(st_id, prod)].append({
                "dep": dep,
                "dep_name": DEPARTAMENTOS[dep],
                "prod_name": PRODUCTOS[prod],
                "nombre": item.get("nombre"),
                "saldo_estado": item.get("saldo_estado"),
                "saldo_litros": item.get("saldo_litros"),
                "fecha_ultima_venta": item.get("fecha_ultima_venta"),
                "updated_at": item.get("updated_at"),
            })

    print(f"\nTotal registros analizados: {total_registros}")

    # 1. Duplicados interdepartamentales (Mismo ID en más de un departamento)
    cross_dept_duplicates = {st_id: deps for st_id, deps in by_station_deps.items() if len(deps) > 1}

    print("\n" + "=" * 70)
    print(f"1. ESTACIONES QUE APARECEN EN MÁS DE UN DEPARTAMENTO: {len(cross_dept_duplicates)}")
    print("=" * 70)
    for st_id, deps in cross_dept_duplicates.items():
        dep_names = [DEPARTAMENTOS[d] for d in deps]
        sample_name = by_station_product[(st_id, 0)][0]["nombre"] if by_station_product.get((st_id, 0)) else "Desconocido"
        print(f" -> ANH ID {st_id} ('{sample_name}'): Encontrado en {dep_names}")

    # 2. Duplicados exactos (Mismo ID y Mismo Producto devueltos más de una vez)
    exact_duplicates = {k: v for k, v in by_station_product.items() if len(v) > 1}

    print("\n" + "=" * 70)
    print(f"2. DUPLICADOS POR COMBUSTIBLE (Mismo station_id y producto devueltos > 1 vez): {len(exact_duplicates)}")
    print("=" * 70)
    for (st_id, prod_id), occurrences in exact_duplicates.items():
        print(f"\n[!] ANH ID: {st_id} | Producto: {PRODUCTOS[prod_id]} | Veces devuelto: {len(occurrences)}")
        for occ in occurrences:
            print(f"    - Depto: {occ['dep_name']} | Estado: {occ['saldo_estado']} | Venta: {occ['fecha_ultima_venta']} | Update: {occ['updated_at']}")

    # 3. Caso de prueba: Estación 2470 (Automóvil Club Boliviano)
    print("\n" + "=" * 70)
    print("3. CASO ESPECÍFICO: ESTACIÓN 2470 (AUTOMÓVIL CLUB BOLIVIANO)")
    print("=" * 70)
    encontrado_2470 = False
    for prod_id in PRODUCTOS:
        key = (2470, prod_id)
        if key in by_station_product:
            encontrado_2470 = True
            for occ in by_station_product[key]:
                print(f" -> Prod: {occ['prod_name']} | Depto: {occ['dep_name']} | Saldo: {occ['saldo_estado']} ({occ['saldo_litros']}L) | Última venta: {occ['fecha_ultima_venta']}")

    if not encontrado_2470:
        print(" -> No se encontró ningún registro para el ID 2470 en este momento.")


if __name__ == "__main__":
    asyncio.run(main())