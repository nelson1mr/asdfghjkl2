"""
MONITOR DE GEOFENCING Y LLEGADA DE CISTERNAS -> SURTIVEO (CRON 5 MINUTOS)
========================================================================
1. Consulta despachos activos en tránsito ('in_transit' o 'delivering') con estación física asignada.
2. Consulta telemetría satelital en tiempo real desde Kyros API para camiones con GPS/placa.
3. Actualiza el radar en vivo ('public.active_cisterns') con la última posición, velocidad y movimiento.
4. Calcula geocerca (distancia Haversine al surtidor de destino):
   - Si distancia <= 250m: Confirma llegada ('delivered'), estampa 'confirmed_arrival_at'
     y remueve la cisterna del radar.
5. Respaldo temporal (fallback):
   - Si la cisterna no tiene GPS o el satélite no responde, y han transcurrido más de
     estimated_arrival_at + 25 minutos, confirma la llegada por inferencia física.
6. Al marcar 'delivered', el trigger PostgreSQL 'sync_dispatch_bridge' dispara automáticamente
   la notificación push: "⛽✅ ¡Combustible disponible en [Estación]!".
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import math
import os
import sys
import time

import aiohttp
from dotenv import load_dotenv
from supabase import Client, create_client

# Cargar variables de entorno (.env)
load_dotenv()

SUPABASE_URL = os.getenv("V3") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("V4") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

KYROS_BASE_URL = "https://nominac.kyros-tech.com/api"
DEFAULT_GEOFENCE_RADIUS_METERS = 250.0  # Umbral de llegada física al surtidor
FALLBACK_BUFFER_MINUTES = 25            # Margen de tiempo para camiones sin GPS
MAX_CONCURRENT_GPS_REQUESTS = 5
KYROS_TIMEOUT_SECONDS = 5.0

# Zona horaria de Bolivia (UTC-4)
BOLIVIA_TZ = timezone(timedelta(hours=-4))


def haversine_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia geodésica entre dos puntos en metros."""
    R = 6371000.0  # Radio de la Tierra en metros
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


def parse_gps_timestamp(raw_dt_str: str | None) -> datetime:
    """Parsea el timestamp del GPS de Kyros (generalmente en hora local Bolivia) a UTC."""
    now_utc = datetime.now(timezone.utc)
    if not raw_dt_str:
        return now_utc

    clean_str = str(raw_dt_str).strip()
    try:
        # Formato habitual Kyros: "2026-10-08 14:35:10"
        dt_naive = datetime.strptime(clean_str[:19], "%Y-%m-%d %H:%M:%S")
        dt_local = dt_naive.replace(tzinfo=BOLIVIA_TZ)
        return dt_local.astimezone(timezone.utc)
    except Exception:
        pass

    try:
        dt_iso = datetime.fromisoformat(clean_str.replace("Z", "+00:00"))
        if dt_iso.tzinfo is None:
            dt_iso = dt_iso.replace(tzinfo=BOLIVIA_TZ)
        return dt_iso.astimezone(timezone.utc)
    except Exception:
        return now_utc


async def fetch_truck_gps(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    plate: str
) -> dict | None:
    """Consulta la posición satelital de una placa específica en la API pública de Kyros."""
    clean_plate = plate.strip().upper()
    url = f"{KYROS_BASE_URL}/gps/publico/{clean_plate}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SurtiveoWatcher/3.0",
        "Accept": "application/json"
    }

    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=KYROS_TIMEOUT_SECONDS)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, dict) and "latitud" in data and "longitud" in data:
                        return {
                            "plate": clean_plate,
                            "latitude": float(data["latitud"]),
                            "longitude": float(data["longitud"]),
                            "is_moving": bool(data.get("en_movimiento")),
                            "speed_kmh": float(data.get("velocidad") or 0.0),
                            "hora_gps_raw": data.get("hora_gps"),
                            "status": "online"
                        }
                elif resp.status == 404:
                    # Cisterna sin señal o sin equipo GPS homologado
                    return {"plate": clean_plate, "status": "not_found"}
        except asyncio.TimeoutError:
            return {"plate": clean_plate, "status": "timeout"}
        except Exception as e:
            return {"plate": clean_plate, "status": f"error: {e}"}

    return None


async def verify_arrivals_async(
    db: Client,
    geofence_radius: float = DEFAULT_GEOFENCE_RADIUS_METERS,
    dry_run: bool = False
) -> dict:
    t_start = time.time()
    now_utc = datetime.now(timezone.utc)

    print("\n" + "=" * 75)
    print(f"🛰️  MONITOR DE LLEGADAS DE CISTERNAS (SURTIVEO GEOFENCING) - {now_utc.isoformat()[:19]} UTC")
    print("=" * 75)

    # 1. Obtener despachos en tránsito o descargando
    print("[1/4] Buscando despachos en tránsito ('in_transit' / 'delivering') en Supabase...")
    dispatches_res = db.table("dispatches") \
        .select("id, station_id, license_plate, has_gps, raw_station_name, raw_product_name, dispatched_at, estimated_arrival_at, status") \
        .in_("status", ["in_transit", "delivering"]) \
        .not_.is_("station_id", "null") \
        .execute()

    active_dispatches = dispatches_res.data or []
    print(f"  -> {len(active_dispatches)} despachos activos encontrados en ruta.")

    if not active_dispatches:
        print("  [INFO] No hay cisternas en tránsito actualmente. Finalizando verificación.")
        return {"active": 0, "delivered": 0, "radar_updated": 0, "elapsed": time.time() - t_start}

    # 2. Cargar coordenadas de las estaciones destino
    station_ids = list({d["station_id"] for d in active_dispatches if d.get("station_id")})
    print(f"[2/4] Consultando coordenadas de {len(station_ids)} estaciones destino...")
    stations_res = db.table("stations") \
        .select("id, name, latitude, longitude") \
        .in_("id", station_ids) \
        .execute()

    stations_map = {s["id"]: s for s in (stations_res.data or [])}

    # 3. Separar camiones con GPS vs sin GPS y consultar telemetría satelital
    plates_to_poll = list({
        d["license_plate"].strip().upper()
        for d in active_dispatches
        if d.get("has_gps") and d.get("license_plate")
    })

    print(f"[3/4] Consultando GPS para {len(plates_to_poll)} cisternas activas en Kyros API...")
    gps_results_by_plate = {}

    if plates_to_poll:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_GPS_REQUESTS)
        connector = aiohttp.TCPConnector(limit_per_host=6)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [fetch_truck_gps(session, semaphore, plate) for plate in plates_to_poll]
            responses = await asyncio.gather(*tasks)
            for r in responses:
                if r and r.get("plate"):
                    gps_results_by_plate[r["plate"]] = r

    # 4. Evaluación de geocercas y estimación de llegada
    print(f"[4/4] Evaluando distancias geodésicas (Radio: {geofence_radius}m) y tiempos de viaje...")

    dispatches_to_deliver = []
    cisterns_to_upsert = []
    plates_to_remove_from_radar = []

    delivered_by_gps = 0
    delivered_by_fallback = 0

    for d in active_dispatches:
        disp_id = d["id"]
        st_id = d["station_id"]
        plate = (d.get("license_plate") or "").strip().upper()
        has_gps = bool(d.get("has_gps")) and bool(plate)
        st_info = stations_map.get(st_id)
        st_name = (st_info.get("name") if st_info else None) or d.get("raw_station_name") or f"Estación #{st_id}"

        st_lat = float(st_info["latitude"]) if (st_info and st_info.get("latitude")) else None
        st_lng = float(st_info["longitude"]) if (st_info and st_info.get("longitude")) else None

        gps_data = gps_results_by_plate.get(plate) if has_gps else None
        gps_online = bool(gps_data and gps_data.get("status") == "online")

        is_delivered = False
        confirmed_arrival_dt = None
        delivery_reason = ""

        # CASO A: Telemetría GPS en Vivo
        if gps_online and st_lat is not None and st_lng is not None:
            truck_lat = gps_data["latitude"]
            truck_lng = gps_data["longitude"]
            dist_meters = haversine_distance_meters(truck_lat, truck_lng, st_lat, st_lng)
            recorded_dt = parse_gps_timestamp(gps_data.get("hora_gps_raw"))

            if dist_meters <= geofence_radius:
                is_delivered = True
                confirmed_arrival_dt = recorded_dt
                delivery_reason = f"🎯 Geocerca detectada ({dist_meters:.1f}m del surtidor <= {geofence_radius}m)"
                delivered_by_gps += 1
                plates_to_remove_from_radar.append(plate)
            else:
                # La cisterna aún está en ruta: actualizamos su posición en el radar
                cisterns_to_upsert.append({
                    "license_plate": plate,
                    "dispatch_id": disp_id,
                    "station_id": st_id,
                    "latitude": truck_lat,
                    "longitude": truck_lng,
                    "speed_kmh": gps_data["speed_kmh"],
                    "heading": 0.0,
                    "is_moving": gps_data["is_moving"],
                    "is_public": True,
                    "recorded_at": recorded_dt.isoformat(),
                    "updated_at": now_utc.isoformat()
                })
                print(f"  🚛 [{plate}] En ruta a '{st_name}' | Distancia restante: {dist_meters / 1000.0:.2f} km | Vel: {gps_data['speed_kmh']} km/h")

        # CASO B: Respaldo Temporal (Cisterna sin GPS o satélite no responde)
        if not is_delivered and not gps_online:
            est_arrival_str = d.get("estimated_arrival_at")
            if est_arrival_str:
                try:
                    est_arrival_dt = datetime.fromisoformat(est_arrival_str.replace("Z", "+00:00"))
                    if est_arrival_dt.tzinfo is None:
                        est_arrival_dt = est_arrival_dt.replace(tzinfo=timezone.utc)

                    # Si el tiempo actual excede la hora estimada + margen de seguridad
                    deadline_dt = est_arrival_dt + timedelta(minutes=FALLBACK_BUFFER_MINUTES)
                    if now_utc >= deadline_dt:
                        is_delivered = True
                        confirmed_arrival_dt = est_arrival_dt
                        delivery_reason = f"⏱️ Tiempo transcurrido cumplido (Estimado + {FALLBACK_BUFFER_MINUTES} min de buffer)"
                        delivered_by_fallback += 1
                        if plate:
                            plates_to_remove_from_radar.append(plate)
                    else:
                        min_restantes = (deadline_dt - now_utc).total_seconds() / 60.0
                        print(f"  ⏳ [{plate or 'Sin Placa'}] Esperando a '{st_name}' | Faltan ~{min_restantes:.0f} min para confirmación temporal.")
                except Exception as e:
                    print(f"  [WARN] Error evaluando fecha estimada para despacho {disp_id}: {e}")

        # Si se confirmó la entrega
        if is_delivered:
            print(f"  ✅ [LLEGADA CONFIRMADA] Despacho #{disp_id} -> '{st_name}' | Razón: {delivery_reason}")
            dispatches_to_deliver.append({
                "id": disp_id,
                "confirmed_arrival_at": confirmed_arrival_dt.isoformat() if confirmed_arrival_dt else now_utc.isoformat()
            })

    # 5. Aplicar cambios en la Base de Datos
    updated_deliveries_count = 0
    if dispatches_to_deliver and not dry_run:
        print(f"\n  [DB] Actualizando {len(dispatches_to_deliver)} despachos a 'delivered' en Supabase...")
        for item in dispatches_to_deliver:
            try:
                db.table("dispatches").update({
                    "status": "delivered",
                    "confirmed_arrival_at": item["confirmed_arrival_at"],
                    "updated_at": now_utc.isoformat()
                }).eq("id", item["id"]).execute()
                updated_deliveries_count += 1
            except Exception as e:
                print(f"    [ERROR] Falló confirmación de despacho #{item['id']}: {e}")

    # 6. Actualizar radar de cisternas activas (Digital Twin)
    radar_upserted_count = 0
    if cisterns_to_upsert and not dry_run:
        print(f"  [DB] Actualizando telemetría de {len(cisterns_to_upsert)} cisternas en 'public.active_cisterns'...")
        try:
            db.table("active_cisterns").upsert(cisterns_to_upsert, on_conflict="license_plate").execute()
            radar_upserted_count = len(cisterns_to_upsert)
        except Exception as e:
            print(f"    [WARN] No se pudo actualizar 'active_cisterns': {e}")

    # 7. Limpiar del radar las cisternas que ya llegaron
    if plates_to_remove_from_radar and not dry_run:
        print(f"  [DB] Removiendo {len(plates_to_remove_from_radar)} cisternas entregadas del radar...")
        for pl in plates_to_remove_from_radar:
            try:
                db.table("active_cisterns").delete().eq("license_plate", pl).execute()
            except Exception:
                pass

    elapsed = time.time() - t_start
    print("\n" + "=" * 75)
    print(f"🏁 Verificación finalizada en {elapsed:.2f} segundos:")
    print(f"   * Despachos activos evaluados: {len(active_dispatches)}")
    print(f"   * Entregas confirmadas por Geocerca GPS: {delivered_by_gps}")
    print(f"   * Entregas confirmadas por Fallback temporal: {delivered_by_fallback}")
    print(f"   * Posiciones activas en radar: {radar_upserted_count}")
    print("=" * 75 + "\n")

    return {
        "active_evaluated": len(active_dispatches),
        "delivered_gps": delivered_by_gps,
        "delivered_fallback": delivered_by_fallback,
        "delivered_total": updated_deliveries_count if not dry_run else len(dispatches_to_deliver),
        "radar_active": radar_upserted_count,
        "elapsed_seconds": elapsed
    }


def check_cistern_arrivals(
    db: Client | None = None,
    geofence_radius: float = DEFAULT_GEOFENCE_RADIUS_METERS,
    dry_run: bool = False
) -> dict:
    """Función de entrada síncrona para ser llamada desde otros scripts o schedulers."""
    if db is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Variables de entorno SUPABASE_URL (V3) o SUPABASE_KEY (V4) no encontradas.")
        db = create_client(SUPABASE_URL, SUPABASE_KEY)

    return asyncio.run(
        verify_arrivals_async(db, geofence_radius=geofence_radius, dry_run=dry_run)
    )


def main():
    parser = argparse.ArgumentParser(description="Monitor de Geocercas y Llegadas de Cisternas para Surtiveo (Cron 5m)")
    parser.add_argument("--loop", action="store_true", help="Ejecutar en bucle continuo cada N segundos")
    parser.add_argument("--interval", type=int, default=300, help="Intervalo en segundos para el bucle (default: 300s = 5m)")
    parser.add_argument("--radius", type=float, default=DEFAULT_GEOFENCE_RADIUS_METERS, help="Radio de geocerca en metros (default: 250m)")
    parser.add_argument("--dry-run", action="store_true", help="Simular verificación sin escribir en la base de datos")

    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Variables de entorno V3 y V4 (Supabase) requeridas.")
        sys.exit(1)

    db = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.loop:
        print(f"🔄 Iniciando servicio continuo de monitoreo cada {args.interval} segundos (Ctrl+C para detener)...")
        while True:
            try:
                check_cistern_arrivals(db=db, geofence_radius=args.radius, dry_run=args.dry_run)
            except Exception as e:
                print(f"[ERROR EN CICLO] {e}")
            time.sleep(args.interval)
    else:
        check_cistern_arrivals(db=db, geofence_radius=args.radius, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
