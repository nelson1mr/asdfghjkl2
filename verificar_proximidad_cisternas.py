"""
MOTOR DE PROXIMIDAD Y GEOFENCING DE CISTERNAS -> SURTIVEO (CRON CADA 5 MINUTOS)
================================================================================
1. Obtiene despachos activos ('in_transit' y 'delivering') con estación física asignada.
2. Consulta telemetría GPS satelital en tiempo real en Kyros API para placas únicas.
   - Aplica semáforo de concurrencia (5 hilos) y pacing para evitar HTTP 429 (Too Many Requests).
   - Maneja reintentos automáticos con backoff en caso de saturación temporal.
3. Actualiza el radar en vivo ('public.active_cisterns') para visualización en mapa móvil.
4. Transiciones de estado automáticas:
   - 'in_transit' -> 'delivering': Cisterna llega a la geocerca de la estación (<= 150m)
     y reduce velocidad (< 20 km/h). Registra 'confirmed_arrival_at' y metadata['arrived_at'].
   - 'delivering' -> 'delivered': Cisterna completa la descarga de combustible (tiempo estimado
     según volumen) o abandona la estación (> 300m).
5. Respaldo por inferencia temporal (Fallback):
   - Si la cisterna no tiene GPS o el satélite no reporta (404), y se superó 'estimated_arrival_at'
     + margen de seguridad (30 min) o fecha pasada, se transiciona a 'delivered'.
"""

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
import math
import os
import sys
import time
from typing import Any

import aiohttp
from dotenv import load_dotenv
from supabase import Client, create_client

# Asegurar codificación UTF-8 en stdout/stderr para Windows y CI
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Cargar variables de entorno
load_dotenv()

SUPABASE_URL = os.getenv("V3") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("V4") or os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")

KYROS_BASE_URL = "https://nominac.kyros-tech.com/api"
DEFAULT_GEOFENCE_RADIUS_METERS = 100.0   # Umbral de llegada física a la estación
DEPARTURE_RADIUS_METERS = 300.0         # Umbral de alejamiento post-descarga
DEFAULT_ARRIVAL_MAX_SPEED_KMH = 10.0    # Velocidad máxima para considerar que está detenido/ingresando
FALLBACK_BUFFER_MINUTES = 45            # Margen para camiones sin GPS o fuera de ventana
MAX_CONCURRENT_GPS_REQUESTS = 10         # Concurrencia segura para evitar 429
PACING_DELAY_SECONDS = 0.12             # Espaciado entre peticiones
KYROS_TIMEOUT_SECONDS = 6.0

# Zona horaria de Bolivia (UTC-4)
BOLIVIA_TZ = timezone(timedelta(hours=-4))


def haversine_distance_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia geodésica entre dos puntos en metros con la fórmula de Haversine."""
    R = 6371000.0  # Radio medio de la Tierra en metros
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c


def parse_gps_timestamp(raw_dt_str: str | None) -> datetime:
    """Parsea el timestamp retornado por Kyros (hora Bolivia) a datetime UTC aware."""
    now_utc = datetime.now(timezone.utc)
    if not raw_dt_str:
        return now_utc

    clean_str = str(raw_dt_str).strip()
    try:
        # Formato habitual Kyros: "2026-10-17 11:32" o "2026-10-17 11:32:00"
        if len(clean_str) >= 16:
            fmt = "%Y-%m-%d %H:%M:%S" if len(clean_str) >= 19 else "%Y-%m-%d %H:%M"
            dt_naive = datetime.strptime(clean_str[:19], fmt)
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


def estimate_discharge_minutes(liters: float | None) -> int:
    """Calcula el tiempo promedio de descarga en minutos según el volumen en litros."""
    if not liters or liters <= 0:
        return 30
    # ~500 litros por minuto de caudal + acople de mangueras (mínimo 20 min, máximo 60 min)
    return max(20, min(60, round(liters / 500.0)))


async def fetch_single_truck_gps(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    plate: str
) -> dict | None:
    """Consulta la API de Kyros para una placa con semáforo y reintento en caso de 429."""
    clean_plate = plate.strip().upper()
    url = f"{KYROS_BASE_URL}/gps/publico/{clean_plate}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SurtiveoProximity/3.0",
        "Accept": "application/json"
    }

    async with semaphore:
        # Espaciado preventivo de tráfico
        await asyncio.sleep(PACING_DELAY_SECONDS)

        for attempt in range(2):
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
                        return {"plate": clean_plate, "status": "not_found"}
                    elif resp.status == 429:
                        # Too Many Requests: aplicar backoff y reintentar
                        if attempt == 0:
                            await asyncio.sleep(2.0)
                            continue
                        return {"plate": clean_plate, "status": "rate_limited"}
                    else:
                        return {"plate": clean_plate, "status": f"http_{resp.status}"}
            except asyncio.TimeoutError:
                if attempt == 0:
                    continue
                return {"plate": clean_plate, "status": "timeout"}
            except Exception as e:
                return {"plate": clean_plate, "status": f"error: {e}"}

    return None


async def verify_proximity_async(
    db: Client,
    geofence_radius: float = DEFAULT_GEOFENCE_RADIUS_METERS,
    dry_run: bool = False,
    verbose: bool = False
) -> dict[str, Any]:
    t_start = time.time()
    now_utc = datetime.now(timezone.utc)

    print("\n" + "=" * 78)
    print(f"🛰️  MOTOR DE PROXIMIDAD Y GEOFENCING DE CISTERNAS (SURTIVEO)")
    print(f"    Hora de ejecución: {now_utc.isoformat()[:19]} UTC | Radio geocerca: {geofence_radius}m")
    if dry_run:
        print("    ⚠️  MODO SIMULACIÓN (DRY-RUN): No se aplicarán cambios a la base de datos.")
    print("=" * 78)

    # 1. Obtener despachos activos en tránsito o descargando
    print("[1/4] Buscando despachos activos ('in_transit' / 'delivering') en Supabase...")
    dispatches_res = db.table("dispatches") \
        .select("id, station_id, license_plate, status, volume_liters, scheduled_date, dispatched_at, estimated_arrival_at, confirmed_arrival_at, metadata") \
        .in_("status", ["in_transit", "delivering"]) \
        .not_.is_("station_id", "null") \
        .execute()

    active_dispatches = dispatches_res.data or []
    print(f"  -> {len(active_dispatches)} despachos activos encontrados.")

    if not active_dispatches:
        print("  [INFO] No hay cisternas en tránsito ni descargando actualmente.")
        return {"active": 0, "delivering": 0, "delivered": 0, "radar_updated": 0, "elapsed": time.time() - t_start}

    # 2. Cargar coordenadas de las estaciones destino
    station_ids = list({d["station_id"] for d in active_dispatches if d.get("station_id")})
    print(f"[2/4] Consultando coordenadas de {len(station_ids)} estaciones asociadas...")
    stations_res = db.table("stations") \
        .select("id, name, latitude, longitude") \
        .in_("id", station_ids) \
        .execute()

    stations_map = {
        s["id"]: s for s in (stations_res.data or [])
        if s.get("latitude") is not None and s.get("longitude") is not None
    }

    # 3. Consultar telemetría satelital por placa única (evita duplicar requests)
    unique_plates = list({
        d["license_plate"].strip().upper()
        for d in active_dispatches
        if d.get("license_plate")
    })

    print(f"[3/4] Consultando telemetría GPS para {len(unique_plates)} placas únicas en Kyros API...")
    gps_results_by_plate: dict[str, dict] = {}

    if unique_plates:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_GPS_REQUESTS)
        connector = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT_GPS_REQUESTS + 2)
        async with aiohttp.ClientSession(connector=connector) as session:
            tasks = [fetch_single_truck_gps(session, semaphore, plate) for plate in unique_plates]
            responses = await asyncio.gather(*tasks)
            for r in responses:
                if r and r.get("plate"):
                    gps_results_by_plate[r["plate"]] = r

    online_count = sum(1 for g in gps_results_by_plate.values() if g.get("status") == "online")
    print(f"  -> Telemetría obtenida: {online_count} online / {len(unique_plates)} consultadas.")

    # 4. Evaluación de geocercas y transiciones de estado
    print(f"[4/4] Evaluando proximidad física, tiempos de descarga y reglas de transición...")

    updates_to_delivering = []
    updates_to_delivered = []
    cisterns_to_upsert_radar = []
    plates_to_remove_from_radar = set()

    for d in active_dispatches:
        disp_id = d["id"]
        current_status = d.get("status")
        st_id = d.get("station_id")
        plate = (d.get("license_plate") or "").strip().upper()
        vol_liters = d.get("volume_liters")
        meta = d.get("metadata") or {}
        if not isinstance(meta, dict):
            meta = {}

        st_info = stations_map.get(st_id)
        st_name = (st_info.get("name") if st_info else None) or f"Estación #{st_id}"
        st_lat = float(st_info["latitude"]) if st_info else None
        st_lng = float(st_info["longitude"]) if st_info else None

        gps_data = gps_results_by_plate.get(plate)
        gps_online = bool(gps_data and gps_data.get("status") == "online")

        # ---------------------------------------------------------------------
        # CASO 1: LA CISTERNA ESTÁ ACTUALMENTE 'in_transit'
        # ---------------------------------------------------------------------
        if current_status == "in_transit":
            arrived_at_station = False
            arrival_dt = None
            arrival_reason = ""

            if gps_online and st_lat is not None and st_lng is not None:
                truck_lat = gps_data["latitude"]
                truck_lng = gps_data["longitude"]
                speed = gps_data["speed_kmh"]
                dist = haversine_distance_meters(truck_lat, truck_lng, st_lat, st_lng)
                recorded_dt = parse_gps_timestamp(gps_data.get("hora_gps_raw"))

                # Regla de llegada: dentro del radio de geocerca y velocidad moderada/detenido
                if dist <= geofence_radius and speed <= DEFAULT_ARRIVAL_MAX_SPEED_KMH:
                    arrived_at_station = True
                    arrival_dt = recorded_dt
                    arrival_reason = f"Geocerca alcanzada ({dist:.1f}m <= {geofence_radius}m, vel: {speed:.1f} km/h)"
                else:
                    # En ruta activa: actualizar radar
                    cisterns_to_upsert_radar.append({
                        "license_plate": plate,
                        "dispatch_id": disp_id,
                        "station_id": st_id,
                        "latitude": truck_lat,
                        "longitude": truck_lng,
                        "speed_kmh": speed,
                        "heading": 0.0,
                        "is_moving": gps_data["is_moving"],
                        "is_public": True,
                        "recorded_at": recorded_dt.isoformat(),
                        "updated_at": now_utc.isoformat()
                    })
                    if verbose:
                        print(f"  🚛 [{plate}] En tránsito a '{st_name}' | {dist/1000.0:.2f} km restantes | Vel: {speed:.1f} km/h")

            # Fallback temporal si no hay GPS o satélite no reporta
            if not arrived_at_station and not gps_online:
                est_arrival_str = d.get("estimated_arrival_at")
                if est_arrival_str:
                    try:
                        est_dt = datetime.fromisoformat(est_arrival_str.replace("Z", "+00:00"))
                        if est_dt.tzinfo is None:
                            est_dt = est_dt.replace(tzinfo=timezone.utc)

                        # Si ya expiró la hora estimada + buffer
                        if now_utc >= est_dt + timedelta(minutes=FALLBACK_BUFFER_MINUTES):
                            # Si es de días anteriores o excedió largamente, marcar delivered directamente
                            if now_utc >= est_dt + timedelta(minutes=FALLBACK_BUFFER_MINUTES + 60):
                                updates_to_delivered.append({
                                    "id": disp_id,
                                    "confirmed_arrival_at": est_dt.isoformat(),
                                    "metadata": {
                                        **meta,
                                        "arrived_at": est_dt.isoformat(),
                                        "delivered_at": (est_dt + timedelta(minutes=30)).isoformat(),
                                        "closed_by": "fallback_past_date"
                                    }
                                })
                                if plate:
                                    plates_to_remove_from_radar.add(plate)
                                continue
                            else:
                                arrived_at_station = True
                                arrival_dt = est_dt
                                arrival_reason = f"Fallback de tiempo estimado (+{FALLBACK_BUFFER_MINUTES} min)"
                    except Exception as e:
                        if verbose:
                            print(f"  [WARN] Error parseando estimated_arrival_at #{disp_id}: {e}")

            if arrived_at_station:
                print(f"  🎯 [LLEGADA -> DESCARGANDO] Despacho #{disp_id} ({plate}) en '{st_name}' | {arrival_reason}")
                new_meta = {
                    **meta,
                    "arrived_at": arrival_dt.isoformat() if arrival_dt else now_utc.isoformat(),
                    "arrival_reason": arrival_reason,
                    "estimated_discharge_minutes": estimate_discharge_minutes(vol_liters)
                }
                updates_to_delivering.append({
                    "id": disp_id,
                    "confirmed_arrival_at": arrival_dt.isoformat() if arrival_dt else now_utc.isoformat(),
                    "metadata": new_meta
                })
                # Mantener en radar mientras descarga si tenemos GPS
                if gps_online:
                    cisterns_to_upsert_radar.append({
                        "license_plate": plate,
                        "dispatch_id": disp_id,
                        "station_id": st_id,
                        "latitude": gps_data["latitude"],
                        "longitude": gps_data["longitude"],
                        "speed_kmh": gps_data["speed_kmh"],
                        "heading": 0.0,
                        "is_moving": False,
                        "is_public": True,
                        "recorded_at": now_utc.isoformat(),
                        "updated_at": now_utc.isoformat()
                    })

        # ---------------------------------------------------------------------
        # CASO 2: LA CISTERNA YA ESTÁ 'delivering' (EN PROCESO DE DESCARGA)
        # ---------------------------------------------------------------------
        elif current_status == "delivering":
            discharge_minutes = meta.get("estimated_discharge_minutes") or estimate_discharge_minutes(vol_liters)

            # Determinar fecha/hora de inicio de descarga
            arrived_str = meta.get("arrived_at") or d.get("confirmed_arrival_at") or d.get("estimated_arrival_at")
            arrived_dt = None
            if arrived_str:
                try:
                    arrived_dt = datetime.fromisoformat(str(arrived_str).replace("Z", "+00:00"))
                    if arrived_dt.tzinfo is None:
                        arrived_dt = arrived_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    arrived_dt = now_utc - timedelta(minutes=discharge_minutes + 1)

            elapsed_minutes = (now_utc - arrived_dt).total_seconds() / 60.0 if arrived_dt else 999.0

            is_finished = False
            finish_reason = ""

            # Condición A: GPS indica que la cisterna se alejó de la estación (> 300m)
            if gps_online and st_lat is not None and st_lng is not None:
                truck_lat = gps_data["latitude"]
                truck_lng = gps_data["longitude"]
                dist = haversine_distance_meters(truck_lat, truck_lng, st_lat, st_lng)
                if dist > DEPARTURE_RADIUS_METERS:
                    is_finished = True
                    finish_reason = f"Cisterna partió de la estación ({dist:.0f}m > {DEPARTURE_RADIUS_METERS}m)"

            # Condición B: Tiempo de descarga transcurrido completamente
            if not is_finished and elapsed_minutes >= discharge_minutes:
                is_finished = True
                finish_reason = f"Tiempo de descarga completado ({elapsed_minutes:.0f}m >= {discharge_minutes}m)"

            if is_finished:
                print(f"  ✅ [DESCARGA FINALIZADA -> ENTREGADO] Despacho #{disp_id} ({plate}) en '{st_name}' | {finish_reason}")
                new_meta = {
                    **meta,
                    "delivered_at": now_utc.isoformat(),
                    "delivery_reason": finish_reason,
                    "discharge_duration_minutes": round(elapsed_minutes, 1)
                }
                updates_to_delivered.append({
                    "id": disp_id,
                    "confirmed_arrival_at": arrived_str or now_utc.isoformat(),
                    "metadata": new_meta
                })
                if plate:
                    plates_to_remove_from_radar.add(plate)
            else:
                if verbose:
                    print(f"  ⛽ [{plate}] Descargando en '{st_name}' ({elapsed_minutes:.0f}/{discharge_minutes} min transcurridos)")
                if gps_online:
                    cisterns_to_upsert_radar.append({
                        "license_plate": plate,
                        "dispatch_id": disp_id,
                        "station_id": st_id,
                        "latitude": gps_data["latitude"],
                        "longitude": gps_data["longitude"],
                        "speed_kmh": gps_data["speed_kmh"],
                        "heading": 0.0,
                        "is_moving": False,
                        "is_public": True,
                        "recorded_at": now_utc.isoformat(),
                        "updated_at": now_utc.isoformat()
                    })

    # 5. Aplicar cambios a la Base de Datos
    print(f"\n[5/5] Resumen y aplicación de cambios...")
    print(f"   * Despachos cambiando a 'delivering' (descargando): {len(updates_to_delivering)}")
    print(f"   * Despachos cambiando a 'delivered' (completados):   {len(updates_to_delivered)}")
    print(f"   * Posiciones para actualizar en radar:              {len(cisterns_to_upsert_radar)}")
    print(f"   * Cisternas para remover del radar:                 {len(plates_to_remove_from_radar)}")

    if not dry_run:
        # A) Actualizar a 'delivering'
        for item in updates_to_delivering:
            try:
                db.table("dispatches").update({
                    "status": "delivering",
                    "confirmed_arrival_at": item["confirmed_arrival_at"],
                    "metadata": item["metadata"],
                    "updated_at": now_utc.isoformat()
                }).eq("id", item["id"]).execute()
            except Exception as e:
                print(f"  [ERROR] Falló actualización a delivering #{item['id']}: {e}")

        # B) Actualizar a 'delivered'
        for item in updates_to_delivered:
            try:
                db.table("dispatches").update({
                    "status": "delivered",
                    "confirmed_arrival_at": item["confirmed_arrival_at"],
                    "metadata": item["metadata"],
                    "updated_at": now_utc.isoformat()
                }).eq("id", item["id"]).execute()
            except Exception as e:
                print(f"  [ERROR] Falló actualización a delivered #{item['id']}: {e}")

        # C) Upsert a active_cisterns (radar en tiempo real)
        if cisterns_to_upsert_radar:
            try:
                # Deduplicar por placa única
                radar_by_plate = {c["license_plate"]: c for c in cisterns_to_upsert_radar}
                db.table("active_cisterns").upsert(list(radar_by_plate.values()), on_conflict="license_plate").execute()
            except Exception as e:
                print(f"  [WARN] Error actualizando 'active_cisterns': {e}")

        # D) Eliminar cisternas finalizadas del radar
        for pl in plates_to_remove_from_radar:
            try:
                db.table("active_cisterns").delete().eq("license_plate", pl).execute()
            except Exception:
                pass

    elapsed = time.time() - t_start
    print(f"\n✨ Ciclo completado en {elapsed:.2f} segundos.\n" + "=" * 78)

    return {
        "active_evaluated": len(active_dispatches),
        "transited_to_delivering": len(updates_to_delivering),
        "transited_to_delivered": len(updates_to_delivered),
        "radar_updated": len(cisterns_to_upsert_radar),
        "elapsed_seconds": elapsed
    }


def check_proximity(
    db: Client | None = None,
    geofence_radius: float = DEFAULT_GEOFENCE_RADIUS_METERS,
    dry_run: bool = False,
    verbose: bool = False
) -> dict:
    """Punto de entrada síncrono para schedulers o scripts externos."""
    if db is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Variables V3 y V4 de Supabase requeridas en entorno.")
        db = create_client(SUPABASE_URL, SUPABASE_KEY)

    return asyncio.run(
        verify_proximity_async(
            db,
            geofence_radius=geofence_radius,
            dry_run=dry_run,
            verbose=verbose
        )
    )


def main():
    parser = argparse.ArgumentParser(description="Motor de Proximidad y Geofencing de Cisternas (Surtiveo)")
    parser.add_argument("--radius", type=float, default=DEFAULT_GEOFENCE_RADIUS_METERS, help="Radio de geocerca en metros (default: 150m)")
    parser.add_argument("--dry-run", action="store_true", help="Simular verificación sin persistir cambios en BD")
    parser.add_argument("--verbose", action="store_true", help="Mostrar logs detallados de cada camión")
    parser.add_argument("--loop", action="store_true", help="Ejecutar en bucle continuo cada N segundos")
    parser.add_argument("--interval", type=int, default=300, help="Intervalo en segundos para el bucle continuo (default: 300s = 5m)")

    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Variables de entorno V3 y V4 (Supabase) requeridas.")
        sys.exit(1)

    db = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.loop:
        print(f"🔄 Modo daemon activo: ejecutando cada {args.interval}s (Ctrl+C para salir)...")
        while True:
            try:
                check_proximity(db=db, geofence_radius=args.radius, dry_run=args.dry_run, verbose=args.verbose)
            except Exception as e:
                print(f"[ERROR EN BUCLE] {e}")
            time.sleep(args.interval)
    else:
        check_proximity(db=db, geofence_radius=args.radius, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
