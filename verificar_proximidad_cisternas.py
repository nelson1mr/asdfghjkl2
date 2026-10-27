"""
MOTOR DE PROXIMIDAD Y GEOFENCING DE CISTERNAS -> SURTIVEO (v4)
===============================================================
Cron cada 5 min. REQUIERE la migración SQL v4 (fusión de metadata + estados terminales).

Flujo:
  0. Barrido de expirados: activos > 72 h y pendientes > 2 días -> 'cancelled' (sin push).
  1. Despachos activos (in_transit / delivering), CON y SIN estación.
  2. Estaciones destino y radar previo (active_cisterns).
  3. GPS por camión único: placa saneada, variantes solo ante 404, reintentos,
     circuit breaker y presupuesto de tiempo total.
  4. evaluate(): función pura, despacho -> Decision.
  5. Escritura con bloqueo optimista (.eq status) y reconciliación completa del radar.

Reglas:
  A. Sin coordenadas de estación -> cierre solo por tiempo (ETA + 60 min).
  B. GPS fresco (<= 20 min) -> manda la física:
       llegada por geocerca (<= 120 m y <= 12 km/h)
       o por permanencia (<= 400 m, detenido >= 4 min; cubre coordenadas imprecisas).
       Si está lejos NUNCA se entrega: queda in_transit con is_delayed.
  C. Error transitorio de Kyros y camión con GPS conocido -> se espera (hasta ETA + 6 h).
  D. GPS offline / 404 / sin placa -> ETA+45 'delivering', ETA+105 'delivered' (inferido),
       salvo que su última posición conocida (< 45 min) estuviera a > 5 km.
"""

import argparse
import asyncio
import math
import os
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import quote

import aiohttp
from dotenv import load_dotenv
from supabase import Client, create_client

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)

load_dotenv()

SUPABASE_URL = os.getenv("V3") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("V4") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
KYROS_GPS_URL = "https://nominac.kyros-tech.com/api/gps/publico"
BOLIVIA_TZ = timezone(timedelta(hours=-4))

# Catálogo de plantas distribuidoras de YPFB para evaluar salida de planta en loading
from ingestar_despachos_ypfb import CATALOGO_DISTRITOS

# --- Geocercas ----------------------------------------------------------------
GEOFENCE_RADIUS_M = 120.0
ARRIVAL_MAX_SPEED_KMH = 12.0
NEAR_RADIUS_M = 400.0
NEAR_MAX_SPEED_KMH = 5.0
NEAR_DWELL_MIN = 4.0
DEPARTURE_RADIUS_M = 300.0
DELAY_FAR_M = 1000.0
FAR_GUARD_M = 5000.0

# --- GPS y Protección Anti-429 ------------------------------------------------
GPS_FRESH_MIN = 20.0
RADAR_KEEP_STALE_MIN = 45.0
MAX_CONCURRENT_GPS = 2              # Concurrencia estricta para no saturar Kyros (Anti-429)
PACING_S = 1.8                      # Pausa de 1.8 s entre consultas (respeta límite de 30 req/min de Kyros)
HTTP_TIMEOUT_S = 8.0                # Tiempo máximo de espera por respuesta
HTTP_RETRIES = 2                    # Reintentos máximos en timeout
GPS_PHASE_BUDGET_S = 180.0          # Presupuesto total de tiempo para sondeo GPS
BREAKER_LIMIT = 12                  # Fallos consecutivos antes de pausar
COOLDOWN_429_SECONDS = 25.0         # Enfriamiento en memoria si Kyros responde 429

# --- Tiempos de respaldo ------------------------------------------------------
DEFAULT_TRIP_MIN = 60
OFFLINE_DELIVERING_AFTER_MIN = 45
OFFLINE_DELIVERED_AFTER_MIN = 105
NO_STATION_DELIVERED_AFTER_MIN = 60
TRANSIENT_GRACE_MIN = 360
DELAY_FLAG_AFTER_MIN = 20
EXPIRE_ACTIVE_AFTER_H = 72
EXPIRE_PENDING_AFTER_DAYS = 2

# Estados activos monitoreados en cada corrida del scraper
ACTIVE = ("loading", "in_transit", "delivering", "lost")
PAGE_SIZE = 1000
BOLIVIA_BBOX = (-23.5, -9.0, -70.0, -57.0)  # lat_min, lat_max, lng_min, lng_max

# Columnas consultadas de public.dispatches incluyendo nuevos campos temporales
DISPATCH_COLS = (
    "id, station_id, district_id, license_plate, status, volume_liters, has_gps, "
    "dispatched_at, estimated_dispatched_at, travel_duration_minutes, "
    "discharge_duration_minutes, estimated_arrived_at, arrived_at, "
    "estimated_delivered_at, delivered_at, metadata"
)
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SurtiveoProximity/4.0",
    "Accept": "application/json",
}


# =============================================================================
# Utilidades
# =============================================================================
def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def minutes(delta: timedelta) -> float:
    return delta.total_seconds() / 60.0


def to_float(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


_FRAC_RE = re.compile(r"\.(\d+)")


def _fromiso(value: str, naive_tz: timezone) -> datetime | None:
    s = value.strip().replace("Z", "+00:00")
    # Python 3.10 no acepta fracciones de 5 dígitos: normalizar a 6
    s = _FRAC_RE.sub(lambda m: "." + (m.group(1) + "000000")[:6], s, count=1)
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=naive_tz)
    return dt.astimezone(timezone.utc)


def parse_ts(value: Any) -> datetime | None:
    """Timestamps de la BD. Sin zona -> UTC."""
    return _fromiso(str(value), timezone.utc) if value else None


_GPS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M")


def parse_gps_ts(value: Any) -> datetime | None:
    """hora_gps de Kyros (hora local de Bolivia). Devuelve None si no se puede leer: nunca inventa 'ahora'."""
    if not value:
        return None
    s = str(value).strip()
    for fmt in _GPS_FORMATS:
        try:
            return datetime.strptime(s[:19], fmt).replace(tzinfo=BOLIVIA_TZ).astimezone(timezone.utc)
        except ValueError:
            continue
    return _fromiso(s, BOLIVIA_TZ)


def valid_coords(lat: float, lng: float) -> bool:
    lat_min, lat_max, lng_min, lng_max = BOLIVIA_BBOX
    return lat_min <= lat <= lat_max and lng_min <= lng <= lng_max


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(y, x)) + 360) % 360


def station_coords(st: dict | None) -> tuple[float, float] | None:
    if not st:
        return None
    lat, lng = to_float(st.get("latitude")), to_float(st.get("longitude"))
    if lat is None or lng is None or not valid_coords(lat, lng):
        return None
    return lat, lng


def estimate_discharge_minutes(liters: Any) -> int:
    v = to_float(liters) or 0.0
    return 30 if v <= 0 else max(20, min(60, round(v / 500.0)))


# --- Placas y Sanitización ---------------------------------------------------
# Extrae y normaliza la placa vehicular en mayúsculas sin mutaciones que saturen Kyros
def clean_plate(raw: Any) -> str | None:
    if not raw:
        return None
    s = str(raw).strip().upper()
    return s if s else None


def plate_candidates(raw: Any) -> list[str]:
    # Consulta la placa única limpia sin generar múltiples variantes duplicadas
    p = clean_plate(raw)
    return [p] if p else []


def truck_key(raw: Any) -> str | None:
    return clean_plate(raw)


def with_retry(fn: Callable[[], Any], what: str, tries: int = 3) -> Any:
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            if i == tries - 1:
                print(f"  [ERROR] {what}: {e}")
                raise
            time.sleep(1.5 * (i + 1))


# =============================================================================
# GPS Kyros
# =============================================================================
@dataclass
class GpsResult:
    key: str
    status: str  # online | not_found | error
    lat: float = 0.0
    lng: float = 0.0
    speed: float = 0.0
    moving: bool = False
    heading: float | None = None
    recorded_at: datetime | None = None
    plate_used: str | None = None
    detail: str = ""


class Breaker:
    """Si Kyros acumula errores seguidos, se dejan de enviar peticiones en este ciclo."""

    def __init__(self, limit: int = BREAKER_LIMIT):
        self.fails, self.limit = 0, limit

    @property
    def open(self) -> bool:
        return self.fails >= self.limit

    def ok(self) -> None:
        self.fails = 0

    def fail(self) -> None:
        self.fails += 1


def parse_gps_payload(data: Any) -> dict | None:
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        return None
    lat, lng = to_float(data.get("latitud")), to_float(data.get("longitud"))
    if lat is None or lng is None or not valid_coords(lat, lng):
        return None
    speed = max(0.0, to_float(data.get("velocidad")) or 0.0)
    heading = None
    for k in ("rumbo", "heading", "curso"):
        h = to_float(data.get(k))
        if h is not None:
            heading = h % 360
            break
    return {
        "lat": lat, "lng": lng, "speed": speed,
        "moving": bool(data.get("en_movimiento")) or speed > 3.0,
        "heading": heading,
        "recorded_at": parse_gps_ts(data.get("hora_gps")),
    }


async def _query_plate(session: aiohttp.ClientSession, cand: str) -> tuple[str, Any]:
    url = f"{KYROS_GPS_URL}/{quote(cand, safe='')}"
    last = "error"
    for attempt in range(HTTP_RETRIES):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_S)) as resp:
                if resp.status == 200:
                    parsed = parse_gps_payload(await resp.json(content_type=None))
                    return ("online", parsed) if parsed else ("not_found", None)
                if resp.status == 404:
                    return ("not_found", None)
                if resp.status in (429, 503):
                    # Rate limit temporal en la API de Kyros para esta placa específica
                    print(f"  [RATE-LIMIT] Kyros HTTP {resp.status} en placa {cand}. Continuando...")
                    await asyncio.sleep(1.0)
                    return ("error", f"http_{resp.status}")
                last = f"http_{resp.status}"
                if resp.status not in (500, 502, 504):
                    return ("error", last)
                ra = resp.headers.get("Retry-After", "")
                await asyncio.sleep(min(float(ra) if ra.isdigit() else 1.5 * (2 ** attempt), 10.0))
        except (asyncio.TimeoutError, aiohttp.ClientError, ValueError) as e:
            last = type(e).__name__
            await asyncio.sleep(1.0 * (attempt + 1))
    return ("error", last)


# Consulta los datos satelitales de una placa específica
async def fetch_gps(session: aiohttp.ClientSession, breaker: Breaker, key: str, candidates: list[str]) -> GpsResult:
    for cand in candidates:
        if breaker.open:
            return GpsResult(key, "error", detail="circuit_open")
        kind, payload = await _query_plate(session, cand)
        if kind == "online":
            breaker.ok()
            return GpsResult(key, "online", plate_used=cand, **payload)
        if kind == "error":
            # Un 429 de placa específica no rompe el circuito de todo el país
            if payload != "http_429":
                breaker.fail()
            return GpsResult(key, "error", detail=payload)
        breaker.ok()  # 404: Respuesta válida de Kyros indicando que no tiene GPS
    return GpsResult(key, "not_found")


# Consulta satelital secuencial de camiones únicos con pacing suave (Anti-429)
async def fetch_all_gps(want: dict[str, list[str]]) -> dict[str, GpsResult]:
    if not want:
        return {}
    breaker = Breaker()
    connector = aiohttp.TCPConnector(limit_per_host=2)
    out: dict[str, GpsResult] = {}
    items = list(want.items())

    async with aiohttp.ClientSession(connector=connector, headers=HTTP_HEADERS) as session:
        for k, c in items:
            if breaker.open:
                print("  [WARN] Circuit breaker abierto por fallos consecutivos en Kyros.")
                break
            res = await fetch_gps(session, breaker, k, c)
            out[k] = res
            # Pacing de 500 ms entre cada cisterna para proteger Uvicorn
            await asyncio.sleep(PACING_S)

    # Rellenar con error si el breaker omitió placas
    for k, _ in items:
        if k not in out:
            out[k] = GpsResult(k, "error", detail="circuit_breaker_skipped")

    return out


# =============================================================================
# Carga de datos
# =============================================================================
def load_active_dispatches(db: Client) -> list[dict]:
    rows, start = [], 0
    while True:
        res = with_retry(lambda: db.table("dispatches").select(DISPATCH_COLS)
                         .in_("status", list(ACTIVE)).order("id")
                         .range(start, start + PAGE_SIZE - 1).execute(), "cargar despachos")
        batch = res.data or []
        rows += batch
        if len(batch) < PAGE_SIZE:
            return rows
        start += PAGE_SIZE


def load_stations(db: Client, ids: set[int]) -> dict[int, dict]:
    out, ids_l = {}, sorted(ids)
    for i in range(0, len(ids_l), 200):
        chunk = ids_l[i:i + 200]
        res = with_retry(lambda: db.table("stations").select("id, name, latitude, longitude")
                         .in_("id", chunk).execute(), "cargar estaciones")
        out.update({s["id"]: s for s in res.data or []})
    return out


def load_radar(db: Client) -> list[dict]:
    res = with_retry(lambda: db.table("active_cisterns")
                     .select("id, license_plate, dispatch_id, latitude, longitude, heading, recorded_at, driver_user_id")
                     .limit(5000).execute(), "cargar radar")
    return res.data or []


def sweep_expired(db: Client, now: datetime, dry_run: bool) -> int:
    """Activos > 72 h y pendientes > 2 días -> 'cancelled'. Nunca 'delivered' (no debe generar push)."""
    cutoff_active = iso(now - timedelta(hours=EXPIRE_ACTIVE_AFTER_H))
    cutoff_pending = (now.astimezone(BOLIVIA_TZ).date() - timedelta(days=EXPIRE_PENDING_AFTER_DAYS)).isoformat()
    targets: list[tuple[dict, str]] = []
    try:
        # 1. Despachos en ruta o demorados con más de 72h sin concluir
        res = with_retry(lambda: db.table("dispatches").select("id, status, metadata")
                         .in_("status", ["in_transit", "delivering", "lost"]).lt("dispatched_at", cutoff_active)
                         .limit(500).execute(), "barrido activos")
        targets += [(r, "expired_active_72h") for r in res.data or []]
        # 2. Cupos programados sin placa con más de 2 días de antigüedad
        res = with_retry(lambda: db.table("dispatches").select("id, status, metadata")
                         .in_("status", ["scheduled"]).lt("scheduled_date", cutoff_pending)
                         .limit(500).execute(), "barrido pendientes")
        targets += [(r, "expired_pending") for r in res.data or []]
    except Exception:
        return 0

    closed = 0
    for row, reason in targets:
        meta = row["metadata"] if isinstance(row.get("metadata"), dict) else {}
        payload = {"status": "cancelled", "updated_at": iso(now),
                   "metadata": {**meta, "closed_by": reason, "closed_at": iso(now)}}
        if dry_run:
            closed += 1
            continue
        try:
            res = db.table("dispatches").update(payload).eq("id", row["id"]).eq("status", row["status"]).execute()
            closed += 1 if res.data else 0
        except Exception as e:
            print(f"  [ERROR] Barrido #{row['id']}: {e}")
    return closed


# =============================================================================
# Motor de reglas (función pura: testeable sin red ni BD)
# =============================================================================
@dataclass
class Decision:
    new_status: str | None = None
    has_gps: bool | None = None
    dispatched_at: datetime | None = None
    arrived_at: datetime | None = None
    delivered_at: datetime | None = None
    patch: dict[str, Any] = field(default_factory=dict)
    note: str = ""

    @property
    def changed(self) -> bool:
        return bool(
            self.new_status
            or self.has_gps is not None
            or self.dispatched_at
            or self.arrived_at
            or self.delivered_at
            or self.patch
        )


# Registra el arribo a la estación (transición a delivering)
def _arrive(dec: Decision, when: datetime, source: str, reason: str, liters: Any) -> None:
    dec.new_status = "delivering"
    dec.arrived_at = when
    dec.patch.update({
        "arrived_at": iso(when),
        "arrival_source": source,
        "arrival_reason": reason,
        "estimated_discharge_minutes": estimate_discharge_minutes(liters),
        "near_since": None,
        "is_delayed": False,
    })
    dec.note = f"LLEGÓ ({source}): {reason}"


# Registra la entrega final de combustible (transición a delivered)
def _deliver(dec: Decision, arrived: datetime, confidence: str, closed_by: str, reason: str, now: datetime) -> None:
    dec.new_status = "delivered"
    dec.delivered_at = now
    if not dec.arrived_at:
        dec.arrived_at = arrived
    dec.patch.update({
        "delivered_at": iso(now),
        "delivery_confidence": confidence,
        "closed_by": closed_by,
        "delivery_reason": reason,
        "discharge_duration_minutes": round(max(0.0, minutes(now - arrived)), 1),
        "near_since": None,
        "is_delayed": False,
    })
    dec.note = f"ENTREGADO ({confidence}): {reason}"


def _last_position_far(prev: dict | None, st: tuple[float, float], now: datetime) -> bool:
    """Si su última posición conocida y reciente estaba lejos, no se infiere una llegada solo por reloj."""
    if not prev:
        return False
    t = parse_ts(prev.get("recorded_at"))
    lat, lng = to_float(prev.get("latitude")), to_float(prev.get("longitude"))
    if not t or lat is None or lng is None or minutes(now - t) > RADAR_KEEP_STALE_MIN:
        return False
    return haversine_m(lat, lng, st[0], st[1]) > FAR_GUARD_M


# Evalúa el despacho aplicando reglas según su estado y telemetría satelital
def evaluate(d: dict, station: dict | None, gps: GpsResult | None, prev_radar: dict | None,
             now: datetime) -> Decision:
    dec = Decision()
    meta = d["metadata"] if isinstance(d.get("metadata"), dict) else {}
    status, liters = d["status"], d.get("volume_liters")

    dispatched = parse_ts(d.get("dispatched_at"))
    eta = parse_ts(d.get("estimated_arrived_at")) or (
        dispatched + timedelta(minutes=DEFAULT_TRIP_MIN) if dispatched else now)
    overrun = minutes(now - eta)
    st = station_coords(station)

    gps_fresh, gps_time, dist = False, None, None
    if gps and gps.status == "online":
        gps_time = min(gps.recorded_at or now, now)  # reloj del GPS adelantado -> se acota a ahora
        gps_fresh = minutes(now - gps_time) <= GPS_FRESH_MIN
        if gps_fresh and not meta.get("gps_seen_at"):
            dec.patch["gps_seen_at"] = iso(gps_time)
        if gps_fresh and st:
            dist = haversine_m(gps.lat, gps.lng, st[0], st[1])
    gps_known = bool(meta.get("gps_seen_at")) or gps_fresh
    gps_transient = bool(gps and gps.status == "error")

    # ------------------------------------------------------------------ LOADING
    if status == "loading":
        # 1. Detección de salida por GPS satelital fuera de la planta
        if gps_fresh and gps:
            dec.has_gps = True
            plant_meta = CATALOGO_DISTRITOS.get(d.get("district_id"))
            dist_plant = None
            if plant_meta and plant_meta.get("plant_lat") and plant_meta.get("plant_lng"):
                dist_plant = haversine_m(gps.lat, gps.lng, plant_meta["plant_lat"], plant_meta["plant_lng"])

            # Si se alejó > 1.5 km de la planta con velocidad > 12 km/h -> salió a carretera
            if dist_plant and dist_plant > 1500.0 and gps.speed > 12.0:
                dec.new_status = "in_transit"
                dec.dispatched_at = gps_time
                dec.patch["departure_source"] = "gps"
                dec.note = f"SALIÓ DE PLANTA (GPS): a {dist_plant/1000:.1f} km a {gps.speed:.0f} km/h"
                return dec

        # 2. Respaldo por horario estimado si YPFB no reportó hora efectiva en balanza
        est_disp = parse_ts(d.get("estimated_dispatched_at"))
        if est_disp and now >= est_disp:
            dec.new_status = "in_transit"
            dec.dispatched_at = est_disp
            dec.has_gps = True if gps_fresh else False
            dec.patch["departure_source"] = "time_fallback"
            dec.note = f"SALIÓ DE PLANTA (Tiempo): cumplió hora estimada {iso(est_disp)}"
            return dec

        dec.note = "Cargando en planta"
        return dec

    # ------------------------------------------------------------------ IN_TRANSIT
    if status == "in_transit":
        if gps_fresh:
            dec.has_gps = True

        # Regla A: sin coordenadas de destino no hay geocerca posible
        if not st:
            if overrun >= NO_STATION_DELIVERED_AFTER_MIN:
                _deliver(dec, eta, "inferred", "no_station_time_fallback",
                         f"sin coordenadas de estación; ETA+{overrun:.0f} min", now)
            return dec

        # Regla B: GPS satelital fresco en vivo -> Manda la física sobre el reloj
        if dist is not None:
            near_since = parse_ts(meta.get("near_since"))
            # Arribo por geocerca directa (<= 120m y baja velocidad)
            if dist <= GEOFENCE_RADIUS_M and gps.speed <= ARRIVAL_MAX_SPEED_KMH:
                _arrive(dec, gps_time, "gps", f"geocerca {dist:.0f} m a {gps.speed:.0f} km/h", liters)
            # Arribo por zona de permanencia (<= 400m detenido >= 4 min)
            elif dist <= NEAR_RADIUS_M and gps.speed <= NEAR_MAX_SPEED_KMH:
                if near_since and minutes(gps_time - near_since) >= NEAR_DWELL_MIN:
                    _arrive(dec, near_since, "gps",
                            f"detenido a {dist:.0f} m durante {minutes(gps_time - near_since):.0f} min", liters)
                elif not near_since:
                    dec.patch["near_since"] = iso(gps_time)
            else:
                if meta.get("near_since"):
                    dec.patch["near_since"] = None
                # Truth Override: Sigue en carretera, no se inventa entrega
                delayed = overrun >= DELAY_FLAG_AFTER_MIN and dist > DELAY_FAR_M
                if bool(meta.get("is_delayed")) != delayed:
                    dec.patch["is_delayed"] = delayed
                    if delayed:
                        dec.note = f"DEMORADO en carretera a {dist / 1000:.1f} km (ETA+{overrun:.0f} min)"
            return dec

        # Regla C: Error transitorio de Kyros en camión con GPS conocido -> esperar
        if gps_transient and gps_known and overrun < TRANSIENT_GRACE_MIN:
            dec.note = f"Kyros con error ({gps.detail}); se espera satélite"
            return dec

        # Regla D: Despacho Tipo B (sin GPS) -> avanza por cronómetro estimado
        if not d.get("has_gps") and not gps_known:
            if overrun >= 0:
                _arrive(dec, eta, "inferred", f"inferred_time_arrival (ETA+{overrun:.0f} min)", liters)
            return dec

        # Regla E: Camión que tenía GPS pero perdió señal en carretera
        if _last_position_far(prev_radar, st, now):
            # Última posición conocida quedó a > 5 km: camión varado o sin cobertura
            est_deliv = parse_ts(d.get("estimated_delivered_at")) or (eta + timedelta(minutes=60))
            if now > est_deliv + timedelta(hours=3):
                # Pasaron más de 3 horas de la entrega estimada y la señal se perdió en ruta
                dec.new_status = "lost"
                dec.patch["lost_at"] = iso(now)
                dec.patch["lost_reason"] = "signal_lost_far_from_station"
                dec.note = "SEÑAL PERDIDA en carretera a >1km; pasa a estado lost"
                return dec
            dec.note = "GPS sin señal reciente pero última posición lejos de estación; se espera"
            return dec

        # Si no estaba lejos (ej. se apagó corta-corriente a <= 400m de la estación)
        if overrun >= OFFLINE_DELIVERED_AFTER_MIN:
            _deliver(dec, eta, "inferred", "gps_offline_time_fallback",
                     f"sin GPS; ETA+{overrun:.0f} min", now)
        elif overrun >= OFFLINE_DELIVERING_AFTER_MIN:
            _arrive(dec, eta, "inferred", f"sin GPS; ETA+{overrun:.0f} min", liters)
        return dec

    # ------------------------------------------------------------------ DELIVERING
    if status == "delivering":
        if gps_fresh:
            dec.has_gps = True

        arrived = parse_ts(d.get("arrived_at")) or parse_ts(meta.get("arrived_at")) or eta
        # Obtener duración de descarga de la columna o calcularla
        discharge = d.get("discharge_duration_minutes") or estimate_discharge_minutes(liters)
        discharge = max(15, min(90, int(discharge)))
        elapsed = minutes(now - arrived)
        source = meta.get("arrival_source") or "inferred"

        # Fin de entrega por alejamiento físico (> 250 m)
        if dist is not None and dist > DEPARTURE_RADIUS_M:
            _deliver(dec, arrived, "gps", "gps_departure",
                     f"se alejó {dist:.0f} m tras {elapsed:.0f} min", now)
        # Fin de entrega al transcurrir el tiempo físico de vaciado
        elif elapsed >= discharge:
            _deliver(dec, arrived, "gps" if source == "gps" else "inferred", "discharge_time",
                     f"descarga completa {elapsed:.0f}/{discharge} min", now)
        else:
            dec.note = f"Descargando en surtidor ({elapsed:.0f}/{discharge} min)"
        return dec

    # ------------------------------------------------------------------ LOST
    if status == "lost":
        # 1. Recuperación: si el satélite vuelve a transmitir y está en la estación
        if gps_fresh and dist is not None:
            dec.has_gps = True
            if dist <= GEOFENCE_RADIUS_M and gps.speed <= ARRIVAL_MAX_SPEED_KMH:
                _arrive(dec, gps_time, "gps", f"recuperado de lost: arribó a geocerca {dist:.0f} m", liters)
                return dec
            elif dist <= NEAR_RADIUS_M and gps.speed <= NEAR_MAX_SPEED_KMH:
                _arrive(dec, gps_time, "gps", f"recuperado de lost: arribó a estación {dist:.0f} m", liters)
                return dec
            else:
                # Sigue demorado en ruta pero recuperó telemetría -> vuelve a in_transit
                dec.new_status = "in_transit"
                dec.patch["truth_override"] = "true"
                dec.patch["recovered_from_lost"] = iso(now)
                dec.note = f"RECUPERADO DE LOST: satélite activo a {dist/1000:.1f} km; vuelve a in_transit"
                return dec

        # 2. Expiración de camión perdido: si lleva más de 24 horas en lost -> cancelled
        lost_at = parse_ts(meta.get("lost_at")) or (eta + timedelta(hours=3))
        if minutes(now - lost_at) >= 1440:  # 24 horas
            dec.new_status = "cancelled"
            dec.patch["cancelled_at"] = iso(now)
            dec.patch["cancellation_reason"] = "lost_unrecovered_24h"
            dec.note = "CANCELADO: camión perdido sin reaparición en 24 horas"
            return dec

        dec.note = "En estado lost (esperando señal o llegada)"
        return dec

    return dec


# =============================================================================
# Persistencia
# =============================================================================
# Guarda los cambios de estado y fechas en la base de datos Supabase
def persist_decision(db: Client, d: dict, dec: Decision, now: datetime) -> bool:
    meta = d["metadata"] if isinstance(d.get("metadata"), dict) else {}
    payload: dict[str, Any] = {"metadata": {**meta, **dec.patch}, "updated_at": iso(now)}
    # Actualizar estado si cambió
    if dec.new_status:
        payload["status"] = dec.new_status
    # Actualizar bandera has_gps si fue validada
    if dec.has_gps is not None:
        payload["has_gps"] = dec.has_gps
    # Actualizar salida confirmada si fue detectada en loading
    if dec.dispatched_at is not None:
        payload["dispatched_at"] = iso(dec.dispatched_at)
    # Actualizar llegada confirmada
    if dec.arrived_at is not None:
        payload["arrived_at"] = iso(dec.arrived_at)
    # Actualizar entrega confirmada
    if dec.delivered_at is not None:
        payload["delivered_at"] = iso(dec.delivered_at)

    # Bloqueo optimista: si otro proceso cambió el status, no se pisa
    res = with_retry(lambda: db.table("dispatches").update(payload)
                     .eq("id", d["id"]).eq("status", d["status"]).execute(), f"actualizar #{d['id']}")
    return bool(res.data)


def _heading(gps: GpsResult, prev: dict | None) -> float:
    if gps.heading is not None:
        return gps.heading
    if prev:
        plat, plng = to_float(prev.get("latitude")), to_float(prev.get("longitude"))
        if plat is not None and plng is not None and haversine_m(plat, plng, gps.lat, gps.lng) >= 15:
            return bearing_deg(plat, plng, gps.lat, gps.lng)
        return to_float(prev.get("heading")) or 0.0
    return 0.0


def build_radar(dispatches: list[dict], decisions: dict[int, Decision], gps_by_key: dict[str, GpsResult],
                prev_by_key: dict[str, dict], now: datetime) -> tuple[list[dict], set[str]]:
    """Un marcador por CAMIÓN (no por despacho). Prioriza el despacho que está descargando."""
    by_truck: dict[str, list[tuple[dict, str]]] = defaultdict(list)
    for d in dispatches:
        key = truck_key(d.get("license_plate"))
        if not key or not d.get("station_id"):
            continue
        final = decisions.get(d["id"], Decision()).new_status or d["status"]
        # El radar muestra cisternas activas en carretera o descargando
        if final in ("in_transit", "delivering"):
            by_truck[key].append((d, final))

    upserts, keep = [], set()
    for key, items in by_truck.items():
        items.sort(key=lambda it: (0 if it[1] == "delivering" else 1,
                                   parse_ts(it[0].get("estimated_arrived_at")) or now))
        d = items[0][0]
        gps, prev = gps_by_key.get(key), prev_by_key.get(key)
        if gps and gps.status == "online":
            t = min(gps.recorded_at or now, now)
            if minutes(now - t) <= GPS_FRESH_MIN:
                upserts.append({
                    "license_plate": key, "dispatch_id": d["id"], "station_id": d["station_id"],
                    "latitude": gps.lat, "longitude": gps.lng, "speed_kmh": round(gps.speed, 1),
                    "heading": round(_heading(gps, prev), 1), "is_moving": gps.moving,
                    "is_public": True, "recorded_at": iso(t), "updated_at": iso(now),
                })
                keep.add(key)
                continue
        # Sin GPS fresco: se conserva la última posición un rato (la app la muestra en gris)
        prev_t = parse_ts(prev.get("recorded_at")) if prev else None
        if prev_t and minutes(now - prev_t) <= RADAR_KEEP_STALE_MIN:
            keep.add(key)
    return upserts, keep


def reconcile_radar(db: Client, upserts: list[dict], keep: set[str], prev_rows: list[dict],
                    dry_run: bool) -> tuple[int, int]:
    keep_all = keep | {u["license_plate"] for u in upserts}
    delete_ids = []
    for r in prev_rows:
        if r.get("driver_user_id"):  # filas de choferes con app propia: no se tocan
            continue
        plate = r.get("license_plate")
        key = truck_key(plate)
        if key not in keep_all or plate != key:  # también limpia filas con formato de placa viejo
            delete_ids.append(r["id"])
    if dry_run:
        return len(upserts), len(delete_ids)
    if upserts:
        try:
            with_retry(lambda: db.table("active_cisterns").upsert(upserts, on_conflict="license_plate").execute(),
                       "upsert radar")
        except Exception:
            pass
    for i in range(0, len(delete_ids), 200):
        chunk = delete_ids[i:i + 200]
        try:
            with_retry(lambda: db.table("active_cisterns").delete().in_("id", chunk).execute(), "limpieza radar")
        except Exception:
            pass
    return len(upserts), len(delete_ids)


# =============================================================================
# Orquestador
# =============================================================================
async def run_async(db: Client, dry_run: bool = False, verbose: bool = False) -> dict:
    t0 = time.time()
    now = datetime.now(timezone.utc)
    print("\n" + "=" * 78)
    print(f"🛰️  PROXIMIDAD DE CISTERNAS v4 | {now.isoformat()[:19]} UTC" + (" | DRY-RUN" if dry_run else ""))
    print("=" * 78)

    print("[0/5] Barrido de despachos expirados...")
    expired = sweep_expired(db, now, dry_run)
    print(f"  -> {expired} cancelados por expiración.")

    print("[1/5] Cargando despachos activos (con y sin estación)...")
    dispatches = load_active_dispatches(db)
    print(f"  -> {len(dispatches)} activos.")

    print("[2/5] Cargando estaciones y radar previo...")
    stations = load_stations(db, {d["station_id"] for d in dispatches if d.get("station_id")})
    prev_rows = load_radar(db)
    prev_by_key = {k: r for r in prev_rows if (k := truck_key(r.get("license_plate")))}

    want: dict[str, list[str]] = {}
    for d in dispatches:
        key = truck_key(d.get("license_plate"))
        # Sondea GPS para todo camión activo con placa válida
        if key:
            want[key] = plate_candidates(d.get("license_plate"))

    print(f"[3/5] Consultando GPS de {len(want)} camiones únicos...")
    gps_by_key = await fetch_all_gps(want)
    gstats = Counter()
    for g in gps_by_key.values():
        if g.status == "online":
            fresh = minutes(now - min(g.recorded_at or now, now)) <= GPS_FRESH_MIN
            gstats["online_fresco" if fresh else "online_desactualizado"] += 1
        else:
            gstats[g.status] += 1
    print(f"  -> {dict(gstats)}")
    if gstats["error"] > max(5, len(want) // 2):
        print("  [WARN] Kyros con fallas masivas: los camiones con GPS conocido no se cerrarán por tiempo.")

    print("[4/5] Evaluando reglas...")
    decisions: dict[int, Decision] = {}
    transitions = Counter()

    # Identificar despachos viejos superados (la misma placa ya salió en un viaje posterior)
    by_truck: dict[str, list[dict]] = defaultdict(list)
    for d in dispatches:
        k = truck_key(d.get("license_plate"))
        if k and d.get("dispatched_at"):
            by_truck[k].append(d)

    superseded_ids = set()
    for k, d_list in by_truck.items():
        if len(d_list) > 1:
            # Ordenar por salida más reciente primero
            d_list.sort(key=lambda x: parse_ts(x.get("dispatched_at")) or now, reverse=True)
            latest_time = parse_ts(d_list[0].get("dispatched_at"))
            for old_d in d_list[1:]:
                old_time = parse_ts(old_d.get("dispatched_at"))
                # Si salió con más de 90 min de diferencia respecto al viaje más nuevo,
                # físicamente ya completó y vació el tanque del viaje anterior.
                if old_time and latest_time and minutes(latest_time - old_time) > 90.0:
                    superseded_ids.add(old_d["id"])

    for d in dispatches:
        # Si el camión ya tiene un viaje posterior, cerramos este viaje viejo directamente
        if d["id"] in superseded_ids:
            dec = Decision()
            eta_val = parse_ts(d.get("estimated_arrived_at")) or now
            _deliver(dec, eta_val, "inferred", "superseded_by_newer_dispatch",
                     "viaje previo de la cisterna; ya inició un despacho posterior", now)
            decisions[d["id"]] = dec
            transitions["delivered"] += 1
            st_name = (stations.get(d.get("station_id")) or {}).get("name") or "sin estación"
            print(f"  • #{d['id']} [{d.get('license_plate') or 'S/P'}] -> {st_name}: {dec.note}")
            continue

        key = truck_key(d.get("license_plate"))
        try:
            dec = evaluate(d, stations.get(d.get("station_id")),
                           gps_by_key.get(key) if key else None,
                           prev_by_key.get(key) if key else None, now)
        except Exception as e:
            print(f"  [ERROR] Evaluando #{d.get('id')}: {e}")
            dec = Decision()
        decisions[d["id"]] = dec
        if dec.new_status:
            transitions[dec.new_status] += 1
        if dec.note and (dec.new_status or verbose or dec.note.startswith("DEMORADO")):
            st_name = (stations.get(d.get("station_id")) or {}).get("name") or "sin estación"
            print(f"  • #{d['id']} [{d.get('license_plate') or 'S/P'}] -> {st_name}: {dec.note}")

    print("[5/5] Persistiendo...")
    written = conflicts = failed = 0
    if not dry_run:
        for d in dispatches:
            dec = decisions[d["id"]]
            if not dec.changed:
                continue
            try:
                if persist_decision(db, d, dec, now):
                    written += 1
                else:
                    conflicts += 1
            except Exception:
                failed += 1
    upserts, keep = build_radar(dispatches, decisions, gps_by_key, prev_by_key, now)
    radar_up, radar_del = reconcile_radar(db, upserts, keep, prev_rows, dry_run)

    elapsed = time.time() - t0
    summary = {
        "expired": expired, "active": len(dispatches), "gps": dict(gstats),
        "to_in_transit": transitions["in_transit"],
        "to_delivering": transitions["delivering"],
        "to_delivered": transitions["delivered"],
        "to_lost": transitions["lost"],
        "written": written, "conflicts": conflicts, "failed": failed,
        "radar_upserted": radar_up, "radar_deleted": radar_del, "elapsed_s": round(elapsed, 2),
    }
    print(f"  -> {summary}")
    print("=" * 78)
    return summary


def check_proximity(db: Client | None = None, dry_run: bool = False, verbose: bool = False) -> dict:
    if db is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("Variables V3 y V4 requeridas.")
        db = create_client(SUPABASE_URL, SUPABASE_KEY)
    return asyncio.run(run_async(db, dry_run=dry_run, verbose=verbose))


def main() -> None:
    p = argparse.ArgumentParser(description="Proximidad y geofencing de cisternas (Surtiveo v4)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--loop", action="store_true")
    p.add_argument("--interval", type=int, default=300)
    args = p.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("[FATAL] Variables V3 y V4 requeridas.")
        sys.exit(1)
    db = create_client(SUPABASE_URL, SUPABASE_KEY)

    if args.loop:
        while True:
            try:
                check_proximity(db, args.dry_run, args.verbose)
            except Exception as e:
                print(f"[ERROR EN BUCLE] {e}")
            time.sleep(args.interval)
    else:
        try:
            check_proximity(db, args.dry_run, args.verbose)
        except Exception as e:
            print(f"[FATAL] {e}")
            sys.exit(1)  # el Action queda en rojo y te enteras


if __name__ == "__main__":
    main()