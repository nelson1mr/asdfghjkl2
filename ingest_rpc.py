"""CLIENTE DE LA RPC api.ingest_official_reports (contrato v1, SUR-32 / SUR-79)
=============================================================================
Los scrapers mandan lo que leen y el servidor mapea la estación, deduplica e inserta.
El contrato es contracts/ingest/v1.schema.json (copia del monorepo de Surtiveo).

Reglas del contrato que se respetan aquí:
  - una fuente por llamada y un máximo de 1000 items (por defecto bloques de 300);
  - los items de una misma estación van en el mismo bloque;
  - run_id nuevo por bloque; se reintenta con el MISMO run_id (la RPC es idempotente);
  - source con el texto exacto de siempre.

Este módulo no lee variables con secretos ni usa red por sí mismo: recibe el cliente `db`.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import os
import time
import uuid

CONTRACT_VERSION = 1
MAX_ITEMS = 1000
DEFAULT_BATCH_SIZE = 300
RPC_NAME = "ingest_official_reports"

COUNTER_KEYS = (
    "received", "inserted", "heartbeats", "duplicates", "duplicates_of_last",
    "duplicates_in_batch", "stale", "superseded", "unknown", "invalid",
)


def get_batch_size() -> int:
    """Tamaño de bloque: INGEST_BATCH_SIZE (1..1000); si falta o es inválido, 300."""
    raw = (os.getenv("INGEST_BATCH_SIZE") or "").strip()
    try:
        size = int(raw)
    except ValueError:
        return DEFAULT_BATCH_SIZE
    return max(1, min(size, MAX_ITEMS))


def to_rfc3339(dt: datetime) -> str:
    """RFC 3339 con zona (UTC). Un datetime sin zona se rechaza: dependería del reloj local."""
    if dt.tzinfo is None:
        raise ValueError("observed_at necesita zona horaria")
    return dt.astimezone(timezone.utc).isoformat()


# =============================================================================
#  CONSTRUCCIÓN DE ITEMS
# =============================================================================

def _clean_liters(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        liters = float(value)
    except (TypeError, ValueError):
        return None
    return liters if math.isfinite(liters) and liters >= 0 else None


def build_chain_items(records: list[dict], source: str) -> tuple[list[dict], int]:
    """
    Convierte los registros de una cadena (formato legacy) a items del contrato.
    Manda station_id y NO manda reported_at ni liters_reported_at (el servidor usa observed_at).
    Devuelve (items, rechazados). Un registro con otro `source` o sin ids válidos no se envía.
    """
    items: list[dict] = []
    rejected = 0
    for rec in records:
        station_id = rec.get("station_id")
        fuel_type_id = rec.get("fuel_type_id")
        condition = rec.get("official_condition")
        valid = (
            rec.get("source") == source
            and isinstance(station_id, int) and not isinstance(station_id, bool)
            and isinstance(fuel_type_id, int) and not isinstance(fuel_type_id, bool)
            and condition in ("available", "unavailable")
        )
        if not valid:
            rejected += 1
            continue
        item = {"station_id": station_id, "fuel_type_id": fuel_type_id, "condition": condition}
        liters = _clean_liters(rec.get("available_liters"))
        if liters is not None:
            item["available_liters"] = liters
        items.append(item)
    return items, rejected


def _ref(item: dict) -> tuple:
    if "anh_id" in item:
        return (0, item["anh_id"])
    return (1, item.get("station_id", 0))


def chunk_items(items: list[dict], size: int) -> list[list[dict]]:
    """
    Parte los items en bloques de `size` como máximo sin separar las filas de la misma
    estación (anh_id o station_id). Orden estable por referencia.
    """
    size = max(1, min(size, MAX_ITEMS))
    groups: dict[tuple, list[dict]] = {}
    for item in items:
        groups.setdefault(_ref(item), []).append(item)

    chunks: list[list[dict]] = []
    current: list[dict] = []
    for key in sorted(groups):
        group = groups[key]
        if current and len(current) + len(group) > size:
            chunks.append(current)
            current = []
        current.extend(group)
    if current:
        chunks.append(current)
    return chunks


def build_payload(source: str, observed_at: datetime, items: list[dict], run_id: str | None = None) -> dict:
    return {
        "contract_version": CONTRACT_VERSION,
        "source": source,
        "observed_at": to_rfc3339(observed_at),
        "run_id": run_id or str(uuid.uuid4()),
        "items": items,
    }


# =============================================================================
#  LLAMADA A LA RPC
# =============================================================================

def _call_rpc(db, payload: dict) -> dict:
    response = db.schema("api").rpc(RPC_NAME, {"payload": payload}).execute()
    data = response.data
    if isinstance(data, str):
        data = json.loads(data)
    if isinstance(data, list) and len(data) == 1:
        data = data[0]
    if not isinstance(data, dict):
        raise ValueError(f"respuesta inesperada de la RPC: {type(data).__name__}")
    return data


def _call_with_retry(db, payload: dict, label: str) -> dict:
    """Un solo reintento con el mismo run_id: si la primera llegó, el servidor devuelve lo guardado."""
    try:
        return _call_rpc(db, payload)
    except Exception as first:
        print(f"    [RPC] {label}: falló ({first}); se reintenta una vez con el mismo run_id.")
        time.sleep(1.0)
        return _call_rpc(db, payload)


def _merge_unknown(acc: dict, entries: list[dict]) -> None:
    for entry in entries or []:
        key = (entry.get("anh_id"), entry.get("station_id"), entry.get("reason"))
        if key in acc:
            acc[key]["items"] = acc[key].get("items", 0) + entry.get("items", 0)
        else:
            acc[key] = dict(entry)


def summary_line(source: str, summary: dict) -> str:
    counters = " ".join(f"{k}={summary[k]}" for k in COUNTER_KEYS)
    return (
        f"[INGEST] {source}: bloques={summary['chunks']} fallidos={summary['failed_chunks']} "
        f"enviados={summary['sent']} {counters}"
    )


def ingest(db, source: str, items: list[dict], observed_at: datetime,
           batch_size: int | None = None, dry_run: bool = False) -> dict:
    """
    Envía los items de UNA fuente a api.ingest_official_reports en bloques.
    Nunca lanza: un bloque que falla se registra (::warning::) y se sigue con el resto.
    Devuelve el resumen con los contadores sumados de las respuestas.
    """
    size = batch_size or get_batch_size()
    chunks = chunk_items(items, size)
    summary: dict = {k: 0 for k in COUNTER_KEYS}
    summary.update({
        "source": source, "chunks": len(chunks), "failed_chunks": 0, "replayed": 0,
        "sent": len(items), "dry_run": dry_run, "errors": [],
        "unknown_stations": [], "invalid_items": [],
    })
    unknown_acc: dict = {}

    for n, chunk in enumerate(chunks, start=1):
        payload = build_payload(source, observed_at, chunk)
        label = f"{source} bloque {n}/{len(chunks)} ({len(chunk)} items)"
        if dry_run:
            print(f"    [DRY-RUN] {label}: no se llama a la RPC.")
            continue
        t0 = time.time()
        try:
            result = _call_with_retry(db, payload, label)
        except Exception as e:
            summary["failed_chunks"] += 1
            summary["errors"].append(f"{label}: {e}")
            print(f"::warning::SUR-79 {label} falló en la RPC: {e}")
            continue

        for key in COUNTER_KEYS:
            summary[key] += int(result.get(key) or 0)
        if result.get("replayed"):
            summary["replayed"] += 1
        _merge_unknown(unknown_acc, result.get("unknown_stations"))
        for entry in result.get("invalid_items") or []:
            summary["invalid_items"].append({"chunk": n, **entry})
        print(f"    [RPC] {label} en {time.time() - t0:.2f}s: "
              f"inserted={result.get('inserted')} duplicates={result.get('duplicates')} "
              f"superseded={result.get('superseded')} unknown={result.get('unknown')} "
              f"invalid={result.get('invalid')}")

    summary["unknown_stations"] = list(unknown_acc.values())
    print("  " + summary_line(source, summary))
    if summary["unknown_stations"]:
        shown = ", ".join(
            f"{'anh_id' if 'anh_id' in u else 'station_id'}={u.get('anh_id', u.get('station_id'))}"
            f"({u.get('reason')}, {u.get('items')} items)"
            for u in summary["unknown_stations"][:20]
        )
        print(f"::warning::SUR-79 {source}: estaciones desconocidas para el servidor: {shown}")
    if summary["invalid"]:
        print(f"::warning::SUR-79 {source}: {summary['invalid']} items inválidos: "
              f"{summary['invalid_items'][:10]}")
    return summary
