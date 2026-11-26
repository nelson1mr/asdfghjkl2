"""Configuración común. Ninguna prueba puede tocar la red ni producción:
- V0/V3/V4 ficticios ANTES de importar los módulos (load_dotenv no pisa lo ya definido);
- se bloquean los sockets;
- se limpian los switches del entorno."""
import json
import os
import socket
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"

os.environ["V0"] = "http://anh.invalid/{dep}/{prod}"
os.environ["V3"] = "http://supabase.invalid"
os.environ["V4"] = "clave-ficticia-de-pruebas"
os.environ["gx"] = "http://genex.invalid"
os.environ["bp"] = "http://bp.invalid"
os.environ["gg"] = "http://gg.invalid"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pytest  # noqa: E402

SWITCH_VARS = [
    "SCRAPE_ANH", "SCRAPE_GENEX", "SCRAPE_BIOPETROL", "SCRAPE_GASGROUP", "SCRAPE_YPFB",
    "INGEST_RPC_ANH", "INGEST_RPC_GENEX", "INGEST_RPC_BIOPETROL", "INGEST_RPC_GASGROUP",
    "INGEST_DRY_RUN", "INGEST_BATCH_SIZE",
]


@pytest.fixture(autouse=True)
def _aislamiento(monkeypatch):
    for name in SWITCH_VARS:
        monkeypatch.delenv(name, raising=False)

    original_connect = socket.socket.connect

    def _bloqueado(self, address, *args, **kwargs):
        # El loop de asyncio en Windows usa un socketpair local: solo se permite loopback
        host = address[0] if isinstance(address, tuple) else address
        if host in ("127.0.0.1", "::1", "localhost"):
            return original_connect(self, address, *args, **kwargs)
        raise RuntimeError(f"Las pruebas no pueden abrir conexiones de red ({host})")

    monkeypatch.setattr(socket.socket, "connect", _bloqueado)
    monkeypatch.setattr("time.sleep", lambda *_: None)


def load_fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


class FakeDb:
    """Cliente falso: registra cada llamada. rpc_handler(payload) -> dict de respuesta."""

    def __init__(self, rpc_handler=None):
        self.calls = []
        self.rpc_payloads = []
        self.inserted = []
        self.rpc_handler = rpc_handler or (lambda payload: {
            "received": len(payload["items"]), "inserted": len(payload["items"]),
            "unknown_stations": [], "invalid_items": [], "replayed": False,
        })
        self._schema = None
        self._pending = None

    def schema(self, name):
        self._schema = name
        return self

    def rpc(self, name, params=None):
        self.calls.append(("rpc", name, self._schema))
        self._pending = ("rpc", name, params)
        return self

    def table(self, name):
        self.calls.append(("table", name))
        self._pending = ("table", name, None)
        return self

    def insert(self, rows):
        self.inserted.append((self._pending[1], rows))
        return self

    def execute(self):
        kind, name, params = self._pending
        result = type("Resp", (), {"data": None})()
        if kind == "rpc" and name == "ingest_official_reports":
            self.rpc_payloads.append(params["payload"])
            result.data = self.rpc_handler(params["payload"])
        return result
