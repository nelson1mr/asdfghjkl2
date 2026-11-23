import os

_OFF_VALUES = {"false", "off", "0", "no", "disabled"}


def is_enabled(name: str, default: bool = True) -> bool:
    """Devuelve False solo si la variable existe y tiene un valor de apagado."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in _OFF_VALUES


def describe(names: list[str]) -> str:
    """Resumen legible del estado de varios switches para los logs."""
    return " | ".join(f"{n}={'ON' if is_enabled(n) else 'OFF'}" for n in names)
