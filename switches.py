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


_ON_VALUES = {"true", "on", "1", "yes", "enabled"}


def is_opted_in(name: str) -> bool:
    """Switch de adhesión: APAGADO por defecto. Solo un valor explícito de encendido lo activa
    (vacío, ausente o cualquier otro texto = apagado)."""
    raw = os.getenv(name)
    if raw is None:
        return False
    return raw.strip().lower() in _ON_VALUES


def describe_opt_in(names: list[str]) -> str:
    return " | ".join(f"{n}={'ON' if is_opted_in(n) else 'OFF'}" for n in names)
