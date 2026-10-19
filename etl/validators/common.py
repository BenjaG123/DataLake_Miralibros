"""Constantes y helpers compartidos por los validadores."""

from __future__ import annotations

from typing import Any

PLACEHOLDERS = frozenset(
    {
        "",
        "0",
        "null",
        "undefined",
        "none",
        "n/a",
        "n.a.",
        "na",
        "-",
        "--",
        "---",
        "sin nombre",
        "sin dato",
        "sin informacion",
        "sin información",
        "no informado",
        "no informa",
        "no registra",
        "no aplica",
    }
)

NUMERIC_PLACEHOLDERS = frozenset(
    {
        "",
        "null",
        "undefined",
        "none",
        "nan",
        "n/a",
        "n.a.",
        "na",
        "-",
        "--",
        "---",
        "sin dato",
        "sin informacion",
        "sin información",
        "no informado",
        "no informa",
        "no registra",
    }
)


def is_placeholder(value: Any) -> bool:
    """Indica si un valor es vacío o un texto usado como dato faltante."""
    if value is None:
        return True
    normalized = str(value).strip().casefold()
    return normalized in PLACEHOLDERS
