"""Validadores numéricos."""

from __future__ import annotations

from typing import Any

from etl.validators.common import NUMERIC_PLACEHOLDERS


def parse_non_negative_bigint(
    value: Any,
    *,
    default: int | None = 0,
) -> int | None:
    """Convierte a entero no negativo; retorna default si es inválido."""
    raw = "" if value is None else str(value).strip()
    if not raw or raw.casefold() in NUMERIC_PLACEHOLDERS:
        return default

    cleaned = raw.replace(".", "").replace(",", "")
    if not cleaned.isdigit():
        return default

    return int(cleaned)
