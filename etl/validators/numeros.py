"""Validadores numéricos."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
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


def parse_decimal(
    value: Any,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal | None:
    """Convierte un decimal; retorna None si está vacío o es inválido."""
    raw = "" if value is None else str(value).strip()
    if not raw or raw.casefold() in NUMERIC_PLACEHOLDERS:
        return None

    try:
        parsed = Decimal(raw.replace(",", "."))
    except InvalidOperation:
        return None

    if not parsed.is_finite():
        return None
    if minimum is not None and parsed < minimum:
        return None
    if maximum is not None and parsed > maximum:
        return None

    return parsed


def parse_integer(
    value: Any,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    """Convierte un entero exacto; retorna None si no es integral o es inválido."""
    parsed = parse_decimal(value)
    if parsed is None or parsed != parsed.to_integral_value():
        return None

    result = int(parsed)
    if minimum is not None and result < minimum:
        return None
    if maximum is not None and result > maximum:
        return None

    return result
