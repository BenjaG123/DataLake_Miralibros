"""Validadores específicos de datos climáticos."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from etl.validators.common import NUMERIC_PLACEHOLDERS
from etl.validators.numeros import parse_decimal, parse_integer

WIND_DIRECTIONS = frozenset(
    {
        "N",
        "NE",
        "E",
        "SE",
        "S",
        "SW",
        "W",
        "NW",
        "VRB",
    }
)


def parse_precipitation(value: Any) -> Decimal | None:
    """Convierte una precipitación; interpreta ``s/p`` como 0.0 mm."""
    raw = "" if value is None else str(value).strip()
    if raw.casefold() == "s/p":
        return Decimal("0.0")

    return parse_decimal(raw, minimum=Decimal("0"))


def parse_wind(value: Any) -> tuple[str | None, int | None]:
    """Separa una observación de viento en dirección e intensidad en kt."""
    raw = "" if value is None else str(value).strip()
    if not raw or raw.casefold() in NUMERIC_PLACEHOLDERS:
        return None, None

    if raw.casefold() == "calma":
        return "CALMA", 0

    direction, separator, speed_text = raw.partition("/")
    if not separator:
        return None, None

    direction = direction.strip().upper()
    if direction not in WIND_DIRECTIONS:
        return None, None

    speed = parse_integer(speed_text, minimum=0)
    if speed is None:
        return None, None

    return direction, speed
