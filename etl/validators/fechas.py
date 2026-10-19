"""Validadores de fechas y meses en español."""

from __future__ import annotations

import unicodedata
from datetime import date, datetime
from typing import Any

from etl.validators.common import is_placeholder

MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}

MAX_DAYS = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}


def parse_date_dmy(
    value: Any,
    *,
    min_date: date | None = None,
    max_date: date | None = None,
) -> date | None:
    """Convierte una fecha d/m/yyyy o dd-mm-yyyy; retorna None si es inválida."""
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None

    parsed: date | None = None
    for date_format in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            parsed = datetime.strptime(raw, date_format).date()
            break
        except ValueError:
            continue

    if parsed is None:
        return None
    if min_date is not None and parsed < min_date:
        return None
    if max_date is not None and parsed > max_date:
        return None

    return parsed


def _strip_accents(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )


def parse_day_month_es(value: Any) -> tuple[int | None, int | None]:
    """Convierte D - Mes a (día, mes); retorna (None, None) si es inválido."""
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None, None

    parts = raw.split("-")
    if len(parts) != 2:
        return None, None

    day_text, month_text = parts[0].strip(), parts[1].strip()
    if not day_text.isdigit() or not month_text:
        return None, None

    day = int(day_text)
    month_name = _strip_accents(month_text).casefold()
    month = MONTHS.get(month_name)

    if month is None or day < 1 or day > MAX_DAYS[month]:
        return None, None

    return day, month
