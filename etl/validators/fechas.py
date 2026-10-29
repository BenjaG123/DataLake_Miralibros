"""Validadores de fechas y meses en español."""

from __future__ import annotations

import re
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

# Acepta tanto el formato humano "14 de junio" como el histórico "14 - junio".
# El separador "de" exige espacios para no admitir textos pegados ("14dejunio");
# el guión tolera espacios a ambos lados ("2-Agosto", "2 -Agosto", "2- agosto").
_DAY_MONTH_PATTERN = re.compile(
    r"^(?P<day>\d{1,2})(?:\s+de\s+|\s*-\s*)(?P<month>[a-z]+)$"
)

# Puntuación final tolerada ("14 de junio.", "14 de junio,").
_TRAILING_PUNCTUATION = ".,;:"


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


def parse_date_iso(
    value: Any,
    *,
    min_date: date | None = None,
    max_date: date | None = None,
) -> date | None:
    """Convierte una fecha ISO estricta ``YYYY-MM-DD``; retorna ``None`` si falla.

    A diferencia de :func:`parse_date_dmy`, no acepta separadores ``/`` ni
    componentes sin cero a la izquierda. La expresión regular se evalúa antes
    de ``strptime`` para que variantes como ``2024-1-2`` no pasen.
    """
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return None

    try:
        parsed = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
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


def _normalize_month_text(value: str) -> str:
    """Quita acentos, convierte espacios duros en normales y pasa a minúsculas."""
    return _strip_accents(value.replace("\u00a0", " ")).casefold()


def parse_day_month_es(value: Any) -> tuple[int | None, int | None]:
    """Convierte "D de Mes" o "D - Mes" a (día, mes); (None, None) si es inválido.

    Acepta el formato humano sin año ("14 de junio", "2-Agosto") y mantiene la
    compatibilidad con el histórico separado por guión ("14 - junio"). Tolera
    mayúsculas, acentos, espacios repetidos, nbsp y puntuación final. No acepta
    años ("14 de junio de 1990") ni fechas numéricas ("1990-06-14", "14/06"):
    el año lo estima el pipeline de clientes a partir del RUT.
    """
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None, None

    normalized = (
        _normalize_month_text(raw).strip().rstrip(_TRAILING_PUNCTUATION).strip()
    )
    match = _DAY_MONTH_PATTERN.fullmatch(normalized)
    if match is None:
        return None, None

    day = int(match.group("day"))
    month = MONTHS.get(match.group("month"))

    if month is None or day < 1 or day > MAX_DAYS[month]:
        return None, None

    return day, month
