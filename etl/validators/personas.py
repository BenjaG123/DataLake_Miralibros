"""Validadores de RUT, nombres y emails."""

from __future__ import annotations

from typing import Any

from email_validator import EmailNotValidError, validate_email

from etl.validators.common import is_placeholder

PARTICLES = frozenset(
    {
        "de",
        "del",
        "la",
        "las",
        "los",
        "y",
        "e",
        "da",
        "das",
        "do",
        "dos",
        "van",
        "von",
        "der",
        "den",
        "ter",
        "ten",
    }
)

_CORRUPT_EDGES = "|^~*_=+[]{}()<>/\\\"“”,;:'.-"


def calculate_rut_dv(body: str) -> str:
    """Calcula el dígito verificador de un cuerpo de RUT chileno."""
    normalized_body = str(body).strip()
    if not normalized_body.isdigit() or not 1 <= len(normalized_body) <= 8:
        raise ValueError(f"Cuerpo de RUT inválido: {body!r}")

    factors = (2, 3, 4, 5, 6, 7)
    total = sum(
        int(digit) * factors[index % len(factors)]
        for index, digit in enumerate(reversed(normalized_body))
    )
    remainder = 11 - (total % 11)

    if remainder == 11:
        return "0"
    if remainder == 10:
        return "K"
    return str(remainder)


def validate_rut_dv(body: str, dv: str) -> bool:
    """Comprueba si el dígito verificador corresponde al cuerpo indicado."""
    try:
        expected = calculate_rut_dv(body)
    except ValueError:
        return False
    return str(dv).strip().upper() == expected


def normalize_rut(value: Any) -> str | None:
    """Normaliza un RUT; retorna None si está vacío o es inválido."""
    raw = "" if value is None else str(value).strip().upper()
    if not raw:
        return None

    cleaned = raw.replace(".", "").replace(" ", "").replace("\u00a0", "")

    if cleaned.count("-") == 1:
        body, dv = cleaned.split("-", 1)
    elif "-" not in cleaned and len(cleaned) > 1:
        body, dv = cleaned[:-1], cleaned[-1]
    else:
        return None

    if not body.isdigit() or not 7 <= len(body) <= 8:
        return None
    if len(dv) != 1 or dv not in "0123456789K":
        return None
    if dv != calculate_rut_dv(body):
        return None

    return f"{body}-{dv}"


def split_rut(value: Any) -> tuple[str | None, str | None]:
    """Devuelve cuerpo y DV de un RUT; (None, None) si es inválido."""
    normalized = normalize_rut(value)
    if normalized is None:
        return None, None
    body, dv = normalized.split("-", 1)
    return body, dv


def _title_token(token: str, index: int) -> str:
    if index > 0 and token.casefold() in PARTICLES:
        return token.casefold()
    return token.title()


def _clean_name_token(token: str) -> str | None:
    if is_placeholder(token):
        return None

    cleaned = token.replace("´", "").replace("`", "").strip(_CORRUPT_EDGES)
    if any(character.isdigit() for character in cleaned):
        return None
    if not any(character.isalpha() for character in cleaned):
        return None

    return cleaned


def normalize_name(value: Any) -> str | None:
    """Filtra términos inválidos y capitaliza los nombres válidos."""
    text = " ".join(str(value).split()) if value is not None else ""
    if not text or is_placeholder(text):
        return None

    tokens = [
        cleaned
        for token in text.split(" ")
        if (cleaned := _clean_name_token(token)) is not None
    ]
    if not tokens:
        return None

    cleaned_name = " ".join(
        _title_token(token, index) for index, token in enumerate(tokens)
    )
    if not any(character.isalpha() for character in cleaned_name):
        return None

    return cleaned_name


def normalize_email(value: Any) -> str | None:
    """Normaliza un email; retorna None si no es válido."""
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None

    try:
        validated = validate_email(raw.lower(), check_deliverability=False)
    except EmailNotValidError:
        return None

    return validated.normalized.lower()
