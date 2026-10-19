"""Validadores y normalizadores tolerantes a fallos para la capa Silver."""

from __future__ import annotations

from etl.validators.common import NUMERIC_PLACEHOLDERS, PLACEHOLDERS, is_placeholder
from etl.validators.fechas import (
    MAX_DAYS,
    MONTHS,
    parse_date_dmy,
    parse_day_month_es,
)
from etl.validators.numeros import parse_non_negative_bigint
from etl.validators.personas import (
    PARTICLES,
    calculate_rut_dv,
    normalize_email,
    normalize_name,
    normalize_rut,
    split_rut,
    validate_rut_dv,
)

__all__ = [
    "MAX_DAYS",
    "MONTHS",
    "NUMERIC_PLACEHOLDERS",
    "PARTICLES",
    "PLACEHOLDERS",
    "calculate_rut_dv",
    "is_placeholder",
    "normalize_email",
    "normalize_name",
    "normalize_rut",
    "parse_date_dmy",
    "parse_day_month_es",
    "parse_non_negative_bigint",
    "split_rut",
    "validate_rut_dv",
]
