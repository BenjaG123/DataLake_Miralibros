"""Pipeline de normalización y carga de clientes hacia la capa Silver."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd

from etl import db
from etl.config import BRONZE_DIR, SQL_DIR, configure_logging, get_schema
from etl.readers import read_excel_as_str, require_columns
from etl.validators import (
    is_placeholder,
    normalize_email,
    normalize_name,
    normalize_rut,
    parse_date_dmy,
    parse_day_month_es,
    parse_non_negative_bigint,
)

logger = logging.getLogger(__name__)

INPUT_PATH = BRONZE_DIR / "CLIENTES LIBRERIA.xlsx"
SQL_FILE = SQL_DIR / "clientes.sql"
TABLE = "clientes"

SOURCE_COLUMNS = (
    "Rut cliente",
    "Nombre",
    "Email",
    "Alta",
    "Canal",
    "Cumple.",
    "Puntos",
    "Compras",
    "Canjes",
)

COLUMNS = (
    "rut",
    "nombre",
    "email",
    "alta",
    "canal",
    "cumple",
    "puntos",
    "compras",
    "canjes",
)

Row = tuple[str, str, str, date, str, date, int, int, int]

CANAL_MAP = {
    "local": "Local",
    "web": "Web",
}

EDAD_POR_TRAMO = (
    (26, 10),
    (24, 14),
    (22, 18),
    (20, 24),
    (19, 29),
    (17, 38),
    (15, 52),
    (13, 62),
    (10, 68),
    (7, 73),
    (4, 80),
    (0, 88),
)

REFERENCE_YEAR = date.today().year
MIN_ALTA_DATE = date(2000, 1, 1)


def normalize_canal(value: Any) -> str | None:
    """Normaliza el canal a Local o Web."""
    raw = "" if value is None else str(value).strip().casefold()
    if not raw or is_placeholder(raw):
        return None
    return CANAL_MAP.get(raw)


def estimar_anio(rut: str, *, reference_year: int = REFERENCE_YEAR) -> int | None:
    """Estima el año de nacimiento a partir de los millones del RUT."""
    normalized = normalize_rut(rut)
    if normalized is None:
        return None

    body = normalized.split("-", 1)[0]
    millones = int(body[:3]) / (10 ** (9 - len(body)))

    for minimo, edad in EDAD_POR_TRAMO:
        if millones >= minimo:
            return reference_year - edad

    return None


def _build_cumple_date(year: int, month: int, day: int) -> date | None:
    try:
        return date(year, month, day)
    except ValueError:
        if month == 2 and day == 29:
            return date(year, 2, 28)
        return None


def transform_row(
    row: Mapping[str, Any],
    *,
    reference_year: int = REFERENCE_YEAR,
) -> tuple[Row | None, str | None]:
    """Transforma una fila; retorna (fila_normalizada, motivo_de_descarte)."""
    errors: list[str] = []

    rut = normalize_rut(row["Rut cliente"])
    if rut is None:
        errors.append("RUT inválido")

    nombre = normalize_name(row["Nombre"])
    if nombre is None:
        errors.append("nombre inválido")

    email = normalize_email(row["Email"])
    if email is None:
        errors.append("email inválido")

    alta = parse_date_dmy(
        row["Alta"],
        min_date=MIN_ALTA_DATE,
        max_date=date.today(),
    )
    if alta is None:
        errors.append("alta inválida")

    canal = normalize_canal(row["Canal"])
    if canal is None:
        errors.append("canal inválido")

    cumple: date | None = None
    day, month = parse_day_month_es(row["Cumple."])
    if day is None or month is None:
        errors.append("cumple inválido")
    elif rut is not None:
        year = estimar_anio(rut, reference_year=reference_year)
        if year is None:
            errors.append("año estimado inválido")
        else:
            cumple = _build_cumple_date(year, month, day)
            if cumple is None:
                errors.append("cumple inválido")

    puntos = parse_non_negative_bigint(row["Puntos"], default=None)
    if puntos is None:
        errors.append("puntos inválido")

    compras = parse_non_negative_bigint(row["Compras"], default=None)
    if compras is None:
        errors.append("compras inválido")

    canjes = parse_non_negative_bigint(row["Canjes"], default=None)
    if canjes is None:
        errors.append("canjes inválido")

    if errors:
        return None, "; ".join(errors)

    normalized_row = cast(
        Row,
        (rut, nombre, email, alta, canal, cumple, puntos, compras, canjes),
    )
    return normalized_row, None


def transform(
    dataframe: pd.DataFrame,
    *,
    reference_year: int = REFERENCE_YEAR,
) -> tuple[list[Row], list[dict[str, Any]]]:
    """Transforma el lote y separa filas válidas de descartadas."""
    rows: list[Row] = []
    rejected: list[dict[str, Any]] = []

    for row in dataframe.to_dict(orient="records"):
        result, error = transform_row(row, reference_year=reference_year)
        if result is None:
            rejected.append(
                {
                    "rut": row["Rut cliente"],
                    "motivo": error or "fila inválida",
                }
            )
            logger.debug(
                "Fila descartada RUT=%s: %s",
                row["Rut cliente"],
                error or "fila inválida",
            )
            continue
        rows.append(result)

    logger.info(
        "Transformación finalizada: %d filas válidas, %d descartadas",
        len(rows),
        len(rejected),
    )
    return rows, rejected


def run(
    input_path: str | Path = INPUT_PATH,
    *,
    dry_run: bool = False,
    schema_only: bool = False,
) -> None:
    """Ejecuta el pipeline completo o una de sus etapas."""
    if schema_only:
        schema = get_schema()
        with db.connection() as conn:
            db.execute_sql_file(conn, SQL_FILE, schema=schema)
        logger.info("Schema y tabla %s.%s verificados", schema, TABLE)
        return

    dataframe = read_excel_as_str(input_path)
    require_columns(dataframe, SOURCE_COLUMNS, source=input_path)
    rows, _ = transform(dataframe)

    if dry_run:
        logger.info("Dry run finalizado; no se escribió en PostgreSQL")
        return

    schema = get_schema()
    with db.connection() as conn:
        db.execute_sql_file(conn, SQL_FILE, schema=schema)
        inserted = db.replace_table(
            conn,
            schema,
            TABLE,
            COLUMNS,
            rows,
        )

    logger.info("Carga finalizada en %s.%s: %d filas", schema, TABLE, inserted)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normaliza y carga clientes en silver.clientes.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_PATH,
        help="Ruta del Excel de clientes.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Lee y transforma, pero no escribe en PostgreSQL.",
    )
    mode.add_argument(
        "--schema-only",
        action="store_true",
        help="Solo crea el schema y la tabla.",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Nivel de logging.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    try:
        run(
            args.input,
            dry_run=args.dry_run,
            schema_only=args.schema_only,
        )
    except Exception:
        logger.exception("Error ejecutando el pipeline de clientes")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
