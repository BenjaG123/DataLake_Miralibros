"""Pipeline de normalización y carga de clientes hacia la capa Silver."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import logging
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any, cast

import pandas as pd

from etl import db
from etl.config import (
    BRONZE_DIR,
    SQL_DIR,
    configure_logging,
    get_pii_hash_key,
    get_schema,
)
from etl.readers import read_excel_as_str, require_columns
from etl.validators import (
    is_placeholder,
    normalize_rut,
    parse_date_dmy,
    parse_day_month_es,
    parse_non_negative_bigint_or_zero,
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
    "cliente_id",
    "alta",
    "canal",
    "edad",
    "mes_cumple",
    "puntos",
    "compras",
    "canjes",
)

Row = tuple[str, date | None, str, int, int | None, int, int, int]

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


def generar_cliente_id(rut: str) -> str:
    """Genera un identificador anónimo determinístico a partir del RUT.

    Usa HMAC-SHA256 con la clave configurada en ``PII_HASH_KEY``. El RUT nunca
    se persiste ni se registra en logs.
    """
    return hmac.new(
        get_pii_hash_key().encode("utf-8"),
        rut.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


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


def transform_row(
    row: Mapping[str, Any],
    *,
    reference_year: int = REFERENCE_YEAR,
) -> tuple[Row | None, str | None]:
    """Transforma una fila; retorna (fila_normalizada, motivo_de_descarte).

    Valida el RUT, el alta y el canal. ``Nombre`` y ``Email`` se ignoran por
    completo: no se validan ni se persisten. ``Cumple.`` solo se usa para
    derivar ``mes_cumple`` cuando es parseable; si no lo es, la fila sigue
    válida con ``mes_cumple = NULL``. La edad se calcula exclusivamente desde
    el año estimado a partir del RUT, sin depender de ``Cumple.``.
    """
    errors: list[str] = []

    rut = normalize_rut(row["Rut cliente"])
    cliente_id = generar_cliente_id(rut) if rut is not None else None
    if cliente_id is None:
        errors.append("RUT inválido")

    alta = parse_date_dmy(row["Alta"], min_date=MIN_ALTA_DATE)
    if alta is None:
        errors.append("alta inválida")
    elif alta.year > date.today().year:
        logger.warning(
            "Alta futura para cliente_id %s: %s; se guarda alta=NULL",
            cliente_id or "sin_id",
            alta,
        )
        alta = None

    canal = normalize_canal(row["Canal"])
    if canal is None:
        errors.append("canal inválido")

    day, month = parse_day_month_es(row["Cumple."])
    mes_cumple = month if day is not None and month is not None else None

    edad: int | None = None
    if rut is not None:
        year = estimar_anio(rut, reference_year=reference_year)
        if year is None:
            errors.append("año estimado inválido")
        else:
            edad = reference_year - year
            if not 0 <= edad <= 120:
                errors.append("edad inválida")

    puntos = parse_non_negative_bigint_or_zero(row["Puntos"])
    if puntos is None:
        errors.append("puntos inválido")

    compras = parse_non_negative_bigint_or_zero(row["Compras"])
    if compras is None:
        errors.append("compras inválido")

    canjes = parse_non_negative_bigint_or_zero(row["Canjes"])
    if canjes is None:
        errors.append("canjes inválido")

    if errors:
        return None, "; ".join(errors)
    if cliente_id is None or canal is None or edad is None:
        return None, "fila inválida"

    normalized_row = cast(
        Row,
        (cliente_id, alta, canal, edad, mes_cumple, puntos, compras, canjes),
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
        rut = normalize_rut(row["Rut cliente"])
        cliente_id = generar_cliente_id(rut) if rut is not None else None
        result, error = transform_row(row, reference_year=reference_year)
        if result is None:
            rejected.append(
                {
                    "cliente_id": cliente_id,
                    "motivo": error or "fila inválida",
                }
            )
            logger.debug(
                "Fila descartada cliente_id=%s: %s",
                cliente_id or "sin_id",
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
