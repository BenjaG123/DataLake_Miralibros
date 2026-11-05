"""Pipeline incremental de stock: normaliza el Excel y hace upsert por ISBN.

La tabla ``stock`` guarda el estado actual por ISBN. Además se mantiene
``stock_historial`` como SCD2 simple: cada cambio de precio o de cantidades
cierra la versión vigente (``valid_to``) y abre una nueva (``is_current``).
Las columnas de validez son explícitas:

- ``valid_from`` / ``valid_to``: intervalo de vigencia de la versión.
- ``is_current``: indica la versión vigente por ISBN.
- ``updated_at``: última vez que el pipeline vio la fila.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd
from psycopg import sql

from etl import db
from etl.config import BRONZE_DIR, SQL_DIR, configure_logging, get_schema
from etl.readers import read_excel_as_str, require_columns
from etl.validators import (
    is_placeholder,
    parse_non_negative_bigint,
    parse_non_negative_bigint_or_zero,
)

logger = logging.getLogger(__name__)

INPUT_PATH = BRONZE_DIR / "STOCK LIBRERIA.xlsx"
SQL_FILE = SQL_DIR / "stock.sql"
TABLE = "stock"
HISTORY_TABLE = "stock_historial"

SOURCE_COLUMNS = (
    "ISBN",
    "Titulo",
    "Autor",
    "Editorial",
    "Clasificación",
    "PVP",
    "Stock Consig.",
    "Stock Propio",
    "Stock Total",
    "Ventas",
)

COLUMNS = (
    "isbn",
    "titulo",
    "autor",
    "editorial",
    "clasificacion",
    "pvp",
    "stock_consignacion",
    "stock_propio",
    "stock_total",
    "ventas",
    "valid_from",
    "updated_at",
    "_source_file",
    "_loaded_at",
)

KEY_COLUMNS = ("isbn",)

UPDATE_COLUMNS = (
    "titulo",
    "autor",
    "editorial",
    "clasificacion",
    "pvp",
    "stock_consignacion",
    "stock_propio",
    "stock_total",
    "ventas",
    "updated_at",
    "_source_file",
    "_loaded_at",
)

HISTORY_COMPARE_COLUMNS = (
    "titulo",
    "autor",
    "editorial",
    "clasificacion",
    "pvp",
    "stock_consignacion",
    "stock_propio",
    "stock_total",
    "ventas",
)

Row = tuple[
    str,
    str | None,
    str | None,
    str | None,
    str | None,
    int,
    int,
    int,
    int,
    int,
    datetime,
    datetime,
    str,
    datetime,
]

_ISBN_PATTERN = re.compile(r"^(?:[0-9]{13}|[0-9]{9}[0-9X])$")


def normalize_isbn(value: Any) -> str | None:
    """Normaliza un ISBN quitando guiones, puntos y espacios.

    Acepta ISBN-13 (13 dígitos) e ISBN-10 (9 dígitos + dígito o ``X``).
    No valida el dígito verificador porque los crudos de Miralibros no lo
    garantizan; solo se exige la forma. Un valor vacío o con otra forma
    retorna ``None``.
    """
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None

    cleaned = (
        raw.replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .upper()
    )
    if not _ISBN_PATTERN.fullmatch(cleaned):
        return None
    return cleaned


def _clean_text(value: Any) -> str | None:
    """Colapsa espacios y convierte placeholders en ``None``."""
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None
    return " ".join(raw.split())


def transform_row(
    row: Mapping[str, Any],
    *,
    source_file: str,
    loaded_at: datetime,
) -> tuple[Row | None, str | None]:
    """Transforma una fila de stock; retorna (fila_normalizada, motivo)."""
    errors: list[str] = []

    isbn = normalize_isbn(row["ISBN"])
    if isbn is None:
        errors.append("ISBN inválido o vacío")

    titulo = _clean_text(row["Titulo"])
    autor = _clean_text(row["Autor"])
    editorial = _clean_text(row["Editorial"])
    clasificacion = _clean_text(row["Clasificación"])

    pvp = parse_non_negative_bigint(row["PVP"], default=None)
    if pvp is None:
        errors.append("PVP inválido o negativo")

    consignacion = parse_non_negative_bigint_or_zero(row["Stock Consig."])
    if consignacion is None:
        errors.append("stock consignación inválido")

    propio = parse_non_negative_bigint_or_zero(row["Stock Propio"])
    if propio is None:
        errors.append("stock propio inválido")

    total = parse_non_negative_bigint(row["Stock Total"], default=None)
    if total is None:
        errors.append("stock total inválido o negativo")
    elif (
        consignacion is not None
        and propio is not None
        and total != consignacion + propio
    ):
        errors.append("stock total inconsistente")

    ventas = parse_non_negative_bigint_or_zero(row["Ventas"])
    if ventas is None:
        errors.append("ventas inválidas")

    if errors:
        return None, "; ".join(errors)

    normalized = cast(
        Row,
        (
            isbn,
            titulo,
            autor,
            editorial,
            clasificacion,
            pvp,
            consignacion,
            propio,
            total,
            ventas,
            loaded_at,
            loaded_at,
            source_file,
            loaded_at,
        ),
    )
    return normalized, None


def transform(
    dataframe: pd.DataFrame,
    *,
    source_file: str,
    loaded_at: datetime | None = None,
) -> tuple[list[Row], list[dict[str, Any]]]:
    """Transforma el lote y separa filas válidas de descartadas."""
    if loaded_at is None:
        loaded_at = datetime.now(timezone.utc)

    rows: list[Row] = []
    rejected: list[dict[str, Any]] = []

    for row in dataframe.to_dict(orient="records"):
        result, error = transform_row(
            row,
            source_file=source_file,
            loaded_at=loaded_at,
        )
        if result is None:
            motivo = error or "fila inválida"
            rejected.append({"isbn": row["ISBN"], "motivo": motivo})
            logger.debug("Fila descartada ISBN=%r: %s", row["ISBN"], motivo)
            continue
        rows.append(result)

    logger.info(
        "Transformación stock: %d filas válidas, %d descartadas",
        len(rows),
        len(rejected),
    )
    return rows, rejected


def deduplicate(rows: Sequence[Row]) -> tuple[list[Row], int]:
    """Deduplica por ISBN conservando la última fila vista."""
    seen: dict[str, Row] = {}
    duplicates = 0

    for row in rows:
        isbn = row[0]
        if isbn in seen:
            duplicates += 1
            logger.debug(
                "ISBN duplicado %s; se conserva la última fila",
                isbn,
            )
        seen[isbn] = row

    if duplicates:
        logger.warning(
            "Stock: %d filas duplicadas por ISBN; se conserva la última",
            duplicates,
        )
    return list(seen.values()), duplicates


def _log_rejection_summary(rejected: Sequence[Mapping[str, Any]]) -> None:
    """Registra el total y los motivos agregados de las filas descartadas."""
    if not rejected:
        logger.info("Stock: sin filas rechazadas")
        return

    counts = Counter(
        motivo
        for item in rejected
        for motivo in str(item.get("motivo", "fila inválida")).split("; ")
    )
    logger.warning("Stock: %d filas rechazadas", len(rejected))
    for motivo, cantidad in sorted(counts.items()):
        logger.info("  - %s: %d filas", motivo, cantidad)


def _refresh_history(conn: db.Connection, schema: str) -> tuple[int, int]:
    """Cierra versiones cambiadas y abre las nuevas en el historial SCD2.

    Se usan dos comparaciones complementarias, ambas null-safe:

    - **Cierre**: la versión vigente se cierra cuando cambia *cualquier*
      columna de ``HISTORY_COMPARE_COLUMNS`` (comparaciones unidas con
      ``OR``). Si se unieran con ``AND`` solo se cerraría cuando todas
      cambiaran a la vez, dejando la versión vieja como vigente y chocando
      contra el índice único parcial ``stock_historial_isbn_actual_uidx``.
    - **Inserción**: solo se abre una versión nueva si *todas* las columnas
      son iguales a la vigente (``IS NOT DISTINCT FROM`` unidas con ``AND``),
      de modo que una corrida idéntica no duplique historial.
    """
    current = sql.Identifier(schema, TABLE)
    history = sql.Identifier(schema, HISTORY_TABLE)
    source_alias = sql.Identifier("s")
    history_alias = sql.Identifier("h")

    def _column(alias: sql.Identifier, column: str) -> sql.Composable:
        return sql.SQL("{}.{}").format(alias, sql.Identifier(column))

    def _compare(operator: str, joiner: str = " AND ") -> sql.Composable:
        """Combina comparaciones null-safe con ``AND`` u ``OR``."""
        return sql.SQL(joiner).join(
            sql.SQL("{left} {operator} {right}").format(
                left=_column(history_alias, column),
                operator=sql.SQL(operator),
                right=_column(source_alias, column),
            )
            for column in HISTORY_COMPARE_COLUMNS
        )

    # "Alguna columna cambió": ``IS DISTINCT FROM`` es null-safe, así que
    # NULL -> valor y valor -> NULL también cuentan como cambio.
    close_statement = sql.SQL(
        "UPDATE {history} AS {h} "
        "SET {valid_to} = {source_updated_at}, {is_current} = FALSE "
        "FROM {current} AS {s} "
        "WHERE {history_isbn} = {source_isbn} "
        "AND {history_is_current} AND ({distinct})"
    ).format(
        history=history,
        h=history_alias,
        s=source_alias,
        current=current,
        valid_to=sql.Identifier("valid_to"),
        source_updated_at=_column(source_alias, "updated_at"),
        is_current=sql.Identifier("is_current"),
        history_isbn=_column(history_alias, "isbn"),
        source_isbn=_column(source_alias, "isbn"),
        history_is_current=_column(history_alias, "is_current"),
        distinct=_compare("IS DISTINCT FROM", " OR "),
    )

    history_columns = ("isbn", *HISTORY_COMPARE_COLUMNS)
    audit_columns = (
        "valid_from",
        "valid_to",
        "is_current",
        "_source_file",
        "_loaded_at",
    )
    insert_columns = sql.SQL(", ").join(
        sql.Identifier(column) for column in history_columns
    )
    audit_insert_columns = sql.SQL(", ").join(
        sql.Identifier(column) for column in audit_columns
    )
    select_columns = sql.SQL(", ").join(
        _column(source_alias, column) for column in history_columns
    )
    audit_select = sql.SQL(", ").join(
        (
            _column(source_alias, "updated_at"),
            sql.SQL("NULL"),
            sql.SQL("TRUE"),
            _column(source_alias, "_source_file"),
            _column(source_alias, "_loaded_at"),
        )
    )

    # "Todas las columnas iguales": se evita abrir una versión duplicada
    # cuando la corrida repite exactamente los mismos valores.
    insert_statement = sql.SQL(
        "INSERT INTO {history} ({columns}, {audit_columns}) "
        "SELECT {select_columns}, {audit_select} "
        "FROM {current} AS {s} "
        "WHERE NOT EXISTS ("
        "SELECT 1 FROM {history} AS {h} "
        "WHERE {history_isbn} = {source_isbn} "
        "AND {history_is_current} AND ({same})"
        ")"
    ).format(
        history=history,
        columns=insert_columns,
        audit_columns=audit_insert_columns,
        select_columns=select_columns,
        audit_select=audit_select,
        current=current,
        s=source_alias,
        h=history_alias,
        history_isbn=_column(history_alias, "isbn"),
        source_isbn=_column(source_alias, "isbn"),
        history_is_current=_column(history_alias, "is_current"),
        same=_compare("IS NOT DISTINCT FROM"),
    )

    with conn.cursor() as cur:
        cur.execute(close_statement)
        closed = cur.rowcount
        cur.execute(insert_statement)
        opened = cur.rowcount

    return closed, opened


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
        logger.info(
            "Schema y tablas %s.%s/%s verificados",
            schema,
            TABLE,
            HISTORY_TABLE,
        )
        return

    dataframe = read_excel_as_str(input_path)
    require_columns(dataframe, SOURCE_COLUMNS, source=input_path)

    loaded_at = datetime.now(timezone.utc)
    source_file = Path(input_path).name
    valid_rows, rejected = transform(
        dataframe,
        source_file=source_file,
        loaded_at=loaded_at,
    )
    rows, duplicates = deduplicate(valid_rows)

    _log_rejection_summary(rejected)
    logger.info(
        "Resumen stock: leídas=%d válidas=%d rechazadas=%d "
        "duplicadas=%d a_cargar=%d",
        len(dataframe),
        len(valid_rows),
        len(rejected),
        duplicates,
        len(rows),
    )

    if dry_run:
        logger.info("Dry run finalizado; no se escribió en PostgreSQL")
        return

    schema = get_schema()
    with db.connection() as conn:
        db.execute_sql_file(conn, SQL_FILE, schema=schema)
        with conn.transaction():
            loaded = db.upsert_table(
                conn,
                schema,
                TABLE,
                COLUMNS,
                KEY_COLUMNS,
                rows,
                update_columns=UPDATE_COLUMNS,
                temp_table="stock_staging",
            )
            closed, opened = _refresh_history(conn, schema)

    logger.info(
        "Carga finalizada en %s.%s: %d filas en staging, "
        "%d versiones cerradas, %d versiones abiertas",
        schema,
        TABLE,
        loaded,
        closed,
        opened,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normaliza y carga stock en silver.stock de forma incremental.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_PATH,
        help="Ruta del Excel de stock.",
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
        help="Solo crea el schema, la tabla y el historial.",
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
        logger.exception("Error ejecutando el pipeline de stock")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
