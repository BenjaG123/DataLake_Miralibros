"""Pipeline incremental de ventas de librería y cafetería.

Ambos archivos comparten columnas y se cargan en una sola tabla
``silver.ventas``. La columna ``origen`` (``LIBRERIA``/``CAFETERIA``) preserva
la procedencia y forma parte de la clave de negocio
``(origen, correlativo)``, de modo que el upsert es idempotente por archivo y
entre archivos.
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd

from etl import db
from etl.config import BRONZE_DIR, SQL_DIR, configure_logging, get_schema
from etl.readers import read_excel_as_str, require_columns
from etl.validators import (
    is_placeholder,
    parse_date_iso,
    parse_integer,
    parse_non_negative_bigint,
)

logger = logging.getLogger(__name__)

INPUT_PATHS = (
    BRONZE_DIR / "VENTAS LIBRERIA.xlsx",
    BRONZE_DIR / "VENTAS CAFETERIA.xlsx",
)
SQL_FILE = SQL_DIR / "ventas.sql"
TABLE = "ventas"

SOURCE_COLUMNS = (
    "Correlativo",
    "Orden Web",
    "Fecha",
    "Medio de Pago",
    "Tipo Documento",
    "Folio",
    "Voucher Máquina de F",
    "Total Venta",
    "Cant. Items",
    "Canal",
    "Feria",
    "Tiene Descuento",
    "Estado",
    "Es Dev.",
)

COLUMNS = (
    "origen",
    "correlativo",
    "orden_web",
    "fecha",
    "medio_pago",
    "tipo_documento",
    "folio",
    "voucher_maquina_f",
    "total_venta",
    "cant_items",
    "canal",
    "feria",
    "tiene_descuento",
    "es_devolucion",
    "estado",
    "_source_file",
    "_loaded_at",
    "updated_at",
)

KEY_COLUMNS = ("origen", "correlativo")

UPDATE_COLUMNS = (
    "orden_web",
    "fecha",
    "medio_pago",
    "tipo_documento",
    "folio",
    "voucher_maquina_f",
    "total_venta",
    "cant_items",
    "canal",
    "feria",
    "tiene_descuento",
    "es_devolucion",
    "estado",
    "_source_file",
    "_loaded_at",
    "updated_at",
)

Row = tuple[
    str,
    int,
    str | None,
    date,
    str | None,
    str | None,
    str | None,
    str | None,
    int,
    int,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str,
    datetime,
    datetime,
]


def _clean_text(value: Any) -> str | None:
    """Colapsa espacios y convierte placeholders en ``None``."""
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None
    return " ".join(raw.split())


def inferir_origen(path: str | Path) -> str:
    """Infiere ``LIBRERIA`` o ``CAFETERIA`` desde el nombre del archivo."""
    nombre = Path(path).name.upper().replace("Í", "I")
    if "CAFETERIA" in nombre:
        return "CAFETERIA"
    if "LIBRERIA" in nombre:
        return "LIBRERIA"
    raise ValueError(
        "No se pudo inferir el origen del archivo "
        f"{Path(path).name!r}: el nombre debe contener LIBRERIA o CAFETERIA."
    )


def normalize_canal(value: Any) -> str | None:
    """Normaliza el canal a ``Local`` o ``Web``; otro valor queda ``None``."""
    raw = _clean_text(value)
    if raw is None:
        return None
    return {"local": "Local", "web": "Web"}.get(raw.casefold())


def normalize_si_no(value: Any) -> str | None:
    """Normaliza un flag a ``SI``/``NO``; un valor no reconocido queda ``None``."""
    raw = _clean_text(value)
    if raw is None:
        return None
    key = raw.upper()
    if key in ("SI", "SÍ", "S"):
        return "SI"
    if key in ("NO", "N"):
        return "NO"
    return None


def transform_row(
    row: Mapping[str, Any],
    *,
    origen: str,
    source_file: str,
    loaded_at: datetime,
) -> tuple[Row | None, str | None]:
    """Transforma una fila de ventas; retorna (fila_normalizada, motivo)."""
    errors: list[str] = []

    correlativo = parse_integer(row["Correlativo"], minimum=1)
    if correlativo is None:
        errors.append("correlativo inválido")

    fecha = parse_date_iso(row["Fecha"])
    if fecha is None:
        errors.append("fecha inválida")

    total_venta = parse_non_negative_bigint(
        row["Total Venta"],
        default=None,
    )
    if total_venta is None:
        errors.append("total venta inválido o negativo")

    cant_items = parse_integer(row["Cant. Items"], minimum=1)
    if cant_items is None:
        errors.append("cantidad de ítems inválida")

    orden_web = _clean_text(row["Orden Web"])
    medio_pago = _clean_text(row["Medio de Pago"])
    tipo_documento = _clean_text(row["Tipo Documento"])
    folio = _clean_text(row["Folio"])
    voucher = _clean_text(row["Voucher Máquina de F"])
    canal = normalize_canal(row["Canal"])
    feria = _clean_text(row["Feria"])
    tiene_descuento = normalize_si_no(row["Tiene Descuento"])
    es_devolucion = normalize_si_no(row["Es Dev."])
    estado = _clean_text(row["Estado"])

    if errors:
        return None, "; ".join(errors)

    normalized = cast(
        Row,
        (
            origen,
            correlativo,
            orden_web,
            fecha,
            medio_pago,
            tipo_documento,
            folio,
            voucher,
            total_venta,
            cant_items,
            canal,
            feria,
            tiene_descuento,
            es_devolucion,
            estado,
            source_file,
            loaded_at,
            loaded_at,
        ),
    )
    return normalized, None


def transform(
    dataframe: pd.DataFrame,
    *,
    origen: str,
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
            origen=origen,
            source_file=source_file,
            loaded_at=loaded_at,
        )
        if result is None:
            motivo = error or "fila inválida"
            rejected.append(
                {
                    "origen": origen,
                    "correlativo": row["Correlativo"],
                    "motivo": motivo,
                }
            )
            logger.debug(
                "Fila descartada origen=%s correlativo=%r: %s",
                origen,
                row["Correlativo"],
                motivo,
            )
            continue
        rows.append(result)

    logger.info(
        "Transformación ventas %s: %d filas válidas, %d descartadas",
        origen,
        len(rows),
        len(rejected),
    )
    return rows, rejected


def deduplicate(
    rows: Sequence[Row],
) -> tuple[list[Row], int]:
    """Deduplica por ``(origen, correlativo)`` conservando la última fila."""
    seen: dict[tuple[str, int], Row] = {}
    duplicates = 0

    for row in rows:
        key = (row[0], row[1])
        if key in seen:
            duplicates += 1
            logger.debug(
                "Clave duplicada origen=%s correlativo=%s; "
                "se conserva la última fila",
                key[0],
                key[1],
            )
        seen[key] = row

    if duplicates:
        logger.warning(
            "Ventas: %d filas duplicadas por (origen, correlativo); "
            "se conserva la última",
            duplicates,
        )
    return list(seen.values()), duplicates


def _log_rejection_summary(rejected: Sequence[Mapping[str, Any]]) -> None:
    """Registra el total y los motivos agregados de las filas descartadas."""
    if not rejected:
        logger.info("Ventas: sin filas rechazadas")
        return

    counts = Counter(
        motivo
        for item in rejected
        for motivo in str(item.get("motivo", "fila inválida")).split("; ")
    )
    logger.warning("Ventas: %d filas rechazadas", len(rejected))
    for motivo, cantidad in sorted(counts.items()):
        logger.info("  - %s: %d filas", motivo, cantidad)


def run(
    input_paths: Sequence[str | Path] = INPUT_PATHS,
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

    paths = [Path(path) for path in input_paths]
    loaded_at = datetime.now(timezone.utc)
    valid_rows: list[Row] = []
    rejected: list[dict[str, Any]] = []
    total_read = 0
    per_file: list[tuple[str, str, int, int, int]] = []

    for path in paths:
        origen = inferir_origen(path)
        dataframe = read_excel_as_str(path)
        require_columns(dataframe, SOURCE_COLUMNS, source=path)

        file_rows, file_rejected = transform(
            dataframe,
            origen=origen,
            source_file=path.name,
            loaded_at=loaded_at,
        )
        valid_rows.extend(file_rows)
        rejected.extend(file_rejected)
        total_read += len(dataframe)
        per_file.append(
            (
                path.name,
                origen,
                len(dataframe),
                len(file_rows),
                len(file_rejected),
            )
        )

    rows, duplicates = deduplicate(valid_rows)

    for name, origen, read, valid, rejected_count in per_file:
        logger.info(
            "Archivo %s (origen=%s): leídas=%d válidas=%d rechazadas=%d",
            name,
            origen,
            read,
            valid,
            rejected_count,
        )

    _log_rejection_summary(rejected)
    logger.info(
        "Resumen ventas: leídas=%d válidas=%d rechazadas=%d "
        "duplicadas=%d a_cargar=%d",
        total_read,
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
        loaded = db.upsert_table(
            conn,
            schema,
            TABLE,
            COLUMNS,
            KEY_COLUMNS,
            rows,
            update_columns=UPDATE_COLUMNS,
            temp_table="ventas_staging",
        )

    logger.info(
        "Carga finalizada en %s.%s: %d filas en staging",
        schema,
        TABLE,
        loaded,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Normaliza y carga ventas de librería y cafetería en "
            "silver.ventas de forma incremental."
        ),
    )
    parser.add_argument(
        "--input",
        dest="inputs",
        type=Path,
        action="append",
        default=None,
        help=(
            "Ruta de un Excel de ventas; se puede repetir. "
            "Por defecto usa los dos archivos de bronze/."
        ),
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
            args.inputs if args.inputs else INPUT_PATHS,
            dry_run=args.dry_run,
            schema_only=args.schema_only,
        )
    except Exception:
        logger.exception("Error ejecutando el pipeline de ventas")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
