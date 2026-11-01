"""Pipeline de normalización y carga del clima diario hacia la capa Silver."""

from __future__ import annotations

import argparse
import logging
from calendar import monthrange
from collections.abc import Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from etl import db
from etl.config import BRONZE_DIR, SQL_DIR, configure_logging, get_schema
from etl.readers import read_html_grid_as_str, require_columns
from etl.validators import (
    MONTHS,
    parse_decimal,
    parse_integer,
    parse_precipitation,
    parse_wind,
)

logger = logging.getLogger(__name__)

YEAR = 2025
TABLE = "clima_diario"
SQL_FILE = SQL_DIR / "clima.sql"

MONTH_COLUMNS = (
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
)

SOURCES = {
    "temp_max": "TemperaturaMax2025.xls",
    "temp_min": "TemperaturaMin2025.xls",
    "temp_media": "TemperaturaMedia2025.xls",
    "humedad_relativa": "HumedadRelativaAire2025.xls",
    "indice_uv": "IndiceUV2025.xls",
    "precipitacion_mm": "Precipitaciones2025.xls",
    "viento": "Viento2025.xls",
}

COLUMNS = (
    "fecha",
    "temp_max",
    "temp_min",
    "temp_media",
    "humedad_relativa",
    "indice_uv",
    "precipitacion_mm",
    "viento_direccion",
    "viento_velocidad_kt",
    "_loaded_at",
)

KEY_COLUMNS = ("fecha",)

UPDATE_COLUMNS = (
    "temp_max",
    "temp_min",
    "temp_media",
    "humedad_relativa",
    "indice_uv",
    "precipitacion_mm",
    "viento_direccion",
    "viento_velocidad_kt",
    "_loaded_at",
)

Row = tuple[
    date,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    int | None,
    Decimal | None,
    str | None,
    int | None,
    datetime,
]

MIN_TEMPERATURE = Decimal("-50")
MAX_TEMPERATURE = Decimal("60")
MIN_HUMIDITY = Decimal("0")
MAX_HUMIDITY = Decimal("100")
MIN_UV = 0
MAX_UV = 20

MISSING_FIELDS = (
    "temp_max",
    "temp_min",
    "temp_media",
    "humedad_relativa",
    "indice_uv",
    "precipitacion_mm",
    "viento_direccion",
    "viento_velocidad_kt",
)


def load_grid(path: str | Path, *, year: int = YEAR) -> dict[date, str]:
    """Lee una grilla mensual y la convierte en valores indexados por fecha."""
    dataframe = read_html_grid_as_str(path)
    require_columns(dataframe, ("Dia", *MONTH_COLUMNS), source=path)

    values: dict[date, str] = {}
    for record in dataframe.to_dict(orient="records"):
        day = int(record["Dia"])
        for month_name in MONTH_COLUMNS:
            month = MONTHS[month_name.casefold()]
            try:
                fecha = date(year, month, day)
            except ValueError:
                continue
            values[fecha] = str(record[month_name])

    return values


def build_calendar(year: int = YEAR) -> list[date]:
    """Genera todas las fechas reales del año indicado."""
    return [
        date(year, month, day)
        for month in range(1, 13)
        for day in range(1, monthrange(year, month)[1] + 1)
    ]


def transform(
    input_dir: str | Path = BRONZE_DIR,
    *,
    year: int = YEAR,
    loaded_at: datetime | None = None,
) -> list[Row]:
    """Combina los 7 archivos de clima en una fila por fecha."""
    if loaded_at is None:
        loaded_at = datetime.now(timezone.utc)

    base_dir = Path(input_dir)
    sources = {
        name: load_grid(base_dir / filename, year=year)
        for name, filename in SOURCES.items()
    }

    rows: list[Row] = []
    for fecha in build_calendar(year):
        temperature_max = parse_decimal(
            sources["temp_max"].get(fecha),
            minimum=MIN_TEMPERATURE,
            maximum=MAX_TEMPERATURE,
        )
        temperature_min = parse_decimal(
            sources["temp_min"].get(fecha),
            minimum=MIN_TEMPERATURE,
            maximum=MAX_TEMPERATURE,
        )
        temperature_mean = parse_decimal(
            sources["temp_media"].get(fecha),
            minimum=MIN_TEMPERATURE,
            maximum=MAX_TEMPERATURE,
        )
        humidity = parse_decimal(
            sources["humedad_relativa"].get(fecha),
            minimum=MIN_HUMIDITY,
            maximum=MAX_HUMIDITY,
        )
        uv_index = parse_integer(
            sources["indice_uv"].get(fecha),
            minimum=MIN_UV,
            maximum=MAX_UV,
        )
        precipitation = parse_precipitation(
            sources["precipitacion_mm"].get(fecha)
        )
        wind_direction, wind_speed = parse_wind(sources["viento"].get(fecha))

        rows.append(
            (
                fecha,
                temperature_max,
                temperature_min,
                temperature_mean,
                humidity,
                uv_index,
                precipitation,
                wind_direction,
                wind_speed,
                loaded_at,
            )
        )

    return rows


def _missing_counts(rows: Sequence[Row]) -> dict[str, int]:
    """Cuenta valores nulos por variable en las filas transformadas."""
    counts = {field: 0 for field in MISSING_FIELDS}
    for row in rows:
        for field, value in zip(MISSING_FIELDS, row[1:]):
            if value is None:
                counts[field] += 1
    return counts


def _log_missing(counts: dict[str, int]) -> None:
    """Registra las variables que quedaron con valores faltantes."""
    for field, count in counts.items():
        if count:
            logger.info("  - %s: %d nulos", field, count)


def run(
    input_dir: str | Path = BRONZE_DIR,
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

    rows = transform(input_dir)
    logger.info("Transformación clima: %d filas", len(rows))
    _log_missing(_missing_counts(rows))

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
            temp_table="clima_staging",
        )

    logger.info(
        "Carga finalizada en %s.%s: %d filas en staging",
        schema,
        TABLE,
        loaded,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normaliza y carga el clima diario en silver.clima_diario.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=BRONZE_DIR,
        help="Directorio con los archivos .xls de clima.",
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
            args.input_dir,
            dry_run=args.dry_run,
            schema_only=args.schema_only,
        )
    except Exception:
        logger.exception("Error ejecutando el pipeline de clima")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
