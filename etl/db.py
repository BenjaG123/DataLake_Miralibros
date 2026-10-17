"""Helpers de PostgreSQL para los pipelines."""

from __future__ import annotations

import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import psycopg
from psycopg import sql

from etl.config import get_database_url

Connection = psycopg.Connection[Any]
Row = Sequence[Any]

SCHEMA_PLACEHOLDER = "{{schema}}"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def connect(database_url: str | None = None) -> Connection:
    """Conecta a PostgreSQL."""
    return psycopg.connect(database_url or get_database_url())


@contextmanager
def connection(database_url: str | None = None) -> Iterator[Connection]:
    """Entrega una conexión y asegura su cierre al salir."""
    with connect(database_url) as conn:
        yield conn


def execute_sql_file(
    conn: Connection,
    sql_path: str | Path,
    *,
    schema: str | None = None,
) -> None:
    """Ejecuta un archivo SQL dentro de una transacción reemplazando {{schema}}."""
    path = Path(sql_path)
    if not path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo SQL: {path}")

    statement = path.read_text(encoding="utf-8").strip()
    if not statement:
        raise ValueError(f"El archivo SQL está vacío: {path}")

    if SCHEMA_PLACEHOLDER in statement:
        if schema is None:
            raise ValueError(
                f"El archivo SQL {path} requiere un schema para {SCHEMA_PLACEHOLDER}."
            )
        if not _IDENTIFIER_RE.fullmatch(schema):
            raise ValueError(f"Schema inválido: {schema!r}")
        statement = statement.replace(SCHEMA_PLACEHOLDER, schema)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(statement)


def truncate_table(conn: Connection, schema: str, table: str) -> None:
    """Vacía una tabla reseteando secuencias y cascada."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{} RESTART IDENTITY CASCADE").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )


def copy_rows(
    conn: Connection,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Row],
) -> int:
    """Carga masiva de filas ultra rápida usando COPY."""
    if not columns:
        raise ValueError("Debe indicar al menos una columna para COPY.")

    statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(col) for col in columns),
    )

    count = 0
    with conn.cursor() as cur:
        with cur.copy(statement) as copy:
            for row in rows:
                copy.write_row(row)
                count += 1
    return count


def replace_table(
    conn: Connection,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Row],
) -> int:
    """Reemplaza atómicamente el contenido de una tabla."""
    with conn.transaction():
        truncate_table(conn, schema, table)
        return copy_rows(conn, schema, table, columns, rows)