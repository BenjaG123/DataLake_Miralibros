"""Helpers de PostgreSQL para los pipelines."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import psycopg
from psycopg import sql

from etl.config import get_database_url

Connection = psycopg.Connection[Any]
Row = Sequence[Any]


def connect(database_url: str | None = None) -> Connection:
    """Conecta a PostgreSQL."""
    return psycopg.connect(database_url or get_database_url())


@contextmanager
def connection(database_url: str | None = None) -> Iterator[Connection]:
    """Entrega una conexión y la cierra al salir del contexto."""
    conn = connect(database_url)
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def execute_sql_file(conn: Connection, sql_path: str | Path) -> None:
    """Ejecuta un archivo SQL completo dentro de una transacción."""
    path = Path(sql_path)
    if not path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo SQL: {path}")

    statement = path.read_text(encoding="utf-8").strip()
    if not statement:
        raise ValueError(f"El archivo SQL está vacío: {path}")

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(statement)


def truncate_table(conn: Connection, schema: str, table: str) -> None:
    """Vacía una tabla sin eliminarla."""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.{}").format(
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
    """Carga filas con COPY y devuelve cuántas se enviaron."""
    if not columns:
        raise ValueError("Debe indicar al menos una columna para COPY.")

    statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
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
    """Reemplaza el contenido de una tabla con TRUNCATE + COPY transaccional."""
    with conn.transaction():
        truncate_table(conn, schema, table)
        return copy_rows(conn, schema, table, columns, rows)
    return None
