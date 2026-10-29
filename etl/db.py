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


def upsert_table(
    conn: Connection,
    schema: str,
    table: str,
    columns: Sequence[str],
    key_columns: Sequence[str],
    rows: Iterable[Row],
    *,
    update_columns: Sequence[str] | None = None,
    temp_table: str = "etl_upsert_staging",
) -> int:
    """Carga filas en una temporal y hace upsert por la clave de negocio.

    Contrato:

    - ``columns`` es la lista ordenada de columnas destino y de cada fila.
    - ``key_columns`` debe existir como PK/UNIQUE en la tabla destino.
    - ``update_columns`` son las columnas que se actualizan en conflicto con
      ``EXCLUDED``; por defecto, todas menos las de la clave. Si se pasa una
      lista vacía se usa ``DO NOTHING``.
    - ``temp_table`` es el nombre de la tabla temporal de staging. Debe ser
      único por llamada dentro de una misma transacción.
    - Las filas deben venir ya deduplicadas por la clave: PostgreSQL rechaza
      un mismo conflicto dos veces dentro de un solo ``INSERT``.
    - Retorna la cantidad de filas copiadas al staging, no el desglose de
      insertadas/actualizadas.

    La tabla temporal se crea con ``LIKE ... INCLUDING DEFAULTS`` y
    ``ON COMMIT DROP``, por lo que desaparece al cerrar la transacción. No se
    vacía ni se reemplaza la tabla destino.
    """
    if not columns:
        raise ValueError("Debe indicar al menos una columna para el upsert.")
    if not key_columns:
        raise ValueError("Debe indicar al menos una columna clave para el upsert.")

    column_list = list(dict.fromkeys(columns))
    if len(column_list) != len(columns):
        raise ValueError("La lista de columnas contiene duplicados.")

    key_list = list(key_columns)
    missing_keys = [column for column in key_list if column not in column_list]
    if missing_keys:
        raise ValueError(
            "Las columnas clave no están en la lista de columnas: "
            f"{', '.join(missing_keys)}"
        )

    if update_columns is None:
        update_list = [
            column for column in column_list if column not in key_list
        ]
    else:
        update_list = list(dict.fromkeys(update_columns))
        missing_updates = [
            column for column in update_list if column not in column_list
        ]
        if missing_updates:
            raise ValueError(
                "Las columnas a actualizar no están en la lista de columnas: "
                f"{', '.join(missing_updates)}"
            )
        duplicated_keys = [
            column for column in update_list if column in key_list
        ]
        if duplicated_keys:
            raise ValueError(
                "No se pueden actualizar columnas de la clave: "
                f"{', '.join(duplicated_keys)}"
            )

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "CREATE TEMP TABLE {} "
                    "(LIKE {}.{} INCLUDING DEFAULTS) ON COMMIT DROP"
                ).format(
                    sql.Identifier(temp_table),
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            )

        count = copy_rows(conn, "pg_temp", temp_table, column_list, rows)
        if count == 0:
            return 0

        insert_columns = sql.SQL(", ").join(
            sql.Identifier(column) for column in column_list
        )
        select_columns = sql.SQL(", ").join(
            sql.Identifier(column) for column in column_list
        )
        conflict_columns = sql.SQL(", ").join(
            sql.Identifier(column) for column in key_list
        )

        if update_list:
            assignments = sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column),
                    sql.Identifier(column),
                )
                for column in update_list
            )
            conflict_action = sql.SQL("DO UPDATE SET {}").format(assignments)
        else:
            conflict_action = sql.SQL("DO NOTHING")

        statement = sql.SQL(
            "INSERT INTO {}.{} ({}) "
            "SELECT {} FROM {}.{} "
            "ON CONFLICT ({}) {}"
        ).format(
            sql.Identifier(schema),
            sql.Identifier(table),
            insert_columns,
            select_columns,
            sql.Identifier("pg_temp"),
            sql.Identifier(temp_table),
            conflict_columns,
            conflict_action,
        )

        with conn.cursor() as cur:
            cur.execute(statement)

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