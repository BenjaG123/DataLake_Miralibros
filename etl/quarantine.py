"""Persistencia de filas rechazadas en la tabla de cuarentena común."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import psycopg
from psycopg import sql

from etl.config import SQL_DIR

QUARANTINE_SQL = SQL_DIR / "cuarentena.sql"
TABLE = "cuarentena"

_COLUMNS = (
    "entidad",
    "archivo_origen",
    "fila",
    "columna",
    "valor_crudo",
    "regla",
    "severidad",
    "motivo",
)


def flatten_rejections(
    rejected: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Aplana los ``detalles`` de cada rechazo al contrato de cuarentena.

    Cada rechazo puede traer una lista ``detalles`` con uno o varios errores.
    Si no la trae, se usa el propio rechazo como único detalle. El resultado
    solo contiene las claves que espera :func:`insert_quarantine`.
    """
    filas: list[dict[str, Any]] = []
    for item in rejected:
        detalles = item.get("detalles") or [item]
        for detalle in detalles:
            filas.append(
                {
                    "fila": detalle.get("fila", item.get("fila")),
                    "columna": detalle.get("columna", item.get("columna")),
                    "valor_crudo": detalle.get(
                        "valor_crudo", item.get("valor_crudo")
                    ),
                    "regla": detalle.get("regla", item.get("regla")),
                    "severidad": detalle.get(
                        "severidad", item.get("severidad", "error")
                    ),
                    "motivo": detalle.get("motivo", item.get("motivo")),
                }
            )
    return filas


def insert_quarantine(
    conn: psycopg.Connection[Any],
    schema: str,
    entidad: str,
    archivo_origen: str,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    """Inserta las filas rechazadas en ``{schema}.cuarentena``.

    ``rows`` es un iterable de mappings con las claves ``fila``, ``columna``,
    ``valor_crudo``, ``regla``, ``severidad`` y ``motivo``. Los identificadores
    se construyen con :class:`psycopg.sql.Identifier`; los valores viajan como
    parámetros. Retorna la cantidad de filas enviadas.
    """
    payload = [
        (
            entidad,
            archivo_origen,
            row.get("fila"),
            row.get("columna"),
            row.get("valor_crudo"),
            row.get("regla") or "SIN-REGLA",
            row.get("severidad") or "error",
            row.get("motivo") or "fila inválida",
        )
        for row in rows
    ]
    if not payload:
        return 0

    statement = sql.SQL(
        "INSERT INTO {schema}.{table} ({columns}) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    ).format(
        schema=sql.Identifier(schema),
        table=sql.Identifier(TABLE),
        columns=sql.SQL(", ").join(
            sql.Identifier(column) for column in _COLUMNS
        ),
    )

    with conn.cursor() as cur:
        cur.executemany(statement, payload)

    return len(payload)
