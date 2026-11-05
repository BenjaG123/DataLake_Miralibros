"""Integración real de ``stock`` y ``ventas`` contra PostgreSQL.

Se omiten por defecto. Solo corren si existe ``TEST_DATABASE_URL`` (nunca se
usa ``DATABASE_URL``). Cada test crea un schema único ``test_miralibros_*``,
carga dos veces los crudos sintéticos y lo elimina con ``DROP SCHEMA CASCADE``
en el ``finally``. El schema de prueba nunca se toma de ``PG_SCHEMA``.
"""

from __future__ import annotations

import os
import re
import uuid
from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql

from etl import db
from etl.pipelines import stock, ventas
from etl.readers import read_excel_as_str

URL_PRUEBA = os.environ.get("TEST_DATABASE_URL", "").strip() or None

pytestmark = pytest.mark.skipif(
    URL_PRUEBA is None,
    reason=(
        "TEST_DATABASE_URL no está definida; "
        "los tests de integración PostgreSQL se omiten"
    ),
)

_PATRON_SCHEMA_PRUEBA = re.compile(r"test_[A-Za-z0-9_]+")


def _validar_schema_prueba(schema: str) -> None:
    """Aborta si el schema no es claramente de prueba."""
    if not schema.startswith("test_") or not _PATRON_SCHEMA_PRUEBA.fullmatch(
        schema
    ):
        raise RuntimeError(
            "Se abortó la integración: el schema de prueba debe empezar "
            f"con 'test_' y usar solo letras, números o guión bajo; "
            f"se recibió {schema!r}"
        )


def _eliminar_schema(conn: db.Connection, schema: str) -> None:
    """Borra el schema de prueba y todo su contenido."""
    _validar_schema_prueba(schema)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                sql.Identifier(schema)
            )
        )
    conn.commit()


@pytest.fixture
def esquema_prueba() -> Iterator[tuple[str, db.Connection]]:
    """Crea un schema único y garantiza su borrado al terminar el test."""
    assert URL_PRUEBA is not None
    schema = f"test_miralibros_{uuid.uuid4().hex[:12]}"
    _validar_schema_prueba(schema)

    with psycopg.connect(URL_PRUEBA) as conn:
        try:
            yield schema, conn
        finally:
            conn.rollback()
            _eliminar_schema(conn, schema)


def _escalar(conn: db.Connection, consulta: sql.Composable) -> Any:
    """Ejecuta una consulta escalar y libera la transacción implícita."""
    with conn.cursor() as cur:
        cur.execute(consulta)
        fila = cur.fetchone()
    conn.commit()
    assert fila is not None
    return fila[0]


def _contar(conn: db.Connection, schema: str, tabla: str) -> int:
    consulta = sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(schema, tabla)
    )
    return int(_escalar(conn, consulta))


def _claves_duplicadas(
    conn: db.Connection,
    schema: str,
    tabla: str,
    columnas: Sequence[str],
) -> list[tuple[Any, ...]]:
    columnas_sql = sql.SQL(", ").join(
        sql.Identifier(columna) for columna in columnas
    )
    consulta = sql.SQL(
        "SELECT {columnas} FROM {tabla} "
        "GROUP BY {columnas} HAVING count(*) > 1"
    ).format(
        columnas=columnas_sql,
        tabla=sql.Identifier(schema, tabla),
    )
    with conn.cursor() as cur:
        cur.execute(consulta)
        filas = cur.fetchall()
    conn.commit()
    return [tuple(fila) for fila in filas]


def _cargar_stock(
    conn: db.Connection,
    schema: str,
    ruta: str | Path,
) -> None:
    """Replica el ``run`` de stock: transformar, deduplicar, upsert e historial."""
    dataframe = read_excel_as_str(ruta)
    filas, _ = stock.transform(
        dataframe,
        source_file=Path(ruta).name,
        loaded_at=datetime.now(timezone.utc),
    )
    filas, _ = stock.deduplicate(filas)

    db.execute_sql_file(conn, stock.SQL_FILE, schema=schema)
    with conn.transaction():
        db.upsert_table(
            conn,
            schema,
            stock.TABLE,
            stock.COLUMNS,
            stock.KEY_COLUMNS,
            filas,
            update_columns=stock.UPDATE_COLUMNS,
            temp_table="stock_staging",
        )
        stock._refresh_history(conn, schema)


def _cargar_ventas(
    conn: db.Connection,
    schema: str,
    rutas: Sequence[str | Path],
) -> None:
    """Replica el ``run`` de ventas: transformar, deduplicar y hacer upsert."""
    loaded_at = datetime.now(timezone.utc)
    validas: list[tuple[Any, ...]] = []
    for ruta in rutas:
        dataframe = read_excel_as_str(ruta)
        filas, _ = ventas.transform(
            dataframe,
            origen=ventas.inferir_origen(ruta),
            source_file=Path(ruta).name,
            loaded_at=loaded_at,
        )
        validas.extend(filas)
    filas, _ = ventas.deduplicate(validas)

    db.execute_sql_file(conn, ventas.SQL_FILE, schema=schema)
    db.upsert_table(
        conn,
        schema,
        ventas.TABLE,
        ventas.COLUMNS,
        ventas.KEY_COLUMNS,
        filas,
        update_columns=ventas.UPDATE_COLUMNS,
        temp_table="ventas_staging",
    )


def _copia_stock_con_pvp(
    origen: Path,
    destino: Path,
    isbn: str,
    nuevo_pvp: int,
) -> Path:
    """Copia el crudo a tmp_path y cambia el PVP de un solo ISBN."""
    dataframe = read_excel_as_str(origen)
    mascara = dataframe["ISBN"].map(stock.normalize_isbn) == isbn
    assert mascara.any(), f"el ISBN {isbn!r} no está en {origen}"
    indice = dataframe.index[mascara][0]
    dataframe.loc[indice, "PVP"] = str(nuevo_pvp)
    dataframe.to_excel(destino, index=False, engine="openpyxl")
    return destino


def _copia_ventas_con_total(
    origen: Path,
    destino: Path,
    correlativo: int,
    nuevo_total: int,
) -> Path:
    """Copia el crudo a tmp_path y cambia el total de un solo correlativo."""
    dataframe = read_excel_as_str(origen)
    mascara = (
        dataframe["Correlativo"].astype(str).str.strip() == str(correlativo)
    )
    assert mascara.any(), f"el correlativo {correlativo!r} no está en {origen}"
    indice = dataframe.index[mascara][0]
    dataframe.loc[indice, "Total Venta"] = str(nuevo_total)
    dataframe.to_excel(destino, index=False, engine="openpyxl")
    return destino


def _versiones_stock(
    conn: db.Connection,
    schema: str,
    isbn: str,
) -> list[tuple[Any, ...]]:
    consulta = sql.SQL(
        "SELECT is_current, pvp, valid_to FROM {} "
        "WHERE isbn = {} ORDER BY id"
    ).format(
        sql.Identifier(schema, "stock_historial"),
        sql.Literal(isbn),
    )
    with conn.cursor() as cur:
        cur.execute(consulta)
        filas = cur.fetchall()
    conn.commit()
    return [tuple(fila) for fila in filas]


def _pvp_stock(conn: db.Connection, schema: str, isbn: str) -> Any:
    consulta = sql.SQL("SELECT pvp FROM {} WHERE isbn = {}").format(
        sql.Identifier(schema, "stock"),
        sql.Literal(isbn),
    )
    return _escalar(conn, consulta)


def _total_venta(
    conn: db.Connection,
    schema: str,
    origen: str,
    correlativo: int,
) -> Any:
    consulta = sql.SQL(
        "SELECT total_venta FROM {} "
        "WHERE origen = {} AND correlativo = {}"
    ).format(
        sql.Identifier(schema, "ventas"),
        sql.Literal(origen),
        sql.Literal(correlativo),
    )
    return _escalar(conn, consulta)


def _contar_origen(conn: db.Connection, schema: str, origen: str) -> int:
    consulta = sql.SQL("SELECT count(*) FROM {} WHERE origen = {}").format(
        sql.Identifier(schema, "ventas"),
        sql.Literal(origen),
    )
    return int(_escalar(conn, consulta))


class TestStockIntegracion:
    """Idempotencia y SCD2 de stock sobre una base desechable."""

    def test_dos_cargas_identicas_no_duplican_ni_crean_historial(
        self,
        esquema_prueba: tuple[str, db.Connection],
        archivos_raw: dict[str, Path],
    ) -> None:
        schema, conn = esquema_prueba

        _cargar_stock(conn, schema, archivos_raw["stock"])
        assert _contar(conn, schema, "stock") == 82
        historial_inicial = _contar(conn, schema, "stock_historial")
        assert historial_inicial == 82

        _cargar_stock(conn, schema, archivos_raw["stock"])

        assert _contar(conn, schema, "stock") == 82
        assert _contar(conn, schema, "stock_historial") == historial_inicial
        assert (
            _claves_duplicadas(conn, schema, "stock", stock.KEY_COLUMNS)
            == []
        )
        vigentes = sql.SQL(
            "SELECT count(*) FROM {} WHERE is_current"
        ).format(sql.Identifier(schema, "stock_historial"))
        assert int(_escalar(conn, vigentes)) == 82

    def test_un_cambio_actualiza_y_abre_version_nueva(
        self,
        esquema_prueba: tuple[str, db.Connection],
        archivos_raw: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        schema, conn = esquema_prueba
        _cargar_stock(conn, schema, archivos_raw["stock"])
        _cargar_stock(conn, schema, archivos_raw["stock"])

        dataframe = read_excel_as_str(archivos_raw["stock"])
        filas, _ = stock.transform(
            dataframe,
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=datetime.now(timezone.utc),
        )
        isbn = filas[0][0]
        pvp_anterior = filas[0][5]
        nuevo_pvp = pvp_anterior + 1000

        copia = _copia_stock_con_pvp(
            archivos_raw["stock"],
            tmp_path / "STOCK LIBRERIA.xlsx",
            isbn,
            nuevo_pvp,
        )
        _cargar_stock(conn, schema, copia)

        assert _contar(conn, schema, "stock") == 82
        assert _contar(conn, schema, "stock_historial") == 83
        assert _pvp_stock(conn, schema, isbn) == nuevo_pvp

        versiones = _versiones_stock(conn, schema, isbn)
        assert len(versiones) == 2
        actuales = [version for version in versiones if version[0] is True]
        cerradas = [version for version in versiones if version[0] is False]
        assert len(actuales) == 1
        assert len(cerradas) == 1
        assert actuales[0][1] == nuevo_pvp
        assert actuales[0][2] is None
        assert cerradas[0][2] is not None


class TestVentasIntegracion:
    """Idempotencia y actualización de ventas sobre una base desechable."""

    def test_dos_cargas_identicas_mantienen_conteos(
        self,
        esquema_prueba: tuple[str, db.Connection],
        archivos_raw: dict[str, Path],
    ) -> None:
        schema, conn = esquema_prueba
        rutas = [
            archivos_raw["ventas_libreria"],
            archivos_raw["ventas_cafeteria"],
        ]

        _cargar_ventas(conn, schema, rutas)
        assert _contar(conn, schema, "ventas") == 207

        _cargar_ventas(conn, schema, rutas)

        assert _contar(conn, schema, "ventas") == 207
        assert (
            _claves_duplicadas(
                conn, schema, "ventas", ventas.KEY_COLUMNS
            )
            == []
        )
        assert _contar_origen(conn, schema, "LIBRERIA") == 137
        assert _contar_origen(conn, schema, "CAFETERIA") == 70

    def test_un_cambio_actualiza_la_fila_sin_duplicar(
        self,
        esquema_prueba: tuple[str, db.Connection],
        archivos_raw: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        schema, conn = esquema_prueba
        rutas = [
            archivos_raw["ventas_libreria"],
            archivos_raw["ventas_cafeteria"],
        ]
        _cargar_ventas(conn, schema, rutas)
        _cargar_ventas(conn, schema, rutas)

        dataframe = read_excel_as_str(archivos_raw["ventas_libreria"])
        filas, _ = ventas.transform(
            dataframe,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
            loaded_at=datetime.now(timezone.utc),
        )
        correlativo = filas[0][1]
        total_anterior = filas[0][8]
        nuevo_total = total_anterior + 1000

        copia = _copia_ventas_con_total(
            archivos_raw["ventas_libreria"],
            tmp_path / "VENTAS LIBRERIA.xlsx",
            correlativo,
            nuevo_total,
        )
        _cargar_ventas(conn, schema, [copia])

        assert _contar(conn, schema, "ventas") == 207
        assert (
            _claves_duplicadas(
                conn, schema, "ventas", ventas.KEY_COLUMNS
            )
            == []
        )
        assert (
            _total_venta(conn, schema, "LIBRERIA", correlativo)
            == nuevo_total
        )
