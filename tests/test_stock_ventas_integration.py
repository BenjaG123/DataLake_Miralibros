"""Integración real de ``stock`` y ``ventas`` contra PostgreSQL.

Se omiten por defecto. Solo corren si existe ``TEST_DATABASE_URL`` (nunca se
usa ``DATABASE_URL``). Cada test crea un schema único ``test_miralibros_*``,
carga dos veces los crudos sintéticos y lo elimina con ``DROP SCHEMA CASCADE``
en el ``finally``. El schema de prueba nunca se toma de ``PG_SCHEMA``.

La carga de ventas replica el pipeline: clientes primero por la FK de
``ventas_ticket.rut_cliente``, luego cabeceras y líneas exactas.
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
from etl.pipelines import clientes, stock, ventas
from etl.quarantine import QUARANTINE_SQL, insert_quarantine
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


def _filas(
    conn: db.Connection,
    consulta: sql.Composable,
) -> list[tuple[Any, ...]]:
    """Ejecuta una consulta y devuelve todas las filas."""
    with conn.cursor() as cur:
        cur.execute(consulta)
        filas = cur.fetchall()
    conn.commit()
    return [tuple(fila) for fila in filas]


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
    return _filas(conn, consulta)


def _cargar_clientes(
    conn: db.Connection,
    schema: str,
    ruta: str | Path,
) -> None:
    """Carga clientes antes de ventas porque la FK lo exige."""
    dataframe = read_excel_as_str(ruta)
    filas, _ = clientes.transform(dataframe)

    db.execute_sql_file(conn, clientes.SQL_FILE, schema=schema)
    db.replace_table(
        conn,
        schema,
        clientes.TABLE,
        clientes.COLUMNS,
        filas,
    )


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
    *,
    clientes_path: str | Path,
    stock_path: str | Path,
    catalogo_cafeteria_path: str | Path,
) -> tuple[int, int]:
    """Replica el ``run`` de ventas con cabecera, líneas y cuarentena."""
    loaded_at = datetime.now(timezone.utc)
    validas: list[ventas.Ticket] = []
    rechazadas: list[dict[str, Any]] = []

    for ruta in rutas:
        dataframe = read_excel_as_str(ruta)
        filas, rechazos = ventas.transform(
            dataframe,
            origen=ventas.inferir_origen(ruta),
            source_file=Path(ruta).name,
            loaded_at=loaded_at,
        )
        validas.extend(filas)
        rechazadas.extend(rechazos)

    filas, _ = ventas.deduplicate(validas)

    origins = {ticket.origen for ticket in filas}
    catalogos: dict[str, ventas.PriceCatalog] = {}
    if "LIBRERIA" in origins:
        catalogos["LIBRERIA"] = ventas.load_stock_catalog(
            stock_path,
            loaded_at=loaded_at,
        )
    if "CAFETERIA" in origins:
        catalogos["CAFETERIA"] = ventas.load_cafeteria_catalog(
            catalogo_cafeteria_path
        )

    max_n = max((ticket.cant_items for ticket in filas), default=0)
    max_x = max((ticket.total_venta for ticket in filas), default=0)
    reachable = {
        origen: ventas.compute_reachable(catalogo.prices, max_n, max_x)
        for origen, catalogo in catalogos.items()
    }

    viables: list[ventas.Ticket] = []
    lineas_por_ticket: dict[
        tuple[str, int], list[ventas.Line]
    ] = {}
    rechazadas_linea: list[dict[str, Any]] = []
    for ticket in filas:
        catalogo = catalogos.get(ticket.origen)
        lineas = (
            ventas.build_lines(
                ticket,
                catalogo,
                reachable[ticket.origen],
                loaded_at=loaded_at,
            )
            if catalogo is not None
            else None
        )
        if lineas is None:
            rechazadas_linea.append(ventas._line_rejection(ticket))
            continue
        viables.append(ticket)
        lineas_por_ticket[(ticket.origen, ticket.correlativo)] = lineas

    clients = ventas.load_clientes(clientes_path) if viables else []
    assignments = ventas.assign_clients(viables, clients)
    todas_rechazadas = [*rechazadas, *rechazadas_linea]
    cuarentena = ventas._quarantine_by_file(todas_rechazadas)

    db.execute_sql_file(conn, QUARANTINE_SQL, schema=schema)
    db.execute_sql_file(conn, ventas.SQL_FILE, schema=schema)
    with conn.transaction():
        ticket_rows = [
            ticket.to_db_row(
                assignments.get((ticket.origen, ticket.correlativo))
            )
            for ticket in viables
        ]
        db.upsert_table(
            conn,
            schema,
            ventas.TICKET_TABLE,
            ventas.TICKET_COLUMNS,
            ventas.KEY_COLUMNS,
            ticket_rows,
            update_columns=ventas.TICKET_UPDATE_COLUMNS,
            temp_table="ventas_ticket_staging",
        )
        ventas._delete_lines_for_batch(conn, schema, viables)
        line_rows = [
            line.to_db_row()
            for ticket in viables
            for line in lineas_por_ticket[
                (ticket.origen, ticket.correlativo)
            ]
        ]
        db.copy_rows(
            conn,
            schema,
            ventas.LINE_TABLE,
            ventas.LINE_COLUMNS,
            line_rows,
        )
        for archivo, filas_cuarentena in cuarentena.items():
            insert_quarantine(
                conn,
                schema,
                ventas.ENTITY,
                archivo,
                filas_cuarentena,
            )

    return len(viables), len(line_rows)


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
    return _filas(conn, consulta)


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
        sql.Identifier(schema, "ventas_ticket"),
        sql.Literal(origen),
        sql.Literal(correlativo),
    )
    return _escalar(conn, consulta)


def _contar_origen(conn: db.Connection, schema: str, origen: str) -> int:
    consulta = sql.SQL(
        "SELECT count(*) FROM {} WHERE origen = {}"
    ).format(
        sql.Identifier(schema, "ventas_ticket"),
        sql.Literal(origen),
    )
    return int(_escalar(conn, consulta))


def _compras_incorrectas(
    conn: db.Connection,
    schema: str,
) -> list[tuple[Any, ...]]:
    consulta = sql.SQL(
        "SELECT c.rut, c.compras, COALESCE(SUM(t.total_venta), 0) "
        "FROM {clientes} AS c "
        "LEFT JOIN {tickets} AS t ON t.rut_cliente = c.rut "
        "WHERE c.compras > 0 "
        "GROUP BY c.rut, c.compras "
        "HAVING COALESCE(SUM(t.total_venta), 0) <> c.compras"
    ).format(
        clientes=sql.Identifier(schema, "clientes"),
        tickets=sql.Identifier(schema, "ventas_ticket"),
    )
    return _filas(conn, consulta)


def _lineas_descuadradas(
    conn: db.Connection,
    schema: str,
) -> list[tuple[Any, ...]]:
    consulta = sql.SQL(
        "SELECT t.origen, t.correlativo "
        "FROM {tickets} AS t "
        "JOIN ("
        "  SELECT origen, correlativo, SUM(cantidad) AS cantidad, "
        "         SUM(total_linea) AS total "
        "  FROM {lineas} GROUP BY origen, correlativo"
        ") AS l USING (origen, correlativo) "
        "WHERE l.cantidad <> t.cant_items "
        "   OR l.total <> t.total_venta"
    ).format(
        tickets=sql.Identifier(schema, "ventas_ticket"),
        lineas=sql.Identifier(schema, "ventas_linea"),
    )
    return _filas(conn, consulta)


def _tickets_sin_lineas(conn: db.Connection, schema: str) -> int:
    consulta = sql.SQL(
        "SELECT count(*) "
        "FROM {tickets} AS t "
        "LEFT JOIN {lineas} AS l "
        "  ON l.origen = t.origen AND l.correlativo = t.correlativo "
        "WHERE l.origen IS NULL"
    ).format(
        tickets=sql.Identifier(schema, "ventas_ticket"),
        lineas=sql.Identifier(schema, "ventas_linea"),
    )
    return int(_escalar(conn, consulta))


def _tickets_fuera_de_alta(conn: db.Connection, schema: str) -> int:
    consulta = sql.SQL(
        "SELECT count(*) "
        "FROM {tickets} AS t "
        "JOIN {clientes} AS c ON c.rut = t.rut_cliente "
        "WHERE c.alta IS NOT NULL AND t.fecha < c.alta"
    ).format(
        tickets=sql.Identifier(schema, "ventas_ticket"),
        clientes=sql.Identifier(schema, "clientes"),
    )
    return int(_escalar(conn, consulta))


def _contar_anonimos(conn: db.Connection, schema: str) -> int:
    consulta = sql.SQL(
        "SELECT count(*) FROM {} WHERE rut_cliente IS NULL"
    ).format(sql.Identifier(schema, "ventas_ticket"))
    return int(_escalar(conn, consulta))


def _definiciones_fk(
    conn: db.Connection,
    schema: str,
    tabla: str,
) -> list[tuple[str, str]]:
    consulta = sql.SQL(
        "SELECT conname, pg_get_constraintdef(oid) "
        "FROM pg_constraint "
        "WHERE conrelid = {}::regclass AND contype = 'f'"
    ).format(sql.Literal(f"{schema}.{tabla}"))
    return [(str(nombre), str(definicion)) for nombre, definicion in
            _filas(conn, consulta)]


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
    """Cabecera/detalle, compras, líneas y FKs en una base desechable."""

    def _cargar_todo(
        self,
        conn: db.Connection,
        schema: str,
        archivos_raw: dict[str, Path],
        rutas: Sequence[str | Path],
        *,
        cargar_clientes: bool = True,
    ) -> tuple[int, int]:
        if cargar_clientes:
            _cargar_clientes(conn, schema, archivos_raw["clientes"])
        return _cargar_ventas(
            conn,
            schema,
            rutas,
            clientes_path=archivos_raw["clientes"],
            stock_path=archivos_raw["stock"],
            catalogo_cafeteria_path=archivos_raw["catalogo_cafeteria"],
        )

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

        viables, lineas = self._cargar_todo(
            conn,
            schema,
            archivos_raw,
            rutas,
        )
        assert viables == 207
        assert lineas > 0
        assert _contar(conn, schema, "ventas_ticket") == 207
        assert _contar(conn, schema, "ventas_linea") == lineas

        self._cargar_todo(
            conn,
            schema,
            archivos_raw,
            rutas,
            cargar_clientes=False,
        )

        assert _contar(conn, schema, "ventas_ticket") == 207
        assert _contar(conn, schema, "ventas_linea") == lineas
        assert (
            _claves_duplicadas(
                conn,
                schema,
                "ventas_ticket",
                ventas.KEY_COLUMNS,
            )
            == []
        )
        assert (
            _claves_duplicadas(
                conn,
                schema,
                "ventas_linea",
                ("origen", "correlativo", "linea"),
            )
            == []
        )
        assert _contar_origen(conn, schema, "LIBRERIA") == 137
        assert _contar_origen(conn, schema, "CAFETERIA") == 70

    def test_compras_y_lineas_cuadran(
        self,
        esquema_prueba: tuple[str, db.Connection],
        archivos_raw: dict[str, Path],
    ) -> None:
        schema, conn = esquema_prueba
        self._cargar_todo(
            conn,
            schema,
            archivos_raw,
            [
                archivos_raw["ventas_libreria"],
                archivos_raw["ventas_cafeteria"],
            ],
        )

        assert _compras_incorrectas(conn, schema) == []
        assert _lineas_descuadradas(conn, schema) == []
        assert _tickets_sin_lineas(conn, schema) == 0
        assert _tickets_fuera_de_alta(conn, schema) == 0
        assert _contar_anonimos(conn, schema) > 0

        con_rut = sql.SQL(
            "SELECT count(*) FROM {} WHERE rut_cliente IS NOT NULL"
        ).format(sql.Identifier(schema, "ventas_ticket"))
        assert int(_escalar(conn, con_rut)) == 22

    def test_fk_de_rut_y_cascada_de_lineas(
        self,
        esquema_prueba: tuple[str, db.Connection],
        archivos_raw: dict[str, Path],
    ) -> None:
        schema, conn = esquema_prueba
        self._cargar_todo(
            conn,
            schema,
            archivos_raw,
            [
                archivos_raw["ventas_libreria"],
                archivos_raw["ventas_cafeteria"],
            ],
        )

        fk_ticket = _definiciones_fk(conn, schema, "ventas_ticket")
        fk_linea = _definiciones_fk(conn, schema, "ventas_linea")

        assert any(
            "FOREIGN KEY (rut_cliente)" in definicion
            and "clientes(rut)" in definicion
            for _, definicion in fk_ticket
        )
        assert any(
            "FOREIGN KEY (origen, correlativo)" in definicion
            and "ventas_ticket(origen, correlativo)" in definicion
            and "ON DELETE CASCADE" in definicion
            for _, definicion in fk_linea
        )
        assert not any(
            "codigo_interno" in definicion
            for _, definicion in fk_linea
        )

        clave = _filas(
            conn,
            sql.SQL(
                "SELECT origen, correlativo FROM {} AS t "
                "WHERE EXISTS (SELECT 1 FROM {} AS l "
                "WHERE l.origen = t.origen "
                "AND l.correlativo = t.correlativo) "
                "ORDER BY origen, correlativo LIMIT 1"
            ).format(
                sql.Identifier(schema, "ventas_ticket"),
                sql.Identifier(schema, "ventas_linea"),
            ),
        )
        assert clave
        origen, correlativo = clave[0]
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL(
                        "DELETE FROM {} WHERE origen = %s AND correlativo = %s"
                    ).format(sql.Identifier(schema, "ventas_ticket")),
                    (origen, correlativo),
                )
                cur.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {} "
                        "WHERE origen = %s AND correlativo = %s"
                    ).format(sql.Identifier(schema, "ventas_linea")),
                    (origen, correlativo),
                )
                restantes = cur.fetchone()
        assert restantes is not None
        assert int(restantes[0]) == 0

    def test_un_cambio_actualiza_la_cabecera_sin_duplicar(
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
        self._cargar_todo(conn, schema, archivos_raw, rutas)
        self._cargar_todo(
            conn,
            schema,
            archivos_raw,
            rutas,
            cargar_clientes=False,
        )

        catalogo = ventas.load_stock_catalog(
            archivos_raw["stock"],
            loaded_at=datetime.now(timezone.utc),
        )
        dataframe = read_excel_as_str(archivos_raw["ventas_libreria"])
        filas, _ = ventas.transform(
            dataframe,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
            loaded_at=datetime.now(timezone.utc),
        )
        ticket = filas[0]
        nuevo_total = None
        for precio in reversed(catalogo.prices):
            candidato = precio * ticket.cant_items
            if candidato != ticket.total_venta:
                nuevo_total = candidato
                break
        assert nuevo_total is not None

        copia = _copia_ventas_con_total(
            archivos_raw["ventas_libreria"],
            tmp_path / "VENTAS LIBRERIA.xlsx",
            ticket.correlativo,
            nuevo_total,
        )
        self._cargar_todo(
            conn,
            schema,
            archivos_raw,
            [copia],
            cargar_clientes=False,
        )

        assert _contar(conn, schema, "ventas_ticket") == 207
        assert (
            _claves_duplicadas(
                conn,
                schema,
                "ventas_ticket",
                ventas.KEY_COLUMNS,
            )
            == []
        )
        assert (
            _total_venta(
                conn,
                schema,
                "LIBRERIA",
                ticket.correlativo,
            )
            == nuevo_total
        )
        assert _lineas_descuadradas(conn, schema) == []
