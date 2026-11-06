"""Pruebas del aplanado y la persistencia de rechazos en cuarentena.

No se conecta a PostgreSQL: los tests de pipeline reemplazan ``db`` y
``insert_quarantine`` por dobles que solo registran lo que se enviaría.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from etl.pipelines import ventas
from etl.quarantine import flatten_rejections


def _rechazo_con_detalles() -> dict[str, Any]:
    """Rechazo de ventas con dos detalles, como los arma ``transform``."""
    return {
        "origen": "LIBRERIA",
        "correlativo": "5",
        "archivo_origen": "VENTAS LIBRERIA.xlsx",
        "fila": 7,
        "columna": "Folio",
        "valor_crudo": "0",
        "regla": "VENTAS-FOLIO-001",
        "severidad": "error",
        "motivo": "folio inválido o vacío; tipo documento sin documento",
        "detalles": [
            {
                "fila": 7,
                "columna": "Folio",
                "valor_crudo": "0",
                "regla": "VENTAS-FOLIO-001",
                "severidad": "error",
                "motivo": "folio inválido o vacío",
            },
            {
                "fila": 7,
                "columna": "Tipo Documento",
                "valor_crudo": "Sin Documento",
                "regla": "VENTAS-DOC-001",
                "severidad": "error",
                "motivo": "tipo documento sin documento",
            },
        ],
    }


class TestFlattenRejections:
    """El contrato de cuarentena usa una fila por cada detalle."""

    def test_aplana_cada_detalle(self) -> None:
        filas = flatten_rejections([_rechazo_con_detalles()])

        assert len(filas) == 2
        assert filas[0] == {
            "fila": 7,
            "columna": "Folio",
            "valor_crudo": "0",
            "regla": "VENTAS-FOLIO-001",
            "severidad": "error",
            "motivo": "folio inválido o vacío",
        }
        assert filas[1]["columna"] == "Tipo Documento"
        assert filas[1]["valor_crudo"] == "Sin Documento"
        assert filas[1]["regla"] == "VENTAS-DOC-001"

    def test_sin_detalles_usa_el_rechazo_completo(self) -> None:
        rechazo = {
            "fila": 3,
            "columna": "PVP",
            "valor_crudo": "99",
            "regla": "STOCK-PVP-001",
            "severidad": "error",
            "motivo": "PVP inválido o menor a 100",
        }

        assert flatten_rejections([rechazo]) == [rechazo]


@contextmanager
def _conexion_falsa(*args: Any, **kwargs: Any) -> Iterator[object]:
    """Conexión mínima: transacción y cursor para borrar líneas."""

    class _Cursor:
        rowcount = 0

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, *args: object, **kwargs: object) -> None:
            self.rowcount = 0

    class _Conexion:
        def transaction(self) -> object:
            return nullcontext()

        def cursor(self) -> _Cursor:
            return _Cursor()

    yield _Conexion()


def _dobles_de_carga(
    monkeypatch: pytest.MonkeyPatch,
) -> list[dict[str, Any]]:
    """Reemplaza la escritura real y devuelve los registros de cuarentena."""
    registros: list[dict[str, Any]] = []

    def execute_falso(
        conn: object, sql_path: Path, *, schema: str | None = None
    ) -> None:
        return None

    def upsert_falso(
        conn: object,
        schema: str,
        table: str,
        columns: object,
        keys: object,
        rows: Iterable[object],
        **kwargs: object,
    ) -> int:
        return len(list(rows))

    def copy_falso(
        conn: object,
        schema: str,
        table: str,
        columns: object,
        rows: Iterable[object],
    ) -> int:
        return len(list(rows))

    def cuarentena_falsa(
        conn: object,
        schema: str,
        entidad: str,
        archivo_origen: str,
        filas: Iterable[object],
    ) -> int:
        lista = list(filas)
        registros.append(
            {
                "entidad": entidad,
                "archivo_origen": archivo_origen,
                "filas": lista,
            }
        )
        return len(lista)

    monkeypatch.setenv("PG_SCHEMA", "test_silver")
    monkeypatch.setattr(ventas.db, "connection", _conexion_falsa)
    monkeypatch.setattr(ventas.db, "execute_sql_file", execute_falso)
    monkeypatch.setattr(ventas.db, "upsert_table", upsert_falso)
    monkeypatch.setattr(ventas.db, "copy_rows", copy_falso)
    monkeypatch.setattr(ventas, "insert_quarantine", cuarentena_falsa)
    return registros


class TestCuarentenaEnPipeline:
    """``ventas.run`` envía a cuarentena cabeceras y líneas rechazadas."""

    def test_ventas_envia_los_rechazos_de_cabecera(
        self,
        monkeypatch: pytest.MonkeyPatch,
        archivos_fixtures: dict[str, Path],
    ) -> None:
        registros = _dobles_de_carga(monkeypatch)

        ventas.run(
            [
                archivos_fixtures["ventas_libreria"],
                archivos_fixtures["ventas_cafeteria"],
            ],
            clientes_path=archivos_fixtures["clientes"],
            stock_path=archivos_fixtures["stock"],
            catalogo_cafeteria_path=archivos_fixtures[
                "catalogo_cafeteria"
            ],
        )

        assert {registro["entidad"] for registro in registros} == {"ventas"}
        assert {registro["archivo_origen"] for registro in registros} == {
            "VENTAS LIBRERIA.xlsx",
            "VENTAS CAFETERIA.xlsx",
        }
        filas = [
            fila
            for registro in registros
            for fila in registro["filas"]
        ]
        assert len(filas) == 35
        reglas = Counter(fila["regla"] for fila in filas)
        assert reglas["VENTAS-FOLIO-001"] == 6
        assert reglas["VENTAS-DOC-001"] == 6
        assert reglas["VENTAS-VALIDACION"] == 23
        assert reglas["VENTAS-LINEA-001"] == 0
        for fila in filas:
            assert set(fila) == {
                "fila",
                "columna",
                "valor_crudo",
                "regla",
                "severidad",
                "motivo",
            }

    def test_ventas_envia_los_rechazos_de_linea(
        self,
        monkeypatch: pytest.MonkeyPatch,
        archivos_fixtures: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        origen = archivos_fixtures["ventas_libreria"]
        dataframe = pd.read_excel(origen, dtype=str).head(1).copy()
        dataframe.loc[0, "Correlativo"] = "9999"
        dataframe.loc[0, "Folio"] = "9999"
        dataframe.loc[0, "Tipo Documento"] = "Boleta"
        dataframe.loc[0, "Total Venta"] = "1"
        dataframe.loc[0, "Cant. Items"] = "1"
        ruta = tmp_path / "VENTAS LIBRERIA EXTRA.xlsx"
        dataframe.to_excel(ruta, index=False, engine="openpyxl")

        registros = _dobles_de_carga(monkeypatch)

        ventas.run(
            [ruta],
            clientes_path=archivos_fixtures["clientes"],
            stock_path=archivos_fixtures["stock"],
            catalogo_cafeteria_path=archivos_fixtures[
                "catalogo_cafeteria"
            ],
        )

        filas = [
            fila
            for registro in registros
            for fila in registro["filas"]
        ]
        assert len(filas) == 1
        assert filas[0]["regla"] == "VENTAS-LINEA-001"
        assert filas[0]["columna"] == "total_venta"
        assert filas[0]["motivo"] == "sin combinación exacta de líneas"
