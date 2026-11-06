"""Transformación de stock y ventas sobre ``tests/fixtures/raw/``.

Los conteos y motivos son el contrato acordado con los pipelines; no se fijan
detalles accidentales de los fixtures (títulos, montos ni correlativos).
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from etl.pipelines import stock, ventas
from etl.readers import read_excel_as_str

CARGA = datetime(2024, 1, 1, tzinfo=timezone.utc)
ISBN_VALIDO = "9789561234567"


def _fila_stock(**cambios: Any) -> dict[str, Any]:
    """Fila cruda mínima que cumple el contrato de stock."""
    fila: dict[str, Any] = {
        "ISBN": ISBN_VALIDO,
        "Titulo": "Libro sintético",
        "Autor": "Autora Ficticia",
        "Editorial": "Editorial de Prueba",
        "Clasificación": "Novela",
        "PVP": "15.000",
        "Stock Consig.": "3",
        "Stock Propio": "5",
        "Stock Total": "8",
        "Ventas": "2",
    }
    fila.update(cambios)
    return fila


def _fila_venta(**cambios: Any) -> dict[str, Any]:
    """Fila cruda mínima que cumple el contrato de ventas."""
    fila: dict[str, Any] = {
        "Correlativo": "1",
        "Orden Web": "",
        "Fecha": "2024-01-02",
        "Medio de Pago": "Efectivo",
        "Tipo Documento": "Boleta",
        "Folio": "100",
        "Voucher Máquina de Pago": "",
        "Total Venta": "15000",
        "Cant. Items": "2",
        "Canal": "Local",
        "Feria": "",
        "Tiene Descuento": "no",
        "Estado": "Emitida",
        "Es Dev.": "N",
    }
    fila.update(cambios)
    return fila


def _lote_ventas(
    ruta: Path,
) -> tuple[int, list[ventas.Ticket], list[dict[str, Any]]]:
    """Lee y transforma un archivo de ventas infiriendo su origen."""
    dataframe = read_excel_as_str(ruta)
    filas, rechazadas = ventas.transform(
        dataframe,
        origen=ventas.inferir_origen(ruta),
        source_file=Path(ruta).name,
        loaded_at=CARGA,
    )
    return len(dataframe), filas, rechazadas


class TestStockLote:
    """Volumen, motivos y contrato de las filas válidas."""

    def test_volumen_y_motivos(
        self, archivos_raw: dict[str, Path]
    ) -> None:
        dataframe = read_excel_as_str(archivos_raw["stock"])
        filas, rechazadas = stock.transform(
            dataframe,
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        assert len(dataframe) == 100
        assert len(filas) == 82
        assert len(rechazadas) == 18

        motivos = Counter(
            motivo
            for fila in rechazadas
            for motivo in fila["motivo"].split("; ")
        )
        assert motivos == Counter(
            {
                "ISBN inválido o vacío": 8,
                "stock total inválido o negativo": 10,
            }
        )

    def test_filas_validas_cumplen_el_contrato(
        self, archivos_raw: dict[str, Path]
    ) -> None:
        dataframe = read_excel_as_str(archivos_raw["stock"])
        filas, _ = stock.transform(
            dataframe,
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        for fila in filas:
            assert len(fila) == len(stock.COLUMNS)
            (
                isbn,
                _titulo,
                _autor,
                _editorial,
                _clasificacion,
                pvp,
                consignacion,
                propio,
                total,
                ventas_unidades,
            ) = fila[:10]
            assert re.fullmatch(r"(?:[0-9]{13}|[0-9]{9}[0-9X])", isbn)
            assert pvp >= 100
            assert consignacion >= 0
            assert propio >= 0
            assert total == consignacion + propio
            assert ventas_unidades >= 0


class TestStockFila:
    """Ramas puntuales de una fila de stock."""

    def test_pvp_con_punto_de_miles(self) -> None:
        resultado, motivo = stock.transform_row(
            _fila_stock(PVP="15.000"),
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        assert motivo is None
        assert resultado is not None
        assert resultado[5] == 15000

    def test_stock_total_negativo_se_rechaza(self) -> None:
        resultado, motivo = stock.transform_row(
            _fila_stock(**{"Stock Total": "-1"}),
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        assert resultado is None
        assert motivo == "stock total inválido o negativo"

    def test_isbn_con_guiones_se_normaliza(self) -> None:
        resultado, motivo = stock.transform_row(
            _fila_stock(ISBN="978-956-12-3456-7"),
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        assert motivo is None
        assert resultado is not None
        assert resultado[0] == ISBN_VALIDO
        assert stock.normalize_isbn("978-956-12-3456-7") == ISBN_VALIDO

    def test_isbn_invalido_retorna_none(self) -> None:
        assert stock.normalize_isbn("123") is None
        assert stock.normalize_isbn("") is None


class TestStockDeduplicacion:
    """El upsert exige una sola fila por ISBN."""

    def test_conserva_la_ultima_fila(self) -> None:
        primera, _ = stock.transform_row(
            _fila_stock(PVP="10.000"),
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )
        segunda, _ = stock.transform_row(
            _fila_stock(PVP="20.000"),
            source_file="STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        )
        assert primera is not None and segunda is not None

        filas, duplicadas = stock.deduplicate([primera, segunda])

        assert duplicadas == 1
        assert len(filas) == 1
        assert filas[0][5] == 20000


class TestVentasLote:
    """Volumen global y por archivo, con los motivos declarados."""

    def test_volumen_global(
        self, archivos_raw: dict[str, Path]
    ) -> None:
        libreria = _lote_ventas(archivos_raw["ventas_libreria"])
        cafeteria = _lote_ventas(archivos_raw["ventas_cafeteria"])

        assert libreria[0] + cafeteria[0] == 240
        assert len(libreria[1]) + len(cafeteria[1]) == 207
        assert len(libreria[2]) + len(cafeteria[2]) == 33

    def test_volumen_por_archivo(
        self, archivos_raw: dict[str, Path]
    ) -> None:
        libreria = _lote_ventas(archivos_raw["ventas_libreria"])
        cafeteria = _lote_ventas(archivos_raw["ventas_cafeteria"])

        assert (libreria[0], len(libreria[1]), len(libreria[2])) == (
            155,
            137,
            18,
        )
        assert (cafeteria[0], len(cafeteria[1]), len(cafeteria[2])) == (
            85,
            70,
            15,
        )

    def test_motivos_globales(
        self, archivos_raw: dict[str, Path]
    ) -> None:
        rechazadas = [
            *_lote_ventas(archivos_raw["ventas_libreria"])[2],
            *_lote_ventas(archivos_raw["ventas_cafeteria"])[2],
        ]

        motivos = Counter(
            motivo
            for fila in rechazadas
            for motivo in fila["motivo"].split("; ")
        )
        assert motivos == Counter(
            {
                "total venta inválido o negativo": 12,
                "cantidad de ítems inválida": 11,
                "folio inválido o vacío": 6,
                "tipo documento sin documento": 6,
            }
        )


class TestVentasFila:
    """Ramas puntuales de una fila de ventas."""

    @pytest.mark.parametrize(
        ("nombre", "esperado"),
        [
            ("VENTAS LIBRERIA.xlsx", "LIBRERIA"),
            ("VENTAS CAFETERIA.xlsx", "CAFETERIA"),
            ("ventas cafetería.xlsx", "CAFETERIA"),
        ],
    )
    def test_inferir_origen(self, nombre: str, esperado: str) -> None:
        assert ventas.inferir_origen(nombre) == esperado

    def test_inferir_origen_falla_sin_marca(self) -> None:
        with pytest.raises(ValueError, match="No se pudo inferir el origen"):
            ventas.inferir_origen("VENTAS 2024.xlsx")

    @pytest.mark.parametrize(
        ("columna", "valor", "motivo_esperado"),
        [
            ("Total Venta", None, "total venta inválido o negativo"),
            ("Total Venta", "-1", "total venta inválido o negativo"),
            ("Cant. Items", None, "cantidad de ítems inválida"),
            ("Cant. Items", "-1", "cantidad de ítems inválida"),
        ],
    )
    def test_total_y_cantidad_invalidos(
        self, columna: str, valor: object, motivo_esperado: str
    ) -> None:
        resultado, motivo = ventas.transform_row(
            _fila_venta(**{columna: valor}),
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        assert resultado is None
        assert motivo == motivo_esperado

    @pytest.mark.parametrize(
        ("valor", "esperado"),
        [
            ("SI", "SI"),
            ("sí", "SI"),
            ("S", "SI"),
            ("NO", "NO"),
            ("n", "NO"),
        ],
    )
    def test_flags_normalizados(self, valor: str, esperado: str) -> None:
        resultado, motivo = ventas.transform_row(
            _fila_venta(**{"Tiene Descuento": valor, "Es Dev.": valor}),
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
            loaded_at=CARGA,
        )

        assert motivo is None
        assert resultado is not None
        assert resultado.tiene_descuento == esperado
        assert resultado.es_devolucion == esperado


class TestVentasDeduplicacion:
    """La clave de negocio es ``(origen, correlativo)``."""

    def test_conserva_la_ultima_por_clave(self) -> None:
        primera, _ = ventas.transform_row(
            _fila_venta(**{"Total Venta": "1000"}),
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
            loaded_at=CARGA,
        )
        segunda, _ = ventas.transform_row(
            _fila_venta(**{"Total Venta": "2000"}),
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
            loaded_at=CARGA,
        )
        otra_origen, _ = ventas.transform_row(
            _fila_venta(**{"Total Venta": "3000"}),
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
            loaded_at=CARGA,
        )
        assert primera is not None
        assert segunda is not None
        assert otra_origen is not None

        filas, duplicadas = ventas.deduplicate(
            [primera, segunda, otra_origen]
        )

        assert duplicadas == 1
        assert len(filas) == 2
        por_clave = {
            (fila.origen, fila.correlativo): fila for fila in filas
        }
        assert por_clave[("LIBRERIA", 1)].total_venta == 2000
        assert por_clave[("CAFETERIA", 1)].total_venta == 3000


@contextmanager
def _conexion_prohibida(*args: Any, **kwargs: Any) -> Iterator[object]:
    """Doble que falla si un dry-run intenta conectarse a PostgreSQL."""
    raise AssertionError("el pipeline intentó conectarse a PostgreSQL")
    yield  # pragma: no cover


class TestSinBaseDeDatos:
    """``--dry-run`` lee y transforma, pero no abre conexión."""

    def test_stock_dry_run_no_toca_la_base(
        self, monkeypatch: pytest.MonkeyPatch, archivos_raw: dict[str, Path]
    ) -> None:
        monkeypatch.setattr(stock.db, "connection", _conexion_prohibida)

        stock.run(archivos_raw["stock"], dry_run=True)

    def test_ventas_dry_run_no_toca_la_base(
        self, monkeypatch: pytest.MonkeyPatch, archivos_raw: dict[str, Path]
    ) -> None:
        monkeypatch.setattr(ventas.db, "connection", _conexion_prohibida)

        ventas.run(
            [
                archivos_raw["ventas_libreria"],
                archivos_raw["ventas_cafeteria"],
            ],
            clientes_path=archivos_raw["clientes"],
            stock_path=archivos_raw["stock"],
            catalogo_cafeteria_path=archivos_raw["catalogo_cafeteria"],
            dry_run=True,
        )
