"""Pruebas de transformación del pipeline de stock."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

import pandas as pd
import pytest

from etl.pipelines.stock import (
    COLUMNS,
    Row,
    deduplicate,
    normalize_isbn,
    transform,
    transform_row,
)

LOADED_AT = datetime(2025, 1, 1, tzinfo=timezone.utc)
SOURCE_FILE = "STOCK LIBRERIA.xlsx"


def _fila_valida() -> dict[str, object]:
    return {
        "ISBN": "9788412345678",
        "Titulo": "Libro",
        "Autor": "Autor",
        "Editorial": "Editorial",
        "Clasificación": "Novela",
        "PVP": "15.000",
        "Stock Consig.": "2",
        "Stock Propio": "3",
        "Stock Total": "5",
        "Ventas": "1",
    }


def _transformar(cambios: dict[str, object]) -> tuple[Row | None, str | None]:
    fila = _fila_valida()
    fila.update(cambios)
    return transform_row(
        fila,
        source_file=SOURCE_FILE,
        loaded_at=LOADED_AT,
    )


class TestNormalizeIsbn:
    """Normalización de ISBN-13 e ISBN-10."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("978-84-1234-567-8", "9788412345678"),
            ("978 84 1234 567 8", "9788412345678"),
            ("978.84.1234.567.8", "9788412345678"),
            ("123456789X", "123456789X"),
            ("1234567890", "1234567890"),
        ],
    )
    def test_normalize_isbn_valido(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_isbn(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        ["", None, "sin dato", "abc", "123", "978-84-1234-567-8-9"],
    )
    def test_normalize_isbn_invalido_retorna_none(
        self, crudo: object
    ) -> None:
        assert normalize_isbn(crudo) is None


class TestTransformRow:
    """Normalización de una fila de stock y motivos de descarte."""

    def test_fila_valida(self) -> None:
        row, error = _transformar({})

        assert error is None
        assert row is not None
        assert len(row) == len(COLUMNS)
        assert row[0] == "9788412345678"
        assert row[1] == "Libro"
        assert row[5] == 15000
        assert row[6:10] == (2, 3, 5, 1)
        assert row[10] == LOADED_AT
        assert row[11] == LOADED_AT
        assert row[12] == SOURCE_FILE
        assert row[13] == LOADED_AT

    @pytest.mark.parametrize(
        ("cambios", "motivo"),
        [
            ({"ISBN": "abc"}, "ISBN inválido o vacío"),
            ({"PVP": "abc"}, "PVP inválido o menor a 100"),
            ({"PVP": "99"}, "PVP inválido o menor a 100"),
            ({"PVP": "0"}, "PVP inválido o menor a 100"),
            (
                {"Stock Total": "-1"},
                "stock total inválido o negativo",
            ),
            (
                {"Stock Total": "10"},
                "stock total inconsistente",
            ),
            (
                {"Stock Consig.": "abc"},
                "stock consignación inválido",
            ),
            (
                {"Stock Propio": "abc"},
                "stock propio inválido",
            ),
            (
                {"Ventas": "abc"},
                "ventas inválidas",
            ),
        ],
    )
    def test_fila_invalida(
        self, cambios: dict[str, object], motivo: str
    ) -> None:
        row, error = _transformar(cambios)

        assert row is None
        assert error is not None
        assert motivo in error

    def test_pvp_de_100_es_valido(self) -> None:
        row, error = _transformar({"PVP": "100"})

        assert error is None
        assert row is not None
        assert row[5] == 100


class TestTransformLote:
    """Contrato de volumen y calidad sobre el lote sintético completo."""

    def test_respeta_el_contrato_de_volumen(
        self, stock_df: pd.DataFrame
    ) -> None:
        rows, rejected = transform(
            stock_df,
            source_file=SOURCE_FILE,
        )

        assert len(rows) == 82
        assert len(rejected) == 18
        assert len(rows) + len(rejected) == 100

    def test_motivos_de_descarte_coinciden(
        self, stock_df: pd.DataFrame
    ) -> None:
        _, rejected = transform(stock_df, source_file=SOURCE_FILE)

        motivos = Counter(
            motivo
            for item in rejected
            for motivo in item["motivo"].split("; ")
        )
        assert motivos == Counter(
            {
                "stock total inválido o negativo": 10,
                "ISBN inválido o vacío": 8,
            }
        )

    def test_filas_validas_cumplen_el_contrato(
        self, stock_df: pd.DataFrame
    ) -> None:
        rows, _ = transform(stock_df, source_file=SOURCE_FILE)

        assert rows
        for row in rows:
            assert len(row) == len(COLUMNS)
            _, _, _, _, _, pvp, consignacion, propio, total, ventas, *rest = row
            assert pvp >= 100
            assert consignacion >= 0
            assert propio >= 0
            assert total == consignacion + propio
            assert ventas >= 0
            assert len(rest) == 4


class TestDeduplicate:
    """Deduplicación por ISBN conservando la última fila."""

    def test_conserva_la_ultima_fila(self) -> None:
        primero, error = _transformar({"Titulo": "Primero"})
        segundo, error_segundo = _transformar({"Titulo": "Segundo"})
        assert error is None and error_segundo is None
        assert primero is not None and segundo is not None

        rows, duplicates = deduplicate([primero, segundo])

        assert duplicates == 1
        assert len(rows) == 1
        assert rows[0][1] == "Segundo"

    def test_sin_duplicados(self) -> None:
        primero, _ = _transformar({"ISBN": "9788412345678"})
        segundo, _ = _transformar({"ISBN": "123456789X"})
        assert primero is not None and segundo is not None

        rows, duplicates = deduplicate([primero, segundo])

        assert duplicates == 0
        assert len(rows) == 2
