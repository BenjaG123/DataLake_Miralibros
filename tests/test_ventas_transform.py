"""Pruebas de transformación del pipeline de ventas."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from etl.pipelines.ventas import (
    COLUMNS,
    Row,
    deduplicate,
    inferir_origen,
    normalize_canal,
    normalize_si_no,
    transform,
    transform_row,
)

LOADED_AT = datetime(2025, 1, 1, tzinfo=timezone.utc)
SOURCE_FILE = "VENTAS LIBRERIA.xlsx"


def _fila_valida() -> dict[str, object]:
    return {
        "Correlativo": "1",
        "Orden Web": "",
        "Fecha": "2025-01-02",
        "Medio de Pago": "Efectivo",
        "Tipo Documento": "Boleta",
        "Folio": "1000",
        "Voucher Máquina de F": "",
        "Total Venta": "1.500",
        "Cant. Items": "2",
        "Canal": "Local",
        "Feria": "",
        "Tiene Descuento": "NO",
        "Es Dev.": "NO",
        "Estado": "Emitida",
    }


def _transformar(
    cambios: dict[str, object], *,
    origen: str = "LIBRERIA",
) -> tuple[Row | None, str | None]:
    fila = _fila_valida()
    fila.update(cambios)
    return transform_row(
        fila,
        origen=origen,
        source_file=SOURCE_FILE,
        loaded_at=LOADED_AT,
    )


class TestInferirOrigen:
    """Procedencia a partir del nombre del archivo."""

    @pytest.mark.parametrize(
        ("nombre", "esperado"),
        [
            ("VENTAS LIBRERIA.xlsx", "LIBRERIA"),
            ("VENTAS CAFETERIA.xlsx", "CAFETERIA"),
            ("VENTAS CAFETERÍA.xlsx", "CAFETERIA"),
        ],
    )
    def test_infiere_origen(self, nombre: str, esperado: str) -> None:
        assert inferir_origen(nombre) == esperado

    def test_nombre_sin_origen_falla(self) -> None:
        with pytest.raises(ValueError):
            inferir_origen("VENTAS.xlsx")


class TestNormalizadores:
    """Normalización de canal y flags SI/NO."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("local", "Local"),
            ("LOCAL", "Local"),
            ("web", "Web"),
            ("Web", "Web"),
        ],
    )
    def test_normalize_canal_valido(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_canal(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo", ["", None, "sin dato", "otro"]
    )
    def test_normalize_canal_invalido(self, crudo: object) -> None:
        assert normalize_canal(crudo) is None

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("SI", "SI"),
            ("SÍ", "SI"),
            ("S", "SI"),
            ("NO", "NO"),
            ("N", "NO"),
            ("si", "SI"),
            ("no", "NO"),
        ],
    )
    def test_normalize_si_no_valido(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_si_no(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo", ["", None, "sin dato", "tal vez"]
    )
    def test_normalize_si_no_invalido(self, crudo: object) -> None:
        assert normalize_si_no(crudo) is None


class TestTransformRow:
    """Normalización de una fila de ventas y motivos de descarte."""

    def test_fila_valida(self) -> None:
        row, error = _transformar({})

        assert error is None
        assert row is not None
        assert len(row) == len(COLUMNS)
        assert row[0] == "LIBRERIA"
        assert row[1] == 1
        assert row[2] is None
        assert row[3] == date(2025, 1, 2)
        assert row[4] == "Efectivo"
        assert row[5] == "Boleta"
        assert row[6] == "1000"
        assert row[7] is None
        assert row[8] == 1500
        assert row[9] == 2
        assert row[10] == "Local"
        assert row[11] is None
        assert row[12] == "NO"
        assert row[13] == "NO"
        assert row[14] == "Emitida"
        assert row[15] == SOURCE_FILE
        assert row[16] == LOADED_AT
        assert row[17] == LOADED_AT

    @pytest.mark.parametrize(
        ("cambios", "motivo"),
        [
            (
                {"Fecha": "31/01/2025"},
                "fecha inválida",
            ),
            (
                {"Total Venta": "-1"},
                "total venta inválido o negativo",
            ),
            (
                {"Cant. Items": "0"},
                "cantidad de ítems inválida",
            ),
            (
                {"Cant. Items": "abc"},
                "cantidad de ítems inválida",
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


class TestTransformLote:
    """Contrato de volumen y calidad sobre los fixtures sintéticos."""

    def test_libreria_respeta_el_contrato(
        self, ventas_libreria_df: pd.DataFrame
    ) -> None:
        rows, rejected = transform(
            ventas_libreria_df,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
        )

        assert len(rows) == 137
        assert len(rejected) == 13
        assert len(rows) + len(rejected) == 150

    def test_cafeteria_respeta_el_contrato(
        self, ventas_cafeteria_df: pd.DataFrame
    ) -> None:
        rows, rejected = transform(
            ventas_cafeteria_df,
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
        )

        assert len(rows) == 70
        assert len(rejected) == 10
        assert len(rows) + len(rejected) == 80

    def test_motivos_de_descarte_coinciden(
        self,
        ventas_libreria_df: pd.DataFrame,
        ventas_cafeteria_df: pd.DataFrame,
    ) -> None:
        _, libreria = transform(
            ventas_libreria_df,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
        )
        _, cafeteria = transform(
            ventas_cafeteria_df,
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
        )

        motivos = Counter(
            motivo
            for item in [*libreria, *cafeteria]
            for motivo in item["motivo"].split("; ")
        )
        assert motivos == Counter(
            {
                "total venta inválido o negativo": 12,
                "cantidad de ítems inválida": 11,
            }
        )

    def test_filas_validas_cumplen_el_contrato(
        self,
        ventas_libreria_df: pd.DataFrame,
        ventas_cafeteria_df: pd.DataFrame,
    ) -> None:
        libreria, _ = transform(
            ventas_libreria_df,
            origen="LIBRERIA",
            source_file="VENTAS LIBRERIA.xlsx",
        )
        cafeteria, _ = transform(
            ventas_cafeteria_df,
            origen="CAFETERIA",
            source_file="VENTAS CAFETERIA.xlsx",
        )

        assert len(libreria) + len(cafeteria) == 207
        for row in [*libreria, *cafeteria]:
            assert len(row) == len(COLUMNS)
            assert row[0] in ("LIBRERIA", "CAFETERIA")
            assert row[1] > 0
            assert row[3]
            assert row[8] >= 0
            assert row[9] > 0
            assert row[10] in (None, "Local", "Web")
            assert row[12] in (None, "SI", "NO")
            assert row[13] in (None, "SI", "NO")


class TestDeduplicate:
    """Deduplicación por ``(origen, correlativo)``."""

    def test_conserva_la_ultima_fila(self) -> None:
        primero, error = _transformar({"Total Venta": "1000"})
        segundo, error_segundo = _transformar({"Total Venta": "2000"})
        assert error is None and error_segundo is None
        assert primero is not None and segundo is not None

        rows, duplicates = deduplicate([primero, segundo])

        assert duplicates == 1
        assert len(rows) == 1
        assert rows[0][8] == 2000

    def test_mismo_correlativo_con_otro_origen_no_es_duplicado(self) -> None:
        libreria, _ = _transformar({}, origen="LIBRERIA")
        cafeteria, _ = _transformar({}, origen="CAFETERIA")
        assert libreria is not None and cafeteria is not None

        rows, duplicates = deduplicate([libreria, cafeteria])

        assert duplicates == 0
        assert len(rows) == 2
