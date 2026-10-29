"""Pruebas de transformación del pipeline de clientes.

Se apoya en los XLSX sintéticos generados por ``tests/fixtures/build_fixtures.py``
y no se conecta a PostgreSQL.
"""

from __future__ import annotations

from collections import Counter
from datetime import date
from typing import Any

import pandas as pd
import pytest

from etl.pipelines.clientes import COLUMNS, transform, transform_row
from etl.validators import validate_rut_dv

TOTAL_FILAS = 150
VALIDAS_ESPERADAS = 127
DESCARTADAS_ESPERADAS = 23
MOTIVOS_ESPERADOS = Counter(
    {
        "RUT inválido": 8,
        "alta inválida": 4,
        "cumple inválido": 4,
        "email inválido": 3,
        "canal inválido": 2,
        "nombre inválido": 2,
    }
)


def _fila_valida() -> dict[str, Any]:
    """Fila cruda mínima que cumple todas las reglas de clientes."""
    return {
        "Rut cliente": "12345678-5",
        "Nombre": "Ana Perez",
        "Email": "ana.perez@example.com",
        "Alta": "01/01/2024",
        "Canal": "Local",
        "Cumple.": "14 - junio",
        "Puntos": "1.250",
        "Compras": "3",
        "Canjes": "0",
    }


class TestTransformLote:
    """Contrato de volumen y calidad sobre el lote sintético completo."""

    def test_respeta_el_contrato_de_volumen(
        self, clientes_df: pd.DataFrame
    ) -> None:
        filas, descartadas = transform(clientes_df)

        assert len(filas) == VALIDAS_ESPERADAS, (
            f"se esperaban {VALIDAS_ESPERADAS} filas válidas y llegaron "
            f"{len(filas)}; si cambió el generador o un validador, "
            "actualizá el contrato y revisá los motivos de descarte"
        )
        assert len(descartadas) == DESCARTADAS_ESPERADAS
        assert len(filas) + len(descartadas) == TOTAL_FILAS

    def test_motivos_de_descarte_coinciden(
        self, clientes_df: pd.DataFrame
    ) -> None:
        _, descartadas = transform(clientes_df)

        motivos: Counter[str] = Counter()
        for fila in descartadas:
            assert fila["motivo"], "toda fila descartada debe traer motivo"
            motivos.update(fila["motivo"].split("; "))

        assert motivos == MOTIVOS_ESPERADOS

    def test_filas_validas_cumplen_el_contrato(
        self, clientes_df: pd.DataFrame
    ) -> None:
        filas, _ = transform(clientes_df)

        assert filas
        for fila in filas:
            assert len(fila) == len(COLUMNS)

            rut, nombre, email, alta, canal, cumple, *contadores = fila
            cuerpo, dv = rut.split("-")
            assert validate_rut_dv(cuerpo, dv), f"RUT con DV incorrecto: {rut}"
            assert nombre
            assert email
            assert isinstance(alta, date)
            assert canal in ("Local", "Web")
            assert isinstance(cumple, date)
            assert 1900 <= cumple.year <= date.today().year

            assert len(contadores) == 3
            for contador in contadores:
                assert isinstance(contador, int)
                assert not isinstance(contador, bool)
                assert contador >= 0

    def test_ninguna_fila_valida_tiene_contadores_nulos(
        self, clientes_df: pd.DataFrame
    ) -> None:
        filas, _ = transform(clientes_df)

        for fila in filas:
            assert all(contador is not None for contador in fila[6:9])


class TestTransformFila:
    """Ramas puntuales de una fila, sin depender del lote completo."""

    @pytest.mark.parametrize("valor", ["sin dato", "", None, "-"])
    def test_placeholder_en_puntos_produce_cero_y_no_descarta(
        self, valor: object
    ) -> None:
        fila = _fila_valida()
        fila["Puntos"] = valor

        resultado, motivo = transform_row(fila, reference_year=2024)

        assert motivo is None
        assert resultado is not None
        assert resultado[6] == 0

    def test_puntos_no_numerico_descarta_con_motivo(self) -> None:
        fila = _fila_valida()
        fila["Puntos"] = "abc"

        resultado, motivo = transform_row(fila, reference_year=2024)

        assert resultado is None
        assert motivo == "puntos inválido"

    def test_cumple_con_de_no_es_motivo_de_rechazo(self) -> None:
        fila = _fila_valida()
        fila["Cumple."] = "14 de junio"

        resultado, motivo = transform_row(fila, reference_year=2024)

        assert motivo is None
        assert "cumple inválido" not in (motivo or "")
        assert resultado is not None
        assert resultado[5].month == 6
        assert resultado[5].day == 14
