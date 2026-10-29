"""Reproducibilidad y nombres exactos del generador de fixtures."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType

import pandas as pd

from etl.readers import read_excel_as_str

ARCHIVOS_ESPERADOS = (
    "CLIENTES LIBRERIA.xlsx",
    "STOCK LIBRERIA.xlsx",
    "VENTAS LIBRERIA.xlsx",
    "VENTAS CAFETERIA.xlsx",
)

FILAS_ESPERADAS = {
    "CLIENTES LIBRERIA.xlsx": 150,
    "STOCK LIBRERIA.xlsx": 100,
    "VENTAS LIBRERIA.xlsx": 150,
    "VENTAS CAFETERIA.xlsx": 80,
}


def _leer_todos(directorio: Path) -> dict[str, pd.DataFrame]:
    """Lee los cuatro XLSX como texto para compararlos entre corridas."""
    return {
        nombre: read_excel_as_str(directorio / nombre)
        for nombre in ARCHIVOS_ESPERADOS
    }


def test_build_all_genera_los_cuatro_archivos_esperados(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    resultados = generador_fixtures.build_all(tmp_path)

    assert set(resultados) == set(ARCHIVOS_ESPERADOS)
    for nombre, (ruta, filas) in resultados.items():
        assert ruta.name == nombre
        assert ruta.is_file()
        assert filas == FILAS_ESPERADAS[nombre]

    assert sorted(ruta.name for ruta in tmp_path.iterdir()) == sorted(
        ARCHIVOS_ESPERADOS
    )


def test_build_all_es_reproducible(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    dir_a = tmp_path / "corrida_a"
    dir_b = tmp_path / "corrida_b"

    generador_fixtures.build_all(dir_a)
    generador_fixtures.build_all(dir_b)

    dataframes_a = _leer_todos(dir_a)
    dataframes_b = _leer_todos(dir_b)

    for nombre in ARCHIVOS_ESPERADOS:
        pd.testing.assert_frame_equal(
            dataframes_a[nombre],
            dataframes_b[nombre],
            obj=nombre,
        )
