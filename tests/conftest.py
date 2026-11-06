"""Fixtures compartidas: crudos sintéticos generados y lecturas derivadas.

La mayoría de los tests usan un directorio temporal de sesión generado con
``tests/fixtures/build_fixtures.py``. Los tests de stock y ventas leen los
crudos versionados de ``tests/fixtures/raw/``; como esos XLSX están
gitignored, ``directorio_raw`` los regenera si falta cualquiera. Todo dato
personal es ficticio y el generador usa una semilla fija.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest

from etl.readers import read_csv_as_str, read_excel_as_str
from tests.fixtures.clima import escribir_grillas_clima

GENERADOR = Path(__file__).resolve().parent / "fixtures" / "build_fixtures.py"
DIRECTORIO_RAW = Path(__file__).resolve().parent / "fixtures" / "raw"
CATALOGO_CAFETERIA = DIRECTORIO_RAW / "CATALOGO CAFETERIA.csv"

ARCHIVOS_ESPERADOS = (
    "CLIENTES LIBRERIA.xlsx",
    "STOCK LIBRERIA.xlsx",
    "VENTAS LIBRERIA.xlsx",
    "VENTAS CAFETERIA.xlsx",
)


def _cargar_generador() -> ModuleType:
    """Carga build_fixtures.py por ruta sin convertirlo en paquete."""
    indice = importlib.util.spec_from_file_location("build_fixtures", GENERADOR)
    if indice is None or indice.loader is None:
        raise RuntimeError(f"No se pudo cargar el generador: {GENERADOR}")
    modulo = importlib.util.module_from_spec(indice)
    indice.loader.exec_module(modulo)
    return modulo


_generador = _cargar_generador()
build_all = _generador.build_all


@pytest.fixture(scope="session")
def generador_fixtures() -> ModuleType:
    """Módulo del generador para pruebas de reproducibilidad."""
    return _generador


@pytest.fixture(scope="session")
def directorio_fixtures(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Genera los cuatro XLSX sintéticos una sola vez por sesión."""
    destino = tmp_path_factory.mktemp("fixtures_raw")
    build_all(destino)
    return destino


@pytest.fixture(scope="session")
def archivos_fixtures(directorio_fixtures: Path) -> dict[str, Path]:
    """Rutas de los crudos generados, indexadas por entidad."""
    return {
        "clientes": directorio_fixtures / "CLIENTES LIBRERIA.xlsx",
        "stock": directorio_fixtures / "STOCK LIBRERIA.xlsx",
        "ventas_libreria": directorio_fixtures / "VENTAS LIBRERIA.xlsx",
        "ventas_cafeteria": directorio_fixtures / "VENTAS CAFETERIA.xlsx",
        "catalogo_cafeteria": CATALOGO_CAFETERIA,
    }


@pytest.fixture(scope="session")
def directorio_raw() -> Path:
    """Garantiza los crudos versionados en ``tests/fixtures/raw/``.

    Los XLSX están gitignored, así que un clon limpio (por ejemplo CI) no los
    trae. Si falta cualquiera de los cuatro, se regeneran ahí mismo con
    ``build_all()`` antes de que los tests los lean.
    """
    faltantes = [
        nombre
        for nombre in ARCHIVOS_ESPERADOS
        if not (DIRECTORIO_RAW / nombre).is_file()
    ]
    if faltantes:
        build_all(DIRECTORIO_RAW)
    return DIRECTORIO_RAW


@pytest.fixture(scope="session")
def archivos_raw(directorio_raw: Path) -> dict[str, Path]:
    """Rutas de los crudos versionados, indexadas por entidad."""
    return {
        "clientes": directorio_raw / "CLIENTES LIBRERIA.xlsx",
        "stock": directorio_raw / "STOCK LIBRERIA.xlsx",
        "ventas_libreria": directorio_raw / "VENTAS LIBRERIA.xlsx",
        "ventas_cafeteria": directorio_raw / "VENTAS CAFETERIA.xlsx",
        "catalogo_cafeteria": CATALOGO_CAFETERIA,
    }


@pytest.fixture(scope="session")
def clientes_df(archivos_fixtures: dict[str, Path]) -> pd.DataFrame:
    """Clientes crudos leídos como texto desde el XLSX sintético."""
    return read_excel_as_str(archivos_fixtures["clientes"])


@pytest.fixture(scope="session")
def stock_df(archivos_fixtures: dict[str, Path]) -> pd.DataFrame:
    """Stock crudo leído como texto desde el XLSX sintético."""
    return read_excel_as_str(archivos_fixtures["stock"])


@pytest.fixture(scope="session")
def ventas_libreria_df(archivos_fixtures: dict[str, Path]) -> pd.DataFrame:
    """Ventas de librería crudas leídas como texto."""
    return read_excel_as_str(archivos_fixtures["ventas_libreria"])


@pytest.fixture(scope="session")
def ventas_cafeteria_df(archivos_fixtures: dict[str, Path]) -> pd.DataFrame:
    """Ventas de cafetería crudas leídas como texto."""
    return read_excel_as_str(archivos_fixtures["ventas_cafeteria"])


@pytest.fixture
def clima_dir(tmp_path: Path) -> Path:
    """Genera las 7 grillas HTML sintéticas de clima."""
    target = tmp_path / "clima"
    escribir_grillas_clima(target)
    return target


@pytest.fixture(scope="session")
def catalogo_cafeteria_df() -> pd.DataFrame:
    """Catálogo sintético de cafetería leído como texto desde el CSV."""
    return read_csv_as_str(CATALOGO_CAFETERIA)
