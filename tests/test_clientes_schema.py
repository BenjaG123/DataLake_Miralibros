"""Contrato de esquema de clientes sin conectarse a PostgreSQL.

El DDL se lee y se parsea como texto: se comparan columnas, tipos, PK y CHECK
contra lo que produce el pipeline (``COLUMNS`` y ``Row``).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from etl.db import SCHEMA_PLACEHOLDER
from etl.pipelines.clientes import COLUMNS

RAIZ = Path(__file__).resolve().parents[1]
DDL_PATH = RAIZ / "sql" / "clientes.sql"
DDL = DDL_PATH.read_text(encoding="utf-8")

TIPOS_ESPERADOS = {
    "rut": "VARCHAR(10)",
    "nombre": "VARCHAR(150)",
    "email": "VARCHAR(254)",
    "alta": "DATE",
    "canal": "VARCHAR(10)",
    "cumple": "DATE",
    "puntos": "BIGINT",
    "compras": "BIGINT",
    "canjes": "BIGINT",
}


def _cuerpo_create_table() -> str:
    """Devuelve el contenido entre paréntesis del CREATE TABLE de clientes."""
    coincidencia = re.search(
        r"CREATE TABLE IF NOT EXISTS\s+\{\{schema\}\}\.clientes\s*\((.*?)\);",
        DDL,
        re.DOTALL,
    )
    assert coincidencia is not None, (
        "no se encontró el CREATE TABLE de {{schema}}.clientes en "
        f"{DDL_PATH}"
    )
    return coincidencia.group(1)


def _columnas_ddl() -> dict[str, str]:
    """Parsea columnas y tipos respetando el orden del DDL."""
    columnas: dict[str, str] = {}
    for linea in _cuerpo_create_table().splitlines():
        definicion = linea.strip().rstrip(",")
        if not definicion:
            continue
        if definicion.upper().startswith(
            ("CONSTRAINT", "PRIMARY KEY", "CHECK")
        ):
            continue
        partes = definicion.split()
        if len(partes) >= 2:
            columnas[partes[0]] = partes[1]
    return columnas


def test_ddl_define_las_nueve_columnas_en_orden() -> None:
    assert list(_columnas_ddl()) == list(COLUMNS)
    assert len(COLUMNS) == 9


def test_ddl_tipos_coinciden_con_el_contrato() -> None:
    assert _columnas_ddl() == TIPOS_ESPERADOS


def test_rut_es_primary_key() -> None:
    cuerpo = _cuerpo_create_table()
    assert re.search(r"\brut\s+VARCHAR\(10\)\s+PRIMARY KEY\b", cuerpo)


@pytest.mark.parametrize("columna", ["nombre", "email", "alta"])
def test_columnas_opcionales_no_tienen_not_null(columna: str) -> None:
    cuerpo = _cuerpo_create_table()
    coincidencia = re.search(
        rf"^\s*{columna}\s+[^\n,]+", cuerpo, re.MULTILINE
    )
    assert coincidencia is not None, f"no se encontró la columna {columna}"
    assert "NOT NULL" not in coincidencia.group(0).upper()


@pytest.mark.parametrize(
    "patron",
    [
        r"canal\s+IN\s*\(\s*'Local'\s*,\s*'Web'\s*\)",
        r"puntos\s*>=\s*0",
        r"compras\s*>=\s*0",
        r"canjes\s*>=\s*0",
    ],
)
def test_ddl_tiene_los_check_esperados(patron: str) -> None:
    assert re.search(patron, DDL), f"falta el CHECK {patron!r} en {DDL_PATH}"


def test_ddl_usa_el_placeholder_de_schema() -> None:
    assert SCHEMA_PLACEHOLDER in DDL
    assert "{{schema}}.clientes" in DDL


def test_ddl_no_hardcodea_el_schema() -> None:
    assert re.search(r"\bsilver\b", DDL, re.IGNORECASE) is None, (
        "sql/clientes.sql no debe mencionar un schema literal; "
        "usá {{schema}}"
    )
