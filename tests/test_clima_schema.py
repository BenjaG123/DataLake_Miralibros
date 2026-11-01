"""Contrato del DDL de clima sin conectarse a PostgreSQL."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from etl.db import SCHEMA_PLACEHOLDER
from etl.pipelines.clima import COLUMNS

RAIZ = Path(__file__).resolve().parents[1]
DDL_PATH = RAIZ / "sql" / "clima.sql"
DDL = DDL_PATH.read_text(encoding="utf-8")

TIPOS_ESPERADOS = {
    "fecha": "DATE",
    "temp_max": "NUMERIC(4,1)",
    "temp_min": "NUMERIC(4,1)",
    "temp_media": "NUMERIC(4,1)",
    "humedad_relativa": "NUMERIC(5,1)",
    "indice_uv": "SMALLINT",
    "precipitacion_mm": "NUMERIC(6,1)",
    "viento_direccion": "VARCHAR(5)",
    "viento_velocidad_kt": "SMALLINT",
    "_loaded_at": "TIMESTAMPTZ",
}


def _cuerpo_create_table() -> str:
    """Devuelve el contenido entre paréntesis del CREATE TABLE de clima."""
    coincidencia = re.search(
        r"CREATE TABLE IF NOT EXISTS\s+\{\{schema\}\}\.clima_diario\s*"
        r"\((.*?)\);",
        DDL,
        re.DOTALL,
    )
    assert coincidencia is not None, (
        "no se encontró el CREATE TABLE de {{schema}}.clima_diario en "
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
            break
        partes = definicion.split()
        if len(partes) >= 2:
            columnas[partes[0]] = partes[1]
    return columnas


def test_ddl_define_las_columnas_en_orden() -> None:
    assert list(_columnas_ddl()) == list(COLUMNS)


def test_ddl_tipos_coinciden_con_el_contrato() -> None:
    assert _columnas_ddl() == TIPOS_ESPERADOS


def test_fecha_es_primary_key() -> None:
    cuerpo = _cuerpo_create_table()
    assert re.search(r"\bfecha\s+DATE\s+PRIMARY KEY\b", cuerpo)


@pytest.mark.parametrize(
    "patron",
    [
        r"temp_max\s+BETWEEN\s+-50\s+AND\s+60",
        r"temp_min\s+BETWEEN\s+-50\s+AND\s+60",
        r"temp_media\s+BETWEEN\s+-50\s+AND\s+60",
        r"humedad_relativa\s+BETWEEN\s+0\s+AND\s+100",
        r"indice_uv\s+BETWEEN\s+0\s+AND\s+20",
        r"precipitacion_mm\s*>=\s*0",
        r"viento_velocidad_kt\s*>=\s*0",
        r"'CALMA'",
    ],
)
def test_ddl_tiene_los_check_esperados(patron: str) -> None:
    assert re.search(patron, DDL), f"falta el CHECK {patron!r} en {DDL_PATH}"


def test_ddl_usa_el_placeholder_de_schema() -> None:
    assert SCHEMA_PLACEHOLDER in DDL
    assert "{{schema}}.clima_diario" in DDL


def test_ddl_no_hardcodea_el_schema() -> None:
    assert re.search(r"\bsilver\b", DDL, re.IGNORECASE) is None, (
        "sql/clima.sql no debe mencionar un schema literal; usá {{schema}}"
    )
