"""Contratos de esquema de stock sin conectarse a PostgreSQL."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from etl.db import SCHEMA_PLACEHOLDER
from etl.pipelines.stock import COLUMNS

RAIZ = Path(__file__).resolve().parents[1]
DDL_PATH = RAIZ / "sql" / "stock.sql"
DDL = DDL_PATH.read_text(encoding="utf-8")

TIPOS_STOCK = {
    "isbn": "TEXT",
    "titulo": "TEXT",
    "autor": "TEXT",
    "editorial": "TEXT",
    "clasificacion": "TEXT",
    "pvp": "NUMERIC(12,0)",
    "stock_consignacion": "INTEGER",
    "stock_propio": "INTEGER",
    "stock_total": "INTEGER",
    "ventas": "INTEGER",
    "valid_from": "TIMESTAMPTZ",
    "updated_at": "TIMESTAMPTZ",
    "_source_file": "TEXT",
    "_loaded_at": "TIMESTAMPTZ",
}

TIPOS_HISTORIAL = {
    "id": "BIGINT",
    "isbn": "TEXT",
    "titulo": "TEXT",
    "autor": "TEXT",
    "editorial": "TEXT",
    "clasificacion": "TEXT",
    "pvp": "NUMERIC(12,0)",
    "stock_consignacion": "INTEGER",
    "stock_propio": "INTEGER",
    "stock_total": "INTEGER",
    "ventas": "INTEGER",
    "valid_from": "TIMESTAMPTZ",
    "valid_to": "TIMESTAMPTZ",
    "is_current": "BOOLEAN",
    "_source_file": "TEXT",
    "_loaded_at": "TIMESTAMPTZ",
}


def _cuerpo_create_table(tabla: str) -> str:
    """Devuelve el contenido entre paréntesis del CREATE TABLE indicado."""
    coincidencia = re.search(
        rf"CREATE TABLE IF NOT EXISTS\s+\{{\{{schema\}}\}}\.{tabla}\s*"
        r"\((.*?)\);",
        DDL,
        re.DOTALL,
    )
    assert coincidencia is not None, (
        f"no se encontró el CREATE TABLE de {tabla} en {DDL_PATH}"
    )
    return coincidencia.group(1)


def _columnas_ddl(cuerpo: str) -> dict[str, str]:
    """Parsea columnas y tipos respetando el orden del DDL."""
    columnas: dict[str, str] = {}
    for linea in cuerpo.splitlines():
        definicion = linea.strip().rstrip(",")
        if not definicion:
            continue
        if definicion.upper().startswith(
            ("CONSTRAINT", "PRIMARY KEY", "CHECK")
        ):
            break
        if definicion.upper().startswith("REFERENCES"):
            continue
        partes = definicion.split()
        if len(partes) >= 2:
            columnas[partes[0]] = partes[1]
    return columnas


def test_stock_define_las_columnas_en_orden() -> None:
    assert list(_columnas_ddl(_cuerpo_create_table("stock"))) == list(COLUMNS)


def test_stock_tipos_coinciden_con_el_contrato() -> None:
    assert _columnas_ddl(_cuerpo_create_table("stock")) == TIPOS_STOCK


def test_historial_define_las_columnas_en_orden() -> None:
    cuerpo = _cuerpo_create_table("stock_historial")
    assert list(_columnas_ddl(cuerpo)) == list(TIPOS_HISTORIAL)


def test_isbn_es_primary_key_de_stock() -> None:
    cuerpo = _cuerpo_create_table("stock")
    assert re.search(r"\bisbn\s+TEXT\s+PRIMARY KEY\b", cuerpo)


def test_id_es_primary_key_del_historial() -> None:
    cuerpo = _cuerpo_create_table("stock_historial")
    assert re.search(
        r"\bid\s+BIGINT\s+GENERATED ALWAYS AS IDENTITY PRIMARY KEY\b",
        cuerpo,
    )


def test_historial_referencia_a_stock() -> None:
    cuerpo = _cuerpo_create_table("stock_historial")
    assert re.search(
        r"REFERENCES\s+\{\{schema\}\}\.stock\s*\(isbn\)",
        cuerpo,
    )


@pytest.mark.parametrize(
    "patron",
    [
        r"isbn ~ '\^\(\?:\[0-9\]\{13\}\|\[0-9\]\{9\}\[0-9X\]\)\$'",
        r"pvp\s*>=\s*0",
        r"stock_consignacion\s*>=\s*0",
        r"stock_propio\s*>=\s*0",
        r"stock_total\s*>=\s*0",
        r"stock_total\s*=\s*stock_consignacion\s*\+\s*stock_propio",
        r"ventas\s*>=\s*0",
    ],
)
def test_stock_tiene_los_check_esperados(patron: str) -> None:
    assert re.search(patron, DDL), f"falta el CHECK {patron!r} en {DDL_PATH}"


@pytest.mark.parametrize(
    "patron",
    [
        r"stock_historial_isbn_formato",
        r"stock_historial_pvp_no_negativo",
        r"stock_historial_stock_no_negativo",
        r"stock_historial_total_consistente",
        r"stock_historial_vigencia",
        r"stock_historial_rango",
        r"is_current AND valid_to IS NULL",
        r"NOT is_current AND valid_to IS NOT NULL",
        r"valid_to\s*>=\s*valid_from",
    ],
)
def test_historial_tiene_los_check_esperados(patron: str) -> None:
    assert re.search(patron, DDL), f"falta {patron!r} en {DDL_PATH}"


def test_indice_parcial_del_historial() -> None:
    assert re.search(
        r"CREATE UNIQUE INDEX IF NOT EXISTS\s+"
        r"stock_historial_isbn_actual_uidx\s+"
        r"ON\s+\{\{schema\}\}\.stock_historial\s*\(isbn\)\s+"
        r"WHERE\s+is_current",
        DDL,
    )


def test_ddl_usa_el_placeholder_de_schema() -> None:
    assert SCHEMA_PLACEHOLDER in DDL
    assert "{{schema}}.stock" in DDL
    assert "{{schema}}.stock_historial" in DDL


def test_ddl_no_hardcodea_el_schema() -> None:
    assert re.search(r"\bsilver\b", DDL, re.IGNORECASE) is None, (
        "sql/stock.sql no debe mencionar un schema literal; usá {{schema}}"
    )
