"""Contrato del DDL cabecera/detalle de ventas sin conectarse a PostgreSQL."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from etl.db import SCHEMA_PLACEHOLDER
from etl.pipelines import ventas

RAIZ = Path(__file__).resolve().parents[1]
DDL_PATH = RAIZ / "sql" / "ventas.sql"
DDL = DDL_PATH.read_text(encoding="utf-8")

TIPOS_TICKET = {
    "origen": "TEXT",
    "correlativo": "BIGINT",
    "fecha": "DATE",
    "hora_simulada": "TIME",
    "rut_cliente": "VARCHAR(10)",
    "medio_pago": "TEXT",
    "tipo_documento": "TEXT",
    "folio": "TEXT",
    "total_venta": "NUMERIC(12,0)",
    "cant_items": "INTEGER",
    "tiene_descuento": "TEXT",
    "es_devolucion": "TEXT",
    "estado": "TEXT",
    "_source_file": "TEXT",
    "_loaded_at": "TIMESTAMPTZ",
    "updated_at": "TIMESTAMPTZ",
}

TIPOS_LINEA = {
    "origen": "TEXT",
    "correlativo": "BIGINT",
    "linea": "INTEGER",
    "codigo_interno": "TEXT",
    "cantidad": "INTEGER",
    "precio_unitario": "NUMERIC(12,0)",
    "total_linea": "NUMERIC(12,0)",
    "_loaded_at": "TIMESTAMPTZ",
}


def _cuerpo_create_table(tabla: str) -> str:
    """Devuelve el contenido entre paréntesis del CREATE TABLE indicado."""
    patron = (
        r"CREATE TABLE IF NOT EXISTS\s+\{\{schema\}\}\."
        + re.escape(tabla)
        + r"\s*\((.*?)\);"
    )
    coincidencia = re.search(patron, DDL, re.DOTALL)
    assert coincidencia is not None, (
        f"no se encontró el CREATE TABLE de {{schema}}.{tabla} en "
        f"{DDL_PATH}"
    )
    return coincidencia.group(1)


def _columnas_ddl(tabla: str) -> dict[str, str]:
    """Parsea columnas y tipos respetando el orden del DDL."""
    columnas: dict[str, str] = {}
    for linea in _cuerpo_create_table(tabla).splitlines():
        definicion = linea.strip().rstrip(",")
        if not definicion:
            continue
        if definicion.upper().startswith(
            (
                "CONSTRAINT",
                "PRIMARY KEY",
                "CHECK",
                "UNIQUE",
                "FOREIGN KEY",
                "REFERENCES",
                "ON",
            )
        ):
            continue
        partes = definicion.split()
        if len(partes) >= 2:
            columnas[partes[0]] = partes[1]
    return columnas


def test_ddl_define_las_columnas_de_ticket_en_orden() -> None:
    assert list(_columnas_ddl("ventas_ticket")) == list(
        ventas.TICKET_COLUMNS
    )


def test_ddl_define_las_columnas_de_linea_en_orden() -> None:
    assert list(_columnas_ddl("ventas_linea")) == list(ventas.LINE_COLUMNS)


def test_ddl_tipos_de_ticket() -> None:
    assert _columnas_ddl("ventas_ticket") == TIPOS_TICKET


def test_ddl_tipos_de_linea() -> None:
    assert _columnas_ddl("ventas_linea") == TIPOS_LINEA


@pytest.mark.parametrize(
    "columna",
    ["orden_web", "canal", "feria", "voucher_maquina_f"],
)
def test_cabecera_no_tiene_columnas_del_modelo_plano(
    columna: str,
) -> None:
    assert columna not in _columnas_ddl("ventas_ticket")


def test_clave_primaria_de_ticket() -> None:
    cuerpo = _cuerpo_create_table("ventas_ticket")
    assert ventas.KEY_COLUMNS == ("origen", "correlativo")
    assert re.search(
        r"PRIMARY KEY\s*\(\s*origen\s*,\s*correlativo\s*\)",
        cuerpo,
    )


def test_clave_primaria_de_linea() -> None:
    cuerpo = _cuerpo_create_table("ventas_linea")
    assert re.search(
        r"PRIMARY KEY\s*\(\s*origen\s*,\s*correlativo\s*,\s*linea\s*\)",
        cuerpo,
    )


def test_fk_de_rut_cliente() -> None:
    cuerpo = _cuerpo_create_table("ventas_ticket")
    assert re.search(
        r"FOREIGN KEY\s*\(\s*rut_cliente\s*\)\s*"
        r"REFERENCES\s+\{\{schema\}\}\.clientes\s*\(\s*rut\s*\)",
        cuerpo,
    )


def test_fk_compuesta_con_on_delete_cascade() -> None:
    cuerpo = _cuerpo_create_table("ventas_linea")
    assert re.search(
        r"FOREIGN KEY\s*\(\s*origen\s*,\s*correlativo\s*\)\s*"
        r"REFERENCES\s+\{\{schema\}\}\.ventas_ticket\s*"
        r"\(\s*origen\s*,\s*correlativo\s*\)\s*"
        r"ON DELETE CASCADE",
        cuerpo,
    )


def test_codigo_interno_no_tiene_fk() -> None:
    cuerpo = _cuerpo_create_table("ventas_linea")
    assert not re.search(
        r"FOREIGN KEY\s*\(\s*codigo_interno\s*\)",
        cuerpo,
    )


@pytest.mark.parametrize(
    "patron",
    [
        r"origen\s+IN\s*\(\s*'LIBRERIA'\s*,\s*'CAFETERIA'\s*\)",
        r"correlativo\s*>\s*0",
        r"total_venta\s*>=\s*0",
        r"cant_items\s*>\s*0",
    ],
)
def test_checks_de_ticket(patron: str) -> None:
    assert re.search(patron, _cuerpo_create_table("ventas_ticket")), (
        f"falta el CHECK {patron!r} en ventas_ticket"
    )


@pytest.mark.parametrize(
    "patron",
    [
        r"linea\s*>\s*0",
        r"cantidad\s*>\s*0",
        r"precio_unitario\s*>=\s*0",
        r"total_linea\s*>=\s*0",
    ],
)
def test_checks_de_linea(patron: str) -> None:
    assert re.search(patron, _cuerpo_create_table("ventas_linea")), (
        f"falta el CHECK {patron!r} en ventas_linea"
    )


@pytest.mark.parametrize(
    "patron",
    [
        r"CREATE INDEX IF NOT EXISTS\s+ventas_ticket_fecha_idx\s+"
        r"ON\s+\{\{schema\}\}\.ventas_ticket\s*\(fecha\)",
        r"CREATE INDEX IF NOT EXISTS\s+ventas_ticket_rut_cliente_idx\s+"
        r"ON\s+\{\{schema\}\}\.ventas_ticket\s*\(rut_cliente\)",
        r"CREATE INDEX IF NOT EXISTS\s+ventas_ticket_origen_fecha_idx\s+"
        r"ON\s+\{\{schema\}\}\.ventas_ticket\s*\(origen,\s*fecha\)",
        r"CREATE INDEX IF NOT EXISTS\s+ventas_linea_ticket_idx\s+"
        r"ON\s+\{\{schema\}\}\.ventas_linea\s*\(origen,\s*correlativo\)",
        r"CREATE INDEX IF NOT EXISTS\s+ventas_linea_codigo_interno_idx\s+"
        r"ON\s+\{\{schema\}\}\.ventas_linea\s*\(codigo_interno\)",
    ],
)
def test_indices_esperados(patron: str) -> None:
    assert re.search(patron, DDL), f"falta el índice {patron!r}"


def test_ddl_usa_el_placeholder_de_schema() -> None:
    assert SCHEMA_PLACEHOLDER in DDL
    assert "{{schema}}.ventas_ticket" in DDL
    assert "{{schema}}.ventas_linea" in DDL


def test_ddl_no_hardcodea_el_schema() -> None:
    assert re.search(r"\bsilver\b", DDL, re.IGNORECASE) is None, (
        "sql/ventas.sql no debe mencionar un schema literal; usá {{schema}}"
    )
