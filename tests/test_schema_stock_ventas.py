"""Contrato estático de ``stock`` y ``ventas`` sin conectarse a PostgreSQL.

El DDL se lee como texto y se compara con ``COLUMNS``, ``KEY_COLUMNS`` y
``UPDATE_COLUMNS`` de cada pipeline. También se verifica que los pipelines
incrementales no vacíen tablas y que el upsert viva en ``etl/db.py``.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from etl import db
from etl.pipelines import stock, ventas
from etl.validators import parse_date_iso

RAIZ = Path(__file__).resolve().parents[1]
DDL_STOCK = (RAIZ / "sql" / "stock.sql").read_text(encoding="utf-8")
DDL_VENTAS = (RAIZ / "sql" / "ventas.sql").read_text(encoding="utf-8")
CODIGO_STOCK = (RAIZ / "etl" / "pipelines" / "stock.py").read_text(
    encoding="utf-8"
)
CODIGO_VENTAS = (RAIZ / "etl" / "pipelines" / "ventas.py").read_text(
    encoding="utf-8"
)
CODIGO_DB = (RAIZ / "etl" / "db.py").read_text(encoding="utf-8")

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

TIPOS_VENTAS = {
    "origen": "TEXT",
    "correlativo": "BIGINT",
    "orden_web": "TEXT",
    "fecha": "DATE",
    "medio_pago": "TEXT",
    "tipo_documento": "TEXT",
    "folio": "TEXT",
    "voucher_maquina_f": "TEXT",
    "total_venta": "NUMERIC(12,0)",
    "cant_items": "INTEGER",
    "canal": "TEXT",
    "feria": "TEXT",
    "tiene_descuento": "TEXT",
    "es_devolucion": "TEXT",
    "estado": "TEXT",
    "_source_file": "TEXT",
    "_loaded_at": "TIMESTAMPTZ",
    "updated_at": "TIMESTAMPTZ",
}


def _cuerpo_create_table(ddl: str, tabla: str) -> str:
    """Devuelve el contenido entre paréntesis del CREATE TABLE indicado."""
    patron = (
        r"CREATE TABLE IF NOT EXISTS\s+\{\{schema\}\}\."
        + re.escape(tabla)
        + r"\s*\((.*?)\);"
    )
    coincidencia = re.search(patron, ddl, re.DOTALL)
    assert coincidencia is not None, (
        f"no se encontró el CREATE TABLE de {tabla!r}"
    )
    return coincidencia.group(1)


def _columnas_ddl(ddl: str, tabla: str) -> dict[str, str]:
    """Parsea columnas y tipos respetando el orden del DDL."""
    columnas: dict[str, str] = {}
    for linea in _cuerpo_create_table(ddl, tabla).splitlines():
        definicion = linea.strip().rstrip(",")
        if not definicion:
            continue
        if definicion.upper().startswith(
            ("CONSTRAINT", "PRIMARY KEY", "CHECK", "UNIQUE", "FOREIGN KEY")
        ):
            continue
        partes = definicion.split()
        if len(partes) >= 2:
            columnas[partes[0]] = partes[1]
    return columnas


class TestColumnas:
    """Orden y tipos del DDL contra ``COLUMNS`` de cada pipeline."""

    def test_stock_columnas_en_orden(self) -> None:
        assert list(_columnas_ddl(DDL_STOCK, "stock")) == list(stock.COLUMNS)

    def test_ventas_columnas_en_orden(self) -> None:
        assert list(_columnas_ddl(DDL_VENTAS, "ventas")) == list(
            ventas.COLUMNS
        )

    def test_tipos_de_stock(self) -> None:
        assert _columnas_ddl(DDL_STOCK, "stock") == TIPOS_STOCK

    def test_tipos_de_ventas(self) -> None:
        assert _columnas_ddl(DDL_VENTAS, "ventas") == TIPOS_VENTAS

    def test_auditoria_se_compara_sin_exclusiones(self) -> None:
        """Los DDL no agregan columnas audit fuera de ``COLUMNS``.

        ``_source_file`` y ``_loaded_at`` ya viajan en ``COLUMNS``; por eso la
        comparación es uno a uno. Si el DDL agregara una columna por defecto,
        este test obligaría a decidir si también debe ir en el pipeline.
        """
        assert {"_source_file", "_loaded_at"} <= set(stock.COLUMNS)
        assert {"_source_file", "_loaded_at", "updated_at"} <= set(
            ventas.COLUMNS
        )


class TestClaves:
    """Claves primarias y columnas actualizables."""

    def test_stock_pk_es_isbn(self) -> None:
        cuerpo = _cuerpo_create_table(DDL_STOCK, "stock")
        assert re.search(r"\bisbn\s+TEXT\s+PRIMARY KEY\b", cuerpo)
        assert stock.KEY_COLUMNS == ("isbn",)

    def test_ventas_pk_es_origen_y_correlativo(self) -> None:
        cuerpo = _cuerpo_create_table(DDL_VENTAS, "ventas")
        assert re.search(
            r"PRIMARY KEY\s*\(\s*origen\s*,\s*correlativo\s*\)",
            cuerpo,
        )
        assert ventas.KEY_COLUMNS == ("origen", "correlativo")

    def test_update_columns_no_tocan_la_clave(self) -> None:
        assert not set(stock.KEY_COLUMNS) & set(stock.UPDATE_COLUMNS)
        assert not set(ventas.KEY_COLUMNS) & set(ventas.UPDATE_COLUMNS)


@pytest.mark.parametrize(
    ("ddl", "patron"),
    [
        (DDL_STOCK, r"stock_total\s*>=\s*0"),
        (
            DDL_STOCK,
            r"stock_total\s*=\s*stock_consignacion\s*\+\s*stock_propio",
        ),
        (DDL_VENTAS, r"total_venta\s*>=\s*0"),
        (DDL_VENTAS, r"cant_items\s*>\s*0"),
        (
            DDL_VENTAS,
            r"origen\s+IN\s*\(\s*'LIBRERIA'\s*,\s*'CAFETERIA'\s*\)",
        ),
        (
            DDL_VENTAS,
            r"tiene_descuento\s+IN\s*\(\s*'SI'\s*,\s*'NO'\s*\)",
        ),
        (
            DDL_VENTAS,
            r"es_devolucion\s+IN\s*\(\s*'SI'\s*,\s*'NO'\s*\)",
        ),
    ],
)
def test_checks_clave(ddl: str, patron: str) -> None:
    assert re.search(patron, ddl), f"falta el CHECK {patron!r}"


class TestSchema:
    """El DDL crea el schema con placeholder y nunca lo hardcodea."""

    @pytest.mark.parametrize("ddl", [DDL_STOCK, DDL_VENTAS])
    def test_crea_schema_con_placeholder(self, ddl: str) -> None:
        assert "CREATE SCHEMA IF NOT EXISTS {{schema}};" in ddl
        assert db.SCHEMA_PLACEHOLDER in ddl

    @pytest.mark.parametrize(
        ("ddl", "tabla"),
        [(DDL_STOCK, "stock"), (DDL_VENTAS, "ventas")],
    )
    def test_tabla_usa_placeholder(self, ddl: str, tabla: str) -> None:
        assert f"{{{{schema}}}}.{tabla}" in ddl

    @pytest.mark.parametrize("ddl", [DDL_STOCK, DDL_VENTAS])
    def test_no_hardcodea_silver(self, ddl: str) -> None:
        assert re.search(r"\bsilver\b", ddl, re.IGNORECASE) is None


class TestUpsertSinDestruccion:
    """Los pipelines incrementales no vacían ni alteran tablas."""

    @pytest.mark.parametrize("codigo", [CODIGO_STOCK, CODIGO_VENTAS])
    def test_no_ejecutan_operaciones_destructivas(self, codigo: str) -> None:
        for operacion in ("TRUNCATE", "DROP", "ALTER"):
            assert (
                re.search(rf"\b{operacion}\b", codigo, re.IGNORECASE) is None
            ), f"el pipeline contiene {operacion}"

    def test_stock_usa_upsert_table(self) -> None:
        assert "db.upsert_table(" in CODIGO_STOCK
        assert "db.replace_table(" not in CODIGO_STOCK
        assert "db.truncate_table(" not in CODIGO_STOCK

    def test_ventas_usa_upsert_table(self) -> None:
        assert "db.upsert_table(" in CODIGO_VENTAS
        assert "db.replace_table(" not in CODIGO_VENTAS
        assert "db.truncate_table(" not in CODIGO_VENTAS

    def test_db_upsert_hace_on_conflict(self) -> None:
        assert "def upsert_table(" in CODIGO_DB
        assert "ON CONFLICT" in CODIGO_DB
        assert "DO UPDATE SET" in CODIGO_DB


class TestHistorialStock:
    """Contrato SCD2 de ``stock_historial``."""

    def test_tiene_columnas_de_vigencia(self) -> None:
        columnas = list(_columnas_ddl(DDL_STOCK, "stock_historial"))
        assert columnas[0] == "id"
        for columna in ("isbn", "valid_from", "valid_to", "is_current"):
            assert columna in columnas

    def test_un_solo_vigente_por_isbn(self) -> None:
        patron = (
            r"CREATE UNIQUE INDEX IF NOT EXISTS\s+"
            r"stock_historial_isbn_actual_uidx\s+ON\s+"
            r"\{\{schema\}\}\.stock_historial\s*\(isbn\)\s+"
            r"WHERE\s+is_current"
        )
        assert re.search(patron, DDL_STOCK, re.DOTALL)

    def test_rango_de_vigencia_consistente(self) -> None:
        assert re.search(
            r"valid_to\s+IS\s+NULL\s+OR\s+valid_to\s*>=\s*valid_from",
            DDL_STOCK,
        )


class TestParseDateIso:
    """``parse_date_iso`` acepta ISO estricto y rechaza variantes laxas."""

    def test_acepta_fecha_iso(self) -> None:
        assert parse_date_iso("2024-01-02") == date(2024, 1, 2)

    @pytest.mark.parametrize(
        "valor",
        [
            "2024-1-2",
            "29/02/2024",
            "2024-02-30",
            "",
            None,
            " ",
            "2024-13-01",
        ],
    )
    def test_rechaza_fechas_invalidas(self, valor: object) -> None:
        assert parse_date_iso(valor) is None

    def test_respeta_limites(self) -> None:
        assert (
            parse_date_iso("2024-01-01", min_date=date(2024, 1, 2)) is None
        )
        assert (
            parse_date_iso("2024-01-02", min_date=date(2024, 1, 2))
            == date(2024, 1, 2)
        )
        assert (
            parse_date_iso("2024-01-02", max_date=date(2024, 1, 1)) is None
        )
