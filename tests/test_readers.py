"""Pruebas de los lectores: texto forzado, CSV con BOM y .xls que es HTML."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from etl.pipelines.clientes import SOURCE_COLUMNS
from etl.readers import (
    read_csv_as_str,
    read_excel_as_str,
    read_html_grid_as_str,
    require_columns,
)

MESES = (
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
)


def _html_clima(
    *,
    dias: range = range(1, 32),
    meses: tuple[str, ...] = MESES,
    incluir_tabla: bool = True,
) -> str:
    """Arma una tabla HTML con dos filas de cabecera, como los .xls de clima."""
    if not incluir_tabla:
        return "<html><body><p>sin tabla</p></body></html>"

    cabecera = (
        f"<tr><th>Dia</th><th colspan='{len(meses)}'>Meses</th></tr>"
        "<tr><th>Dia</th>"
        + "".join(f"<th>{mes}</th>" for mes in meses)
        + "</tr>"
    )
    # Valores no numéricos a propósito: pd.read_html interpreta comas como
    # separador de miles y no queremos que el test dependa de esa coerción.
    filas = "".join(
        "<tr><td>{dia}</td>{valores}</tr>".format(
            dia=dia,
            valores="".join(
                f"<td>d{dia}-m{indice}</td>" for indice in range(len(meses))
            ),
        )
        for dia in dias
    )
    return f"<html><body><table>{cabecera}{filas}</table></body></html>"


def _escribir_html(tmp_path: Path, **kwargs: object) -> Path:
    ruta = tmp_path / "clima_2024.xls"
    ruta.write_text(_html_clima(**kwargs), encoding="utf-8")
    return ruta


def _html_clima_con_decimales() -> str:
    """Tabla HTML con ``thead`` de dos filas y decimales con coma."""
    cabecera = (
        "<thead><tr><th rowspan='2'>Dia</th>"
        f"<th colspan='{len(MESES)}'>Meses</th></tr><tr>"
        + "".join(f"<th>{mes}</th>" for mes in MESES)
        + "</tr></thead>"
    )
    primera = ["22,4", "0,0", "-1,2"] + ["15"] * (len(MESES) - 3)
    filas = "<tr><td>1</td>" + "".join(
        f"<td>{valor}</td>" for valor in primera
    ) + "</tr>"
    celda_resto = "<td>15</td>" * len(MESES)
    filas += "".join(
        f"<tr><td>{dia}</td>{celda_resto}</tr>" for dia in range(2, 32)
    )
    return (
        "<html><body><table>"
        f"{cabecera}<tbody>{filas}</tbody></table></body></html>"
    )


def _html_clima_con_valores_crudos() -> str:
    """Tabla HTML con ceros, comas, ``s/p``, ``CALMA`` y una celda vacía."""
    cabecera = (
        "<thead><tr><th rowspan='2'>Dia</th>"
        f"<th colspan='{len(MESES)}'>Meses</th></tr><tr>"
        + "".join(f"<th>{mes}</th>" for mes in MESES)
        + "</tr></thead>"
    )
    crudos = [
        "007",
        "0012",
        "22,4",
        "0,0",
        "s/p",
        "CALMA",
        "",
        *["15"] * (len(MESES) - 7),
    ]
    filas = "<tr><td>1</td>" + "".join(
        f"<td>{valor}</td>" for valor in crudos
    ) + "</tr>"
    celda_resto = "<td>15</td>" * len(MESES)
    filas += "".join(
        f"<tr><td>{dia}</td>{celda_resto}</tr>" for dia in range(2, 32)
    )
    return (
        "<html><body><table>"
        f"{cabecera}<tbody>{filas}</tbody></table></body></html>"
    )


class TestReadExcelAsStr:
    """XLSX leído siempre como texto, sin inferir tipos."""

    def test_respeta_las_columnas_del_crudo(
        self, clientes_df: pd.DataFrame
    ) -> None:
        assert list(clientes_df.columns) == list(SOURCE_COLUMNS)

    def test_todos_los_valores_son_texto(
        self, clientes_df: pd.DataFrame
    ) -> None:
        for columna in clientes_df.columns:
            valores = clientes_df[columna]
            assert all(
                isinstance(valor, str) for valor in valores
            ), f"la columna {columna!r} tiene valores que no son str"

    def test_conserva_ceros_a_la_izquierda_y_signos(
        self, tmp_path: Path
    ) -> None:
        ruta = tmp_path / "codigos.xlsx"
        original = pd.DataFrame(
            {
                "Codigo": ["007", "0012"],
                "Telefono": ["+56900000000", "09 1234 5678"],
            }
        )
        original.to_excel(ruta, index=False)

        leido = read_excel_as_str(ruta)

        assert leido.loc[0, "Codigo"] == "007"
        assert leido.loc[1, "Codigo"] == "0012"
        assert leido.loc[0, "Telefono"] == "+56900000000"

    def test_falla_sobre_un_xls_que_es_html(self, tmp_path: Path) -> None:
        ruta = _escribir_html(tmp_path)

        with pytest.raises(ValueError):
            read_excel_as_str(ruta)


class TestReadCsvAsStr:
    """CSV con separador explícito y BOM de Excel."""

    def test_respeta_separador_y_bom(self, tmp_path: Path) -> None:
        ruta = tmp_path / "codigos.csv"
        ruta.write_text(
            "codigo;descripcion\n007;Libro ñandú\n",
            encoding="utf-8-sig",
        )

        leido = read_csv_as_str(ruta, sep=";")

        assert list(leido.columns) == ["codigo", "descripcion"]
        assert leido.loc[0, "codigo"] == "007"
        assert leido.loc[0, "descripcion"] == "Libro ñandú"
        assert all(
            isinstance(valor, str) for valor in leido.to_numpy().ravel()
        )

    def test_por_defecto_lee_coma_y_bom(self, tmp_path: Path) -> None:
        ruta = tmp_path / "codigos_coma.csv"
        ruta.write_text("codigo,valor\n0010,abc\n", encoding="utf-8-sig")

        leido = read_csv_as_str(ruta)

        assert leido.loc[0, "codigo"] == "0010"
        assert leido.loc[0, "valor"] == "abc"


class TestReadHtmlGridAsStr:
    """Grilla día x mes desde un .xls que en realidad es una tabla HTML."""

    def test_lee_los_31_dias_y_los_12_meses(self, tmp_path: Path) -> None:
        ruta = _escribir_html(tmp_path)

        grilla = read_html_grid_as_str(ruta)

        assert list(grilla.columns) == ["Dia", *MESES]
        assert grilla.shape == (31, 13)
        assert grilla["Dia"].tolist() == [str(dia) for dia in range(1, 32)]
        assert grilla.loc[0, "Enero"] == "d1-m0"
        assert grilla.loc[30, "Diciembre"] == "d31-m11"
        assert all(
            isinstance(valor, str) for valor in grilla.to_numpy().ravel()
        )

    def test_read_html_conserva_decimales_con_coma(
        self, tmp_path: Path
    ) -> None:
        """Regresión: ``thousands=","`` convertía "22,4" en 224."""
        ruta = tmp_path / "clima_decimales.xls"
        ruta.write_text(_html_clima_con_decimales(), encoding="utf-8")

        grilla = read_html_grid_as_str(ruta)

        assert grilla.shape == (31, 13)
        assert list(grilla.columns) == ["Dia", *MESES]
        assert grilla.iloc[0].tolist()[1:5] == [
            "22,4",
            "0,0",
            "-1,2",
            "15",
        ]
        assert grilla.loc[0, "Enero"] == "22,4"
        assert grilla.loc[0, "Febrero"] == "0,0"
        assert grilla.loc[0, "Marzo"] == "-1,2"
        assert grilla.loc[0, "Abril"] == "15"
        assert {str(dtype) for dtype in grilla.dtypes} == {"str"}
        assert all(
            isinstance(valor, str) for valor in grilla.to_numpy().ravel()
        )

    def test_conserva_texto_crudo_sin_inferencia(
        self, tmp_path: Path
    ) -> None:
        ruta = tmp_path / "clima_crudo.xls"
        ruta.write_text(_html_clima_con_valores_crudos(), encoding="utf-8")

        grilla = read_html_grid_as_str(ruta)

        assert grilla.iloc[0].tolist()[1:8] == [
            "007",
            "0012",
            "22,4",
            "0,0",
            "s/p",
            "CALMA",
            "",
        ]
        assert grilla.loc[0, "Enero"] == "007"
        assert grilla.loc[0, "Julio"] == ""
        assert {str(dtype) for dtype in grilla.dtypes} == {"str"}
        assert all(
            isinstance(valor, str) for valor in grilla.to_numpy().ravel()
        )

    def test_falla_si_no_hay_tabla(self, tmp_path: Path) -> None:
        ruta = _escribir_html(tmp_path, incluir_tabla=False)

        with pytest.raises(ValueError, match="No se encontró la cabecera"):
            read_html_grid_as_str(ruta)

    def test_falla_si_faltan_dias(self, tmp_path: Path) -> None:
        ruta = _escribir_html(tmp_path, dias=range(1, 31))

        with pytest.raises(ValueError, match="Se esperaban los días 1 a 31"):
            read_html_grid_as_str(ruta)

    def test_falla_si_faltan_meses(self, tmp_path: Path) -> None:
        ruta = _escribir_html(tmp_path, meses=MESES[:11])

        with pytest.raises(ValueError, match="12 meses"):
            read_html_grid_as_str(ruta)

    def test_archivo_inexistente_falla(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            read_html_grid_as_str(tmp_path / "no_existe.xls")


class TestRequireColumns:
    """Esquema mínimo obligatorio."""

    def test_acepta_el_esquema_correcto(self) -> None:
        dataframe = pd.DataFrame(
            {"Rut cliente": ["12345678-5"], "Nombre": ["Ana Perez"]}
        )

        resultado = require_columns(dataframe, ["Rut cliente", "Nombre"])

        assert resultado is None

    def test_lista_las_columnas_faltantes(self) -> None:
        dataframe = pd.DataFrame({"Rut cliente": ["12345678-5"]})

        with pytest.raises(ValueError) as excinfo:
            require_columns(
                dataframe,
                ["Rut cliente", "Nombre", "Email"],
                source="CLIENTES.xlsx",
            )

        mensaje = str(excinfo.value)
        assert "Nombre" in mensaje
        assert "Email" in mensaje
        assert "CLIENTES.xlsx" in mensaje
        assert "Rut cliente" not in mensaje
