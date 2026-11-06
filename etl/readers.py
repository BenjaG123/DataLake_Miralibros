"""Lectura genérica de archivos tabulares como texto plano."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

import pandas as pd
from lxml import html as lxml_html

from etl.validators.fechas import MONTHS

_MONTH_NAMES = frozenset(MONTHS)


def _cell_text(cell: Any) -> str:
    """Extrae el texto exacto de una celda HTML, sin interpretar su tipo."""
    return "".join(cell.itertext()).strip()


def _month_header(rows: Sequence[Any]) -> tuple[int, list[str]] | None:
    """Localiza la fila de meses y devuelve (índice, nombres de meses).

    Los archivos reales usan dos cabeceras: una primera fila con ``Dia`` y
    ``Meses`` (con ``colspan``) y una segunda con los 12 meses. También se
    acepta el caso en que ``Dia`` ocupa ambas filas con ``rowspan``.
    """
    for index, row in enumerate(rows):
        texts = [
            _cell_text(cell) for cell in row.xpath("./th|./td")
        ]
        if (
            len(texts) == 13
            and texts[0].casefold() == "dia"
            and all(texts[1:])
        ):
            return index, texts[1:]

        if (
            len(texts) == 12
            and texts[0].casefold() != "dia"
            and all(texts)
            and all(text.casefold() in _MONTH_NAMES for text in texts)
            and index > 0
        ):
            previous = [
                _cell_text(cell)
                for cell in rows[index - 1].xpath("./th|./td")
            ]
            if previous and previous[0].casefold() == "dia":
                return index, texts

    return None


def read_html_grid_as_str(path: str | Path) -> pd.DataFrame:
    """Lee una grilla diaria (día x mes) desde un .xls que en realidad es HTML.

    Los archivos de clima descargados del sitio no son Excel binarios: son
    tablas HTML con extensión ``.xls``. Se usa ``lxml.html`` directamente para
    quedarse con el texto exacto de cada celda mediante ``itertext()``; así se
    conservan los ceros a la izquierda (``"007"``), los decimales con coma
    (``"22,4"``, ``"0,0"``), ``"s/p"``, ``"CALMA"`` y las celdas vacías sin
    que pandas infiera tipos ni altere formatos.

    El resultado mantiene el contrato de 31 filas, 13 columnas (``Dia`` + 12
    meses) y todos los valores como ``str``.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    document = lxml_html.parse(str(file_path))
    tables = document.xpath("//table")
    if not tables:
        raise ValueError(
            f"No se encontró la cabecera 'Dia' en {file_path}"
        )

    for table in tables:
        rows = table.xpath(".//tr")
        header = _month_header(rows)
        if header is None:
            continue

        header_index, months = header
        data_rows: list[list[str]] = []
        for row in rows[header_index + 1:]:
            cells = row.xpath("./th|./td")
            if len(cells) != 13:
                continue
            data_rows.append([_cell_text(cell) for cell in cells])
            if len(data_rows) == 31:
                break

        days = [fila[0].strip() for fila in data_rows]
        if days != [str(day) for day in range(1, 32)]:
            raise ValueError(
                f"Se esperaban los días 1 a 31 en {file_path}, "
                f"se encontraron: {days}"
            )

        data = pd.DataFrame(data_rows, columns=["Dia", *months])
        return data.astype(str).reset_index(drop=True)

    raise ValueError(
        f"No se encontró la cabecera 'Dia' con 12 meses en {file_path}"
    )


def read_excel_as_str(
    path: str | Path,
    sheet_name: str | int = 0,
) -> pd.DataFrame:
    """Lee una hoja de Excel como texto crudo sin inferir tipos."""
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    return pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        dtype=str,
        keep_default_na=False,
    )


def read_csv_as_str(
    path: str | Path,
    *,
    sep: str = ",",
    encoding: str = "utf-8-sig",
) -> pd.DataFrame:
    """Lee un CSV como texto crudo sin inferir tipos."""
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    return pd.read_csv(
        file_path,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        encoding=encoding,
  )


def require_columns(
    dataframe: pd.DataFrame,
    required: Sequence[str],
    *,
    source: str | Path | None = None,
) -> None:
    """Verifica que el DataFrame contenga el esquema mínimo obligatorio."""
    missing = [col for col in required if col not in dataframe.columns]
    if missing:
        location = f" en {source}" if source else ""
        raise ValueError(
            f"Esquema inválido. Faltan columnas requeridas{location}:"
            f" {', '.join(missing)}"
        )