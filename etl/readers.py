"""Lectura genérica de archivos tabulares como texto plano."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd


def read_html_grid_as_str(path: str | Path) -> pd.DataFrame:
    """Lee una grilla diaria (día x mes) desde un .xls que en realidad es HTML.

    Los archivos de clima descargados del sitio no son Excel binarios: son
    tablas HTML con extensión .xls. Pandas consume las filas de cabecera
    ``Dia``/``Meses`` como un ``MultiIndex``; acá se aplana para recuperar
    los nombres de meses y se validan los 31 días.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise FileNotFoundError(f"No se encontró el archivo: {file_path}")

    try:
        tables = pd.read_html(
            file_path,
            match="Dia",
            flavor="lxml",
            header=None,
            keep_default_na=False,
        )
    except ValueError as exc:
        if "No tables found" in str(exc):
            raise ValueError(
                f"No se encontró la cabecera 'Dia' en {file_path}"
            ) from exc
        raise

    for table in tables:
        if table.shape[1] != 13 or not isinstance(table.columns, pd.MultiIndex):
            continue

        table = table.astype("string").fillna("")
        columns = [
            str(value).strip()
            for value in table.columns.get_level_values(-1)
        ]
        if columns[0].casefold() != "dia" or not all(columns[1:]):
            continue

        data = table.iloc[:31].copy()
        data.columns = ["Dia", *columns[1:]]

        days = data.iloc[:, 0].astype(str).str.strip().tolist()
        if days != [str(day) for day in range(1, 32)]:
            raise ValueError(
                f"Se esperaban los días 1 a 31 en {file_path}, "
                f"se encontraron: {days}"
            )

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