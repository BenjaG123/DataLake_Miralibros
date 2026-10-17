"""Lectura genérica de archivos tabulares como texto plano."""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import pandas as pd


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