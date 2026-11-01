"""Pruebas de transformación del pipeline de clima diario."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from etl.pipelines.clima import COLUMNS, Row, transform


def _rows_by_date(rows: list[Row]) -> dict[date, Row]:
    return {row[0]: row for row in rows}


def test_genera_una_fila_por_fecha_real(clima_dir: Path) -> None:
    rows = transform(clima_dir)

    assert len(rows) == 365
    assert all(len(row) == len(COLUMNS) for row in rows)
    assert rows[0][0] == date(2025, 1, 1)
    assert rows[-1][0] == date(2025, 12, 31)

    fechas = {(row[0].month, row[0].day) for row in rows}
    assert (2, 29) not in fechas
    assert (4, 31) not in fechas


def test_valores_conocidos(clima_dir: Path) -> None:
    raw_rows = transform(clima_dir)
    rows = _rows_by_date(raw_rows)

    inicio = rows[date(2025, 1, 1)]
    assert inicio[1:9] == (
        Decimal("24.3"),
        Decimal("3.0"),
        Decimal("10.0"),
        Decimal("65"),
        13,
        Decimal("2.5"),
        "CALMA",
        0,
    )
    assert inicio[9].tzinfo is not None

    assert rows[date(2025, 6, 30)][2] == Decimal("-5.5")
    assert rows[date(2025, 1, 2)][6] == Decimal("0.0")
    assert rows[date(2025, 1, 2)][7:9] == ("W", 9)

    for day in range(24, 30):
        assert rows[date(2025, 12, day)][5] is None
    assert rows[date(2025, 12, 23)][5] == 13
    assert rows[date(2025, 12, 30)][5] == 13

    assert len({row[9] for row in raw_rows}) == 1


def test_archivo_faltante_falla(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        transform(tmp_path)
