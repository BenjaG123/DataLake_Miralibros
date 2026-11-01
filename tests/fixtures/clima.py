"""Genera grillas HTML sintéticas de clima para los tests."""

from __future__ import annotations

from calendar import monthrange
from collections.abc import Callable
from pathlib import Path

YEAR = 2025
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

ValueFactory = Callable[[int, int], str]


def _temperatura_max(month: int, day: int) -> str:
    return "24.3"


def _temperatura_min(month: int, day: int) -> str:
    if month == 6 and day == 30:
        return "-5.5"
    return "3.0"


def _temperatura_media(month: int, day: int) -> str:
    return "10.0"


def _humedad(month: int, day: int) -> str:
    return "65"


def _indice_uv(month: int, day: int) -> str:
    if month == 12 and 24 <= day <= 29:
        return "."
    return "13"


def _precipitacion(month: int, day: int) -> str:
    if month == 1 and day == 1:
        return "2.5"
    return "s/p"


def _viento(month: int, day: int) -> str:
    if month == 1 and day == 1:
        return "CALMA"
    return "W/9"


FUENTES: dict[str, ValueFactory] = {
    "TemperaturaMax2025.xls": _temperatura_max,
    "TemperaturaMin2025.xls": _temperatura_min,
    "TemperaturaMedia2025.xls": _temperatura_media,
    "HumedadRelativaAire2025.xls": _humedad,
    "IndiceUV2025.xls": _indice_uv,
    "Precipitaciones2025.xls": _precipitacion,
    "Viento2025.xls": _viento,
}


def _build_html(value_factory: ValueFactory) -> str:
    header = (
        '<tr><th rowspan="2">Dia</th><th colspan="12">Meses</th></tr>'
    )
    months = "<tr>" + "".join(f"<th>{month}</th>" for month in MESES) + "</tr>"

    rows: list[str] = []
    for day in range(1, 32):
        cells = [f"<td>{day}</td>"]
        for month in range(1, 13):
            value = ""
            if day <= monthrange(YEAR, month)[1]:
                value = value_factory(month, day)
            cells.append(f"<td>{value}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")

    return "<table>" + header + months + "".join(rows) + "</table>"


def escribir_grillas_clima(directory: str | Path) -> dict[str, Path]:
    """Escribe las 7 grillas sintéticas y devuelve sus rutas."""
    target = Path(directory)
    target.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Path] = {}
    for filename, value_factory in FUENTES.items():
        path = target / filename
        path.write_text(_build_html(value_factory), encoding="utf-8")
        paths[filename] = path

    return paths
