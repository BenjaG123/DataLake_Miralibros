"""Pruebas de validadores específicos de clima."""

from __future__ import annotations

from decimal import Decimal

import pytest

from etl.validators import parse_precipitation, parse_wind


class TestParsePrecipitation:
    """Precipitación en mm; ``s/p`` representa ausencia de lluvia."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("s/p", Decimal("0.0")),
            ("S/P", Decimal("0.0")),
            ("0.0", Decimal("0.0")),
            ("23.4", Decimal("23.4")),
        ],
    )
    def test_parse_precipitation_valida(
        self, crudo: str, esperado: Decimal
    ) -> None:
        assert parse_precipitation(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [None, "", ".", "-1.0", "abc"],
    )
    def test_parse_precipitation_invalida_retorna_none(
        self, crudo: object
    ) -> None:
        assert parse_precipitation(crudo) is None


class TestParseWind:
    """Dirección e intensidad del viento; ``CALMA`` no tiene rumbo."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("CALMA", ("CALMA", 0)),
            ("calma", ("CALMA", 0)),
            ("W/9", ("W", 9)),
            ("w/9", ("W", 9)),
            ("VRB/2", ("VRB", 2)),
            ("NE/14", ("NE", 14)),
        ],
    )
    def test_parse_wind_valido(
        self, crudo: str, esperado: tuple[str, int]
    ) -> None:
        assert parse_wind(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [None, "", ".", "W", "XX/9", "W/abc", "W/-1", "W/"],
    )
    def test_parse_wind_invalido_retorna_none(
        self, crudo: object
    ) -> None:
        assert parse_wind(crudo) == (None, None)
