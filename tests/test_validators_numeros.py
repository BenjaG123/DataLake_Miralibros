"""Pruebas de validadores numéricos."""

from __future__ import annotations

from decimal import Decimal

import pytest

from etl.validators import (
    parse_decimal,
    parse_integer,
    parse_non_negative_bigint,
    parse_non_negative_bigint_or_zero,
)


class TestNumeros:
    """Enteros no negativos con separadores de miles y placeholders."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            (None, 0),
            ("", 0),
            ("-", 0),
            ("--", 0),
            ("sin dato", 0),
            ("n/a", 0),
            ("null", 0),
            ("0", 0),
            (0, 0),
            ("1.250", 1250),
            ("12.000", 12000),
            (" 42 ", 42),
            (7, 7),
        ],
    )
    def test_parse_non_negative_bigint_or_zero(
        self, crudo: object, esperado: int
    ) -> None:
        assert parse_non_negative_bigint_or_zero(crudo) == esperado

    @pytest.mark.parametrize("invalido", ["abc", "-5", "12a"])
    def test_parse_non_negative_bigint_or_zero_invalido_retorna_none(
        self, invalido: str
    ) -> None:
        assert parse_non_negative_bigint_or_zero(invalido) is None

    def test_parse_non_negative_bigint_usa_default(self) -> None:
        assert parse_non_negative_bigint("abc") == 0
        assert parse_non_negative_bigint("abc", default=None) is None
        assert parse_non_negative_bigint("1.250") == 1250


class TestParseDecimal:
    """Decimales con punto o coma, signo y límites opcionales."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("24.3", Decimal("24.3")),
            ("-5.5", Decimal("-5.5")),
            ("1,5", Decimal("1.5")),
            ("0", Decimal("0")),
            ("100", Decimal("100")),
            (" 42.0 ", Decimal("42.0")),
        ],
    )
    def test_parse_decimal_valido(
        self, crudo: str, esperado: Decimal
    ) -> None:
        assert parse_decimal(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [None, "", ".", "s/p", "sin dato", "abc", "nan", "Infinity"],
    )
    def test_parse_decimal_invalido_retorna_none(
        self, crudo: object
    ) -> None:
        assert parse_decimal(crudo) is None

    def test_parse_decimal_respeta_limites(self) -> None:
        assert parse_decimal(
            "-50.0", minimum=Decimal("-50"), maximum=Decimal("60")
        ) == Decimal("-50.0")
        assert parse_decimal(
            "60.0", minimum=Decimal("-50"), maximum=Decimal("60")
        ) == Decimal("60.0")
        assert parse_decimal(
            "-50.1", minimum=Decimal("-50"), maximum=Decimal("60")
        ) is None
        assert parse_decimal(
            "60.1", minimum=Decimal("-50"), maximum=Decimal("60")
        ) is None


class TestParseInteger:
    """Enteros exactos y rechazo de decimales no integrales."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("13", 13),
            ("13.0", 13),
            ("13.00", 13),
            (" 7 ", 7),
            ("0", 0),
            ("-1", -1),
        ],
    )
    def test_parse_integer_valido(
        self, crudo: str, esperado: int
    ) -> None:
        assert parse_integer(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [None, "", ".", "13.5", "abc", "s/p"],
    )
    def test_parse_integer_invalido_retorna_none(
        self, crudo: object
    ) -> None:
        assert parse_integer(crudo) is None

    def test_parse_integer_respeta_limites(self) -> None:
        assert parse_integer("14", minimum=0, maximum=20) == 14
        assert parse_integer("-1", minimum=0) is None
        assert parse_integer("21", maximum=20) is None
