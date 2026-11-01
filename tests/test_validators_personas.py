"""Pruebas de validadores de RUT, nombres y emails."""

from __future__ import annotations

import unicodedata

import pytest

from etl.validators import (
    calculate_rut_dv,
    normalize_email,
    normalize_name,
    normalize_rut,
    split_rut,
    validate_rut_dv,
)


class TestCalculoRut:
    """Dígito verificador módulo 11 y validación del DV."""

    @pytest.mark.parametrize(
        ("cuerpo", "esperado"),
        [
            # 8*2+7*3+6*4+5*5+4*6+3*7+2*2+1*3 = 138 -> 138%11=6 -> 11-6=5
            ("12345678", "5"),
            # 7*2+6*3+5*4+4*5+3*6+2*7+1*2 = 106 -> 106%11=7 -> 11-7=4
            ("1234567", "4"),
            # 3*2+2*3 = 12 -> 12%11=1 -> 11-1=10 -> K
            ("20000003", "K"),
            # 7 dígitos: 5*2+1*2 = 12 -> K
            ("1000005", "K"),
            # 2 -> 11-2=9
            ("1000000", "9"),
        ],
    )
    def test_calculate_rut_dv_casos_conocidos(
        self, cuerpo: str, esperado: str
    ) -> None:
        assert calculate_rut_dv(cuerpo) == esperado

    @pytest.mark.parametrize("invalido", ["", "123456789", "12a", " 1 2 "])
    def test_calculate_rut_dv_cuerpo_invalido_lanza_value_error(
        self, invalido: str
    ) -> None:
        with pytest.raises(ValueError):
            calculate_rut_dv(invalido)

    @pytest.mark.parametrize(
        ("cuerpo", "dv", "esperado"),
        [
            ("12345678", "5", True),
            ("12345678", "9", False),
            ("20000003", "K", True),
            ("20000003", "k", True),
            ("abc", "5", False),
            ("12345678", "", False),
        ],
    )
    def test_validate_rut_dv(
        self, cuerpo: str, dv: str, esperado: bool
    ) -> None:
        assert validate_rut_dv(cuerpo, dv) is esperado


class TestNormalizeRut:
    """Normalización de formatos de RUT y rechazo de datos inválidos."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("12.345.678-5", "12345678-5"),
            ("12345678-5", "12345678-5"),
            (" 12345678 - 5 ", "12345678-5"),
            ("123456785", "12345678-5"),
            ("20.000.003-k", "20000003-K"),
            ("20000003K", "20000003-K"),
            ("1.000.005-K", "1000005-K"),
        ],
    )
    def test_normalize_rut_valido(self, crudo: str, esperado: str) -> None:
        assert normalize_rut(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [
            "12345678-9",
            "1234567-8",
            "123-4",
            "12345678-5-6",
            "abcdefgh-5",
            "20000003-9",
            "",
            None,
        ],
    )
    def test_normalize_rut_invalido_retorna_none(self, crudo: object) -> None:
        assert normalize_rut(crudo) is None

    def test_split_rut_entrega_cuerpo_y_dv(self) -> None:
        assert split_rut("12.345.678-5") == ("12345678", "5")

    def test_split_rut_invalido_retorna_none(self) -> None:
        assert split_rut("malo") == (None, None)


class TestPersonas:
    """Limpieza de nombres y emails, y rechazo de basura."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("  ana   perez  ", "Ana Perez"),
            ("ANA PEREZ", "Ana Perez"),
            ("maria de la cruz", "Maria de la Cruz"),
            ("  juan  de   la    fuente ", "Juan de la Fuente"),
            ("josé miguel", "José Miguel"),
        ],
    )
    def test_normalize_name_valido(self, crudo: str, esperado: str) -> None:
        assert normalize_name(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo", ["", None, "123 ///", "***", "-", "123", "///"]
    )
    def test_normalize_name_basura_retorna_none(self, crudo: object) -> None:
        assert normalize_name(crudo) is None

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("  Ana.Perez@Example.COM ", "ana.perez@example.com"),
            ("ANA@EXAMPLE.COM", "ana@example.com"),
        ],
    )
    def test_normalize_email_valido(self, crudo: str, esperado: str) -> None:
        assert normalize_email(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [
            "correo-sin-arroba.cl",
            "ana@@gmail.com",
            "juan..perez@gmail.com",
            "nombre apellido@gmail.com",
            "",
            "sin dato",
            None,
        ],
    )
    def test_normalize_email_invalido_retorna_none(
        self, crudo: object
    ) -> None:
        assert normalize_email(crudo) is None


class TestNormalizeNameRegresion:
    """Regresión: tildes descompuestas, separadores y partículas internas."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            (unicodedata.normalize("NFD", "MARÍA PÉREZ"), "María Pérez"),
            (unicodedata.normalize("NFD", "JOSÉ MUÑOZ"), "José Muñoz"),
        ],
    )
    def test_normalize_name_nfd_capitaliza_por_tramo(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_name(crudo) == esperado

    def test_normalize_name_nfc_y_nfd_coinciden(self) -> None:
        nfd = unicodedata.normalize("NFD", "MARÍA PÉREZ")
        nfc = unicodedata.normalize("NFC", "MARÍA PÉREZ")

        assert normalize_name(nfd) == normalize_name(nfc) == "María Pérez"

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("O'HIGGINS", "O'Higgins"),
            ("JEAN-PIERRE", "Jean-Pierre"),
        ],
    )
    def test_normalize_name_conserva_apostrofo_y_guion(
        self, crudo: str, esperado: str
    ) -> None:
        assert normalize_name(crudo) == esperado

    def test_normalize_name_conserva_particulas_internas(self) -> None:
        assert normalize_name("JUAN DE LA FUENTE") == "Juan de la Fuente"

    @pytest.mark.parametrize("crudo", ["123 ///", "sin nombre"])
    def test_normalize_name_basura_retorna_none(self, crudo: str) -> None:
        assert normalize_name(crudo) is None
