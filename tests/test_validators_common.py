"""Pruebas de helpers compartidos de validación."""

from __future__ import annotations

import pytest

from etl.validators import is_placeholder


class TestIsPlaceholder:
    """Detección de vacíos y textos usados como dato faltante."""

    @pytest.mark.parametrize(
        "crudo",
        [
            None,
            "",
            "   ",
            "0",
            "sin dato",
            "N/A",
            "No Informado",
            "---",
        ],
    )
    def test_is_placeholder_true(self, crudo: object) -> None:
        assert is_placeholder(crudo) is True

    @pytest.mark.parametrize(
        "crudo",
        ["Ana", "1", "0.0", "sin datos", "2025-01-01"],
    )
    def test_is_placeholder_false(self, crudo: str) -> None:
        assert is_placeholder(crudo) is False
