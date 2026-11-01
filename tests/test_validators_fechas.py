"""Pruebas de validadores de fechas y meses en español."""

from __future__ import annotations

from datetime import date

import pytest

from etl.validators import (
    parse_date_dmy,
    parse_date_iso,
    parse_day_month_es,
)


class TestParseDateDmy:
    """Fechas d/m/Y y d-m-Y con bordes de calendario y de rango."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("14/06/2024", date(2024, 6, 14)),
            ("14-06-2024", date(2024, 6, 14)),
            ("1/2/2024", date(2024, 2, 1)),
            ("29/02/2024", date(2024, 2, 29)),
            ("31/12/1999", date(1999, 12, 31)),
            ("01/01/2000", date(2000, 1, 1)),
        ],
    )
    def test_parse_date_dmy_valida(
        self, crudo: str, esperado: date
    ) -> None:
        assert parse_date_dmy(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [
            "29/02/2023",
            "31/02/2024",
            "32/01/2024",
            "14/13/2024",
            "0/1/2024",
            "2024-06-14",
            "14/06",
            "texto",
            "",
            None,
        ],
    )
    def test_parse_date_dmy_invalida_retorna_none(self, crudo: object) -> None:
        assert parse_date_dmy(crudo) is None

    def test_parse_date_dmy_respeta_min_date(self) -> None:
        minimo = date(2000, 1, 1)
        assert parse_date_dmy("01/01/2000", min_date=minimo) == minimo
        assert parse_date_dmy("31/12/1999", min_date=minimo) is None

    def test_parse_date_dmy_respeta_max_date(self) -> None:
        maximo = date(2024, 6, 30)
        assert parse_date_dmy("30/06/2024", max_date=maximo) == maximo
        assert parse_date_dmy("01/07/2024", max_date=maximo) is None


class TestParseDayMonthEs:
    """Matriz aprobada de "día y mes" en español (sin año)."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("14 de junio", (14, 6)),
            ("2 de agosto", (2, 8)),
            ("14 - junio", (14, 6)),
            ("2 - agosto", (2, 8)),
            ("2-Agosto", (2, 8)),
            ("14 DE JUNIO", (14, 6)),
            ("14 De Junio", (14, 6)),
            ("14\u00a0de\u00a0junio", (14, 6)),
            ("  14  -  junio  ", (14, 6)),
            ("14 de junio.", (14, 6)),
            ("29 de febrero", (29, 2)),
            ("29 de setiembre", (29, 9)),
        ],
    )
    def test_parse_day_month_es_valida(
        self, crudo: str, esperado: tuple[int, int]
    ) -> None:
        assert parse_day_month_es(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [
            "14 de junio de 1990",
            "32 de junio",
            "30 de febrero",
            "31 de abril",
            "0 de junio",
            "14 de mes inexistente",
            "14dejunio",
            "14/06",
            "1990-06-14",
            "",
            None,
            "-",
            "sin dato",
        ],
    )
    def test_parse_day_month_es_invalida_retorna_none(
        self, crudo: object
    ) -> None:
        assert parse_day_month_es(crudo) == (None, None)


class TestParseDateIso:
    """Fechas ISO estrictas YYYY-MM-DD con bordes de calendario y rango."""

    @pytest.mark.parametrize(
        ("crudo", "esperado"),
        [
            ("2025-01-01", date(2025, 1, 1)),
            ("2024-02-29", date(2024, 2, 29)),
            ("1999-12-31", date(1999, 12, 31)),
        ],
    )
    def test_parse_date_iso_valida(
        self, crudo: str, esperado: date
    ) -> None:
        assert parse_date_iso(crudo) == esperado

    @pytest.mark.parametrize(
        "crudo",
        [
            "2025-1-1",
            "2025-02-29",
            "2025-02-30",
            "2025-13-01",
            "01/01/2025",
            "2025/01/01",
            "2025-01-01T00:00:00",
            "",
            None,
            "sin dato",
        ],
    )
    def test_parse_date_iso_invalida_retorna_none(
        self, crudo: object
    ) -> None:
        assert parse_date_iso(crudo) is None

    def test_parse_date_iso_respeta_min_date(self) -> None:
        minimo = date(2025, 1, 2)
        assert parse_date_iso("2025-01-02", min_date=minimo) == minimo
        assert parse_date_iso("2025-01-01", min_date=minimo) is None

    def test_parse_date_iso_respeta_max_date(self) -> None:
        maximo = date(2024, 12, 31)
        assert parse_date_iso("2024-12-31", max_date=maximo) == maximo
        assert parse_date_iso("2025-01-01", max_date=maximo) is None
