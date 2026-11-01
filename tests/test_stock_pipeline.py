"""Flags y ejecución del pipeline de stock sin tocar PostgreSQL."""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, Iterator

import pytest

from etl.pipelines import stock


@contextmanager
def _conexion_prohibida(*args: Any, **kwargs: Any) -> Iterator[object]:
    """Doble que falla si el pipeline intenta conectarse a PostgreSQL."""
    raise AssertionError("el pipeline intentó conectarse a PostgreSQL")
    yield  # pragma: no cover


def _escritura_prohibida(*args: Any, **kwargs: Any) -> None:
    """Doble que falla si el pipeline intenta escribir en PostgreSQL."""
    raise AssertionError("el pipeline intentó escribir en PostgreSQL")


class TestArgumentos:
    """Valores por defecto y validación de flags."""

    def test_valores_por_defecto(self) -> None:
        args = stock.build_parser().parse_args([])

        assert args.input == stock.INPUT_PATH
        assert args.dry_run is False
        assert args.schema_only is False
        assert args.log_level == "INFO"

    @pytest.mark.parametrize(
        ("flag", "atributo"),
        [("--dry-run", "dry_run"), ("--schema-only", "schema_only")],
    )
    def test_flags_de_modo(self, flag: str, atributo: str) -> None:
        args = stock.build_parser().parse_args([flag])

        assert getattr(args, atributo) is True

    def test_input_y_log_level(self) -> None:
        ruta = Path("tests") / "STOCK sintetico.xlsx"

        args = stock.build_parser().parse_args(
            ["--input", str(ruta), "--log-level", "debug"]
        )

        assert args.input == ruta
        assert args.log_level == "DEBUG"

    def test_dry_run_y_schema_only_son_excluyentes(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            stock.build_parser().parse_args(["--dry-run", "--schema-only"])

        assert excinfo.value.code == 2

    def test_log_level_invalido_falla(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            stock.build_parser().parse_args(["--log-level", "VERBOSE"])

        assert excinfo.value.code == 2


class TestSinBaseDeDatos:
    """``--help`` y ``--dry-run`` no requieren DATABASE_URL."""

    def test_help_no_requiere_base(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            stock.main(["--help"])

        assert excinfo.value.code == 0
        salida = capsys.readouterr().out
        for flag in ("--dry-run", "--schema-only", "--input", "--log-level"):
            assert flag in salida

    def test_dry_run_no_toca_la_base(
        self, monkeypatch: pytest.MonkeyPatch, archivos_fixtures: dict[str, Path]
    ) -> None:
        monkeypatch.setattr(stock.db, "connection", _conexion_prohibida)
        monkeypatch.setattr(stock.db, "execute_sql_file", _escritura_prohibida)
        monkeypatch.setattr(stock.db, "upsert_table", _escritura_prohibida)

        stock.run(archivos_fixtures["stock"], dry_run=True)

    def test_main_dry_run_retorna_cero_sin_base(
        self, monkeypatch: pytest.MonkeyPatch, archivos_fixtures: dict[str, Path]
    ) -> None:
        monkeypatch.setattr(stock.db, "connection", _conexion_prohibida)

        codigo = stock.main(
            ["--input", str(archivos_fixtures["stock"]), "--dry-run"]
        )

        assert codigo == 0


class TestSchemaOnly:
    """``--schema-only`` usa la conexión pero no carga filas."""

    def test_schema_only_usa_la_conexion_sin_escribir(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        llamadas: dict[str, Any] = {}

        @contextmanager
        def conexion_falsa(*args: Any, **kwargs: Any) -> Iterator[object]:
            llamadas["conectado"] = True
            yield object()

        def execute_falso(
            conn: object, sql_path: Path, *, schema: str | None = None
        ) -> None:
            llamadas["sql"] = sql_path
            llamadas["schema"] = schema

        monkeypatch.setenv("PG_SCHEMA", "test_silver")
        monkeypatch.setattr(stock.db, "connection", conexion_falsa)
        monkeypatch.setattr(stock.db, "execute_sql_file", execute_falso)
        monkeypatch.setattr(stock.db, "upsert_table", _escritura_prohibida)

        stock.run(schema_only=True)

        assert llamadas["conectado"] is True
        assert llamadas["sql"] == stock.SQL_FILE
        assert llamadas["schema"] == "test_silver"


class TestCarga:
    """La carga usa upsert y refresca el historial sin abrir PostgreSQL real."""

    def test_ejecuta_sql_upsert_e_historial(
        self, monkeypatch: pytest.MonkeyPatch, archivos_fixtures: dict[str, Path]
    ) -> None:
        llamadas: dict[str, Any] = {}

        class _ConexionFalsa:
            def transaction(self) -> object:
                return nullcontext()

        @contextmanager
        def conexion_falsa(*args: Any, **kwargs: Any) -> Iterator[object]:
            yield _ConexionFalsa()

        def execute_falso(
            conn: object, sql_path: Path, *, schema: str | None = None
        ) -> None:
            llamadas["sql"] = sql_path

        def upsert_falso(
            conn: object,
            schema: str,
            table: str,
            columns: object,
            keys: object,
            rows: object,
            **kwargs: object,
        ) -> int:
            llamadas["upsert"] = (table, len(rows))
            return len(rows)

        def historial_falso(conn: object, schema: str) -> tuple[int, int]:
            llamadas["historial"] = True
            return (0, 82)

        monkeypatch.setenv("PG_SCHEMA", "test_silver")
        monkeypatch.setattr(stock.db, "connection", conexion_falsa)
        monkeypatch.setattr(stock.db, "execute_sql_file", execute_falso)
        monkeypatch.setattr(stock.db, "upsert_table", upsert_falso)
        monkeypatch.setattr(stock, "_refresh_history", historial_falso)

        stock.run(archivos_fixtures["stock"])

        assert llamadas["sql"] == stock.SQL_FILE
        assert llamadas["upsert"] == ("stock", 82)
        assert llamadas["historial"] is True
