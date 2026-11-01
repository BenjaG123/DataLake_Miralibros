"""Flags y ejecución del pipeline de clima sin tocar PostgreSQL."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import pytest

from etl.pipelines import clima


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
        args = clima.build_parser().parse_args([])

        assert args.input_dir == clima.BRONZE_DIR
        assert args.dry_run is False
        assert args.schema_only is False
        assert args.log_level == "INFO"

    @pytest.mark.parametrize(
        ("flag", "atributo"),
        [("--dry-run", "dry_run"), ("--schema-only", "schema_only")],
    )
    def test_flags_de_modo(self, flag: str, atributo: str) -> None:
        args = clima.build_parser().parse_args([flag])

        assert getattr(args, atributo) is True

    def test_input_dir_y_log_level(self, tmp_path: Path) -> None:
        args = clima.build_parser().parse_args(
            ["--input-dir", str(tmp_path), "--log-level", "debug"]
        )

        assert args.input_dir == tmp_path
        assert args.log_level == "DEBUG"

    def test_dry_run_y_schema_only_son_excluyentes(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            clima.build_parser().parse_args(["--dry-run", "--schema-only"])

        assert excinfo.value.code == 2

    def test_log_level_invalido_falla(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            clima.build_parser().parse_args(["--log-level", "VERBOSE"])

        assert excinfo.value.code == 2


class TestSinBaseDeDatos:
    """``--help`` y ``--dry-run`` no requieren DATABASE_URL."""

    def test_help_no_requiere_base(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            clima.main(["--help"])

        assert excinfo.value.code == 0
        salida = capsys.readouterr().out
        for flag in ("--dry-run", "--schema-only", "--input-dir", "--log-level"):
            assert flag in salida

    def test_dry_run_no_toca_la_base(
        self, monkeypatch: pytest.MonkeyPatch, clima_dir: Path
    ) -> None:
        monkeypatch.setattr(clima.db, "connection", _conexion_prohibida)
        monkeypatch.setattr(clima.db, "execute_sql_file", _escritura_prohibida)
        monkeypatch.setattr(clima.db, "upsert_table", _escritura_prohibida)

        clima.run(clima_dir, dry_run=True)

    def test_main_dry_run_retorna_cero_sin_base(
        self, monkeypatch: pytest.MonkeyPatch, clima_dir: Path
    ) -> None:
        monkeypatch.setattr(clima.db, "connection", _conexion_prohibida)

        codigo = clima.main(["--input-dir", str(clima_dir), "--dry-run"])

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
        monkeypatch.setattr(clima.db, "connection", conexion_falsa)
        monkeypatch.setattr(clima.db, "execute_sql_file", execute_falso)
        monkeypatch.setattr(clima.db, "upsert_table", _escritura_prohibida)

        clima.run(schema_only=True)

        assert llamadas["conectado"] is True
        assert llamadas["sql"] == clima.SQL_FILE
        assert llamadas["schema"] == "test_silver"
