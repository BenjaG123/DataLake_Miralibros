"""Pruebas del runner secuencial de pipelines."""

from __future__ import annotations

from typing import Any

import pytest

from etl.pipelines import __main__ as runner


def _instalar_pipelines(
    monkeypatch: pytest.MonkeyPatch,
    calls: list[tuple[str, dict[str, Any]]],
    *,
    fail: str | None = None,
) -> None:
    """Reemplaza ``run`` en cada pipeline por un doble que registra llamadas."""
    for name in runner.ORDER:
        module = runner.PIPELINES[name]

        def fake_run(
            *args: Any, _name: str = name, **kwargs: Any
        ) -> None:
            calls.append((_name, kwargs))
            if _name == fail:
                raise RuntimeError(f"falló {_name}")

        monkeypatch.setattr(module, "run", fake_run)


class TestSelectedPipelines:
    """Selección y orden canónico."""

    def test_sin_only_devuelve_todos(self) -> None:
        assert runner.selected_pipelines(None) == list(runner.ORDER)
        assert runner.selected_pipelines([]) == list(runner.ORDER)

    def test_un_pipeline(self) -> None:
        assert runner.selected_pipelines(["clima"]) == ["clima"]

    def test_respeta_el_orden_canonico(self) -> None:
        assert runner.selected_pipelines(["clima", "clientes"]) == [
            "clientes",
            "clima",
        ]

    def test_deduplica(self) -> None:
        assert runner.selected_pipelines(["clima", "clima"]) == ["clima"]


class TestArgumentos:
    """Defaults y validación de flags."""

    def test_valores_por_defecto(self) -> None:
        args = runner.build_parser().parse_args([])

        assert args.only is None
        assert args.dry_run is False
        assert args.schema_only is False
        assert args.log_level == "INFO"

    def test_only_repetible(self) -> None:
        args = runner.build_parser().parse_args(
            ["--only", "clima", "--only", "clientes"]
        )

        assert args.only == ["clima", "clientes"]

    def test_only_invalido_falla(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            runner.build_parser().parse_args(["--only", "otro"])

        assert excinfo.value.code == 2

    @pytest.mark.parametrize(
        ("flag", "atributo"),
        [("--dry-run", "dry_run"), ("--schema-only", "schema_only")],
    )
    def test_flags_de_modo(self, flag: str, atributo: str) -> None:
        args = runner.build_parser().parse_args([flag])

        assert getattr(args, atributo) is True

    def test_modos_excluyentes(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            runner.build_parser().parse_args(
                ["--dry-run", "--schema-only"]
            )

        assert excinfo.value.code == 2

    def test_log_level_invalido_falla(self) -> None:
        with pytest.raises(SystemExit) as excinfo:
            runner.build_parser().parse_args(["--log-level", "VERBOSE"])

        assert excinfo.value.code == 2


class TestMain:
    """Ejecución secuencial sin tocar pipelines reales."""

    def test_help_no_ejecuta(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        _instalar_pipelines(monkeypatch, calls)

        with pytest.raises(SystemExit) as excinfo:
            runner.main(["--help"])

        assert excinfo.value.code == 0
        assert calls == []
        salida = capsys.readouterr().out
        for flag in ("--only", "--dry-run", "--schema-only", "--log-level"):
            assert flag in salida

    def test_dry_run_ejecuta_todos_en_orden(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        niveles: list[str] = []
        _instalar_pipelines(monkeypatch, calls)
        monkeypatch.setattr(runner, "configure_logging", niveles.append)

        codigo = runner.main(["--dry-run", "--log-level", "debug"])

        assert codigo == 0
        assert [name for name, _ in calls] == list(runner.ORDER)
        assert all(
            kwargs == {"dry_run": True, "schema_only": False}
            for _, kwargs in calls
        )
        assert niveles == ["DEBUG"]

    def test_only_clima(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        _instalar_pipelines(monkeypatch, calls)

        codigo = runner.main(["--only", "clima", "--dry-run"])

        assert codigo == 0
        assert [name for name, _ in calls] == ["clima"]

    def test_only_respeta_el_orden_canonico(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        _instalar_pipelines(monkeypatch, calls)

        codigo = runner.main(
            ["--only", "clima", "--only", "clientes", "--dry-run"]
        )

        assert codigo == 0
        assert [name for name, _ in calls] == ["clientes", "clima"]

    def test_only_duplicado_no_repite(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        _instalar_pipelines(monkeypatch, calls)

        codigo = runner.main(
            ["--only", "clima", "--only", "clima", "--dry-run"]
        )

        assert codigo == 0
        assert [name for name, _ in calls] == ["clima"]

    def test_schema_only_se_propaga(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        _instalar_pipelines(monkeypatch, calls)

        codigo = runner.main(["--schema-only"])

        assert codigo == 0
        assert all(
            kwargs == {"dry_run": False, "schema_only": True}
            for _, kwargs in calls
        )

    def test_se_detiene_al_fallar(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, dict[str, Any]]] = []
        _instalar_pipelines(monkeypatch, calls, fail="clientes")

        codigo = runner.main(["--dry-run"])

        assert codigo == 1
        assert [name for name, _ in calls] == ["clientes"]
