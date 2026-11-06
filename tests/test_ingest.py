"""Pruebas de la ingesta bronze: checksum, manifiesto e idempotencia."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from etl import ingest

CONTENIDO_XLSX = b"PK\x03\x04contenido-xlsx-sintetico"
CONTENIDO_HTML = (
    b"<html><body><table><tr><td>1</td></tr></table></body></html>"
)
CAMPOS_OBLIGATORIOS = {
    "archivo",
    "ruta_relativa",
    "tamano_bytes",
    "mtime_utc",
    "sha256",
    "ingerido_en_utc",
    "entidad",
    "formato",
    "version",
    "hash_anterior",
}


def _escribir(ruta: Path, contenido: bytes) -> Path:
    """Escribe un archivo ficticio y devuelve su ruta."""
    ruta.write_bytes(contenido)
    return ruta


def _leer_manifiesto(ruta: Path) -> list[dict[str, Any]]:
    """Lee las líneas JSONL del manifiesto."""
    return [
        json.loads(linea)
        for linea in ruta.read_text(encoding="utf-8").splitlines()
        if linea.strip()
    ]


class TestSha256:
    """El checksum calculado debe coincidir con hashlib."""

    def test_coincide_con_hashlib(self, tmp_path: Path) -> None:
        contenido = b"abc" * 1000
        ruta = _escribir(tmp_path / "CLIENTES LIBRERIA.xlsx", contenido)

        assert ingest.sha256_archivo(ruta) == hashlib.sha256(
            contenido
        ).hexdigest()

    def test_por_bloques_pequenos(self, tmp_path: Path) -> None:
        contenido = bytes(range(256)) * 10
        ruta = _escribir(tmp_path / "lote.xlsx", contenido)

        assert ingest.sha256_archivo(ruta, bloque=7) == hashlib.sha256(
            contenido
        ).hexdigest()


class TestEntidad:
    """Inferencia de entidad por nombre exacto."""

    @pytest.mark.parametrize(
        ("nombre", "esperado"),
        [
            ("CLIENTES LIBRERIA.xlsx", "clientes"),
            ("STOCK LIBRERIA.xlsx", "stock"),
            ("VENTAS LIBRERIA.xlsx", "ventas_libreria"),
            ("VENTAS CAFETERIA.xlsx", "ventas_cafeteria"),
            ("TemperaturaMax2025.xls", "clima_temp_max"),
            ("TemperaturaMin2025.xls", "clima_temp_min"),
            ("TemperaturaMedia2025.xls", "clima_temp_media"),
            ("HumedadRelativaAire2025.xls", "clima_humedad_relativa"),
            ("IndiceUV2025.xls", "clima_indice_uv"),
            ("Precipitaciones2025.xls", "clima_precipitaciones"),
            ("Viento2025.xls", "clima_viento"),
            ("archivo sin pistas.xls", "desconocido"),
        ],
    )
    def test_nombres_conocidos(self, nombre: str, esperado: str) -> None:
        assert ingest.inferir_entidad(nombre) == esperado


class TestFormato:
    """Detección por firma de bytes, sin pandas."""

    def test_zip_es_xlsx(self, tmp_path: Path) -> None:
        ruta = _escribir(tmp_path / "real.xlsx", CONTENIDO_XLSX)

        assert ingest.detectar_formato(ruta) == "xlsx"

    def test_html_en_mayusculas(self, tmp_path: Path) -> None:
        ruta = _escribir(
            tmp_path / "clima.xls",
            b"<HTML><BODY><TABLE></TABLE></BODY></HTML>",
        )

        assert ingest.detectar_formato(ruta) == "html"

    def test_contenido_desconocido(self, tmp_path: Path) -> None:
        ruta = _escribir(tmp_path / "raro.xls", b"no es excel ni html")

        assert ingest.detectar_formato(ruta) == "desconocido"

    def test_archivo_vacio_es_desconocido(self, tmp_path: Path) -> None:
        ruta = _escribir(tmp_path / "vacio.xlsx", b"")

        assert ingest.detectar_formato(ruta) == "desconocido"


class TestManifiesto:
    """Campos, versiones e idempotencia del manifiesto JSONL."""

    def test_registra_campos_y_entidad(self, tmp_path: Path) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "meta" / "manifest.jsonl"
        archivo = _escribir(
            entrada / "CLIENTES LIBRERIA.xlsx",
            CONTENIDO_XLSX,
        )

        assert ingest.ingerir(entrada, manifiesto) == 0

        registros = _leer_manifiesto(manifiesto)
        assert len(registros) == 1
        registro = registros[0]
        assert CAMPOS_OBLIGATORIOS <= set(registro)
        assert registro["archivo"] == "CLIENTES LIBRERIA.xlsx"
        assert registro["ruta_relativa"] == "CLIENTES LIBRERIA.xlsx"
        assert not Path(registro["ruta_relativa"]).is_absolute()
        assert registro["tamano_bytes"] == archivo.stat().st_size
        assert registro["sha256"] == hashlib.sha256(
            CONTENIDO_XLSX
        ).hexdigest()
        assert registro["entidad"] == "clientes"
        assert registro["formato"] == "xlsx"
        assert registro["version"] == 1
        assert registro["hash_anterior"] is None
        assert registro["estado"] == "nuevo"
        datetime.fromisoformat(registro["mtime_utc"])
        datetime.fromisoformat(registro["ingerido_en_utc"])

    def test_xls_html_y_entidad_clima(self, tmp_path: Path) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "manifest.jsonl"
        _escribir(entrada / "TemperaturaMax2025.xls", CONTENIDO_HTML)

        assert ingest.ingerir(entrada, manifiesto) == 0

        registro = _leer_manifiesto(manifiesto)[0]
        assert registro["formato"] == "html"
        assert registro["entidad"] == "clima_temp_max"

    def test_segunda_corrida_es_duplicada(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "manifest.jsonl"
        _escribir(entrada / "STOCK LIBRERIA.xlsx", CONTENIDO_XLSX)

        assert ingest.ingerir(entrada, manifiesto) == 0
        contenido_original = manifiesto.read_bytes()

        with caplog.at_level(logging.INFO, logger="etl.ingest"):
            assert ingest.ingerir(entrada, manifiesto) == 0

        assert manifiesto.read_bytes() == contenido_original
        assert len(_leer_manifiesto(manifiesto)) == 1
        assert "duplicado" in caplog.text.lower()

    def test_archivo_modificado_crea_nueva_version(
        self, tmp_path: Path
    ) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "manifest.jsonl"
        archivo = _escribir(entrada / "Viento2025.xls", b"PK\x03\x04v1")

        assert ingest.ingerir(entrada, manifiesto) == 0
        primero = _leer_manifiesto(manifiesto)[0]

        contenido_nuevo = b"PK\x03\x04v2"
        archivo.write_bytes(contenido_nuevo)

        assert ingest.ingerir(entrada, manifiesto) == 0

        registros = _leer_manifiesto(manifiesto)
        assert len(registros) == 2
        assert registros[0] == primero
        segundo = registros[1]
        assert segundo["version"] == 2
        assert segundo["hash_anterior"] == primero["sha256"]
        assert segundo["sha256"] == hashlib.sha256(
            contenido_nuevo
        ).hexdigest()
        assert segundo["estado"] == "nueva_version"

    def test_dry_run_no_crea_manifiesto(self, tmp_path: Path) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "meta" / "manifest.jsonl"
        _escribir(entrada / "CLIENTES LIBRERIA.xlsx", CONTENIDO_XLSX)

        assert ingest.ingerir(entrada, manifiesto, dry_run=True) == 0

        assert not manifiesto.exists()
        assert not manifiesto.parent.exists()

    def test_directorio_vacio_retorna_cero(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        manifiesto = tmp_path / "manifest.jsonl"

        with caplog.at_level(logging.WARNING, logger="etl.ingest"):
            assert ingest.ingerir(tmp_path, manifiesto) == 0

        assert not manifiesto.exists()
        assert "no se encontraron archivos" in caplog.text.lower()

    def test_directorio_inexistente_falla(self, tmp_path: Path) -> None:
        with pytest.raises(
            FileNotFoundError,
            match="No existe el directorio de entrada",
        ):
            ingest.ingerir(
                tmp_path / "no_existe",
                tmp_path / "manifest.jsonl",
            )

    def test_ignora_archivos_no_excel(self, tmp_path: Path) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "manifest.jsonl"
        _escribir(entrada / "Z STOCK LIBRERIA.xlsx", CONTENIDO_XLSX)
        _escribir(entrada / "a CLIENTES LIBRERIA.xlsx", CONTENIDO_XLSX)
        _escribir(entrada / "notas.txt", b"texto")
        _escribir(entrada / "datos.csv", b"a,b\n1,2\n")

        assert ingest.ingerir(entrada, manifiesto) == 0

        nombres = [
            registro["archivo"]
            for registro in _leer_manifiesto(manifiesto)
        ]
        assert nombres == [
            "a CLIENTES LIBRERIA.xlsx",
            "Z STOCK LIBRERIA.xlsx",
        ]

    def test_extension_en_mayusculas_se_incluye(self, tmp_path: Path) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "manifest.jsonl"
        _escribir(entrada / "REAL.XLSX", CONTENIDO_XLSX)

        assert ingest.ingerir(entrada, manifiesto) == 0

        assert _leer_manifiesto(manifiesto)[0]["archivo"] == "REAL.XLSX"


class TestMain:
    """CLI y manejo de errores del punto de entrada."""

    def test_dry_run_por_cli_no_escribe(self, tmp_path: Path) -> None:
        entrada = tmp_path / "entrada"
        entrada.mkdir()
        manifiesto = tmp_path / "manifest.jsonl"
        _escribir(entrada / "clientes.xlsx", CONTENIDO_XLSX)

        codigo = ingest.main(
            [
                "--input-dir",
                str(entrada),
                "--manifest",
                str(manifiesto),
                "--dry-run",
            ]
        )

        assert codigo == 0
        assert not manifiesto.exists()

    def test_directorio_inexistente_retorna_uno(
        self, tmp_path: Path
    ) -> None:
        codigo = ingest.main(
            [
                "--input-dir",
                str(tmp_path / "no_existe"),
                "--manifest",
                str(tmp_path / "manifest.jsonl"),
            ]
        )

        assert codigo == 1

    def test_parser_valores_por_defecto(self) -> None:
        args = ingest.build_parser().parse_args([])

        assert args.input_dir == ingest.BRONZE_DIR
        assert args.manifest == ingest.MANIFEST_PATH
        assert args.dry_run is False
        assert args.log_level == "INFO"
