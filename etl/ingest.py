"""Ingesta Bronze: checksum SHA-256 y manifiesto de auditoría de Excels.

El script recorre un directorio (por defecto ``bronze/``), calcula el
checksum de cada ``*.xlsx``/``*.xls`` y mantiene un manifiesto JSONL
append-only con una línea por versión única ``(archivo, sha256)``.

No transforma valores, no mueve ni renombra archivos crudos y no toca la
base de datos: solo lee los primeros bytes para detectar el formato real y
agrega metadatos de trazabilidad al manifiesto.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import unicodedata
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from etl.config import BASE_DIR, BRONZE_DIR, configure_logging

logger = logging.getLogger(__name__)

MANIFEST_PATH = BRONZE_DIR / "manifest.jsonl"
EXTENSIONES = frozenset({".xlsx", ".xls"})
BLOQUE_LECTURA = 1 << 20
CABECERA_BYTES = 1024


def sha256_archivo(ruta: str | Path, bloque: int = BLOQUE_LECTURA) -> str:
    """Calcula el SHA-256 por bloques para no cargar archivos grandes."""
    hasher = hashlib.sha256()
    with Path(ruta).open("rb") as archivo:
        for trozo in iter(lambda: archivo.read(bloque), b""):
            hasher.update(trozo)
    return hasher.hexdigest()


def _normalizar_nombre(nombre: str) -> str:
    """Pasa el nombre a minúsculas sin acentos ni separadores variables."""
    descompuesto = unicodedata.normalize("NFKD", nombre)
    sin_acentos = "".join(
        caracter
        for caracter in descompuesto
        if not unicodedata.combining(caracter)
    )
    return re.sub(
        r"[^a-z0-9]+",
        "_",
        sin_acentos.casefold(),
    ).strip("_")


def inferir_entidad(nombre: str) -> str:
    """Infiere la entidad de negocio a partir del nombre exacto del archivo."""
    normalizado = _normalizar_nombre(nombre)

    if "cliente" in normalizado:
        return "clientes"
    if "stock" in normalizado:
        return "stock"
    if "venta" in normalizado:
        if "cafeteria" in normalizado:
            return "ventas_cafeteria"
        if "libreria" in normalizado:
            return "ventas_libreria"
        return "desconocido"
    if "temperatura" in normalizado or "temp" in normalizado:
        if "max" in normalizado or "maxim" in normalizado:
            return "clima_temp_max"
        if "min" in normalizado or "minim" in normalizado:
            return "clima_temp_min"
        if "media" in normalizado or "promedio" in normalizado:
            return "clima_temp_media"
        return "desconocido"
    if "humedad" in normalizado:
        return "clima_humedad_relativa"
    if "uv" in normalizado:
        return "clima_indice_uv"
    if "precipitacion" in normalizado:
        return "clima_precipitaciones"
    if "viento" in normalizado:
        return "clima_viento"
    return "desconocido"


def detectar_formato(ruta: str | Path) -> str:
    """Detecta el formato real mirando la firma de bytes, no la extensión."""
    with Path(ruta).open("rb") as archivo:
        cabecera = archivo.read(CABECERA_BYTES)

    if cabecera.startswith(b"PK\x03\x04"):
        return "xlsx"

    texto = cabecera.lower()
    if b"<html" in texto or b"<table" in texto:
        return "html"

    return "desconocido"


def leer_manifiesto(ruta: str | Path) -> list[dict[str, Any]]:
    """Lee el manifiesto JSONL y devuelve sus registros en orden."""
    archivo_manifiesto = Path(ruta)
    if not archivo_manifiesto.is_file():
        return []

    registros: list[dict[str, Any]] = []
    with archivo_manifiesto.open("r", encoding="utf-8") as archivo:
        for numero, linea in enumerate(archivo, start=1):
            linea = linea.strip()
            if not linea:
                continue
            try:
                registro = json.loads(linea)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Manifiesto inválido en {archivo_manifiesto}, "
                    f"línea {numero}: JSON malformado"
                ) from exc
            if not isinstance(registro, dict):
                raise ValueError(
                    f"Manifiesto inválido en {archivo_manifiesto}, "
                    f"línea {numero}: se esperaba un objeto JSON"
                )
            registros.append(registro)

    return registros


def _ultimas_versiones(
    registros: Sequence[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Devuelve el último registro de cada nombre de archivo."""
    ultimas: dict[str, dict[str, Any]] = {}
    for registro in registros:
        nombre = registro.get("archivo")
        if isinstance(nombre, str):
            ultimas[nombre] = registro
    return ultimas


def _ruta_relativa(ruta: Path, input_dir: Path) -> str:
    """Prefiere la ruta relativa a la raíz del repo y cae a la de entrada."""
    absoluta = ruta.resolve()
    for base in (BASE_DIR, input_dir):
        try:
            return absoluta.relative_to(base.resolve()).as_posix()
        except ValueError:
            continue
    return ruta.name


def _iso_utc(timestamp: float) -> str:
    """Convierte un timestamp POSIX a ISO 8601 en UTC."""
    return datetime.fromtimestamp(
        timestamp,
        tz=timezone.utc,
    ).isoformat(timespec="seconds")


def construir_registro(
    ruta: str | Path,
    input_dir: str | Path,
    *,
    sha256: str,
    ingerido_en_utc: str,
    version: int,
    hash_anterior: str | None,
    estado: str,
) -> dict[str, Any]:
    """Arma la línea de manifiesto para una versión del archivo."""
    archivo = Path(ruta)
    metadatos = archivo.stat()

    return {
        "archivo": archivo.name,
        "ruta_relativa": _ruta_relativa(archivo, Path(input_dir)),
        "tamano_bytes": metadatos.st_size,
        "mtime_utc": _iso_utc(metadatos.st_mtime),
        "sha256": sha256,
        "ingerido_en_utc": ingerido_en_utc,
        "entidad": inferir_entidad(archivo.name),
        "formato": detectar_formato(archivo),
        "version": version,
        "hash_anterior": hash_anterior,
        "estado": estado,
    }


def _listar_excels(input_dir: Path) -> list[Path]:
    """Lista solo archivos Excel, ordenados por nombre sin distinguir mayúsculas."""
    archivos = [
        ruta
        for ruta in input_dir.iterdir()
        if ruta.is_file() and ruta.suffix.lower() in EXTENSIONES
    ]
    return sorted(archivos, key=lambda ruta: ruta.name.casefold())


def ingerir(
    input_dir: str | Path = BRONZE_DIR,
    manifest: str | Path = MANIFEST_PATH,
    *,
    dry_run: bool = False,
) -> int:
    """Calcula checksums y agrega versiones nuevas al manifiesto.

    Retorna 0 cuando la ingesta termina, aunque no haya archivos. Si el
    directorio de entrada no existe, levanta ``FileNotFoundError``.
    """
    directorio = Path(input_dir)
    ruta_manifiesto = Path(manifest)

    if not directorio.is_dir():
        raise FileNotFoundError(
            f"No existe el directorio de entrada: {directorio}"
        )

    archivos = _listar_excels(directorio)
    if not archivos:
        logger.warning(
            "No se encontraron archivos .xlsx ni .xls en %s",
            directorio,
        )
        return 0

    registros_previos = leer_manifiesto(ruta_manifiesto)
    ultimas = _ultimas_versiones(registros_previos)
    ingerido_en_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    nuevos = 0
    duplicados = 0
    versiones_nuevas = 0
    pendientes: list[dict[str, Any]] = []

    for archivo in archivos:
        checksum = sha256_archivo(archivo)
        anterior = ultimas.get(archivo.name)

        if anterior is not None and anterior.get("sha256") == checksum:
            duplicados += 1
            logger.info(
                "duplicado sin cambios: %s (sha256=%s)",
                archivo.name,
                checksum,
            )
            continue

        if anterior is None:
            version = 1
            hash_anterior = None
            estado = "nuevo"
            nuevos += 1
        else:
            version = int(anterior.get("version", 0)) + 1
            hash_anterior = anterior.get("sha256")
            estado = "nueva_version"
            versiones_nuevas += 1

        registro = construir_registro(
            archivo,
            directorio,
            sha256=checksum,
            ingerido_en_utc=ingerido_en_utc,
            version=version,
            hash_anterior=hash_anterior,
            estado=estado,
        )
        pendientes.append(registro)
        logger.info(
            "archivo %s: %s, versión %d, formato %s",
            archivo.name,
            estado,
            version,
            registro["formato"],
        )

    if dry_run:
        logger.info(
            "Dry run: %d archivos nuevos o modificados; "
            "no se escribió el manifiesto",
            len(pendientes),
        )
    elif pendientes:
        ruta_manifiesto.parent.mkdir(parents=True, exist_ok=True)
        with ruta_manifiesto.open(
            "a",
            encoding="utf-8",
            newline="\n",
        ) as salida:
            for registro in pendientes:
                salida.write(
                    json.dumps(registro, ensure_ascii=False) + "\n"
                )

    logger.info(
        "Ingesta finalizada: %d nuevos, %d duplicados, %d versiones nuevas",
        nuevos,
        duplicados,
        versiones_nuevas,
    )
    return 0


def run(
    input_dir: str | Path = BRONZE_DIR,
    manifest: str | Path = MANIFEST_PATH,
    *,
    dry_run: bool = False,
) -> int:
    """Alias de ``ingerir`` para mantener la convención de los pipelines."""
    return ingerir(input_dir, manifest, dry_run=dry_run)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Calcula SHA-256 y registra los Excels de bronze en un "
            "manifiesto JSONL append-only."
        ),
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=BRONZE_DIR,
        help="Directorio con los archivos crudos (por defecto bronze/).",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=MANIFEST_PATH,
        help="Ruta del manifiesto JSONL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Calcula y muestra, pero no escribe el manifiesto.",
    )
    parser.add_argument(
        "--log-level",
        type=str.upper,
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Nivel de logging.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)

    try:
        ingerir(
            args.input_dir,
            args.manifest,
            dry_run=args.dry_run,
        )
    except Exception:
        logger.exception("Error durante la ingesta de la capa bronze")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
