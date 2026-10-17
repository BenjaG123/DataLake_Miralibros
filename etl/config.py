"""Configuración compartida del pipeline ETL."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "bronze"
SQL_DIR = BASE_DIR / "sql"
ENV_FILE = BASE_DIR / ".env"

DEFAULT_SCHEMA = "silver"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

load_dotenv(ENV_FILE)


def get_database_url() -> str:
    """Devuelve la URL de conexión a PostgreSQL o falla con un mensaje claro."""
    database_url = (os.getenv("DATABASE_URL") or "").strip()
    if not database_url:
        raise RuntimeError(
            "Falta la variable DATABASE_URL. "
            "Copiá .env.example a .env y configurá la conexión a PostgreSQL."
        )
    return database_url


def get_schema() -> str:
    """Devuelve y valida el schema donde se cargan las tablas."""
    schema = (os.getenv("PG_SCHEMA") or DEFAULT_SCHEMA).strip()
    if not _IDENTIFIER_RE.fullmatch(schema):
        raise RuntimeError(
            f"PG_SCHEMA inválido: {schema!r}. "
            "Usá solo letras, números y guión bajo."
        )
    return schema


def configure_logging(level: str = "INFO") -> None:
    """Configura logging uniforme para todos los módulos del ETL."""
    logging.basicConfig(
        level=level.upper() if isinstance(level, str) else level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )
