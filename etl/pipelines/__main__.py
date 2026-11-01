"""Ejecutor secuencial de los pipelines del Data Lake."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from etl.config import configure_logging
from etl.pipelines import clientes, clima, stock, ventas

logger = logging.getLogger(__name__)

PIPELINES = {
    "clientes": clientes,
    "stock": stock,
    "ventas": ventas,
    "clima": clima,
}
ORDER = ("clientes", "stock", "ventas", "clima")


def selected_pipelines(only: Sequence[str] | None) -> list[str]:
    """Devuelve los pipelines elegidos en el orden canónico."""
    if not only:
        return list(ORDER)

    selected = set(only)
    return [name for name in ORDER if name in selected]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Ejecuta los pipelines del Data Lake en orden secuencial."
        ),
    )
    parser.add_argument(
        "--only",
        action="append",
        choices=ORDER,
        default=None,
        help=(
            "Pipeline a ejecutar; se puede repetir. "
            "Por defecto ejecuta todos."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Lee y transforma, pero no escribe en PostgreSQL.",
    )
    mode.add_argument(
        "--schema-only",
        action="store_true",
        help="Solo crea schemas y tablas.",
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

    names = selected_pipelines(args.only)
    try:
        for name in names:
            logger.info("Ejecutando pipeline %s", name)
            PIPELINES[name].run(
                dry_run=args.dry_run,
                schema_only=args.schema_only,
            )
    except Exception:
        logger.exception("Error ejecutando los pipelines")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
