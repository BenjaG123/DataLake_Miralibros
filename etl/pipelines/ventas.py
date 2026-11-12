"""Pipeline incremental de ventas con cabecera y detalle.

Cada archivo de ventas (librería o cafetería) se normaliza y se carga en dos
tablas:

- ``{{schema}}.ventas_ticket``: cabecera con clave de negocio
  ``(origen, correlativo)``.
- ``{{schema}}.ventas_linea``: detalle reconstruido de forma determinística a
  partir de ``total_venta`` y ``cant_items``.

El enriquecimiento es **in-memory**: el pipeline lee clientes, stock y el
catálogo de cafetería desde los archivos crudos y no consulta la base para
esas entidades. ``--dry-run`` no abre conexión a PostgreSQL.

Decisiones de algoritmos:

- Las líneas se reconstruyen con programación dinámica de bitsets por origen.
  Para cada ``k`` se calculan las sumas alcanzables con exactamente ``k``
  precios; luego se reconstruye una multiset determinística (sin azar).
- Los ``cliente_id`` se asignan con subset sum exacto por cliente. Los
  clientes se ordenan por ``compras`` descendente y luego por ``cliente_id``
  para dar prioridad a los montos mayores; un cliente sin subconjunto exacto
  queda sin asignación y se registra un ``WARNING``. La búsqueda es iterativa
  y tiene límites de iteraciones y de tiempo para no bloquear el pipeline.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import time as time_module
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from math import gcd
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg import sql

from etl import db
from etl.config import BRONZE_DIR, SQL_DIR, configure_logging, get_schema
from etl.pipelines import clientes, stock
from etl.quarantine import (
    QUARANTINE_SQL,
    flatten_rejections,
    insert_quarantine,
)
from etl.readers import (
    read_csv_as_str,
    read_excel_as_str,
    require_columns,
)
from etl.validators import (
    is_placeholder,
    parse_date_iso,
    parse_integer,
    parse_non_negative_bigint,
)

logger = logging.getLogger(__name__)

INPUT_PATHS = (
    BRONZE_DIR / "VENTAS LIBRERIA.xlsx",
    BRONZE_DIR / "VENTAS CAFETERIA.xlsx",
)
CLIENTES_PATH = BRONZE_DIR / "CLIENTES LIBRERIA.xlsx"
STOCK_PATH = BRONZE_DIR / "STOCK LIBRERIA.xlsx"
CATALOGO_CAFETERIA_PATH = BRONZE_DIR / "CATALOGO CAFETERIA.csv"
SQL_FILE = SQL_DIR / "ventas.sql"

TICKET_TABLE = "ventas_ticket"
LINE_TABLE = "ventas_linea"
TABLE = TICKET_TABLE
ENTITY = "ventas"

SOURCE_COLUMNS = (
    "Correlativo",
    "Orden Web",
    "Fecha",
    "Medio de Pago",
    "Tipo Documento",
    "Folio",
    "Voucher Máquina de Pago",
    "Total Venta",
    "Cant. Items",
    "Canal",
    "Feria",
    "Tiene Descuento",
    "Estado",
    "Es Dev.",
)

# Columnas mínimas para validar y cargar cada ticket. Las columnas opcionales
# (por ejemplo ``Orden Web``, ``Canal`` o ``Estado``) pueden faltar sin romper
# la lectura; ``Origen`` nunca se exige porque se infiere del nombre del archivo.
REQUIRED_COLUMNS = (
    "Correlativo",
    "Fecha",
    "Tipo Documento",
    "Folio",
    "Total Venta",
    "Cant. Items",
)

# Columnas que se descartan en cuanto se lee el archivo: no forman parte del
# modelo ``ventas_ticket``/``ventas_linea`` y no deben llegar a la validación.
DROPPED_COLUMNS = (
    "Voucher Máquina de Pago",
    "Feria",
)

TICKET_COLUMNS = (
    "origen",
    "correlativo",
    "fecha",
    "hora_simulada",
    "cliente_id",
    "medio_pago",
    "tipo_documento",
    "folio",
    "total_venta",
    "cant_items",
    "tiene_descuento",
    "es_devolucion",
    "estado",
    "_source_file",
    "_loaded_at",
    "updated_at",
)

TICKET_UPDATE_COLUMNS = (
    "fecha",
    "hora_simulada",
    "cliente_id",
    "medio_pago",
    "tipo_documento",
    "folio",
    "total_venta",
    "cant_items",
    "tiene_descuento",
    "es_devolucion",
    "estado",
    "_source_file",
    "_loaded_at",
    "updated_at",
)

LINE_COLUMNS = (
    "origen",
    "correlativo",
    "linea",
    "codigo_interno",
    "cantidad",
    "precio_unitario",
    "total_linea",
    "_loaded_at",
)

KEY_COLUMNS = ("origen", "correlativo")

# Alias de compatibilidad para consumidores que importaban ``COLUMNS``.
COLUMNS = TICKET_COLUMNS
UPDATE_COLUMNS = TICKET_UPDATE_COLUMNS

REGLA_LINEA = "VENTAS-LINEA-001"
MOTIVO_SIN_COMBINACION = "sin combinación exacta de líneas"

MAX_ITERACIONES_SUBSET_SUM = 250_000
MAX_SEGUNDOS_SUBSET_SUM = 2.0

# Máximo de tickets por consulta de borrado de líneas. Cada ticket aporta dos
# parámetros ``(origen, correlativo)``; 5.000 tickets = 10.000 parámetros, muy
# por debajo del límite de PostgreSQL (65.535) sin perder eficiencia.
LINE_DELETE_CHUNK_SIZE = 5_000


class SubsetSumTimeout(RuntimeError):
    """La búsqueda de subconjunto superó el límite de iteraciones o tiempo.

    El mensaje nunca incluye montos ni RUT para no filtrar datos personales.
    """


@dataclass(frozen=True)
class Ticket:
    """Fila normalizada de la cabecera de una venta."""

    origen: str
    correlativo: int
    fecha: date
    hora_simulada: time | None
    medio_pago: str | None
    tipo_documento: str | None
    folio: str | None
    total_venta: int
    cant_items: int
    tiene_descuento: str | None
    es_devolucion: str | None
    estado: str | None
    source_file: str
    loaded_at: datetime
    fila: int

    def to_db_row(self, cliente_id: str | None) -> tuple[Any, ...]:
        """Devuelve la tupla en el orden exacto de ``TICKET_COLUMNS``."""
        return (
            self.origen,
            self.correlativo,
            self.fecha,
            self.hora_simulada,
            cliente_id,
            self.medio_pago,
            self.tipo_documento,
            self.folio,
            self.total_venta,
            self.cant_items,
            self.tiene_descuento,
            self.es_devolucion,
            self.estado,
            self.source_file,
            self.loaded_at,
            self.loaded_at,
        )


# Alias histórico conservado para importaciones existentes.
Row = Ticket


@dataclass(frozen=True)
class Line:
    """Fila normalizada de una línea de venta."""

    origen: str
    correlativo: int
    linea: int
    codigo_interno: str
    cantidad: int
    precio_unitario: int
    total_linea: int
    loaded_at: datetime

    def to_db_row(self) -> tuple[Any, ...]:
        """Devuelve la tupla en el orden exacto de ``LINE_COLUMNS``."""
        return (
            self.origen,
            self.correlativo,
            self.linea,
            self.codigo_interno,
            self.cantidad,
            self.precio_unitario,
            self.total_linea,
            self.loaded_at,
        )


@dataclass(frozen=True)
class Client:
    """Cliente anónimo en memoria para el cuadre de montos."""

    cliente_id: str
    alta: date | None
    compras: int


@dataclass(frozen=True)
class PriceCatalog:
    """Precios distintos y códigos determinísticos por precio."""

    prices: tuple[int, ...]
    codes_by_price: dict[int, tuple[str, ...]]


def _clean_text(value: Any) -> str | None:
    """Colapsa espacios y convierte placeholders en ``None``."""
    raw = "" if value is None else str(value).strip()
    if not raw or is_placeholder(raw):
        return None
    return " ".join(raw.split())


def inferir_origen(path: str | Path) -> str:
    """Infiere ``LIBRERIA`` o ``CAFETERIA`` desde el nombre del archivo."""
    nombre = Path(path).name.upper().replace("Í", "I")
    if "CAFETERIA" in nombre:
        return "CAFETERIA"
    if "LIBRERIA" in nombre:
        return "LIBRERIA"
    raise ValueError(
        "No se pudo inferir el origen del archivo "
        f"{Path(path).name!r}: el nombre debe contener LIBRERIA o CAFETERIA."
    )


def _leer_ventas(path: str | Path) -> pd.DataFrame:
    """Lee un Excel de ventas y limpia las columnas que no se usan.

    Solo se exigen las columnas de ``REQUIRED_COLUMNS``; las opcionales pueden
    faltar y la columna ``Origen`` del archivo (si existe) se acepta como extra
    sin exigirse. Las columnas de ``DROPPED_COLUMNS`` se eliminan de inmediato
    para que la validación y el modelo no las consideren.
    """
    dataframe = read_excel_as_str(path)
    require_columns(dataframe, REQUIRED_COLUMNS, source=path)
    return dataframe.drop(columns=list(DROPPED_COLUMNS), errors="ignore")


def normalize_canal(value: Any) -> str | None:
    """Normaliza el canal a ``Local`` o ``Web``; otro valor queda ``None``."""
    raw = _clean_text(value)
    if raw is None:
        return None
    return {"local": "Local", "web": "Web"}.get(raw.casefold())


def normalize_si_no(value: Any) -> str | None:
    """Normaliza un flag a ``SI``/``NO``; un valor no reconocido queda ``None``."""
    raw = _clean_text(value)
    if raw is None:
        return None
    key = raw.upper()
    if key in ("SI", "SÍ", "S"):
        return "SI"
    if key in ("NO", "N"):
        return "NO"
    return None


def _detalle(
    columna: str,
    valor_crudo: Any,
    regla: str,
    motivo: str,
) -> dict[str, Any]:
    """Construye un detalle de rechazo para el resumen y la cuarentena."""
    return {
        "columna": columna,
        "valor_crudo": None if valor_crudo is None else str(valor_crudo),
        "regla": regla,
        "severidad": "error",
        "motivo": motivo,
    }


def _folio_invalido(value: Any) -> bool:
    """Indica si el folio está vacío o compuesto solo por ceros."""
    raw = "" if value is None else str(value).strip()
    if not raw:
        return True
    digits = (
        raw.replace(".", "")
        .replace(",", "")
        .replace("-", "")
        .replace(" ", "")
    )
    return digits.isdigit() and int(digits) == 0


def _sin_documento(value: Any) -> bool:
    """Compara ``Tipo Documento`` con ``Sin Documento`` sin distinguir caja."""
    raw = "" if value is None else " ".join(str(value).split()).casefold()
    return raw == "sin documento"


def generar_hora_simulada(
    origen: str,
    correlativo: int,
    fecha: date,
) -> time | None:
    """Genera una hora sintética, determinística e idempotente por ticket.

    La hora no proviene del POS: se simula con ``hashlib.sha256`` a partir de
    ``origen|correlativo|fecha``. Los domingos retorna ``None`` y deja un
    warning; el resto de los días usa la ventana 10:00-19:00 (sábado hasta las
    18:00) con los pesos de librería o cafetería. El minuto se elige de forma
    uniforme dentro del tramo seleccionado.
    """
    if fecha.weekday() == 6:
        logger.warning(
            "Venta en domingo: hora_simulada NULL "
            "(origen=%s, correlativo=%s, fecha=%s)",
            origen,
            correlativo,
            fecha,
        )
        return None

    cierre = 18 * 60 if fecha.weekday() == 5 else 19 * 60
    digest = hashlib.sha256(
        f"{origen}|{correlativo}|{fecha.isoformat()}".encode("utf-8")
    ).digest()
    valor = int.from_bytes(digest[:8], "big")
    selector = valor % 100
    es_cafeteria = origen.casefold() == "cafeteria"

    if es_cafeteria:
        principal = (12 * 60, 14 * 60)
        secundario = (17 * 60, cierre)
    else:
        principal = (17 * 60, cierre)
        secundario = (12 * 60, 14 * 60)

    if selector < 60:
        inicio, fin = principal
    elif selector < 80:
        inicio, fin = secundario
    else:
        resto = ((10 * 60, 12 * 60), (14 * 60, 17 * 60))
        inicio, fin = resto[(valor >> 8) & 1]

    minuto = inicio + ((valor >> 16) % (fin - inicio))
    return time(minuto // 60, minuto % 60)


def _validate_row(
    row: Mapping[str, Any],
    *,
    origen: str,
    source_file: str,
    loaded_at: datetime,
    fila: int,
) -> tuple[Ticket | None, list[dict[str, Any]]]:
    """Valida una fila; retorna la cabecera normalizada o los rechazos."""
    detalles: list[dict[str, Any]] = []

    correlativo = parse_integer(row["Correlativo"], minimum=1)
    if correlativo is None:
        detalles.append(
            _detalle(
                "Correlativo",
                row["Correlativo"],
                "VENTAS-VALIDACION",
                "correlativo inválido",
            )
        )

    fecha = parse_date_iso(row["Fecha"])
    if fecha is None:
        detalles.append(
            _detalle(
                "Fecha",
                row["Fecha"],
                "VENTAS-VALIDACION",
                "fecha inválida",
            )
        )

    total_venta = parse_non_negative_bigint(
        row["Total Venta"],
        default=None,
    )
    if total_venta is None:
        detalles.append(
            _detalle(
                "Total Venta",
                row["Total Venta"],
                "VENTAS-VALIDACION",
                "total venta inválido o negativo",
            )
        )

    cant_items = parse_integer(row["Cant. Items"], minimum=1)
    if cant_items is None:
        detalles.append(
            _detalle(
                "Cant. Items",
                row["Cant. Items"],
                "VENTAS-VALIDACION",
                "cantidad de ítems inválida",
            )
        )

    folio = row.get("Folio", "")
    if _folio_invalido(folio):
        detalles.append(
            _detalle(
                "Folio",
                folio,
                "VENTAS-FOLIO-001",
                "folio inválido o vacío",
            )
        )

    tipo_documento = row.get("Tipo Documento", "")
    if _sin_documento(tipo_documento):
        detalles.append(
            _detalle(
                "Tipo Documento",
                tipo_documento,
                "VENTAS-DOC-001",
                "tipo documento sin documento",
            )
        )

    if detalles:
        return None, detalles

    normalized = Ticket(
        origen=origen,
        correlativo=correlativo,
        fecha=fecha,
        hora_simulada=generar_hora_simulada(origen, correlativo, fecha),
        medio_pago=_clean_text(row.get("Medio de Pago")),
        tipo_documento=_clean_text(row.get("Tipo Documento")),
        folio=_clean_text(row.get("Folio")),
        total_venta=total_venta,
        cant_items=cant_items,
        tiene_descuento=normalize_si_no(row.get("Tiene Descuento")),
        es_devolucion=normalize_si_no(row.get("Es Dev.")),
        estado=_clean_text(row.get("Estado")),
        source_file=source_file,
        loaded_at=loaded_at,
        fila=fila,
    )
    return normalized, []


def transform_row(
    row: Mapping[str, Any],
    *,
    origen: str,
    source_file: str,
    loaded_at: datetime,
    fila: int = 0,
) -> tuple[Ticket | None, str | None]:
    """Transforma una fila de ventas; retorna (cabecera, motivo)."""
    normalized, detalles = _validate_row(
        row,
        origen=origen,
        source_file=source_file,
        loaded_at=loaded_at,
        fila=fila,
    )
    if normalized is None:
        motivo = "; ".join(
            detalle["motivo"] for detalle in detalles
        ) or "fila inválida"
        return None, motivo
    return normalized, None


def transform(
    dataframe: pd.DataFrame,
    *,
    origen: str,
    source_file: str,
    loaded_at: datetime | None = None,
) -> tuple[list[Ticket], list[dict[str, Any]]]:
    """Transforma el lote y separa tickets válidos de descartados."""
    if loaded_at is None:
        loaded_at = datetime.now(timezone.utc)

    rows: list[Ticket] = []
    rejected: list[dict[str, Any]] = []

    for fila, row in enumerate(
        dataframe.to_dict(orient="records"), start=2
    ):
        result, detalles = _validate_row(
            row,
            origen=origen,
            source_file=source_file,
            loaded_at=loaded_at,
            fila=fila,
        )
        if result is None:
            for detalle in detalles:
                detalle["fila"] = fila
            motivo = "; ".join(
                detalle["motivo"] for detalle in detalles
            ) or "fila inválida"
            principal = detalles[0] if detalles else {}
            rejected.append(
                {
                    "origen": origen,
                    "correlativo": row["Correlativo"],
                    "archivo_origen": source_file,
                    "fila": fila,
                    "columna": principal.get("columna"),
                    "valor_crudo": principal.get("valor_crudo"),
                    "regla": principal.get("regla"),
                    "severidad": principal.get("severidad", "error"),
                    "motivo": motivo,
                    "detalles": detalles,
                }
            )
            logger.debug(
                "Fila %d descartada origen=%s correlativo=%r: %s",
                fila,
                origen,
                row["Correlativo"],
                motivo,
            )
            for detalle in detalles:
                logger.debug(
                    "  fila=%d columna=%s regla=%s valor=%r motivo=%s",
                    fila,
                    detalle["columna"],
                    detalle["regla"],
                    detalle["valor_crudo"],
                    detalle["motivo"],
                )
            continue
        rows.append(result)

    logger.info(
        "Transformación ventas %s: %d tickets válidos, %d descartados",
        origen,
        len(rows),
        len(rejected),
    )
    return rows, rejected


def deduplicate(
    rows: Sequence[Ticket],
) -> tuple[list[Ticket], int]:
    """Deduplica por ``(origen, correlativo)`` conservando la última fila."""
    seen: dict[tuple[str, int], Ticket] = {}
    duplicates = 0

    for row in rows:
        key = (row.origen, row.correlativo)
        if key in seen:
            duplicates += 1
            logger.debug(
                "Clave duplicada origen=%s correlativo=%s; "
                "se conserva la última fila",
                key[0],
                key[1],
            )
        seen[key] = row

    if duplicates:
        logger.warning(
            "Ventas: %d filas duplicadas por (origen, correlativo); "
            "se conserva la última",
            duplicates,
        )
    return list(seen.values()), duplicates


def load_clientes(path: str | Path) -> list[Client]:
    """Carga y transforma clientes en memoria desde el Excel crudo."""
    dataframe = read_excel_as_str(path)
    require_columns(dataframe, clientes.SOURCE_COLUMNS, source=path)
    rows, rejected = clientes.transform(dataframe)

    by_id: dict[str, Client] = {}
    duplicates = 0
    for row in rows:
        cliente_id = row[0]
        if cliente_id is None:
            continue
        if cliente_id in by_id:
            duplicates += 1
            logger.warning(
                "cliente_id duplicado (%s); se conserva la última fila",
                cliente_id,
            )
        by_id[cliente_id] = Client(
            cliente_id=cliente_id,
            alta=row[1],
            compras=row[6],
        )

    clients = list(by_id.values())
    logger.info(
        "Clientes en memoria: %d únicos, %d descartados, %d duplicados",
        len(clients),
        len(rejected),
        duplicates,
    )
    return clients


def _build_price_catalog(
    pairs: Sequence[tuple[int, str]],
) -> PriceCatalog:
    """Agrupa precios y ordena códigos para una elección determinística."""
    codes_by_price: dict[int, set[str]] = {}
    for price, code in pairs:
        if price < 0 or not code:
            continue
        codes_by_price.setdefault(price, set()).add(code)

    prices = tuple(sorted(codes_by_price))
    return PriceCatalog(
        prices=prices,
        codes_by_price={
            price: tuple(sorted(codes))
            for price, codes in codes_by_price.items()
        },
    )


def load_stock_catalog(
    path: str | Path,
    *,
    loaded_at: datetime,
) -> PriceCatalog:
    """Carga los libros válidos y arma el catálogo ``pvp -> isbn``."""
    dataframe = read_excel_as_str(path)
    require_columns(dataframe, stock.SOURCE_COLUMNS, source=path)
    rows, rejected = stock.transform(
        dataframe,
        source_file=Path(path).name,
        loaded_at=loaded_at,
    )

    pairs = [
        (row[5], row[0])
        for row in rows
        if row[5] is not None and row[0] is not None
    ]
    catalog = _build_price_catalog(pairs)
    logger.info(
        "Stock en memoria: %d libros válidos, %d descartados, "
        "%d precios distintos",
        len(rows),
        len(rejected),
        len(catalog.prices),
    )
    return catalog


def load_cafeteria_catalog(path: str | Path) -> PriceCatalog:
    """Carga el catálogo de cafetería y arma el catálogo ``precio -> código``."""
    dataframe = read_csv_as_str(path)
    require_columns(dataframe, ("codigo", "precio"), source=path)

    pairs: list[tuple[int, str]] = []
    invalid = 0
    for row in dataframe.to_dict(orient="records"):
        codigo = _clean_text(row["codigo"])
        precio = parse_non_negative_bigint(row["precio"], default=None)
        if codigo is None or precio is None:
            invalid += 1
            logger.warning(
                "Producto de cafetería inválido: codigo=%r precio=%r",
                row["codigo"],
                row["precio"],
            )
            continue
        pairs.append((precio, codigo))

    catalog = _build_price_catalog(pairs)
    logger.info(
        "Catálogo cafetería en memoria: %d productos válidos, "
        "%d inválidos, %d precios distintos",
        len(pairs),
        invalid,
        len(catalog.prices),
    )
    return catalog


def compute_reachable(
    prices: Sequence[int],
    max_n: int,
    max_x: int,
) -> list[int]:
    """Calcula bitsets de sumas alcanzables con exactamente ``k`` precios.

    ``reachable[k]`` tiene el bit ``s`` encendido si se puede formar la suma
    ``s`` usando exactamente ``k`` precios (con repetición). Se trunca a
    ``max_x`` para acotar la memoria.
    """
    distinct = tuple(sorted({price for price in prices if price >= 0}))
    mask = (1 << (max_x + 1)) - 1
    reachable = [0] * (max_n + 1)
    reachable[0] = 1

    for k in range(1, max_n + 1):
        previous = reachable[k - 1]
        current = 0
        for price in distinct:
            if price > max_x:
                continue
            current |= (previous << price) & mask
        reachable[k] = current

    return reachable


def reconstruct_prices(
    total: int,
    cant_items: int,
    prices: Sequence[int],
    reachable: Sequence[int],
) -> list[int] | None:
    """Reconstruye una multiset determinística de ``cant_items`` precios.

    Recorre ``k = cant_items .. 1`` y elige el primer precio ordenado que deja
    un resto alcanzable con ``k - 1`` precios. Retorna ``None`` si no existe
    combinación exacta.
    """
    if cant_items == 0:
        return [] if total == 0 else None

    distinct = tuple(sorted(set(prices)))
    remaining = total
    chosen: list[int] = []

    for k in range(cant_items, 0, -1):
        for price in distinct:
            rest = remaining - price
            if rest < 0:
                continue
            if (reachable[k - 1] >> rest) & 1:
                chosen.append(price)
                remaining = rest
                break
        else:
            return None

    if remaining != 0 or len(chosen) != cant_items:
        return None
    return chosen


def _pick_code(
    origen: str,
    correlativo: int,
    linea: int,
    precio: int,
    codes: Sequence[str],
) -> str:
    """Elige un código de forma estable con SHA-256, sin azar."""
    if not codes:
        raise ValueError("No hay códigos disponibles para el precio.")
    digest = hashlib.sha256(
        f"{origen}|{correlativo}|{linea}|{precio}".encode("utf-8")
    ).digest()
    index = int.from_bytes(digest[:8], "big") % len(codes)
    return codes[index]


def build_lines(
    ticket: Ticket,
    catalog: PriceCatalog,
    reachable: Sequence[int],
    *,
    loaded_at: datetime,
) -> list[Line] | None:
    """Construye las líneas exactas de un ticket o retorna ``None``."""
    chosen = reconstruct_prices(
        ticket.total_venta,
        ticket.cant_items,
        catalog.prices,
        reachable,
    )
    if chosen is None:
        return None

    counts = Counter(chosen)
    lines: list[Line] = []
    for linea, precio in enumerate(sorted(counts), start=1):
        cantidad = counts[precio]
        codes = catalog.codes_by_price.get(precio)
        if not codes:
            return None
        lines.append(
            Line(
                origen=ticket.origen,
                correlativo=ticket.correlativo,
                linea=linea,
                codigo_interno=_pick_code(
                    ticket.origen,
                    ticket.correlativo,
                    linea,
                    precio,
                    codes,
                ),
                cantidad=cantidad,
                precio_unitario=precio,
                total_linea=cantidad * precio,
                loaded_at=loaded_at,
            )
        )

    if (
        sum(line.cantidad for line in lines) != ticket.cant_items
        or sum(line.total_linea for line in lines) != ticket.total_venta
    ):
        logger.error(
            "Cuadre inconsistente para ticket %s/%s: "
            "cant_items=%d total=%d lineas=%r",
            ticket.origen,
            ticket.correlativo,
            ticket.cant_items,
            ticket.total_venta,
            lines,
        )
        return None
    return lines


# Nodo enlazado del camino elegido: (índice, nodo_padre). Evita copiar listas
# en cada rama y mantiene el uso de memoria acotado al profundidad del DFS.
PathNode = tuple[int, "PathNode | None"]


def _subset_sum_indices(
    values: Sequence[int],
    target: int,
    *,
    max_iteraciones: int = MAX_ITERACIONES_SUBSET_SUM,
    max_segundos: float = MAX_SEGUNDOS_SUBSET_SUM,
) -> list[int] | None:
    """Busca un subconjunto exacto de ``values`` que sume ``target``.

    Usa un DFS iterativo con pila explícita, memoización de estados fallidos,
    poda por suma de sufijo y poda por máximo común divisor. El camino se
    guarda como una lista enlazada de índices para no copiar listas.

    Lanza :class:`SubsetSumTimeout` si se supera ``max_iteraciones`` o
    ``max_segundos``. El mensaje de la excepción no incluye montos ni RUT.
    """
    size = len(values)
    if target < 0:
        return None
    if target == 0:
        return []

    suffix_sum = [0] * (size + 1)
    suffix_gcd = [0] * (size + 1)
    for index in range(size - 1, -1, -1):
        suffix_sum[index] = suffix_sum[index + 1] + values[index]
        suffix_gcd[index] = gcd(suffix_gcd[index + 1], values[index])

    if target > suffix_sum[0]:
        return None

    failed: set[tuple[int, int]] = set()
    limite_tiempo = time_module.monotonic() + max_segundos
    iteraciones = 0

    # Cada entrada es ("nodo", indice, restante, camino) o
    # ("fallo", indice, restante, None). El orden de apilado (fallo, excluir,
    # incluir) preserva el orden incluir/excluir del DFS recursivo.
    stack: list[tuple[str, int, int, PathNode | None]] = [
        ("nodo", 0, target, None)
    ]

    while stack:
        iteraciones += 1
        if iteraciones > max_iteraciones:
            raise SubsetSumTimeout(
                "Búsqueda de subconjunto interrumpida por límite de iteraciones"
            )
        if time_module.monotonic() >= limite_tiempo:
            raise SubsetSumTimeout(
                "Búsqueda de subconjunto interrumpida por límite de tiempo"
            )

        tipo, index, remaining, path = stack.pop()

        if tipo == "fallo":
            failed.add((index, remaining))
            continue

        if remaining == 0:
            indices: list[int] = []
            node = path
            while node is not None:
                node_index, node = node
                indices.append(node_index)
            indices.reverse()
            return indices

        if index == size or remaining > suffix_sum[index]:
            continue
        if suffix_gcd[index] == 0 or remaining % suffix_gcd[index] != 0:
            continue

        state = (index, remaining)
        if state in failed:
            continue

        stack.append(("fallo", index, remaining, None))
        stack.append(("nodo", index + 1, remaining, path))
        if values[index] <= remaining:
            stack.append(
                (
                    "nodo",
                    index + 1,
                    remaining - values[index],
                    (index, path),
                )
            )

    return None


def assign_clients(
    tickets: Sequence[Ticket],
    clients: Sequence[Client],
) -> dict[tuple[str, int], str]:
    """Asigna ``cliente_id`` por subset sum y retorna ticket -> cliente_id.

    Solo los tickets de origen ``LIBRERIA`` son elegibles: las ventas de
    ``CAFETERIA`` nunca se asignan y permanecen estrictamente anónimas. El
    filtro se aplica aquí aunque el llamador pase tickets de ambos orígenes y
    conserva el índice de la secuencia original para mantener el determinismo.

    Los clientes se ordenan por ``compras`` descendente y luego por
    ``cliente_id``. Solo se consideran tickets no usados, con
    ``fecha >= alta`` y con monto individual menor o igual al total histórico
    del cliente. Si se agota el límite de búsqueda, el cliente queda sin
    asignar y se registra un ``WARNING`` con su ``cliente_id``.
    """
    used: set[int] = set()
    assignments: dict[tuple[str, int], str] = {}
    eligible = [
        (index, ticket)
        for index, ticket in enumerate(tickets)
        if ticket.origen == "LIBRERIA"
    ]
    ordered_clients = sorted(
        clients,
        key=lambda client: (-client.compras, client.cliente_id),
    )

    for client in ordered_clients:
        if client.compras <= 0:
            continue
        if client.alta is None:
            logger.warning(
                "Cliente cliente_id=%s sin alta válida; no es elegible",
                client.cliente_id,
            )
            continue

        candidates = [
            (index, ticket)
            for index, ticket in eligible
            if index not in used
            and ticket.fecha >= client.alta
            and ticket.total_venta <= client.compras
        ]
        candidates.sort(
            key=lambda item: (
                -item[1].total_venta,
                item[1].origen,
                item[1].correlativo,
            )
        )

        try:
            selected = _subset_sum_indices(
                [ticket.total_venta for _, ticket in candidates],
                client.compras,
            )
        except SubsetSumTimeout:
            logger.warning(
                "Cuadre de compras interrumpido por límite para "
                "cliente_id=%s; se deja sin asignar",
                client.cliente_id,
            )
            continue
        if selected is None:
            logger.warning(
                "Sin subconjunto exacto para cliente_id=%s: compras=%d, "
                "tickets elegibles=%d",
                client.cliente_id,
                client.compras,
                len(candidates),
            )
            continue

        for position in selected:
            index, ticket = candidates[position]
            used.add(index)
            assignments[(ticket.origen, ticket.correlativo)] = (
                client.cliente_id
            )

    return assignments


def _line_rejection(ticket: Ticket) -> dict[str, Any]:
    """Construye el rechazo de cuarentena de un ticket sin líneas exactas."""
    detalle = _detalle(
        "total_venta",
        ticket.total_venta,
        REGLA_LINEA,
        MOTIVO_SIN_COMBINACION,
    )
    detalle["fila"] = ticket.fila
    return {
        "origen": ticket.origen,
        "correlativo": ticket.correlativo,
        "archivo_origen": ticket.source_file,
        "fila": ticket.fila,
        "columna": detalle["columna"],
        "valor_crudo": detalle["valor_crudo"],
        "regla": detalle["regla"],
        "severidad": detalle["severidad"],
        "motivo": detalle["motivo"],
        "detalles": [detalle],
    }


def _log_rejection_summary(rejected: Sequence[Mapping[str, Any]]) -> None:
    """Registra el total y los motivos agregados de los rechazos."""
    if not rejected:
        logger.info("Ventas: sin filas rechazadas")
        return

    counts = Counter(
        motivo
        for item in rejected
        for motivo in str(item.get("motivo", "fila inválida")).split("; ")
    )
    logger.warning("Ventas: %d filas rechazadas", len(rejected))
    for motivo, cantidad in sorted(counts.items()):
        logger.info("  - %s: %d filas", motivo, cantidad)


def _quarantine_by_file(
    rejected: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    """Agrupa los detalles de rechazo por archivo de origen."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in rejected:
        archivo = str(item.get("archivo_origen") or "desconocido")
        grouped.setdefault(archivo, []).extend(flatten_rejections([item]))
    return grouped


def _delete_lines_for_batch(
    conn: db.Connection,
    schema: str,
    tickets: Sequence[Ticket],
) -> int:
    """Elimina las líneas existentes de los tickets del lote actual.

    Los tickets se procesan en chunks de ``LINE_DELETE_CHUNK_SIZE`` para no
    superar el límite de parámetros de PostgreSQL. Se reutiliza un único cursor
    y se acumula ``rowcount``; no se hace ``commit`` ni ``rollback`` aquí porque
    la transacción la controla el llamador. Si un chunk falla, la excepción se
    propaga para que la transacción externa revierta el lote completo.
    """
    if not tickets:
        return 0

    deleted = 0
    with conn.cursor() as cur:
        for start in range(0, len(tickets), LINE_DELETE_CHUNK_SIZE):
            chunk = tickets[start : start + LINE_DELETE_CHUNK_SIZE]
            values = sql.SQL(", ").join(
                sql.SQL("(%s, %s)") for _ in chunk
            )
            statement = sql.SQL(
                "DELETE FROM {schema}.{line_table} "
                "WHERE (origen, correlativo) IN (VALUES {values})"
            ).format(
                schema=sql.Identifier(schema),
                line_table=sql.Identifier(LINE_TABLE),
                values=values,
            )
            params: list[Any] = []
            for ticket in chunk:
                params.extend((ticket.origen, ticket.correlativo))

            cur.execute(statement, params)
            deleted += cur.rowcount

    return deleted


def run(
    input_paths: Sequence[str | Path] = INPUT_PATHS,
    *,
    clientes_path: str | Path | None = None,
    stock_path: str | Path | None = None,
    catalogo_cafeteria_path: str | Path | None = None,
    dry_run: bool = False,
    schema_only: bool = False,
) -> None:
    """Ejecuta el pipeline completo o una de sus etapas."""
    clientes_path = (
        Path(clientes_path) if clientes_path is not None else CLIENTES_PATH
    )
    stock_path = Path(stock_path) if stock_path is not None else STOCK_PATH
    catalogo_cafeteria_path = (
        Path(catalogo_cafeteria_path)
        if catalogo_cafeteria_path is not None
        else CATALOGO_CAFETERIA_PATH
    )

    if schema_only:
        schema = get_schema()
        with db.connection() as conn:
            db.execute_sql_file(conn, QUARANTINE_SQL, schema=schema)
            db.execute_sql_file(conn, SQL_FILE, schema=schema)
        logger.info(
            "Schema, tablas %s.%s/%s y cuarentena verificados",
            schema,
            TICKET_TABLE,
            LINE_TABLE,
        )
        return

    paths = [Path(path) for path in input_paths]
    loaded_at = datetime.now(timezone.utc)
    valid_rows: list[Ticket] = []
    rejected: list[dict[str, Any]] = []
    total_read = 0
    per_file: list[tuple[str, str, int, int, int]] = []

    for path in paths:
        origen = inferir_origen(path)
        dataframe = _leer_ventas(path)

        file_rows, file_rejected = transform(
            dataframe,
            origen=origen,
            source_file=path.name,
            loaded_at=loaded_at,
        )
        valid_rows.extend(file_rows)
        rejected.extend(file_rejected)
        total_read += len(dataframe)
        per_file.append(
            (
                path.name,
                origen,
                len(dataframe),
                len(file_rows),
                len(file_rejected),
            )
        )

    rows, duplicates = deduplicate(valid_rows)

    for name, origen, read, valid, rejected_count in per_file:
        logger.info(
            "Archivo %s (origen=%s): leídas=%d válidas=%d rechazadas=%d",
            name,
            origen,
            read,
            valid,
            rejected_count,
        )

    origins = {ticket.origen for ticket in rows}
    catalogs: dict[str, PriceCatalog] = {}
    if "LIBRERIA" in origins:
        catalogs["LIBRERIA"] = load_stock_catalog(
            stock_path,
            loaded_at=loaded_at,
        )
    if "CAFETERIA" in origins:
        catalogs["CAFETERIA"] = load_cafeteria_catalog(
            catalogo_cafeteria_path
        )

    max_n = max((ticket.cant_items for ticket in rows), default=0)
    max_x = max((ticket.total_venta for ticket in rows), default=0)
    reachable_by_origin = {
        origin: compute_reachable(catalog.prices, max_n, max_x)
        for origin, catalog in catalogs.items()
    }

    viable: list[Ticket] = []
    lines_by_ticket: dict[tuple[str, int], list[Line]] = {}
    line_rejected: list[dict[str, Any]] = []

    for ticket in rows:
        catalog = catalogs.get(ticket.origen)
        lines = (
            build_lines(
                ticket,
                catalog,
                reachable_by_origin[ticket.origen],
                loaded_at=loaded_at,
            )
            if catalog is not None
            else None
        )
        if lines is None:
            line_rejected.append(_line_rejection(ticket))
            logger.debug(
                "Ticket %s/%s sin combinación exacta: total=%d items=%d",
                ticket.origen,
                ticket.correlativo,
                ticket.total_venta,
                ticket.cant_items,
            )
            continue
        viable.append(ticket)
        lines_by_ticket[(ticket.origen, ticket.correlativo)] = lines

    # Solo las ventas de librería se cuadran con clientes; las de cafetería son
    # estrictamente anónimas. Si el lote no trae librería ni siquiera se lee el
    # archivo de clientes.
    asignables = [
        ticket for ticket in viable if ticket.origen == "LIBRERIA"
    ]
    clients = load_clientes(clientes_path) if asignables else []
    assignments = assign_clients(asignables, clients)

    all_rejected = [*rejected, *line_rejected]
    _log_rejection_summary(all_rejected)

    total_lines = sum(len(lines) for lines in lines_by_ticket.values())
    anonymous = len(viable) - len(assignments)
    logger.info(
        "Resumen ventas: leídas=%d válidas=%d rechazadas=%d "
        "duplicadas=%d tickets_con_lineas=%d sin_lineas=%d "
        "lineas=%d asignados=%d anonimos=%d",
        total_read,
        len(valid_rows),
        len(rejected),
        duplicates,
        len(viable),
        len(line_rejected),
        total_lines,
        len(assignments),
        anonymous,
    )

    cuarentena_por_archivo = _quarantine_by_file(all_rejected)
    total_cuarentena = sum(
        len(filas) for filas in cuarentena_por_archivo.values()
    )

    if dry_run:
        logger.info(
            "Dry run: %d tickets viables y %d líneas exactas",
            len(viable),
            total_lines,
        )
        logger.info(
            "Dry run: %d filas se habrían enviado a cuarentena",
            total_cuarentena,
        )
        logger.info("Dry run finalizado; no se escribió en PostgreSQL")
        return

    schema = get_schema()
    with db.connection() as conn:
        db.execute_sql_file(conn, QUARANTINE_SQL, schema=schema)
        db.execute_sql_file(conn, SQL_FILE, schema=schema)
        with conn.transaction():
            ticket_rows = [
                ticket.to_db_row(
                    None
                    if ticket.origen != "LIBRERIA"
                    else assignments.get(
                        (ticket.origen, ticket.correlativo)
                    )
                )
                for ticket in viable
            ]
            loaded_tickets = db.upsert_table(
                conn,
                schema,
                TICKET_TABLE,
                TICKET_COLUMNS,
                KEY_COLUMNS,
                ticket_rows,
                update_columns=TICKET_UPDATE_COLUMNS,
                temp_table="ventas_ticket_staging",
            )

            deleted_lines = _delete_lines_for_batch(conn, schema, viable)
            line_rows = [
                line.to_db_row()
                for ticket in viable
                for line in lines_by_ticket[
                    (ticket.origen, ticket.correlativo)
                ]
            ]
            loaded_lines = db.copy_rows(
                conn,
                schema,
                LINE_TABLE,
                LINE_COLUMNS,
                line_rows,
            )

            for archivo, filas in cuarentena_por_archivo.items():
                insert_quarantine(
                    conn,
                    schema,
                    ENTITY,
                    archivo,
                    filas,
                )

    logger.info(
        "Carga finalizada en %s.%s/%s: %d tickets en staging, "
        "%d líneas insertadas, %d líneas previas eliminadas, "
        "%d filas en cuarentena",
        schema,
        TICKET_TABLE,
        LINE_TABLE,
        loaded_tickets,
        loaded_lines,
        deleted_lines,
        total_cuarentena,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Normaliza y carga ventas en silver.ventas_ticket y "
            "silver.ventas_linea de forma incremental."
        ),
    )
    parser.add_argument(
        "--input",
        dest="inputs",
        type=Path,
        action="append",
        default=None,
        help=(
            "Ruta de un Excel de ventas; se puede repetir. "
            "Por defecto usa los dos archivos de bronze/."
        ),
    )
    parser.add_argument(
        "--clientes",
        type=Path,
        default=CLIENTES_PATH,
        help="Ruta del Excel de clientes para el cuadre de cliente_id.",
    )
    parser.add_argument(
        "--stock",
        type=Path,
        default=STOCK_PATH,
        help="Ruta del Excel de stock para el catálogo de librería.",
    )
    parser.add_argument(
        "--catalogo-cafeteria",
        dest="catalogo_cafeteria",
        type=Path,
        default=CATALOGO_CAFETERIA_PATH,
        help="Ruta del CSV de catálogo de cafetería.",
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
        help="Solo crea el schema y las tablas.",
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
        run(
            args.inputs if args.inputs else INPUT_PATHS,
            clientes_path=args.clientes,
            stock_path=args.stock,
            catalogo_cafeteria_path=args.catalogo_cafeteria,
            dry_run=args.dry_run,
            schema_only=args.schema_only,
        )
    except Exception:
        logger.exception("Error ejecutando el pipeline de ventas")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
