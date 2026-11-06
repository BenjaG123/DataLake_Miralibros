"""Reproducibilidad e invariantes del generador de fixtures sintéticos."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pandas as pd

from etl.pipelines import ventas
from etl.readers import read_excel_as_str

ARCHIVOS_ESPERADOS = (
    "CLIENTES LIBRERIA.xlsx",
    "STOCK LIBRERIA.xlsx",
    "VENTAS LIBRERIA.xlsx",
    "VENTAS CAFETERIA.xlsx",
)

FILAS_ESPERADAS = {
    "CLIENTES LIBRERIA.xlsx": 150,
    "STOCK LIBRERIA.xlsx": 100,
    "VENTAS LIBRERIA.xlsx": 155,
    "VENTAS CAFETERIA.xlsx": 85,
}

CARGA = datetime(2024, 1, 1, tzinfo=timezone.utc)
CATALOGO_CAFETERIA = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "raw"
    / "CATALOGO CAFETERIA.csv"
)


def _leer_todos(directorio: Path) -> dict[str, pd.DataFrame]:
    """Lee los cuatro XLSX como texto para compararlos entre corridas."""
    return {
        nombre: read_excel_as_str(directorio / nombre)
        for nombre in ARCHIVOS_ESPERADOS
    }


def _leer_tickets(directorio: Path) -> list[ventas.Ticket]:
    """Transforma los dos archivos de ventas generados."""
    tickets: list[ventas.Ticket] = []
    for nombre, origen in (
        ("VENTAS LIBRERIA.xlsx", "LIBRERIA"),
        ("VENTAS CAFETERIA.xlsx", "CAFETERIA"),
    ):
        dataframe = read_excel_as_str(directorio / nombre)
        filas, _ = ventas.transform(
            dataframe,
            origen=origen,
            source_file=nombre,
            loaded_at=CARGA,
        )
        tickets.extend(filas)
    return tickets


def _catalogos(
    directorio: Path,
) -> dict[str, ventas.PriceCatalog]:
    return {
        "LIBRERIA": ventas.load_stock_catalog(
            directorio / "STOCK LIBRERIA.xlsx",
            loaded_at=CARGA,
        ),
        "CAFETERIA": ventas.load_cafeteria_catalog(CATALOGO_CAFETERIA),
    }


def test_build_all_genera_los_cuatro_archivos_esperados(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    resultados = generador_fixtures.build_all(tmp_path)

    assert set(resultados) == set(ARCHIVOS_ESPERADOS)
    for nombre, (ruta, filas) in resultados.items():
        assert ruta.name == nombre
        assert ruta.is_file()
        assert filas == FILAS_ESPERADAS[nombre]

    assert sorted(ruta.name for ruta in tmp_path.iterdir()) == sorted(
        ARCHIVOS_ESPERADOS
    )


def test_build_all_es_reproducible(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    dir_a = tmp_path / "corrida_a"
    dir_b = tmp_path / "corrida_b"

    generador_fixtures.build_all(dir_a)
    generador_fixtures.build_all(dir_b)

    dataframes_a = _leer_todos(dir_a)
    dataframes_b = _leer_todos(dir_b)

    for nombre in ARCHIVOS_ESPERADOS:
        pd.testing.assert_frame_equal(
            dataframes_a[nombre],
            dataframes_b[nombre],
            obj=nombre,
        )


def test_cada_ticket_tiene_suma_exacta(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    generador_fixtures.build_all(tmp_path)
    tickets = _leer_tickets(tmp_path)
    catalogos = _catalogos(tmp_path)

    assert len(tickets) == 207
    max_n = max(ticket.cant_items for ticket in tickets)
    for origen, catalogo in catalogos.items():
        tickets_origen = [
            ticket for ticket in tickets if ticket.origen == origen
        ]
        max_x = max(ticket.total_venta for ticket in tickets_origen)
        reachable = ventas.compute_reachable(
            catalogo.prices,
            max_n,
            max_x,
        )
        for ticket in tickets_origen:
            combinacion = ventas.reconstruct_prices(
                ticket.total_venta,
                ticket.cant_items,
                catalogo.prices,
                reachable,
            )
            assert combinacion is not None, (
                f"{origen}/{ticket.correlativo} sin combinación exacta"
            )
            assert len(combinacion) == ticket.cant_items
            assert sum(combinacion) == ticket.total_venta


def test_compras_son_montos_y_cuadran_con_tickets(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    generador_fixtures.build_all(tmp_path)
    clients = ventas.load_clientes(tmp_path / "CLIENTES LIBRERIA.xlsx")
    tickets = _leer_tickets(tmp_path)

    assignments = ventas.assign_clients(tickets, clients)
    ticket_por_clave = {
        (ticket.origen, ticket.correlativo): ticket for ticket in tickets
    }
    sumas: dict[str, int] = defaultdict(int)
    conteos: dict[str, int] = defaultdict(int)
    for clave, rut in assignments.items():
        sumas[rut] += ticket_por_clave[clave].total_venta
        conteos[rut] += 1

    positivos = [client for client in clients if client.compras > 0]
    assert len(positivos) == 21
    assert sum(client.compras for client in positivos) > 100_000
    for client in positivos:
        assert client.alta is not None
        assert sumas[client.rut] == client.compras
        assert conteos[client.rut] >= 1
        for clave, rut in assignments.items():
            if rut == client.rut:
                assert ticket_por_clave[clave].fecha >= client.alta

    assert any(conteo >= 2 for conteo in conteos.values())
    assert len(assignments) < len(tickets)
    assert len(assignments) == len(set(assignments))


def test_asignacion_es_determinista(
    generador_fixtures: ModuleType, tmp_path: Path
) -> None:
    generador_fixtures.build_all(tmp_path)
    clients = ventas.load_clientes(tmp_path / "CLIENTES LIBRERIA.xlsx")
    tickets = _leer_tickets(tmp_path)

    primera = ventas.assign_clients(tickets, clients)
    segunda = ventas.assign_clients(tickets, clients)

    assert primera == segunda
