"""Contrato del catálogo de cafetería (``CATALOGO CAFETERIA.csv``).

El CSV es un fixture versionado con los precios exactos acordados de la
cafetería. No contiene datos personales; se valida su contenido exacto.
"""

from __future__ import annotations

import pandas as pd

CATALOGO_ESPERADO = [
    ("ESP-001", "Espresso", "Bebidas a base de espresso", 3000),
    ("ESP-002", "Americano", "Bebidas a base de espresso", 3500),
    ("LEC-001", "Macchiato", "Bebidas con leche", 3000),
    ("LEC-002", "Cortado simple", "Bebidas con leche", 3500),
    ("LEC-003", "Cortado doble", "Bebidas con leche", 4000),
    ("LEC-004", "Vienés", "Bebidas con leche", 4000),
    ("LEC-005", "Cappuccino", "Bebidas con leche", 3500),
    ("LEC-006", "Flat white", "Bebidas con leche", 3500),
    ("LEC-007", "Latte", "Bebidas con leche", 4000),
    ("LEC-008", "Mocca", "Bebidas con leche", 4500),
    ("LEC-009", "Mocca blanco", "Bebidas con leche", 4500),
    ("LEC-010", "Chocolate caliente", "Bebidas con leche", 4500),
    ("LEC-011", "Pumpkin spice latte", "Bebidas con leche", 4500),
    ("LEC-012", "Chai latte", "Bebidas con leche", 4500),
    ("LEC-013", "Golden Milk", "Bebidas con leche", 4500),
    ("LEC-014", "Matcha latte", "Bebidas con leche", 4500),
    ("INF-001", "Tetera individual", "Infusiones", 3000),
    ("INF-002", "Tetera para compartir", "Infusiones", 5500),
    ("PAS-001", "Croissant", "Pastelería (Vitrina)", 3000),
    ("PAS-002", "Brownie", "Pastelería (Vitrina)", 3000),
    ("PAS-003", "Galletas", "Pastelería (Vitrina)", 2500),
    ("PAS-004", "Rollo de canela", "Pastelería (Vitrina)", 3000),
    ("PAS-005", "Tartaleta", "Pastelería (Vitrina)", 3500),
]


def test_columnas_y_volumen(catalogo_cafeteria_df: pd.DataFrame) -> None:
    assert list(catalogo_cafeteria_df.columns) == [
        "codigo",
        "nombre",
        "categoria",
        "precio",
    ]
    assert len(catalogo_cafeteria_df) == 23


def test_nombres_precios_y_categorias(catalogo_cafeteria_df: pd.DataFrame) -> None:
    for (codigo, nombre, categoria, precio), fila in zip(
        CATALOGO_ESPERADO,
        catalogo_cafeteria_df.to_dict(orient="records"),
        strict=True,
    ):
        assert fila["codigo"] == codigo
        assert fila["nombre"] == nombre
        assert fila["categoria"] == categoria
        assert int(fila["precio"]) == precio


def test_pasteleria_entre_2500_y_4500(
    catalogo_cafeteria_df: pd.DataFrame,
) -> None:
    pasteleria = catalogo_cafeteria_df[
        catalogo_cafeteria_df["categoria"] == "Pastelería (Vitrina)"
    ]

    assert len(pasteleria) == 5
    precios = pasteleria["precio"].astype(int)
    assert precios.between(2500, 4500).all()
