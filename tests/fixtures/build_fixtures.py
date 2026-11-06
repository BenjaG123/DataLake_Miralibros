"""Generador reproducible de fixtures sintéticos para el ETL de Miralibros.

Este script **no** es una prueba de pytest: produce los crudos sintéticos que
alimentan las pruebas de readers, validadores y pipelines. Todo dato personal
es ficticio (RUT, nombres, emails y teléfonos inventados) y se genera con una
semilla fija, sin ``uuid``, ``time`` ni ``datetime.now()``.

Archivos generados en ``tests/fixtures/raw/`` (nombres exactos que espera el
repo, con espacios y acentos):

- ``CLIENTES LIBRERIA.xlsx``
- ``STOCK LIBRERIA.xlsx``
- ``VENTAS LIBRERIA.xlsx``
- ``VENTAS CAFETERIA.xlsx``

Los ``*.xlsx`` están ignorados por el ``.gitignore`` global; la carpeta se
conserva en git con ``tests/fixtures/raw/.gitkeep`` y los archivos se
regeneran ejecutando::

    .venv\\Scripts\\python.exe tests\\fixtures\\build_fixtures.py

Para comparar reproducibilidad sin pisar los artefactos principales::

    .venv\\Scripts\\python.exe tests\\fixtures\\build_fixtures.py \\
        --output-dir C:\\ruta\\temporal

Los datos se escriben "sucios" a propósito (RUT con DV incorrecto, fechas en
varios formatos, montos negativos o nulos, columnas 100 % vacías) para que los
validadores tengan trabajo real. Además, cada ticket válido se construye con
``Cant. Items`` precios reales de su catálogo: ``Total Venta`` es la suma
exacta. Las compras de clientes son montos históricos en CLP y se asignan con
subconjuntos disjuntos de tickets elegibles por fecha de alta.
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import unicodedata
from collections import defaultdict
from collections.abc import Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etl.pipelines import clientes as clientes_pipeline  # noqa: E402
from etl.pipelines import stock as stock_pipeline  # noqa: E402
from etl.pipelines import ventas as ventas_pipeline  # noqa: E402
from etl.readers import read_csv_as_str  # noqa: E402
from etl.validators import (  # noqa: E402
    calculate_rut_dv,
    parse_non_negative_bigint,
)

# Semilla fija documentada: cambiarla cambia todos los archivos generados.
SEED = 20240517

RAW_DIR = Path(__file__).resolve().parent / "raw"
CATALOGO_CAFETERIA = RAW_DIR / "CATALOGO CAFETERIA.csv"

# Fecha fija para que la transformación de stock/clientes sea reproducible.
CARGA_FIJA = datetime(2024, 1, 1, tzinfo=timezone.utc)

# Clientes con compras históricas > 0: uno con dos tickets y el resto con uno.
CLIENTES_CON_COMPRAS = 21

CLIENTES_COLUMNS = (
    "Rut cliente",
    "Nombre",
    "Email",
    "Alta",
    "Canal",
    "Cumple.",
    "Puntos",
    "Compras",
    "Canjes",
)

STOCK_COLUMNS = (
    "ISBN",
    "Titulo",
    "Autor",
    "Editorial",
    "Clasificación",
    "PVP",
    "Stock Consig.",
    "Stock Propio",
    "Stock Total",
    "Ventas",
)

# El header crudo real vive en el pipeline: una sola fuente de verdad para
# que el generador y ``ventas.SOURCE_COLUMNS`` no puedan divergir.
VENTAS_COLUMNS = ventas_pipeline.SOURCE_COLUMNS

MESES_ES = (
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
)

MAX_DIAS = (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

NOMBRES_PILA = (
    "María",
    "José",
    "Camila",
    "Benjamín",
    "Martina",
    "Vicente",
    "Antonia",
    "Matías",
    "Florencia",
    "Sebastián",
    "Josefa",
    "Diego",
    "Catalina",
    "Ignacio",
    "Fernanda",
    "Tomás",
    "Valentina",
    "Cristóbal",
    "Amanda",
    "Rodrigo",
    "Constanza",
    "Felipe",
    "Isidora",
    "Nicolás",
    "Trinidad",
    "Gonzalo",
    "Javiera",
    "Andrés",
    "Emilia",
    "Maximiliano",
    "Rocío",
    "Pablo",
    "Daniela",
    "Lucas",
    "Sofía",
    "Álvaro",
    "Magdalena",
)

APELLIDOS = (
    "González",
    "Muñoz",
    "Rojas",
    "Díaz",
    "Pérez",
    "Soto",
    "Contreras",
    "Silva",
    "Martínez",
    "Sepúlveda",
    "Morales",
    "Rodríguez",
    "Fuentes",
    "Araya",
    "Vergara",
    "Castillo",
    "Tapia",
    "Hernández",
    "Castro",
    "Vásquez",
    "Riquelme",
    "Cáceres",
    "Sandoval",
    "Valdés",
    "Espinoza",
    "Gutiérrez",
    "Orellana",
    "Ramírez",
    "Torres",
    "Flores",
    "Aguilera",
)

DOMINIOS_EMAIL = (
    "gmail.com",
    "hotmail.com",
    "outlook.cl",
    "yahoo.com",
    "icloud.com",
    "correo.cl",
)

EMAILS_INVALIDOS = (
    "correo-sin-arroba.cl",
    "ana@@gmail.com",
    "juan..perez@gmail.com",
    "@gmail.com",
    "nombre apellido@gmail.com",
)

TITULOS = (
    "La casa de los espíritus",
    "Eva Luna",
    "Cien años de soledad",
    "El amor en los tiempos del cólera",
    "Rayuela",
    "Bestiario",
    "El túnel",
    "Sobre héroes y tumbas",
    "La ciudad y los perros",
    "Conversación en La Catedral",
    "El principito",
    "Matilda",
    "Charlie y la fábrica de chocolate",
    "Harry Potter y la piedra filosofal",
    "La historia interminable",
    "El señor de los anillos",
    "Crónica de una muerte anunciada",
    "Los detectives salvajes",
    "2666",
    "Formas de volver a casa",
    "Bonsái",
    "La literatura nazi en América",
    "El obsceno pájaro de la noche",
    "Hijo de ladrón",
    "El lugar sin límites",
    "Martín Rivas",
    "Sub terra",
    "Casa de campo",
    "La amortajada",
    "Tala",
    "Veinte poemas de amor y una canción desesperada",
    "Residencia en la tierra",
    "Canto general",
    "Desolación",
    "Poema de Chile",
    "El libro de las preguntas",
    "Matemática 1° básico",
    "Lenguaje y comunicación 4° básico",
    "Ciencias naturales 6° básico",
    "Historia de Chile 5° básico",
    "Inglés 3° básico",
    "Cuaderno de ejercicios matemática 2° básico",
    "Atlas de Chile",
    "Diccionario escolar",
    "La Araucana",
    "Papelucho",
    "Papelucho detective",
    "Mi amigo el negro",
    "El pequeño Nicolás",
    "Rebelión en la granja",
    "Café de especialidad: guía práctica",
    "Recetas de repostería chilena",
    "Panadería casera paso a paso",
)

AUTORES = (
    "Isabel Allende",
    "Gabriela Mistral",
    "Pablo Neruda",
    "Roberto Bolaño",
    "Marcela Serrano",
    "Luis Sepúlveda",
    "Nicanor Parra",
    "Violeta Parra",
    "Jorge Edwards",
    "Alejandro Zambra",
    "Antonio Skármeta",
    "María José Ferrada",
    "José Donoso",
    "Manuel Rojas",
    "Marta Brunet",
    "Gabriel García Márquez",
    "Mario Vargas Llosa",
    "Julio Cortázar",
    "Antoine de Saint-Exupéry",
    "Roald Dahl",
    "J. K. Rowling",
    "Michael Ende",
    "J. R. R. Tolkien",
)

EDITORIALES = (
    "Santillana",
    "Zig-Zag",
    "Alfaguara",
    "Planeta",
    "Fondo de Cultura Económica",
    "SM",
    "Ediciones UC",
    "Amanuta",
    "Norma",
    "Penguin Random House",
    "Tusquets",
    "Anagrama",
)

CLASIFICACIONES = (
    "Novela",
    "Escolar",
    "Infantil",
    "Académico",
    "Cafetería",
)

MEDIOS_PAGO = ("Efectivo", "Débito", "Crédito", "Transferencia")

FECHAS_VENTAS = (
    "2024-01-02",
    "2024-01-31",
    "2024-02-14",
    "2024-02-29",
    "2024-03-11",
    "2024-04-01",
    "2024-04-30",
    "2024-05-10",
    "2024-06-21",
    "2024-06-30",
    "2024-07-08",
    "2024-08-15",
    "2024-09-09",
    "2024-09-18",
    "2024-09-30",
    "2024-10-12",
    "2024-11-29",
    "2024-12-24",
    "2024-12-31",
    "2025-01-06",
    "2025-01-31",
    "2025-02-03",
    "2025-03-20",
    "2025-04-19",
    "2025-05-05",
    "2025-06-29",
    "2025-07-16",
    "2025-08-20",
    "2025-10-31",
    "2025-11-02",
    "2025-12-08",
    "2025-12-31",
)

# Filas reservadas para forzar cada invariante sucio en clientes.
CLIENTES_TOTAL = 150
CLIENTES_RUT_INVALIDO = (5, 27, 49, 71, 93, 115, 137, 149)
CLIENTES_ALTA_INVALIDA = (12, 66, 120, 145)
CLIENTES_CUMPLE_INVALIDO = (18, 44, 88, 132)
CLIENTES_EMAIL_INVALIDO = (24, 76, 128)
CLIENTES_CANAL_INVALIDO = (34, 90)
CLIENTES_NOMBRE_INVALIDO = (58, 112)
CLIENTES_NUMERICO_NULO = (7, 39, 63, 84, 102, 111, 124, 139, 143)

STOCK_TOTAL = 100
STOCK_ISBN_VACIO = (3, 17, 28, 41, 55, 69, 82, 95)
STOCK_PVP_TEXTO = (4, 21, 36, 52, 67, 79, 88, 97)
STOCK_TOTAL_NEGATIVO = (2, 13, 24, 35, 46, 57, 68, 79, 90, 98)
PVP_TEXTO = (
    "15.000",
    "9.990",
    "12.500",
    "22.000",
    "7.500",
    "18.990",
    "14.000",
    "11.000",
)

VENTAS_LIBRERIA = {
    "archivo": "VENTAS LIBRERIA.xlsx",
    "filas_base": 150,
    "filas_extra": 5,
    "total": 155,
    "correlativo_base": 1,
    "folio_base": 100_000,
    "monto_min": 2_990,
    "monto_max": 180_000,
    "total_nulo": (5, 35, 65, 95),
    "total_negativo": (15, 45, 75),
    "cant_nula": (25, 55, 85),
    "cant_negativa": (10, 40, 70),
}

VENTAS_CAFETERIA = {
    "archivo": "VENTAS CAFETERIA.xlsx",
    "filas_base": 80,
    "filas_extra": 5,
    "total": 85,
    "correlativo_base": 1_001,
    "folio_base": 200_000,
    "monto_min": 1_200,
    "monto_max": 30_000,
    "total_nulo": (3, 20, 40),
    "total_negativo": (8, 28),
    "cant_nula": (13, 33),
    "cant_negativa": (18, 38, 55),
}

# Filas extra que solo fallan por cabecera. La última acumula Folio y
# Tipo Documento inválidos para ejercitar más de un detalle por rechazo.
FILAS_EXTRA_FOLIO_DOC = (
    {"folio": "0", "tipo_documento": "Boleta"},
    {"folio": None, "tipo_documento": "Factura"},
    {"folio": "VALIDO", "tipo_documento": "Sin Documento"},
    {"folio": "VALIDO", "tipo_documento": "  SIN   DOCUMENTO "},
    {
        "folio": "000",
        "tipo_documento": "Sin Documento",
    },
)


def _slug(texto: str) -> str:
    """Convierte un nombre en una parte local de email sin acentos."""
    descompuesto = unicodedata.normalize("NFKD", texto)
    sin_acentos = "".join(
        caracter
        for caracter in descompuesto
        if not unicodedata.combining(caracter)
    )
    limpio = re.sub(r"[^a-z0-9]+", ".", sin_acentos.casefold()).strip(".")
    return limpio or "cliente"


def _elegir_dv_incorrecto(rng: random.Random, dv_correcto: str) -> str:
    """Devuelve un dígito verificador distinto del correcto."""
    candidatos = [digito for digito in "0123456789K" if digito != dv_correcto]
    return rng.choice(candidatos)


def _formatear_rut(
    rng: random.Random,
    cuerpo: int,
    dv: str,
    *,
    forzar_sucio: bool,
) -> tuple[str, bool]:
    """Formatea un RUT con los formatos irregulares que llegan en el crudo."""
    if forzar_sucio:
        estilo = rng.choice(("compacto", "con_espacios"))
    else:
        estilo = rng.choice(("con_puntos", "sin_puntos", "compacto", "con_espacios"))

    if estilo == "con_puntos":
        texto = f"{cuerpo:,}".replace(",", ".") + f"-{dv}"
    elif estilo == "sin_puntos":
        texto = f"{cuerpo}-{dv}"
    elif estilo == "compacto":
        texto = f"{cuerpo}{dv}"
    else:
        texto = f"  {cuerpo}-{dv} "

    if dv == "K" and rng.random() < 0.5:
        texto = texto.replace("K", "k")

    sucio = estilo in ("compacto", "con_espacios") or "k" in texto
    return texto, sucio


def _nombre_completo(rng: random.Random) -> str:
    """Arma un nombre ficticio verosímil de la zona sur de Chile."""
    nombre = rng.choice(NOMBRES_PILA)
    apellido_1 = rng.choice(APELLIDOS)
    if rng.random() < 0.35:
        apellido_2 = rng.choice(APELLIDOS)
        return f"{nombre} {apellido_1} {apellido_2}"
    return f"{nombre} {apellido_1}"


def _nombre_con_ruido(
    rng: random.Random,
    nombre_limpio: str,
    indice: int,
) -> tuple[Any, bool]:
    """Aplica mayúsculas desordenadas, espacios o basura al nombre."""
    if indice in CLIENTES_NOMBRE_INVALIDO:
        return "123 ///", True

    estilo = rng.choices(
        ("limpio", "mayusculas", "minusculas", "mixto", "basura", "espacios"),
        weights=(45, 12, 8, 15, 12, 8),
        k=1,
    )[0]

    if estilo == "mayusculas":
        return nombre_limpio.upper(), True
    if estilo == "minusculas":
        return nombre_limpio.lower(), True
    if estilo == "mixto":
        return nombre_limpio.swapcase(), True
    if estilo == "basura":
        basura = rng.choice(("***", " (sin verificar)", " 123", " ###"))
        return f"{nombre_limpio}{basura}", True
    if estilo == "espacios":
        return f"   {nombre_limpio}  ", True
    return nombre_limpio, False


def _email_para(
    rng: random.Random,
    nombre_limpio: str,
    indice: int,
) -> tuple[Any, bool]:
    """Genera un email ficticio; algunas filas quedan inválidas a propósito."""
    if indice in CLIENTES_EMAIL_INVALIDO:
        return rng.choice(EMAILS_INVALIDOS), True

    slug = _slug(nombre_limpio)
    if rng.random() < 0.2:
        slug = slug.split(".")[0]
    email = f"{slug}@{rng.choice(DOMINIOS_EMAIL)}"

    if rng.random() < 0.15:
        return email.upper(), True
    if rng.random() < 0.1:
        return f"  {email} ", True
    return email, False


def _fecha_alta(rng: random.Random, indice: int) -> tuple[Any, bool]:
    """Genera una fecha de alta pasada; algunas filas quedan inválidas."""
    if indice in CLIENTES_ALTA_INVALIDA:
        # Determinista: fecha imposible, mes 13, texto libre y fecha previa
        # al mínimo aceptado (2000-01-01).
        opciones = (
            "31/02/2020",
            "13/13/2021",
            "fecha desconocida",
            "05/05/1999",
        )
        posicion = CLIENTES_ALTA_INVALIDA.index(indice)
        return opciones[posicion], True

    # Tope 2023: toda Alta válida queda antes de la primera venta del lote
    # (FECHAS_VENTAS comienza el 2024-01-02). Ver _verificar_invariantes.
    anio = rng.randint(2015, 2023)
    mes = rng.randint(1, 12)
    dia = rng.randint(1, 28)
    separador = rng.choice(("/", "-"))
    if rng.random() < 0.2:
        texto = f"{dia}{separador}{mes}{separador}{anio}"
    else:
        texto = f"{dia:02d}{separador}{mes:02d}{separador}{anio}"
    return texto, separador == "-"


def _cumple(rng: random.Random, indice: int) -> tuple[Any, bool]:
    """Genera día y mes de cumpleaños sin año, con formatos sucios."""
    if indice in CLIENTES_CUMPLE_INVALIDO:
        # Asignación determinista: garantiza día 32, mes inexistente y días
        # que no existen en su mes, sin depender del azar.
        opciones = (
            "32 - junio",
            "14 - mes inexistente",
            "30 - febrero",
            "31 - abril",
        )
        posicion = CLIENTES_CUMPLE_INVALIDO.index(indice)
        return opciones[posicion % len(opciones)], True

    mes_indice = rng.randrange(12)
    dia = rng.randint(1, MAX_DIAS[mes_indice])
    mes = MESES_ES[mes_indice]

    # ``parse_day_month_es`` acepta tanto "14 de junio" como "14 - junio".
    # Se generan variantes con espacios y mayúsculas para ejercitar la
    # normalización; todas siguen siendo parseables.
    if rng.random() < 0.35:
        estilo = rng.choice(("simple", "espacios", "mayusculas"))
        if estilo == "espacios":
            return f"  {dia}  de  {mes}  ", True
        if estilo == "mayusculas":
            return f"{dia} De {mes.capitalize()}", True
        return f"{dia} de {mes}", True

    estilo = rng.choice(("simple", "espacios", "mayusculas"))
    if estilo == "espacios":
        return f"  {dia}  -  {mes}  ", False
    if estilo == "mayusculas":
        return f"{dia} - {mes.upper()}", False
    return f"{dia} - {mes}", False


def _canal(rng: random.Random, indice: int) -> tuple[Any, bool]:
    """Normaliza variantes de canal y agrega placeholders en filas sucias."""
    if indice in CLIENTES_CANAL_INVALIDO:
        opciones: tuple[Any, ...] = ("-", "sin dato", None)
        posicion = CLIENTES_CANAL_INVALIDO.index(indice)
        return opciones[posicion % len(opciones)], True

    estilo = rng.choice(("Local", "local", " LOCAL ", "Web", "web", "Web "))
    return estilo, estilo not in ("Local", "Web")


def _numero_cliente(
    rng: random.Random,
    *,
    maximo: int,
    indice: int,
    separadores: Sequence[str],
) -> tuple[Any, bool]:
    """Genera un entero, un texto con separador de miles o un nulo."""
    if indice in CLIENTES_NUMERICO_NULO:
        return None, True
    if rng.random() < 0.12:
        return rng.choice(separadores), True
    return rng.randint(0, maximo), False


def _generar_clientes(rng: random.Random) -> tuple[pd.DataFrame, dict[str, int]]:
    """Construye el DataFrame de clientes y sus contadores de suciedad."""
    filas: list[dict[str, Any]] = []
    cuerpos_usados: set[int] = set()
    estadisticas = {
        "sucias": 0,
        "dv_invalidos": 0,
        "rut_formato_raro": 0,
        "alta_con_guion": 0,
        "cumple_con_de": 0,
        "cumple_invalido": 0,
        "numericos_con_separador": 0,
        "numericos_nulos": 0,
    }

    for indice in range(CLIENTES_TOTAL):
        while True:
            cuerpo = rng.randint(10_000_000, 26_000_000)
            if cuerpo not in cuerpos_usados:
                cuerpos_usados.add(cuerpo)
                break

        dv_correcto = calculate_rut_dv(str(cuerpo))
        dv = dv_correcto
        rut_sucio = False
        if indice in CLIENTES_RUT_INVALIDO:
            dv = _elegir_dv_incorrecto(rng, dv_correcto)
            rut_sucio = True
            estadisticas["dv_invalidos"] += 1

        rut_texto, formato_raro = _formatear_rut(
            rng,
            cuerpo,
            dv,
            forzar_sucio=indice % 3 == 0,
        )
        if formato_raro:
            estadisticas["rut_formato_raro"] += 1

        nombre_limpio = _nombre_completo(rng)
        nombre, nombre_sucio = _nombre_con_ruido(rng, nombre_limpio, indice)
        email, email_sucio = _email_para(rng, nombre_limpio, indice)
        alta, alta_sucio = _fecha_alta(rng, indice)
        if alta_sucio and isinstance(alta, str) and "-" in alta:
            estadisticas["alta_con_guion"] += 1
        canal, canal_sucio = _canal(rng, indice)
        cumple, cumple_sucio = _cumple(rng, indice)
        if cumple_sucio and isinstance(cumple, str):
            if " de " in cumple or " De " in cumple:
                estadisticas["cumple_con_de"] += 1
            elif indice in CLIENTES_CUMPLE_INVALIDO:
                estadisticas["cumple_invalido"] += 1

        puntos, puntos_sucio = _numero_cliente(
            rng,
            maximo=12_000,
            indice=indice,
            separadores=("1.250", "3.500", "12.000"),
        )
        compras, compras_sucio = _numero_cliente(
            rng,
            maximo=350,
            indice=indice,
            separadores=("1.000", "2.500"),
        )
        canjes, canjes_sucio = _numero_cliente(
            rng,
            maximo=45,
            indice=indice,
            separadores=("1.000",),
        )

        for valor in (puntos, compras, canjes):
            if isinstance(valor, str):
                estadisticas["numericos_con_separador"] += 1
        for valor in (puntos, compras, canjes):
            if valor is None:
                estadisticas["numericos_nulos"] += 1

        filas.append(
            {
                "Rut cliente": rut_texto,
                "Nombre": nombre,
                "Email": email,
                "Alta": alta,
                "Canal": canal,
                "Cumple.": cumple,
                "Puntos": puntos,
                "Compras": compras,
                "Canjes": canjes,
            }
        )

        sucia = any(
            (
                rut_sucio,
                formato_raro,
                nombre_sucio,
                email_sucio,
                alta_sucio,
                canal_sucio,
                cumple_sucio,
                puntos_sucio,
                compras_sucio,
                canjes_sucio,
            )
        )
        if sucia:
            estadisticas["sucias"] += 1

    dataframe = pd.DataFrame(filas, columns=CLIENTES_COLUMNS)
    return dataframe, estadisticas


def _generar_isbn(rng: random.Random, usados: set[str]) -> str:
    """Genera un ISBN de 13 dígitos único, a veces con guiones."""
    while True:
        digitos = "978" + "".join(
            str(rng.randint(0, 9)) for _ in range(10)
        )
        if digitos in usados:
            continue
        usados.add(digitos)
        if rng.random() < 0.7:
            return (
                f"{digitos[:3]}-{digitos[3:6]}-{digitos[6:8]}-"
                f"{digitos[8:12]}-{digitos[12]}"
            )
        return digitos


def _generar_stock(rng: random.Random) -> tuple[pd.DataFrame, dict[str, int]]:
    """Construye el DataFrame de stock con totales negativos e ISBN vacíos."""
    filas: list[dict[str, Any]] = []
    isbn_usados: set[str] = set()
    estadisticas = {
        "isbn_vacios": 0,
        "pvp_texto": 0,
        "stock_total_negativo": 0,
        "stock_inconsistente": 0,
    }

    for indice in range(STOCK_TOTAL):
        isbn: Any = None
        if indice in STOCK_ISBN_VACIO:
            estadisticas["isbn_vacios"] += 1
        else:
            isbn = _generar_isbn(rng, isbn_usados)

        pvp: Any = rng.randrange(2_990, 65_000, 10)
        if indice in STOCK_PVP_TEXTO:
            pvp = PVP_TEXTO[indice % len(PVP_TEXTO)]
            estadisticas["pvp_texto"] += 1

        stock_consignacion = rng.randint(0, 20)
        stock_propio = rng.randint(0, 35)

        if indice in STOCK_TOTAL_NEGATIVO:
            stock_total = -rng.randint(1, 40)
            estadisticas["stock_total_negativo"] += 1
            estadisticas["stock_inconsistente"] += 1
        else:
            stock_total = stock_consignacion + stock_propio

        filas.append(
            {
                "ISBN": isbn,
                "Titulo": rng.choice(TITULOS),
                "Autor": rng.choice(AUTORES),
                "Editorial": rng.choice(EDITORIALES),
                "Clasificación": rng.choice(CLASIFICACIONES),
                "PVP": pvp,
                "Stock Consig.": stock_consignacion,
                "Stock Propio": stock_propio,
                "Stock Total": stock_total,
                "Ventas": rng.randint(0, 60),
            }
        )

    dataframe = pd.DataFrame(filas, columns=STOCK_COLUMNS)
    return dataframe, estadisticas


def _precios_libreria(stock: pd.DataFrame) -> tuple[int, ...]:
    """Precios PVP de los libros que ``stock.transform`` acepta."""
    filas, _ = stock_pipeline.transform(
        _como_texto(stock),
        source_file="STOCK LIBRERIA.xlsx",
        loaded_at=CARGA_FIJA,
    )
    precios = sorted(
        {
            fila[5]
            for fila in filas
            if fila[5] is not None and fila[5] >= 100
        }
    )
    assert precios, "el stock generado no tiene PVP válidos para ventas"
    return tuple(precios)


def _precios_cafeteria() -> tuple[int, ...]:
    """Precios distintos del catálogo versionado de cafetería."""
    dataframe = read_csv_as_str(CATALOGO_CAFETERIA)
    precios = sorted(
        {
            precio
            for valor in dataframe["precio"]
            if (precio := parse_non_negative_bigint(valor, default=None))
            is not None
        }
    )
    assert precios, f"el catálogo {CATALOGO_CAFETERIA.name} no tiene precios"
    return tuple(precios)


def _elegir_ticket_exacto(
    rng: random.Random,
    precios: Sequence[int],
    *,
    usados: set[int] | None = None,
    max_items: int = 6,
) -> tuple[int, int]:
    """Elige ``N`` precios reales y devuelve ``(N, suma exacta)``.

    Si se pasa ``usados`` intenta que el total no se repita; cuando el
    catálogo es pequeño (cafetería) puede agotar los intentos y se permite
    un total repetido. La combinación elegida siempre es reconstruible porque
    usa precios del catálogo correspondiente.
    """
    intentos = 50 if usados is not None else 1
    for _ in range(intentos):
        cantidad = rng.randint(1, max_items)
        total = sum(rng.choice(precios) for _ in range(cantidad))
        if usados is None or total not in usados:
            if usados is not None:
                usados.add(total)
            return cantidad, total

    # Catálogo pequeño: se acepta un total repetido pero exacto.
    cantidad = rng.randint(1, max_items)
    total = sum(rng.choice(precios) for _ in range(cantidad))
    return cantidad, total


def _fila_venta(
    rng: random.Random,
    *,
    correlativo: int,
    folio: Any,
    tipo_documento: str,
    total_venta: Any,
    cantidad: Any,
) -> dict[str, Any]:
    """Arma una fila cruda de ventas con columnas de origen conservadas."""
    canal = "Web" if rng.random() < 0.3 else "Local"
    orden_web: Any = (
        rng.randint(500_000, 599_999) if canal == "Web" else None
    )
    return {
        "Correlativo": correlativo,
        "Orden Web": orden_web,
        "Fecha": rng.choice(FECHAS_VENTAS),
        "Medio de Pago": rng.choice(MEDIOS_PAGO),
        "Tipo Documento": tipo_documento,
        "Folio": folio,
        "Voucher Máquina de Pago": None,
        "Total Venta": total_venta,
        "Cant. Items": cantidad,
        "Canal": canal,
        "Feria": None,
        "Tiene Descuento": "SI" if rng.random() < 0.2 else "NO",
        "Estado": "Anulada" if rng.random() < 0.1 else "Emitida",
        "Es Dev.": "SI" if rng.random() < 0.08 else "NO",
    }


def _generar_ventas(
    rng: random.Random,
    config: dict[str, Any],
    precios: Sequence[int],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Construye ventas con totales exactos y filas sucias reservadas.

    Cada fila válida usa ``Cant. Items`` precios reales del catálogo y
    ``Total Venta`` es su suma exacta. Las filas reservadas para total/cantidad
    inválidos se conservan tal cual; además se agregan filas que solo fallan
    por Folio o Tipo Documento para ejercitar las reglas de cuarentena.
    """
    filas: list[dict[str, Any]] = []
    total_nulo = set(config["total_nulo"])
    total_negativo = set(config["total_negativo"])
    cant_nula = set(config["cant_nula"])
    cant_negativa = set(config["cant_negativa"])
    usados: set[int] = set()
    estadisticas = {
        "filas_con_total_o_cantidad_problematica": 0,
        "total_nulo": 0,
        "total_negativo": 0,
        "cantidad_nula": 0,
        "cantidad_negativa": 0,
        "folio_invalido": 0,
        "documento_invalido": 0,
        "tickets_exactos": 0,
    }

    for indice in range(config["filas_base"]):
        folio: Any = config["folio_base"] + indice
        tipo_documento = "Factura" if rng.random() < 0.15 else "Boleta"

        if indice in total_nulo:
            total_venta: Any = None
            cantidad: Any = rng.randint(1, 6)
            estadisticas["total_nulo"] += 1
        elif indice in total_negativo:
            total_venta = -rng.randint(1_000, config["monto_max"] // 2)
            cantidad = rng.randint(1, 6)
            estadisticas["total_negativo"] += 1
        elif indice in cant_nula:
            _, total_venta = _elegir_ticket_exacto(rng, precios)
            cantidad = None
            estadisticas["cantidad_nula"] += 1
        elif indice in cant_negativa:
            _, total_venta = _elegir_ticket_exacto(rng, precios)
            cantidad = -rng.randint(1, 3)
            estadisticas["cantidad_negativa"] += 1
        else:
            cantidad, total_venta = _elegir_ticket_exacto(
                rng,
                precios,
                usados=usados,
            )
            estadisticas["tickets_exactos"] += 1

        if (
            indice in total_nulo
            or indice in total_negativo
            or indice in cant_nula
            or indice in cant_negativa
        ):
            estadisticas["filas_con_total_o_cantidad_problematica"] += 1

        filas.append(
            _fila_venta(
                rng,
                correlativo=config["correlativo_base"] + indice,
                folio=folio,
                tipo_documento=tipo_documento,
                total_venta=total_venta,
                cantidad=cantidad,
            )
        )

    for offset, extra in enumerate(FILAS_EXTRA_FOLIO_DOC, start=1):
        cantidad, total_venta = _elegir_ticket_exacto(rng, precios)
        base = config["filas_base"] + offset
        if extra["folio"] == "VALIDO":
            folio_extra: Any = config["folio_base"] + base
        else:
            folio_extra = extra["folio"]
            estadisticas["folio_invalido"] += 1
        if extra["tipo_documento"].strip().casefold() == "sin documento":
            estadisticas["documento_invalido"] += 1
        filas.append(
            _fila_venta(
                rng,
                correlativo=config["correlativo_base"] + base,
                folio=folio_extra,
                tipo_documento=extra["tipo_documento"],
                total_venta=total_venta,
                cantidad=cantidad,
            )
        )

    dataframe = pd.DataFrame(filas, columns=VENTAS_COLUMNS)
    assert len(dataframe) == config["total"], (
        f"{config['archivo']}: se esperaban {config['total']} filas y se "
        f"generaron {len(dataframe)}"
    )
    return dataframe, estadisticas


def _es_dv_invalido(valor: Any) -> bool:
    """Recalcula el DV de una celda con formato variable."""
    crudo = str(valor).strip().upper().replace(".", "").replace(" ", "")
    if not crudo:
        return False
    if "-" in crudo:
        cuerpo, dv = crudo.split("-", 1)
    else:
        cuerpo, dv = crudo[:-1], crudo[-1]
    if not cuerpo.isdigit() or len(cuerpo) not in (7, 8):
        return False
    if len(dv) != 1 or dv not in "0123456789K":
        return False
    return dv != calculate_rut_dv(cuerpo)


def _como_texto(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Mimética de ``read_excel_as_str``: todas las celdas como texto.

    Evita que un entero escrito como ``14380`` llegue a los validadores como
    ``14380.0`` y que el punto decimal se interprete como separador de miles.
    """
    def convertir(valor: Any) -> str:
        if valor is None or pd.isna(valor):
            return ""
        if isinstance(valor, float) and valor.is_integer():
            return str(int(valor))
        return str(valor)

    return dataframe.map(convertir)


def _filas_venta_problematicas(dataframe: pd.DataFrame) -> int:
    """Cuenta filas con Total Venta o Cant. Items nulo/negativo."""
    total = 0
    for total_venta, cantidad in zip(
        dataframe["Total Venta"],
        dataframe["Cant. Items"],
        strict=True,
    ):
        if pd.isna(total_venta) or total_venta < 0:
            total += 1
        elif pd.isna(cantidad) or cantidad < 0:
            total += 1
    return total


def _parse_alta_generada(valor: Any) -> date:
    """Parsea una Alta sintética válida en formato ``d/m/aaaa`` o ``d-m-aaaa``."""
    partes = re.split(r"[/-]", str(valor).strip())
    dia, mes, anio = (int(parte) for parte in partes)
    return date(anio, mes, dia)


def _clientes_validos(
    clientes: pd.DataFrame,
) -> list[tuple[int, str, date]]:
    """Devuelve ``(indice, cliente_id, alta)`` de clientes válidos y asignables.

    ``cliente_id`` es el HMAC-SHA256 que genera el pipeline; el RUT crudo no
    se usa para asignar compras ni se escribe en ningún artefacto derivado.
    """
    validos: list[tuple[int, str, date]] = []
    texto = _como_texto(clientes)
    for indice, row in enumerate(texto.to_dict(orient="records")):
        normalizado, _ = clientes_pipeline.transform_row(
            row,
            reference_year=2024,
        )
        if normalizado is None:
            continue
        if indice in CLIENTES_NUMERICO_NULO:
            # Se conserva el caso nulo reservado: el pipeline lo trata como 0.
            continue
        cliente_id, alta = normalizado[0], normalizado[1]
        if cliente_id is None or alta is None:
            continue
        validos.append((indice, cliente_id, alta))
    return validos


def _asignar_compras(
    clientes: pd.DataFrame,
    tickets: Sequence[ventas_pipeline.Ticket],
) -> None:
    """Asigna montos históricos exactos a un subconjunto de clientes.

    El cliente con dos tickets recibe los dos tickets de mayor monto. Luego
    cada cliente recibe un único ticket con un total distinto y descendente.
    ``assign_clients`` procesa a los clientes por ``compras`` descendente, así
    que cada objetivo encuentra directamente el ticket de igual total.
    """
    validos = _clientes_validos(clientes)
    assert len(validos) >= CLIENTES_CON_COMPRAS, (
        f"se necesitan {CLIENTES_CON_COMPRAS} clientes válidos y hay "
        f"{len(validos)}"
    )

    ordenados = sorted(
        tickets,
        key=lambda ticket: (
            -ticket.total_venta,
            ticket.origen,
            ticket.correlativo,
        ),
    )
    assert len(ordenados) > CLIENTES_CON_COMPRAS, (
        "no hay tickets suficientes para asignar compras"
    )

    cliente_doble = validos[0]
    tickets_dobles = ordenados[:2]
    total_doble = sum(ticket.total_venta for ticket in tickets_dobles)

    objetivos: list[int] = []
    vistos = {ticket.total_venta for ticket in tickets_dobles}
    for ticket in ordenados[2:]:
        if ticket.total_venta in vistos:
            continue
        vistos.add(ticket.total_venta)
        objetivos.append(ticket.total_venta)
        if len(objetivos) == CLIENTES_CON_COMPRAS - 1:
            break
    assert len(objetivos) == CLIENTES_CON_COMPRAS - 1, (
        "no hay suficientes totales distintos para asignar compras"
    )

    asignados = {cliente_doble[0]}
    clientes.at[cliente_doble[0], "Compras"] = total_doble
    for (indice, _cliente_id, _alta), total in zip(
        validos[1 : CLIENTES_CON_COMPRAS],
        objetivos,
        strict=True,
    ):
        clientes.at[indice, "Compras"] = total
        asignados.add(indice)

    # Todo cliente válido no asignado queda con compras 0; los casos nulos
    # reservados se conservan para que el pipeline los interprete como 0.
    for indice, _cliente_id, _alta in validos:
        if indice not in asignados:
            clientes.at[indice, "Compras"] = 0


def _tickets_de_ventas(
    ventas_libreria: pd.DataFrame,
    ventas_cafeteria: pd.DataFrame,
) -> tuple[list[ventas_pipeline.Ticket], int]:
    """Transforma ambos archivos y devuelve los tickets válidos y rechazos."""
    libreria, rechazos_libreria = ventas_pipeline.transform(
        _como_texto(ventas_libreria),
        origen="LIBRERIA",
        source_file=VENTAS_LIBRERIA["archivo"],
        loaded_at=CARGA_FIJA,
    )
    cafeteria, rechazos_cafeteria = ventas_pipeline.transform(
        _como_texto(ventas_cafeteria),
        origen="CAFETERIA",
        source_file=VENTAS_CAFETERIA["archivo"],
        loaded_at=CARGA_FIJA,
    )
    return (
        [*libreria, *cafeteria],
        len(rechazos_libreria) + len(rechazos_cafeteria),
    )


def _verificar_invariantes(
    clientes: pd.DataFrame,
    stock: pd.DataFrame,
    ventas_libreria: pd.DataFrame,
    ventas_cafeteria: pd.DataFrame,
    estadisticas_clientes: dict[str, int],
) -> None:
    """Falla ruidosamente si el generador deja de cumplir el contrato."""
    assert tuple(clientes.columns) == CLIENTES_COLUMNS, "columnas de clientes"
    assert tuple(stock.columns) == STOCK_COLUMNS, "columnas de stock"
    assert tuple(ventas_libreria.columns) == VENTAS_COLUMNS, "columnas librería"
    assert tuple(ventas_cafeteria.columns) == VENTAS_COLUMNS, "columnas cafetería"

    assert len(clientes) == CLIENTES_TOTAL, "volumen de clientes"
    assert len(stock) == STOCK_TOTAL, "volumen de stock"
    assert len(ventas_libreria) == VENTAS_LIBRERIA["total"], "volumen librería"
    assert len(ventas_cafeteria) == VENTAS_CAFETERIA["total"], "volumen cafetería"

    assert estadisticas_clientes["sucias"] >= CLIENTES_TOTAL * 0.30, (
        "se esperaba al menos 30 % de filas sucias"
    )
    assert estadisticas_clientes["dv_invalidos"] >= 5, "se esperaban 5+ DV malos"

    dv_invalidos = sum(
        1 for valor in clientes["Rut cliente"] if _es_dv_invalido(valor)
    )
    assert dv_invalidos >= 5, "los RUT con DV inválido no se materializaron"

    con_anio = [
        valor
        for valor in clientes["Cumple."]
        if not pd.isna(valor) and re.search(r"\d{4}", str(valor))
    ]
    assert not con_anio, f"Cumple. no debe tener año: {con_anio[:3]}"

    # Integridad temporal: ninguna Alta válida puede ser posterior a la
    # primera venta del lote. Las Altas sucias reservadas siguen siendo
    # inválidas y no participan de esta comprobación.
    limite_alta = date.fromisoformat(min(FECHAS_VENTAS))
    altas_validas = [
        _parse_alta_generada(valor)
        for indice, valor in enumerate(clientes["Alta"])
        if indice not in CLIENTES_ALTA_INVALIDA and not pd.isna(valor)
    ]
    assert altas_validas, "debe haber Altas válidas para verificar el rango"
    assert max(altas_validas) <= limite_alta, (
        "toda Alta válida debe ser <= "
        f"{limite_alta.isoformat()}; la máxima fue "
        f"{max(altas_validas).isoformat()}"
    )

    assert sum(1 for valor in stock["ISBN"] if pd.isna(valor)) == len(
        STOCK_ISBN_VACIO
    ), "ISBN vacíos"
    assert (
        sum(1 for valor in stock["PVP"] if isinstance(valor, str)) >= 6
    ), "se esperaban 6+ PVP como texto"
    assert (
        sum(1 for valor in stock["Stock Total"] if int(valor) < 0) >= 8
    ), "se esperaban 8+ Stock Total negativos"

    for dataframe in (ventas_libreria, ventas_cafeteria):
        assert all(
            pd.isna(valor)
            for valor in dataframe["Voucher Máquina de Pago"]
        ), "Voucher Máquina de Pago debe estar 100 % vacío"
        assert all(pd.isna(valor) for valor in dataframe["Feria"]), (
            "Feria debe estar 100 % vacía"
        )
        assert all(
            re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(valor))
            for valor in dataframe["Fecha"]
        ), "Fecha debe ser texto YYYY-MM-DD"
        assert _filas_venta_problematicas(dataframe) >= 5, (
            "se esperaban 5+ filas con Total/Cantidad problemáticos"
        )

    correlativos_libreria = set(ventas_libreria["Correlativo"])
    correlativos_cafeteria = set(ventas_cafeteria["Correlativo"])
    folios_libreria = {
        str(valor).strip()
        for valor in ventas_libreria["Folio"]
        if not pd.isna(valor) and str(valor).strip() not in ("", "0", "000")
    }
    folios_cafeteria = {
        str(valor).strip()
        for valor in ventas_cafeteria["Folio"]
        if not pd.isna(valor) and str(valor).strip() not in ("", "0", "000")
    }
    assert not correlativos_libreria & correlativos_cafeteria, (
        "los correlativos no deben colisionar"
    )
    assert not folios_libreria & folios_cafeteria, (
        "los folios válidos no deben colisionar"
    )

    # ------------------------------------------------------------------
    # Invariantes de sumas exactas, compras monetarias y temporalidad.
    # ------------------------------------------------------------------
    precios_libreria = _precios_libreria(stock)
    precios_cafeteria = _precios_cafeteria()
    tickets, total_rechazados = _tickets_de_ventas(
        ventas_libreria,
        ventas_cafeteria,
    )
    assert len(tickets) == 207, "se esperaban 207 tickets válidos"
    assert total_rechazados == 33, (
        f"se esperaban 33 filas rechazadas y hay {total_rechazados}"
    )

    max_n = max(ticket.cant_items for ticket in tickets)
    for origen, precios in (
        ("LIBRERIA", precios_libreria),
        ("CAFETERIA", precios_cafeteria),
    ):
        tickets_origen = [
            ticket for ticket in tickets if ticket.origen == origen
        ]
        max_x = max(ticket.total_venta for ticket in tickets_origen)
        reachable = ventas_pipeline.compute_reachable(
            precios,
            max_n,
            max_x,
        )
        for ticket in tickets_origen:
            combinacion = ventas_pipeline.reconstruct_prices(
                ticket.total_venta,
                ticket.cant_items,
                precios,
                reachable,
            )
            assert combinacion is not None, (
                f"ticket {origen}/{ticket.correlativo} sin combinación "
                f"exacta: total={ticket.total_venta} "
                f"items={ticket.cant_items}"
            )
            assert len(combinacion) == ticket.cant_items
            assert sum(combinacion) == ticket.total_venta

    filas_clientes, rechazos_clientes = clientes_pipeline.transform(
        _como_texto(clientes)
    )
    assert len(filas_clientes) == 136, "se esperaban 136 clientes válidos"
    assert len(rechazos_clientes) == 14, "se esperaban 14 clientes rechazados"

    clients = [
        ventas_pipeline.Client(
            cliente_id=fila[0],
            alta=fila[1],
            compras=fila[6],
        )
        for fila in filas_clientes
    ]
    assignments = ventas_pipeline.assign_clients(tickets, clients)
    ticket_por_clave = {
        (ticket.origen, ticket.correlativo): ticket for ticket in tickets
    }
    sumas: dict[str, int] = defaultdict(int)
    conteos: dict[str, int] = defaultdict(int)
    for clave, cliente_id in assignments.items():
        ticket = ticket_por_clave[clave]
        sumas[cliente_id] += ticket.total_venta
        conteos[cliente_id] += 1

    positivos = [client for client in clients if client.compras > 0]
    assert len(positivos) == CLIENTES_CON_COMPRAS, (
        f"se esperaban {CLIENTES_CON_COMPRAS} clientes con compras y hay "
        f"{len(positivos)}"
    )
    for client in positivos:
        assert client.alta is not None
        assert sumas[client.cliente_id] == client.compras, (
            f"cliente_id {client.cliente_id}: compras={client.compras} y "
            f"tickets asignados={sumas[client.cliente_id]}"
        )
        assert conteos[client.cliente_id] >= 1
        for clave, cliente_id in assignments.items():
            if cliente_id == client.cliente_id:
                assert ticket_por_clave[clave].fecha >= client.alta

    assert any(conteo >= 2 for conteo in conteos.values()), (
        "debe existir al menos un cliente con dos tickets"
    )
    assert len(assignments) == len(set(assignments)), (
        "un ticket no puede asignarse dos veces"
    )
    assert len(assignments) < len(tickets), (
        "debe haber tickets válidos anónimos"
    )


def _escribir_excel(
    dataframe: pd.DataFrame,
    output_dir: Path,
    nombre: str,
) -> Path:
    """Escribe un DataFrame como XLSX reproducible con openpyxl."""
    destino = output_dir / nombre
    dataframe.to_excel(destino, index=False, engine="openpyxl")
    return destino


def build_all(
    output_dir: Path = RAW_DIR,
) -> dict[str, tuple[Path, int]]:
    """Genera los cuatro archivos y devuelve nombre -> (ruta, filas)."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Un Random independiente por archivo: el orden de construcción no altera
    # el resultado de los demás y la semilla es siempre la misma.
    clientes, estadisticas_clientes = _generar_clientes(
        random.Random(SEED)
    )
    stock, _ = _generar_stock(random.Random(SEED))

    # Stock y catálogo de cafetería se generan/leen antes que las ventas para
    # que cada ticket use precios reales de su catálogo.
    precios_libreria = _precios_libreria(stock)
    precios_cafeteria = _precios_cafeteria()
    ventas_libreria, _ = _generar_ventas(
        random.Random(SEED),
        VENTAS_LIBRERIA,
        precios_libreria,
    )
    ventas_cafeteria, _ = _generar_ventas(
        random.Random(SEED),
        VENTAS_CAFETERIA,
        precios_cafeteria,
    )

    tickets, _ = _tickets_de_ventas(ventas_libreria, ventas_cafeteria)
    _asignar_compras(clientes, tickets)

    _verificar_invariantes(
        clientes,
        stock,
        ventas_libreria,
        ventas_cafeteria,
        estadisticas_clientes,
    )

    resultados = {
        "CLIENTES LIBRERIA.xlsx": (
            _escribir_excel(clientes, output_dir, "CLIENTES LIBRERIA.xlsx"),
            len(clientes),
        ),
        "STOCK LIBRERIA.xlsx": (
            _escribir_excel(stock, output_dir, "STOCK LIBRERIA.xlsx"),
            len(stock),
        ),
        "VENTAS LIBRERIA.xlsx": (
            _escribir_excel(
                ventas_libreria,
                output_dir,
                "VENTAS LIBRERIA.xlsx",
            ),
            len(ventas_libreria),
        ),
        "VENTAS CAFETERIA.xlsx": (
            _escribir_excel(
                ventas_cafeteria,
                output_dir,
                "VENTAS CAFETERIA.xlsx",
            ),
            len(ventas_cafeteria),
        ),
    }
    return resultados


def build_parser() -> argparse.ArgumentParser:
    """Define la interfaz de línea de comandos del generador."""
    parser = argparse.ArgumentParser(
        description=(
            "Genera fixtures sintéticos reproducibles para el ETL de "
            "Miralibros."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=RAW_DIR,
        help=(
            "Directorio de salida (por defecto tests/fixtures/raw). "
            "Útil para comparar reproducibilidad en un directorio temporal."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Punto de entrada: genera los archivos e imprime un resumen."""
    args = build_parser().parse_args(argv)
    resultados = build_all(args.output_dir)

    print(f"Semilla fija: {SEED}")
    print(f"Directorio de salida: {args.output_dir.resolve()}")
    print("Archivos generados:")
    for nombre, (ruta, filas) in resultados.items():
        tamano = ruta.stat().st_size
        print(f"  - {nombre}: {filas} filas, {tamano} bytes")

    print("Invariantes sucios incluidos a propósito:")
    print(
        "  - RUT con DV incorrecto: "
        f"{len(CLIENTES_RUT_INVALIDO)} filas"
    )
    print("  - Cumple. sin año y con formatos 'de' / 'd - mes': sí")
    print("  - PVP como texto y Stock Total negativo: sí")
    print("  - Total Venta / Cant. Items nulos o negativos: sí")
    print("  - Tickets válidos con suma exacta de precios: sí")
    print("  - Compras de clientes como monto monetario en CLP: sí")
    print("  - Voucher Máquina de Pago y Feria 100 % vacías: sí")
    print("Regenerá estos XLSX con este mismo script cuando haga falta.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
