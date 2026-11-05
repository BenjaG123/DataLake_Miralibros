# Calidad de ISBN y stock obsoleto

> **Versión:** 1.0.0 · **Estado:** propuesta, no implementada
> **Fecha:** 2026-10-28
> **Owner:** `data-quality-steward`
> **Revisores:** `silver-etl-engineer`, `data-testing-engineer`, `data-governance-steward`
> **Alcance:** documentación y contrato. Este documento **no** modifica `etl/`, `sql/`, `tests/`
> ni configuración. Todo lo marcado como **a crear** requiere implementación posterior.

---

## 0. Resumen ejecutivo

1. **ISBN:** hoy `normalize_isbn` valida solo la forma (13 dígitos o ISBN-10 con `X`), no el
   dígito verificador. Se propone una función única `isbn_check_digit` / `validate_isbn`, con
   adopción en dos fases para no romper los fixtures ni las cargas reales:
   **fase 1 warning + cuarentena**, **fase 2 rechazo**.
2. **Fixtures:** los ISBN sintéticos de `tests/fixtures/raw/STOCK LIBRERIA.xlsx` se generan con
   dígitos aleatorios y **no cumplen** el dígito verificador (aproximadamente 9 de cada 10).
   Si se activara el rechazo hoy, se perdería la mayor parte del lote de prueba. Por eso se
   regeneran con DV válido y se reservan pocas filas inválidas para probar la regla.
3. **Stock obsoleto:** se propone una máquina de estados `VIGENTE → AUSENTE → OBSOLETO` basada
   en cargas exitosas y confiables. Un ISBN pasa a `OBSOLETO` tras `N=2` ausencias consecutivas
   por defecto. **Nunca se borra la fila** y **nunca se ponen los stock en cero**: se conserva el
   último stock conocido y se marca el estado. Los KPIs de inventario y quiebre deben filtrar
   `estado = 'VIGENTE'`.
4. **Trazabilidad:** se propone una tabla operativa `silver.stock_cargas` (a crear) y una tabla
   de cuarentena con archivo, fila, columna, valor, regla, motivo, SHA-256 y timestamp. Una fila
   descartada no puede quedar sin registro.

### Decisiones tomadas

| Tema | Decisión | Motivo |
|---|---|---|
| DV ISBN | Fase 1 `warning`, fase 2 `error` | Evita rechazar ~90 % de los fixtures y permite medir el dato real antes de bloquear |
| Fixtures | Regenerar con DV válido y reservar casos inválidos | Cada regla debe tener un caso que la dispare sin romper el resto de las pruebas |
| Stock al volverse obsoleto | Conservar último stock conocido; no poner 0 | Poner 0 inventa un dato y distorsiona quiebre/rotación; “no observado” no es “stock 0” |
| Borrado | Nunca borrar; solo cambiar `estado` | Se preserva historial, trazabilidad y joins con ventas |
| Umbral | `N=2` por defecto; mínimo recomendado 2 | Una sola ausencia puede ser un archivo incompleto; dos cargas confiables dan más señal |
| Cargas no confiables | No marcan ausencias | Una exportación parcial no debe declarar obsoleto medio catálogo |
| Historial | Cerrar/abrir versión en cada cambio de estado | El estado forma parte de la versión de negocio; como mínimo, al pasar a `OBSOLETO` |
| Observabilidad | `stock_cargas` + cuarentena + logs INFO/WARNING | Permite explicar qué pasó sin depender de `DEBUG` |

---

## 1. Alcance y estado actual del repositorio

### 1.1 Lo que existe hoy

- `etl/pipelines/stock.py`:
  - `normalize_isbn` quita guiones, espacios y puntos, y exige forma
    `^(?:[0-9]{13}|[0-9]{9}[0-9X])$`. **No valida DV ni prefijo 978/979.**
  - `transform_row` rechaza la fila completa si ISBN, PVP, stock, total o ventas son inválidos.
  - `transform` registra el detalle por fila en `DEBUG` y un resumen agregado en `INFO`/`WARNING`.
  - `deduplicate` conserva la última fila por ISBN y avisa con `WARNING`.
  - `run` hace `upsert` por ISBN y luego `_refresh_history`, dentro de una transacción. No hay
    cuarentena persistida ni métricas por corrida.
- `sql/stock.sql`:
  - `silver.stock` tiene PK `isbn`, precios, stocks, ventas y auditoría básica
    (`valid_from`, `updated_at`, `_source_file`, `_loaded_at`).
  - `silver.stock_historial` es SCD2 con `valid_from`, `valid_to`, `is_current` e índice único
    parcial por ISBN vigente.
  - El `CHECK` de ISBN solo valida forma; no valida DV ni prefijo.
- `tests/fixtures/build_fixtures.py`:
  - `_generar_isbn` arma `"978" + 10 dígitos aleatorios`; no calcula DV.
  - `STOCK_TOTAL = 100`, `STOCK_ISBN_VACIO` (8 filas) y `STOCK_TOTAL_NEGATIVO` (10 filas).
- `tests/test_stock_ventas_transform.py`:
  - Contrato actual: 100 leídas, 82 válidas, 18 rechazadas
    (`ISBN inválido o vacío`: 8; `stock total inválido o negativo`: 10).
  - `ISBN_VALIDO = "9789561234567"`, que **sí** tiene DV correcto (ver §3.7).

### 1.2 Lo que no existe

- Validación de dígito verificador ISBN-13/ISBN-10.
- Validación de prefijo 978/979 para ISBN-13.
- Campos de ciclo de vida en `silver.stock` (`estado`, `ausencias_consecutivas`, `faltante_desde`,
  `ultima_vez_visto`, `ultima_carga_exitosa`, `_carga_id`).
- Tabla de cargas (`stock_cargas`) para saber qué cargas fueron exitosas y cuáles no.
- Tabla de cuarentena en el repo (la skill `data-validation-rules` la propone; **a crear**).
- Reporte de rechazos legible y métricas por regla.
- Tests de ciclo de vida de stock y de DV ISBN.
- Integridad referencial `ventas → clientes/productos`: **fuera de alcance** de este documento;
  se gobierna en el contrato de ventas. Acá solo se cubre `stock_historial → stock`.

> **Importante:** este documento no declara nada de lo anterior como implementado. Todo cambio
> de código queda listado en §8 con dueño y criterio de aceptación.

---

## 2. Contrato de datos (fragmento stock)

Contrato embebido, versión **2.0.0**. Si se externaliza, el archivo propuesto es
`_meta/contratos/stock.yaml` (**a crear**; no existe hoy en el repo).

```yaml
version: 2.0.0
entidad: stock
clave_natural: isbn
granularidad: >
  Una fila por ISBN en silver.stock con el último estado conocido.
  Una fila por versión de negocio en silver.stock_historial.
columnas:
  isbn:
    tipo: text
    nulo: false
    unico: true
    dominio: "ISBN-13 con prefijo 978/979 o ISBN-10; sin guiones ni espacios"
  titulo: {tipo: text, nulo: true}
  autor: {tipo: text, nulo: true}
  editorial: {tipo: text, nulo: true}
  clasificacion: {tipo: text, nulo: true}
  pvp:
    tipo: numeric(12,0)
    nulo: false
    dominio: "entero CLP >= 0"
  stock_consignacion: {tipo: integer, nulo: false, dominio: ">= 0"}
  stock_propio: {tipo: integer, nulo: false, dominio: ">= 0"}
  stock_total: {tipo: integer, nulo: false, dominio: ">= 0 y = stock_consignacion + stock_propio"}
  ventas: {tipo: integer, nulo: false, dominio: ">= 0"}
  estado:
    tipo: text
    nulo: false
    dominio: "VIGENTE | AUSENTE | OBSOLETO"
  ausencias_consecutivas: {tipo: integer, nulo: false, dominio: ">= 0"}
  faltante_desde: {tipo: timestamptz, nulo: true}
  ultima_vez_visto: {tipo: timestamptz, nulo: false}
  ultima_carga_exitosa: {tipo: timestamptz, nulo: false}
  _carga_id: {tipo: uuid, nulo: true}
  valid_from: {tipo: timestamptz, nulo: false}
  updated_at: {tipo: timestamptz, nulo: false}
  _source_file: {tipo: text, nulo: false}
  _loaded_at: {tipo: timestamptz, nulo: false}
reglas:
  - id: ISBN-001
    severidad: error
  - id: ISBN-002
    severidad: error
  - id: ISBN-003
    severidad: error
  - id: ISBN-004
    severidad: "warning en fase 1; error en fase 2"
  - id: STOCK-001..STOCK-020
    severidad: "ver §4.12"
umbrales:
  n_ausencias_obsoleto: 2
  tasa_rechazo_max: 0.10
  tasa_isbn_dv_invalido_max: 0.01
  caida_volumen_max: 0.30
  frescura_max_horas: 168
  filas_minimas: 1
accion_si_falla:
  - "publicar filas válidas si hay al menos una"
  - "no marcar ausencias"
  - "contrato FAIL y alerta operativa"
  - "nunca borrar ni truncar stock"
```

### 2.1 Nullabilidad y dominio

| Columna | Nulo | Dominio / regla |
|---|---|---|
| `isbn` | No | ISBN-13 `978`/`979` o ISBN-10; único |
| `titulo`, `autor`, `editorial`, `clasificacion` | Sí | Texto limpio; placeholders se convierten en `NULL` |
| `pvp` | No | Entero CLP `>= 0` |
| `stock_consignacion`, `stock_propio`, `stock_total` | No | Enteros `>= 0`; `stock_total = stock_consignacion + stock_propio` |
| `ventas` | No | Entero `>= 0` |
| `estado` | No | `VIGENTE`, `AUSENTE`, `OBSOLETO` |
| `ausencias_consecutivas` | No | Entero `>= 0`; `0` solo si `estado = 'VIGENTE'` |
| `faltante_desde` | Sí | `NULL` si `VIGENTE`; no nulo si `AUSENTE` u `OBSOLETO` |
| `ultima_vez_visto` | No | Última carga confiable en que el ISBN apareció |
| `ultima_carga_exitosa` | No | Última carga exitosa que tocó la fila; en ausentes solo si la carga fue apta |
| `_carga_id` | Sí | UUID de la última carga que tocó la fila; `NULL` en migración |
| `valid_from` | No | Primera vez que se cargó la fila; la vigencia de versión vive en `stock_historial` |
| `updated_at` | No | Último cambio de dato o de estado; no cambia por solo incrementar ausencias |

### 2.2 Invariantes de estado

```sql
-- Debe devolver 0 filas.
SELECT *
FROM {{schema}}.stock
WHERE (estado = 'VIGENTE'
       AND (ausencias_consecutivas <> 0 OR faltante_desde IS NOT NULL))
   OR (estado <> 'VIGENTE'
       AND (ausencias_consecutivas = 0 OR faltante_desde IS NULL));
```

```sql
-- La última vez visto nunca puede ser posterior a la última evaluación.
SELECT *
FROM {{schema}}.stock
WHERE ultima_vez_visto > ultima_carga_exitosa;
```

---

## 3. Regla ISBN

### 3.1 Definición matemática formal: ISBN-13

Sea un ISBN-13 con dígitos `d_1, d_2, ..., d_13`, todos en `{0, 1, ..., 9}`.

Para `i = 1..12`, el peso es:

```text
w_i = 1  si i es impar
w_i = 3  si i es par
```

La suma ponderada de los primeros 12 dígitos es:

```text
S = Σ_{i=1}^{12} d_i · w_i
```

El dígito verificador es:

```text
check = (10 − (S mod 10)) mod 10
```

El ISBN-13 es válido si `d_13 = check`. Equivalentemente, si se incluye el DV con peso 1:

```text
(S + d_13) mod 10 = 0
```

**Por qué el resultado 10 se representa como 0.** El dígito verificador debe ocupar una sola
posición decimal, es decir, un carácter entre `0` y `9`. Si `S mod 10 = 0`, la fórmula
`10 − 0 = 10`; al aplicar `mod 10` se obtiene `0`. No se escribe `10` ni `X`: `X` es exclusivo del
ISBN-10. El `mod 10` final es justamente lo que mapea el caso 10 a 0.

### 3.2 Definición matemática formal: ISBN-10

Sea un ISBN-10 con caracteres `c_1, ..., c_10`, donde `c_1..c_9` son dígitos y `c_10` puede ser
un dígito o `X`. Se define `x_10 = 10` si `c_10 = X`, y `x_10 = c_10` si es dígito.

Para `i = 1..9`, el peso es `11 − i` (es decir, 10, 9, 8, ..., 2). La suma es:

```text
S = Σ_{i=1}^{9} c_i · (11 − i)
```

El dígito verificador es:

```text
check = (11 − (S mod 11)) mod 11
```

Representación:

```text
10 → "X"
0  → "0"
1..9 → "1".."9"
```

El ISBN-10 es válido si `x_10 = check`. Equivalentemente:

```text
(S + x_10) mod 11 = 0
```

`X` solo es válido en la última posición. Una `X` en el cuerpo, o una `x` minúscula que no se
normalice, hace inválido el ISBN.

### 3.3 Prefijo y diferencia con un EAN-13

- Un **ISBN-13** debe empezar con `978` o `979`. El resto de los 13 dígitos y el DV se calculan
  igual que un EAN-13, pero el prefijo es lo que lo identifica como ISBN.
- Un **EAN-13 válido** que empieza con otro prefijo (por ejemplo `400...`) **no es un ISBN**,
  aunque su dígito verificador EAN sea correcto. Ejemplo: `4006381333931` es un EAN-13 válido,
  pero `validate_isbn` debe devolver `False`.
- `979` puede incluir ISBN y también ISMN (música). Para Miralibros se acepta `979` como ISBN;
  si aparece un `979` que no corresponde a libro, se resuelve en catálogo, no en el validador.

### 3.4 Normalización previa

Antes de validar:

1. Convertir a texto y aplicar `strip`.
2. Eliminar guiones `-`, espacios normales, espacios duros `\u00a0` y puntos `.` (los puntos se
   toleran porque aparecen en exportaciones legadas; no son parte del estándar).
3. Convertir `x` minúscula a `X` mayúscula.
4. No eliminar otros caracteres: si quedan letras, símbolos o paréntesis, el ISBN es inválido.
5. Un valor vacío, `None` o placeholder (`"-"`, `"sin dato"`, etc.) es inválido.

La normalización **no** debe confundirse con la validación: `normalize_isbn` puede devolver una
forma válida aunque el DV sea incorrecto en fase 1. La comparación de DV se hace con
`validate_isbn`.

### 3.5 Pseudocódigo

```text
FUNCIÓN isbn_check_digit(isbn):
    limpio ← normalizar(isbn)          # mayúsculas, sin -, espacios, NBSP ni puntos

    SI limpio tiene 13 dígitos:
        cuerpo ← primeros 12 dígitos   # se ignora el DV provisto
    SINO SI limpio tiene 10 caracteres
             Y primeros 9 son dígitos
             Y último ∈ {0..9, X}:
        cuerpo ← primeros 9 caracteres
    SINO:
        cuerpo ← limpio

    SI cuerpo tiene 12 dígitos:
        S ← suma(d_i * (1 si posición par, 3 si posición impar))
        RETORNAR texto((10 - (S mod 10)) mod 10)

    SI cuerpo tiene 9 dígitos:
        S ← suma(d_i * (10 - posición))   # posición 0..8
        r ← (11 - (S mod 11)) mod 11
        SI r = 10: RETORNAR "X"
        RETORNAR texto(r)

    RETORNAR None


FUNCIÓN validate_isbn(isbn, allow_isbn10 = True):
    limpio ← normalizar(isbn)

    SI limpio tiene 13 dígitos:
        SI prefijo(limpio, 3) ∉ {"978", "979"}: RETORNAR False
        RETORNAR isbn_check_digit(limpio) = último_dígito(limpio)

    SI allow_isbn10 Y limpio tiene 10 caracteres:
        SI primeros 9 no son dígitos: RETORNAR False
        SI último ∉ {0..9, X}: RETORNAR False
        RETORNAR isbn_check_digit(limpio) = último_caracter(limpio)

    RETORNAR False
```

### 3.6 Implementación de referencia en Python

Propuesta para `etl/validators/isbn.py` (**a crear**). Mantiene el contrato del repo: ante un
valor que no se puede interpretar devuelve `None` (en `isbn_check_digit`) o `False` (en
`validate_isbn`); nunca lanza excepción por dato inválido.

```python
from __future__ import annotations

import re

_SEPARADORES = re.compile(r"[\s\-\u00a0\.]")
_ISBN13_CUERPO = re.compile(r"^[0-9]{12}$")
_ISBN10_CUERPO = re.compile(r"^[0-9]{9}$")


def _limpiar(isbn: str | None) -> str:
    """Quita separadores y normaliza la X; no valida forma ni DV."""
    if isbn is None:
        return ""
    return _SEPARADORES.sub("", str(isbn)).upper()


def isbn_check_digit(isbn: str) -> str | None:
    """Calcula el DV de un ISBN-13/10.

    Acepta el cuerpo (12 dígitos para ISBN-13 o 9 para ISBN-10) o el ISBN
    completo. Si recibe el ISBN completo, ignora el DV provisto y devuelve el
    DV que corresponde. Devuelve None si la forma no permite calcularlo.
    """
    limpio = _limpiar(isbn)

    if len(limpio) == 13 and limpio.isdigit():
        cuerpo = limpio[:12]
    elif (
        len(limpio) == 10
        and limpio[:9].isdigit()
        and limpio[9] in "0123456789X"
    ):
        cuerpo = limpio[:9]
    else:
        cuerpo = limpio

    if _ISBN13_CUERPO.fullmatch(cuerpo):
        suma = sum(
            int(digito) * (1 if posicion % 2 == 0 else 3)
            for posicion, digito in enumerate(cuerpo)
        )
        return str((10 - suma % 10) % 10)

    if _ISBN10_CUERPO.fullmatch(cuerpo):
        suma = sum(
            int(digito) * (10 - posicion)
            for posicion, digito in enumerate(cuerpo)
        )
        resto = (11 - suma % 11) % 11
        return "X" if resto == 10 else str(resto)

    return None


def validate_isbn(isbn: str, *, allow_isbn10: bool = True) -> bool:
    """Valida forma, prefijo y dígito verificador de un ISBN."""
    limpio = _limpiar(isbn)

    if len(limpio) == 13 and limpio.isdigit():
        if limpio[:3] not in ("978", "979"):
            return False
        return isbn_check_digit(limpio) == limpio[12]

    if allow_isbn10 and len(limpio) == 10:
        if not limpio[:9].isdigit():
            return False
        if limpio[9] not in "0123456789X":
            return False
        return isbn_check_digit(limpio) == limpio[9]

    return False
```

Notas de integración:

- `normalize_isbn` del pipeline puede reutilizar `_limpiar` y su regex de forma, pero **no debe
  duplicar el algoritmo de DV**. La fuente única es `etl/validators/isbn.py` (**a crear**).
- En fase 1, `normalize_isbn` sigue devolviendo el ISBN normalizado aunque el DV sea inválido.
  `validate_isbn` se usa solo para generar warning/cuarentena.
- En fase 2, `normalize_isbn` devuelve `None` cuando `validate_isbn` es `False`, manteniendo el
  contrato `str | None`. El motivo debe distinguir “forma inválida” de “DV inválido”; para eso se
  propone un helper `classify_isbn` (**a crear**) que devuelva `(normalizado, motivo)`.
- `isbn_check_digit` acepta ISBN completo o cuerpo. Por eso `isbn_check_digit("9780306406157")`
  devuelve `"7"` y `isbn_check_digit("978030640615")` también devuelve `"7"`; la diferencia de
  largo la exige `validate_isbn`.

#### Alineación con `etl/validators/`

- `etl/validators/common.py`: se reutiliza `is_placeholder` para vacíos y placeholders; no se
  cambia su firma.
- `etl/validators/numeros.py`: no se toca; ISBN no es un monto CLP ni una cantidad.
- `etl/validators/fechas.py` y `personas.py`: no se tocan; no aplican al ISBN.
- `normalize_isbn` puede moverse desde `etl/pipelines/stock.py` a `etl/validators/isbn.py`
  conservando **exactamente** la firma `normalize_isbn(value: Any) -> str | None` y el contrato
  de devolver `None` ante forma inválida. El pipeline la importa desde `etl.validators`.
- No se cambian las firmas de `parse_non_negative_bigint`, `parse_non_negative_bigint_or_zero`
  ni de los validadores de RUT/fechas. La regla ISBN es un módulo nuevo.
- En fase 2, `normalize_isbn` sigue devolviendo `str | None`; el rechazo por DV se expresa como
  `None` y el motivo se persiste en cuarentena, no como excepción.

### 3.7 Vectores de prueba

Todos los valores son ficticios o de dominio público; no contienen PII. La columna
`isbn_check_digit` muestra lo que devuelve la función de referencia.

#### Válidos

| ISBN | Tipo | Cuerpo | `S` | DV esperado | `validate_isbn` |
|---|---|---|---:|---:|---|
| `9780306406157` | ISBN-13 | `978030640615` | 93 | `7` | `True` |
| `9783161484100` | ISBN-13 | `978316148410` | 100 | `0` | `True` |
| `9780132350884` | ISBN-13 | `978013235088` | 96 | `4` | `True` |
| `9780131103627` | ISBN-13 | `978013110362` | 73 | `7` | `True` |
| `9789561234567` | ISBN-13 | `978956123456` | 133 | `7` | `True` |
| `9788478886449` | ISBN-13 | `978847888644` | 161 | `9` | `True` |
| `0306406152` | ISBN-10 | `030640615` | 130 | `2` | `True` |
| `080442957X` | ISBN-10 | `080442957` | 199 | `X` | `True` |

> `9789561234567` es el ISBN sintético que ya usa `tests/test_stock_ventas_transform.py`; se
> incluye para que el vector del repo quede verificado.

#### Inválidos

| Valor | Motivo | `isbn_check_digit` | `validate_isbn` |
|---|---|---|---|
| `9780306406158` | Último dígito cambiado: corresponde `7`, no `8` | `"7"` | `False` |
| `978030640615` | Largo incorrecto (12 dígitos, falta el DV) | `"7"` (se interpreta como cuerpo) | `False` |
| `97803064061A7` | Contiene letras | `None` | `False` |
| `""` | Vacío | `None` | `False` |
| `None` | Nulo | `None` | `False` |
| `0X06406152` | `X` en posición distinta de la última | `None` | `False` |
| `4006381333931` | EAN-13 válido, pero prefijo `400` no es ISBN | `"1"` | `False` |
| `9788478886446` | DV no coincide: corresponde `9`, no `6` | `"9"` | `False` |
| `0306406153` | ISBN-10 con DV incorrecto: corresponde `2`, no `3` | `"2"` | `False` |

> **Corrección de un vector de entrada:** `9788478886446` fue propuesto como válido, pero su
> suma es `161`, `161 mod 10 = 1` y el DV correcto es `9`; el valor termina en `6`, por lo que
> **no cumple** el dígito verificador. Se documenta como inválido. La variante válida más cercana
> es `9788478886449`. Si el objetivo era un ISBN real, hay que confirmar la fuente del dato: el
> algoritmo no admite excepciones.
>
> Además, con `allow_isbn10=False`, los ISBN-10 válidos (`0306406152`, `080442957X`) devuelven
> `False`; los ISBN-13 siguen funcionando.

### 3.8 Impacto en los fixtures y política de adopción

#### Situación actual

`_generar_isbn` en `tests/fixtures/build_fixtures.py` genera `"978" + 10 dígitos aleatorios`.
No calcula el DV. Como el DV correcto es 1 de cada 10 combinaciones, **alrededor del 90 %** de
los ISBN generados no cumple. Con el contrato actual (solo forma), esos valores pasan; con un
rechazo por DV, la mayor parte del lote de 100 filas se perdería.

El test actual espera 82 válidas y 18 rechazadas (8 ISBN vacíos + 10 stock total negativo). Si
se activa el rechazo por DV sin regenerar fixtures, ese contrato deja de tener sentido y las
pruebas de pipeline se vuelven frágiles o vacías.

#### Política de adopción en dos fases

**Fase 1 — warning + cuarentena (no rompe el contrato actual)**

- Se agrega `etl/validators/isbn.py` con `isbn_check_digit` y `validate_isbn`.
- `transform_row` calcula `validate_isbn(isbn)` y, si es `False`, **acepta la fila** pero agrega
  un warning con regla `ISBN-004` (código de cuarentena `ISBN_DV_INVALIDO`) y escribe una fila
  de cuarentena con `severidad = 'warning'`.
- `normalize_isbn` no cambia su contrato: sigue devolviendo el ISBN normalizado por forma.
- Métrica nueva: `isbn_dv_invalidos` y `tasa_isbn_dv_invalidos`.
- Flag propuesto: `--isbn-check=warning|error` con default `warning` (**a crear**).
- Los fixtures actuales pueden seguir usándose para probar la fase 1; se espera una tasa alta de
  warnings, no de rechazos.
- La firma de `transform`/`transform_row` puede necesitar una lista de warnings o un objeto de
  resultado. Ese cambio se coordina con `data-testing-engineer`; **no** se cambia la firma de
  `normalize_isbn` (`value: Any -> str | None`).

**Fase 2 — rechazo controlado**

- Se regeneran los fixtures con DV válido (ver abajo) y se actualizan los conteos esperados.
- `normalize_isbn` devuelve `None` cuando el DV no coincide; el motivo es
  `"ISBN inválido: dígito verificador no coincide"`.
- La fila se rechaza y se escribe en cuarentena con `severidad = 'error'`.
- El flag `--isbn-check=error` pasa a ser el default solo cuando la medición de fase 1 en datos
  reales muestre una tasa de DV inválido menor al umbral acordado (propuesta: 1 %).
- Si la tasa real supera el umbral, **no se activa** la fase 2: se investiga el origen (POS,
  exportación, digitación) y se corrige el dato.

#### Regeneración de fixtures

Dueño: `data-testing-engineer`. Revisión: `data-quality-steward`.

- `_generar_isbn` debe generar 9 dígitos aleatorios después de `978`, calcular el DV con
  `isbn_check_digit` (importado, sin duplicar el algoritmo) y formatear el ISBN completo.
- Reservar filas para casos inválidos, sin solaparlas con las filas que ya prueban PVP o stock:
  - `STOCK_ISBN_VACIO` (existentes, 8): ISBN vacío.
  - `STOCK_ISBN_DV_INVALIDO` (nuevas, 3): ISBN con forma correcta y DV cambiado.
  - `STOCK_ISBN_PREFIJO_INVALIDO` (nueva, 1): EAN-13 válido `4006381333931`.
  - `STOCK_ISBN_LARGO_INVALIDO` (nueva, 1): 12 dígitos.
- Las filas reservadas deben tener PVP y stock válidos para que el **único** motivo de rechazo
  sea el ISBN; así cada regla se prueba aislada.
- `_verificar_invariantes` debe comprobar:
  - todos los ISBN no reservados pasan `validate_isbn`;
  - no hay ISBN duplicados en el archivo;
  - los conteos de vacíos, DV inválido, prefijo inválido y largo inválido son los esperados.
- `tests/test_build_fixtures.py` sigue verificando reproducibilidad con la misma semilla.

#### Criterios de aceptación de la política ISBN

- [ ] `etl/validators/isbn.py` existe y está exportado desde `etl/validators/__init__.py`.
- [ ] Los vectores de §3.7 están en `tests/test_validators.py` y pasan.
- [ ] Fase 1: una fila con DV inválido se acepta, genera warning y cuarentena; el pipeline no
      cambia su conteo de filas válidas por DV.
- [ ] Fase 1: `--dry-run` informa `isbn_dv_invalidos` y `tasa_isbn_dv_invalidos`.
- [ ] Fase 2: una fila con DV inválido se rechaza con el mensaje exacto
      `"ISBN inválido: dígito verificador no coincide"`.
- [ ] Fase 2: los fixtures regenerados tienen 100 % de ISBN no reservados con DV válido.
- [ ] Cada regla ISBN tiene al menos un caso que la dispara y una fila de cuarentena asociada.
- [ ] No se usa ningún RUT, email, teléfono ni nombre real en los vectores ni en los fixtures.

### 3.9 Reglas ISBN y mensajes

| ID | Entidad | Columna | Condición | Severidad | Alcance | Mensaje en español |
|---|---|---|---|---|---|---|
| `ISBN-001` | stock | `isbn` | Vacío, `None` o placeholder | `error` | fila | `"ISBN inválido o vacío"` |
| `ISBN-002` | stock | `isbn` | No es 13 dígitos ni ISBN-10 de 9 dígitos + DV | `error` | fila | `"ISBN inválido: formato no reconocido"` |
| `ISBN-003` | stock | `isbn` | ISBN-13 con prefijo distinto de `978`/`979` | `error` | fila | `"ISBN inválido: prefijo distinto de 978/979"` |
| `ISBN-004` | stock | `isbn` | DV no coincide con el calculado | `warning` fase 1 / `error` fase 2 | fila | `"ISBN inválido: dígito verificador no coincide"` |
| `ISBN-005` | stock | `isbn` | ISBN repetido en el mismo archivo | `warning` | fila | `"ISBN duplicado en el archivo: se conserva la última fila"` |
| `ISBN-006` | stock | `isbn` | ISBN-10 cuando `allow_isbn10=False` | `error` | fila | `"ISBN inválido: ISBN-10 no permitido en esta carga"` |

> En fase 1, `ISBN-004` no rechaza la fila: la severidad es `warning` y solo registra. En fase 2
> pasa a `error` y rechaza la fila. El código de regla no cambia; cambia la severidad efectiva.

---

## 4. Regla de stock obsoleto

### 4.1 Definiciones precisas

| Término | Definición operativa |
|---|---|
| **Carga exitosa** | El archivo se leyó, tenía las columnas requeridas, al menos una fila válida y la transacción de escritura terminó con `COMMIT`. Una carga fallida, vacía o sin filas válidas **no** es exitosa. |
| **Carga apta para marcar ausencias** | Carga exitosa que además cumple los umbrales de §4.6 y no está en modo baseline. Solo estas cargas incrementan ausencias. |
| **Vigente** | El ISBN apareció en la última carga apta. `estado = 'VIGENTE'`, `ausencias_consecutivas = 0`, `faltante_desde = NULL`. |
| **Ausente** | El ISBN no apareció en la última carga apta, pero la racha de ausencias es menor que `N`. `estado = 'AUSENTE'`, `1 <= ausencias_consecutivas < N`, `faltante_desde` no nulo. |
| **Obsoleto** | El ISBN acumuló `N` o más ausencias en cargas aptas. `estado = 'OBSOLETO'`, `ausencias_consecutivas >= N`, `faltante_desde` no nulo. La fila **sigue existiendo**. |
| **Ausencia** | Observación de que un ISBN no apareció en una carga apta. No es un borrado ni implica stock 0. |
| **Última vez visto** | `ultima_vez_visto`: timestamp de la última carga apta en que el ISBN apareció. |
| **Faltante desde** | `faltante_desde`: timestamp de la primera carga apta de la racha de ausencias actual. |
| **Carga no apta** | Carga fallida, vacía, con tasa de rechazo alta, con caída de volumen o en modo baseline. No incrementa ni reinicia contadores. |
| **N** | Número de ausencias consecutivas para declarar obsoleto. Default `2`. Se recomienda `N >= 2`. |

### 4.2 Máquina de estados

```text
                 aparece en carga apta
   ┌──────────────────────────────────────────────┐
   │                                              ▼
┌─────────┐   no aparece, 1 <= ausencias < N   ┌─────────┐
│ VIGENTE │ ─────────────────────────────────► │ AUSENTE │
└─────────┘                                    └─────────┘
   ▲                                              │
   │ aparece en carga apta                        │ no aparece,
   │ (reset de contador y nueva versión)          │ ausencias >= N
   │                                              ▼
   │                                          ┌──────────┐
   └──────────────────────────────────────────│ OBSOLETO │
        aparece en carga apta                 └──────────┘
        (reset de contador y nueva versión)         │
                                                    │ sigue ausente
                                                    ▼
                                              sigue OBSOLETO
                                              (contador sigue subiendo)
```

| Estado actual | Aparece en carga apta | No aparece; contador + 1 < N | No aparece; contador + 1 >= N | Carga no apta |
|---|---|---|---|---|
| `VIGENTE` | `VIGENTE`, contador 0, `faltante_desde = NULL` | `AUSENTE`, contador + 1, fija `faltante_desde` | `OBSOLETO`, contador + 1 | Sin cambios |
| `AUSENTE` | `VIGENTE`, contador 0, `faltante_desde = NULL` | `AUSENTE`, contador + 1 | `OBSOLETO`, contador + 1 | Sin cambios |
| `OBSOLETO` | `VIGENTE`, contador 0, `faltante_desde = NULL` | `OBSOLETO`, contador + 1 | `OBSOLETO`, contador + 1 | Sin cambios |

Notas:

- Un ISBN `OBSOLETO` que sigue ausente **no** vuelve a `AUSENTE`; sigue obsoleto y el contador
  aumenta para dimensionar la racha. No se generan versiones nuevas por el solo incremento del
  contador.
- Un ISBN `OBSOLETO` que reaparece vuelve a `VIGENTE` y abre una nueva versión SCD2.
- El estado no reemplaza al historial: `stock_historial` conserva las versiones anteriores.
- La columna “Carga no apta” significa “sin cambios **por ausencia**”: las filas que sí aparecen
  en el archivo se actualizan a `VIGENTE`; las que no aparecen no se tocan.

### 4.3 Campos propuestos en `silver.stock` (a crear)

| Campo | Tipo PostgreSQL | Nulo | Valor inicial / migración | Propósito |
|---|---|---|---|---|
| `estado` | `TEXT` | No | `'VIGENTE'` | Estado del ciclo de vida |
| `ausencias_consecutivas` | `INTEGER` | No | `0` | Racha de ausencias en cargas aptas |
| `faltante_desde` | `TIMESTAMPTZ` | Sí | `NULL` | Inicio de la racha actual |
| `ultima_vez_visto` | `TIMESTAMPTZ` | No | `updated_at` en migración | Última carga apta con presencia |
| `ultima_carga_exitosa` | `TIMESTAMPTZ` | No | `updated_at` en migración | Última carga exitosa que tocó la fila; en ausentes solo si fue apta |
| `_carga_id` | `UUID` | Sí | `NULL` en migración | Linaje con `stock_cargas` y cuarentena |

Restricciones propuestas:

```sql
CONSTRAINT stock_estado_dominio
    CHECK (estado IN ('VIGENTE', 'AUSENTE', 'OBSOLETO')),

CONSTRAINT stock_ausencias_no_negativas
    CHECK (ausencias_consecutivas >= 0),

CONSTRAINT stock_estado_coherente
    CHECK (
        (estado = 'VIGENTE'
         AND ausencias_consecutivas = 0
         AND faltante_desde IS NULL)
        OR
        (estado IN ('AUSENTE', 'OBSOLETO')
         AND ausencias_consecutivas > 0
         AND faltante_desde IS NOT NULL)
    ),

CONSTRAINT stock_ultima_vez_visto_coherente
    CHECK (ultima_vez_visto <= ultima_carga_exitosa)
```

> El DDL actual no tiene estas columnas. Agregarlas es **a crear** y requiere migración
> (`ALTER TABLE ... ADD COLUMN ...` con `DEFAULT` y backfill). No se debe recrear la tabla ni
> perder datos.

### 4.4 `silver.stock_historial` y SCD2

Cambios propuestos (**a crear**):

- Agregar `estado TEXT NOT NULL` al historial, con el mismo dominio que `stock.estado`.
- Agregar `_carga_id UUID` para linaje.
- Incluir `estado` en `HISTORY_COMPARE_COLUMNS` del pipeline. Hoy la comparación no lo incluye,
  así que un cambio de estado no cerraría la versión.
- Cerrar la versión vigente y abrir una nueva cuando:
  1. cambian datos de negocio (`pvp`, stocks, ventas, textos) — comportamiento actual;
  2. cambia `estado` (incluye `VIGENTE → AUSENTE`, `AUSENTE → OBSOLETO`,
     `OBSOLETO → VIGENTE`).
- **No** abrir versión nueva por el solo incremento de `ausencias_consecutivas` si el estado no
  cambió. Ese contador es operativo, no una versión de negocio.
- Al pasar a `OBSOLETO`, el cierre de la versión es obligatorio: `valid_to = loaded_at`,
  `is_current = FALSE`, y se inserta la versión `OBSOLETO` con `is_current = TRUE`.
- Al reaparecer un `OBSOLETO`, se cierra la versión `OBSOLETO` y se abre una `VIGENTE` con los
  datos de la nueva carga.
- Se mantiene el índice único parcial `stock_historial_isbn_actual_uidx` para garantizar una
  sola versión vigente por ISBN.
- La FK `stock_historial.isbn → stock.isbn` sigue vigente: como nunca se borra de `stock`, el
  historial no queda huérfano.
- Migración: agregar `estado` con `DEFAULT 'VIGENTE'` para las filas existentes y luego backfill
  desde `stock.estado`; no recrear la tabla. `_carga_id` puede quedar `NULL` en las versiones
  históricas.

### 4.5 Política de no borrado y valores al vencer

**Decisión: nunca borrar y conservar el último stock conocido. No poner `stock_* = 0`.**

Al pasar a `OBSOLETO`:

- Se conservan `pvp`, `stock_consignacion`, `stock_propio`, `stock_total`, `ventas`, `titulo`,
  `autor`, `editorial` y `clasificacion`.
- Se actualizan `estado`, `ausencias_consecutivas`, `faltante_desde`, `ultima_carga_exitosa` y
  `_carga_id`.
- **No** se ejecuta `DELETE` ni `TRUNCATE`.
- **No** se modifican los stock a 0.

Justificación:

1. **Un archivo que no trae un ISBN no dice que su stock sea 0.** Puede ser una exportación
   parcial, un filtro del POS, un título temporalmente fuera de catálogo o un error de archivo.
   Poner 0 convierte “no observado” en “sin unidades”, que es un dato inventado.
2. **Quiebre:** si se pusieran los stock en 0, todos los obsoletos aparecerían como quiebre y la
   tasa de quiebre se dispararía artificialmente. La definición correcta es:
   - **quiebre operativo:** `estado = 'VIGENTE' AND stock_total = 0` (observado en 0 en la
     última carga);
   - **no observado:** `estado IN ('AUSENTE', 'OBSOLETO')`, que se reporta aparte y no se suma
     como quiebre.
3. **Rotación:** el denominador de rotación debe usar stock de ISBN `VIGENTE` en el período. Un
   obsoleto no debe entrar como stock disponible ni como stock 0; se excluye del cálculo y se
   reporta como “inventario no observado”.
4. **Historial y joins:** conservar la fila permite que las ventas históricas sigan resolviendo
   título, autor y editorial. Borrar rompería la trazabilidad y los análisis por producto.
5. **Precio:** el `pvp` de un obsoleto es el último precio conocido; no es un precio vigente.
   Los análisis de precios actuales deben filtrar `estado = 'VIGENTE'` o usar
   `ultima_vez_visto` para medir antigüedad.

Artefactos de consumo propuestos (**a crear**):

```sql
-- Vista de stock realmente vigente para KPIs de disponibilidad.
CREATE OR REPLACE VIEW {{schema}}.stock_vigente AS
SELECT *
FROM {{schema}}.stock
WHERE estado = 'VIGENTE';
```

- Los marts de inventario, quiebre y rotación deben usar `stock_vigente`.
- Reportar por separado:
  - `unidades_vigentes = SUM(stock_total) WHERE estado = 'VIGENTE'`;
  - `unidades_ultimo_stock_obsoleto = SUM(stock_total) WHERE estado = 'OBSOLETO'`, etiquetado
    como “último stock conocido de obsoletos”, no como inventario actual.

### 4.6 Cargas exitosas, aptas y sospechosas

#### Carga exitosa

- Archivo existente, legible y con columnas requeridas.
- Al menos 1 fila de datos y al menos 1 fila válida.
- Transacción confirmada.
- No es exitosa si: el archivo no existe, no se puede leer, faltan columnas, hay 0 filas, hay 0
  filas válidas o la transacción falla.

#### Carga apta para marcar ausencias

Además de exitosa, debe cumplir:

| Condición | Umbral propuesto | Motivo |
|---|---|---|
| Tasa de rechazo | `<= 10 %` | Un archivo con muchas filas malas puede estar incompleto o corrupto |
| Tasa de ISBN con DV inválido | `<= 1 %` en fase 2 | Un DV malo puede ser un ISBN existente mal digitado; no se puede proteger su clave |
| Volumen vs. mediana de las últimas 4 cargas exitosas | `>= 70 %` | Detecta exportaciones truncadas |
| Filas válidas | `>= 1` | Un lote sin filas válidas no observa nada |
| Modo baseline | `false` | La primera carga no puede declarar obsoletos |
| Errores de esquema/DB | Ninguno | Sin integridad no hay observación confiable |

Si no cumple, la carga puede publicar sus filas válidas, pero:

- `marca_ausencias = false`;
- no se incrementan ni reinician los contadores de ausencia de las filas ausentes; una fila
  presente sí vuelve a `VIGENTE` porque su presencia es evidencia válida;
- se registra `motivo_no_marca`;
- el contrato queda en `FAIL` y se emite `WARNING`;
- se revisa manualmente antes de la próxima carga.

> **Decisión:** una carga no apta **no** rompe la racha por sí misma: se considera una
> “oportunidad de observación no confiable”. La racha sigue contando solo cargas aptas. El riesgo
> residual es que una carga no apta contenga un ISBN que sí estaba presente; por eso `N >= 2` y
> la revisión de las cargas sospechosas son obligatorias.

#### Filas rechazadas por calidad

- Si la fila tiene un ISBN normalizado válido pero falla otra regla (PVP, stock negativo, total
  inconsistente), el ISBN se agrega al conjunto `isbn_protegidos`.
- Un ISBN protegido **no** se marca ausente en esa carga: sabemos que apareció en el archivo,
  aunque su fila no se haya podido cargar. Se conserva la última versión buena.
- Si el ISBN de la fila es inválido o vacío, no se puede proteger ninguna clave; se cuenta en
  `isbn_no_protegidos`. Si la tasa de ISBN inválidos no vacíos supera el umbral, la carga no es
  apta.
- Los ISBN protegidos se informan en `stock_cargas.isbn_protegidos` y en cuarentena con regla
  `STOCK-012` (código de cuarentena `STOCK_ISBN_PROTEGIDO`, **a crear**).

### 4.7 Algoritmo de actualización por lote

Pseudocódigo, en el orden exigido: **upsert → marcar ausentes → aplicar umbral → cerrar/abrir
historial**.

```text
ENTRADA: archivo, N, modo_isbn, flags de operación

1. LEER
   - leer con dtype=str
   - verificar columnas requeridas
   - calcular SHA-256 del archivo
   - contar filas leídas

2. TRANSFORMAR
   - por cada fila:
       isbn_forma ← normalizar forma
       si isbn_forma es nulo:
           rechazo ISBN-001/002; isbn_no_protegido += 1
       sino:
           dv_ok ← validate_isbn(isbn_forma)
           si no dv_ok:
               fase 1: warning ISBN-004, fila sigue válida
               fase 2: rechazo ISBN-004, isbn_protegido += 1
           validar PVP, stocks, total, ventas
           si otra regla falla:
               rechazo; isbn_protegido += 1
           si todo bien:
               fila válida
   - deduplicar por ISBN: última fila gana; duplicados += 1
   - observados ← ISBN de filas válidas ∪ ISBN protegidos

3. DECIDIR SI LA CARGA ES APTA
   - si filas_leidas = 0 o filas_válidas = 0:
         abortar; registrar VACIA/FALLIDA; no escribir stock
   - si no hay carga exitosa previa o modo baseline:
         marca_ausencias ← false
   - si tasa_rechazo > 10 %:
         marca_ausencias ← false; motivo ← "tasa de rechazo"
   - si caída de volumen > 30 %:
         marca_ausencias ← false; motivo ← "caída de volumen"
   - si tasa_isbn_dv_invalido > 1 % (fase 2):
         marca_ausencias ← false; motivo ← "ISBN con DV inválido"
   - si todo OK:
         marca_ausencias ← true

4. TRANSACCIÓN
   a. UPSERT de filas válidas en stock:
        estado = 'VIGENTE'
        ausencias_consecutivas = 0
        faltante_desde = NULL
        ultima_vez_visto = loaded_at
        ultima_carga_exitosa = loaded_at
        _carga_id = carga_id
        (resto de columnas de negocio actualizadas como hoy)

   b. MARCAR AUSENTES (solo si marca_ausencias):
        para cada fila de stock cuyo isbn NO esté en observados:
            ausencias_consecutivas = ausencias_consecutivas + 1
            faltante_desde = COALESCE(faltante_desde, loaded_at)
            estado = 'AUSENTE'
            ultima_carga_exitosa = loaded_at
            _carga_id = carga_id
            (NO tocar pvp, stock_*, ventas ni textos)

   c. APLICAR UMBRAL (solo si marca_ausencias):
        para cada fila con estado = 'AUSENTE'
        y ausencias_consecutivas >= N:
            estado = 'OBSOLETO'
            (NO tocar pvp, stock_*, ventas ni textos)

   d. CERRAR/ABRIR HISTORIAL
        - comparar datos de negocio y estado contra la versión vigente
        - cerrar versión si cambió algún dato o el estado
        - insertar nueva versión con valid_from = loaded_at, is_current = true
        - no crear versión si solo cambió ausencias_consecutivas

   e. REGISTRAR MÉTRICAS
        - upsert en stock_cargas con conteos, marca_ausencias, motivo_no_marca,
          n_ausencias, vigentes, ausentes, obsoletos_nuevos, obsoletos_totales,
          contrato_estado, rechazos_por_regla y top_motivos
        - insertar cuarentena de rechazos y warnings

   f. COMMIT

5. LOG RESUMEN
```

Notas de transacción:

- `execute_sql_file` (DDL) se ejecuta antes de la transacción principal, como hoy. El DDL es
  idempotente (`CREATE TABLE IF NOT EXISTS`); la migración de columnas es **a crear**.
- `upsert_table` crea una tabla temporal `ON COMMIT DROP` y hace `INSERT ... ON CONFLICT`. Se
  mantiene el patrón actual.
- La cuarentena de filas rechazadas debe escribirse **en la misma transacción** que el upsert:
  si la carga se confirma, los rechazos quedan registrados; si se revierte, no hay carga ni
  rechazos huérfanos.
- Para cargas fallidas o vacías, se propone una escritura de auditoría en una transacción
  separada (mejor esfuerzo) para dejar `stock_cargas.estado = 'FALLIDA'/'VACIA'`. Si esa
  escritura falla, se loguea el error; nunca se debe perder el registro silenciosamente.
- No se debe ejecutar `DELETE`, `TRUNCATE`, `DROP` ni `ALTER` dentro del pipeline de carga. La
  migración de esquema es un paso aparte y controlado.

### 4.8 Casos borde

| Caso | Comportamiento esperado |
|---|---|
| **ISBN que reaparece** | `estado = 'VIGENTE'`, `ausencias_consecutivas = 0`, `faltante_desde = NULL`, `ultima_vez_visto = loaded_at`; si venía de `AUSENTE` u `OBSOLETO`, se cierra la versión vigente y se abre una nueva `VIGENTE`. |
| **Carga fallida** | Rollback completo. No se modifican contadores ni estados. Se registra `FALLIDA` en auditoría (mejor esfuerzo). |
| **Carga vacía** | Se aborta antes de escribir en `stock`. No se marcan ausencias. Se registra `VACIA`. No se interpreta “archivo vacío” como “todo el catálogo ausente”. |
| **Archivo con 0 filas válidas** | Igual que carga vacía: no se escribe, no se marcan ausencias, contrato `FAIL`. |
| **Filas rechazadas por calidad con ISBN válido** | El ISBN se protege; no incrementa ausencias; se conserva la última versión buena. |
| **Filas con ISBN inválido o vacío** | No se puede proteger una clave; se cuenta `isbn_no_protegidos`. Si la tasa es alta, la carga no es apta y no marca ausencias. |
| **Duplicados en el mismo archivo** | Se deduplica por ISBN; se conserva la última fila; el ISBN cuenta como observado; se registra `duplicados` y un `WARNING`. No se crean dos versiones en la misma corrida. |
| **Duplicado con contenidos distintos** | Última fila gana, como hoy; se advierte y se recomienda cuarentena de la fila descartada (**a crear**). No se resuelve silenciosamente. |
| **Primera carga** | No puede marcar obsoletos. Todas las filas cargadas quedan `VIGENTE`. Si la tabla ya tenía datos por una migración, la primera corrida es baseline (`marca_ausencias = false`). |
| **N = 1** | Permitido solo con flag explícito; se advierte que una sola ausencia puede ser un archivo incompleto. Default y recomendado: `N >= 2`. |
| **Carga no apta** | Publica filas válidas si las hay, pero no incrementa ni reinicia contadores; contrato `FAIL`; revisión manual. |
| **ISBN-10 e ISBN-13 del mismo título** | Hoy son claves distintas; uno puede quedar obsoleto mientras el otro sigue vigente. Es un riesgo de negocio (§7). La canonicalización a ISBN-13 es **a crear** y no forma parte de esta entrega. |
| **ISBN que cambia de formato en el archivo** | Se normaliza antes de comparar (guiones, espacios, NBSP, `X` minúscula). Si el cambio es ISBN-10 ↔ ISBN-13, son claves distintas; requiere catálogo. |
| **Stock observado en 0** | Es un dato válido: `stock_total = 0` con `estado = 'VIGENTE'`. Es quiebre operativo. No confundir con ausencia. |
| **Stock negativo** | Fila rechazada; ISBN protegido; no se marca ausente; se conserva la última versión buena. |

### 4.9 Observabilidad: `stock_cargas`, logs y consultas

#### Tabla `silver.stock_cargas` (a crear)

```sql
CREATE TABLE IF NOT EXISTS {{schema}}.stock_cargas (
    carga_id              UUID PRIMARY KEY,
    archivo               TEXT        NOT NULL,
    sha256                TEXT,
    iniciada_en           TIMESTAMPTZ NOT NULL,
    finalizada_en         TIMESTAMPTZ,
    estado                TEXT        NOT NULL
        CHECK (estado IN (
            'EN_CURSO', 'EXITOSA', 'EXITOSA_SIN_MARCA', 'FALLIDA', 'VACIA'
        )),
    filas_leidas          INTEGER     NOT NULL DEFAULT 0,
    filas_validas         INTEGER     NOT NULL DEFAULT 0,
    filas_rechazadas      INTEGER     NOT NULL DEFAULT 0,
    filas_duplicadas      INTEGER     NOT NULL DEFAULT 0,
    isbn_protegidos       INTEGER     NOT NULL DEFAULT 0,
    isbn_no_protegidos    INTEGER     NOT NULL DEFAULT 0,
    isbn_dv_invalidos     INTEGER     NOT NULL DEFAULT 0,
    marca_ausencias       BOOLEAN     NOT NULL DEFAULT FALSE,
    motivo_no_marca       TEXT,
    n_ausencias           INTEGER     NOT NULL DEFAULT 2,
    vigentes              INTEGER     NOT NULL DEFAULT 0,
    ausentes              INTEGER     NOT NULL DEFAULT 0,
    obsoletos_nuevos      INTEGER     NOT NULL DEFAULT 0,
    obsoletos_totales     INTEGER     NOT NULL DEFAULT 0,
    contrato_estado       TEXT
        CHECK (contrato_estado IN ('PASS', 'WARN', 'FAIL')),
    rechazos_por_regla    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    top_motivos           JSONB       NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS stock_cargas_finalizada_idx
    ON {{schema}}.stock_cargas (finalizada_en DESC);
```

#### Logs por corrida

Formato propuesto, en español y sin PII:

```text
INFO  stock: carga_id=<uuid> archivo=STOCK LIBRERIA.xlsx sha256=<hex>
INFO  stock: leidas=100 validas=82 rechazadas=18 duplicadas=0
      protegidas=0 isbn_dv_invalidos=0
INFO  stock: marca_ausencias=true n=2
INFO  stock: vigentes=82 ausentes=0 obsoletos_nuevos=0 obsoletos_totales=0
WARN  stock: ausencias no marcadas: tasa_rechazo=0.18 > 0.10
WARN  stock: nuevos obsoletos: 3 (isbn=978..., titulo=...)
```

Reglas de logging:

- Resumen de la corrida **siempre** en `INFO`.
- Rechazos y warnings relevantes en `WARNING`, con límite de 20 ejemplos por corrida.
- Detalle completo de valores en `DEBUG`, nunca en `INFO`.
- Nunca loguear RUT, email, teléfono ni dirección. En stock no hay PII; si se agrega contacto de
  proveedor, escalar a `data-governance-steward` antes de exponerlo.
- El mensaje debe permitir responder: qué archivo, qué fila, qué columna, qué regla, qué valor.

#### Consultas SQL de control

Reemplazar `{{schema}}` por `silver` (o por el esquema configurado) al ejecutar en psql.

```sql
-- 1. Estado actual del catálogo y último stock conocido por estado.
SELECT
    estado,
    count(*)                    AS isbn,
    sum(stock_total)            AS unidades_ultimo_stock_conocido
FROM {{schema}}.stock
GROUP BY estado
ORDER BY CASE estado
    WHEN 'VIGENTE'  THEN 1
    WHEN 'AUSENTE'  THEN 2
    ELSE 3
END;
```

```sql
-- 2. Detalle de ausentes y obsoletos, ordenado por racha.
SELECT
    isbn,
    titulo,
    estado,
    ausencias_consecutivas,
    faltante_desde,
    ultima_vez_visto,
    ultima_carga_exitosa,
    stock_total
FROM {{schema}}.stock
WHERE estado <> 'VIGENTE'
ORDER BY ausencias_consecutivas DESC, faltante_desde ASC;
```

```sql
-- 3. Obsoletos nuevos en la última carga exitosa.
WITH ultima AS (
    SELECT max(finalizada_en) AS ts
    FROM {{schema}}.stock_cargas
    WHERE estado = 'EXITOSA'
)
SELECT
    h.isbn,
    h.titulo,
    h.estado,
    h.valid_from
FROM {{schema}}.stock_historial AS h, ultima AS u
WHERE h.estado = 'OBSOLETO'
  AND h.is_current
  AND h.valid_from >= u.ts
ORDER BY h.valid_from DESC, h.isbn;
```

```sql
-- 4. Rechazos de la última carga por regla.
SELECT
    c.regla,
    c.severidad,
    count(*) AS filas
FROM {{schema}}.cuarentena AS c
WHERE c.entidad = 'stock'
  AND c.carga_id = (
      SELECT carga_id
      FROM {{schema}}.stock_cargas
      ORDER BY finalizada_en DESC NULLS LAST
      LIMIT 1
  )
GROUP BY c.regla, c.severidad
ORDER BY filas DESC, c.regla;
```

```sql
-- 5. Frescura del dato.
SELECT
    max(finalizada_en)                          AS ultima_carga,
    now() - max(finalizada_en)                  AS antiguedad_ultima_carga,
    max(finalizada_en) FILTER (
        WHERE estado = 'EXITOSA'
    )                                           AS ultima_carga_apta
FROM {{schema}}.stock_cargas;
```

```sql
-- 6. Control de invariantes de estado (debe devolver 0 filas).
SELECT *
FROM {{schema}}.stock
WHERE (estado = 'VIGENTE'
       AND (ausencias_consecutivas <> 0 OR faltante_desde IS NOT NULL))
   OR (estado <> 'VIGENTE'
       AND (ausencias_consecutivas = 0 OR faltante_desde IS NULL))
   OR ultima_vez_visto > ultima_carga_exitosa;
```

```sql
-- 7. Estado del contrato de la última carga.
SELECT
    carga_id,
    estado,
    contrato_estado,
    marca_ausencias,
    motivo_no_marca,
    filas_leidas,
    filas_validas,
    filas_rechazadas,
    round(
        100.0 * filas_rechazadas / NULLIF(filas_leidas, 0),
        1
    ) AS tasa_rechazo_pct,
    obsoletos_nuevos
FROM {{schema}}.stock_cargas
ORDER BY finalizada_en DESC NULLS LAST
LIMIT 1;
```

### 4.10 Reporte de rechazos legible por negocio

El reporte debe poder leerlo una persona de operaciones, no solo un desarrollador. Ejemplo
sintético (sin datos reales):

```text
============================================================
REPORTE DE CALIDAD — STOCK LIBRERÍA
============================================================
Archivo:            STOCK LIBRERIA.xlsx
SHA-256:            3f2a... (ejemplo)
Carga:              2026-10-28 09:15 UTC
Modo ISBN:          warning (fase 1)

RESUMEN
  Leídas:           100
  Aceptadas:         82
  Rechazadas:        18  (18,0 %)
  Duplicadas:         0
  ISBN protegidos:    0
  ISBN con DV malo:  83  (solo warning en fase 1)

MOTIVOS PRINCIPALES
  1) stock negativo .......................... 10 filas
  2) ISBN inválido o vacío ...................  8 filas

ESTADO DEL CONTRATO: FAIL
  - La tasa de rechazo (18,0 %) supera el máximo de 10,0 %.
  - Las ausencias NO se marcaron para proteger el catálogo.

QUÉ HACER
  1) Revisar las 10 filas con stock negativo en el archivo de origen.
  2) Completar o corregir los 8 ISBN vacíos.
  3) Volver a cargar el archivo corregido.
  4) Si el archivo está incompleto, no se debe usar para declarar obsoletos.

NOTA
  Un ISBN ausente no significa stock 0. Significa que no vino en esta
  exportación. Por eso el stock anterior se conserva y solo se marca el estado.
============================================================
```

El reporte se puede emitir por consola (logging) y, **a crear**, por archivo
(`--report PATH`) o como fila en `stock_cargas`.

### 4.11 Criterios de aceptación y plan de pruebas

#### Para `silver-etl-engineer`

- [ ] El pipeline mantiene `--dry-run`, `--schema-only`, `--input` y `--log-level`.
- [ ] `--dry-run` no abre conexión ni escribe en PostgreSQL; calcula y muestra las métricas
      hipotéticas de estado.
- [ ] La carga nunca ejecuta `DELETE`, `TRUNCATE`, `DROP` ni `ALTER`.
- [ ] El upsert y las actualizaciones de estado ocurren en la misma transacción.
- [ ] La cuarentena de filas rechazadas se escribe en la misma transacción que el upsert.
- [ ] Una carga fallida o vacía no modifica contadores ni estados.
- [ ] Un ISBN protegido por rechazo no incrementa ausencias.
- [ ] Un ISBN que reaparece resetea contador y `faltante_desde`.
- [ ] El historial cierra/abre versión al cambiar `estado`, y no por solo cambiar el contador.
- [ ] `stock_cargas` registra la corrida con `contrato_estado`, `marca_ausencias` y
      `motivo_no_marca`.
- [ ] Logs en español, resumen en `INFO`, detalle en `WARNING`/`DEBUG`.

#### Para `data-testing-engineer`

Pruebas unitarias (sin base de datos):

- [ ] `isbn_check_digit` con los 8 vectores válidos y los 9 inválidos de §3.7.
- [ ] `isbn_check_digit` acepta cuerpo e ISBN completo y devuelve lo mismo.
- [ ] `validate_isbn` con `allow_isbn10=True` y `False`.
- [ ] Normalización: guiones, espacios, NBSP, `X` minúscula y puntos.
- [ ] `normalize_isbn` sigue devolviendo `None` para formas inválidas.
- [ ] Fase 1: fila con DV inválido se acepta y genera warning/cuarentena.
- [ ] Fase 2: fila con DV inválido se rechaza con el mensaje exacto.
- [ ] ISBN-13 con prefijo `400` se rechaza aunque el EAN sea válido.
- [ ] Fila con stock negativo e ISBN válido se rechaza, pero el ISBN queda protegido.
- [ ] Duplicado por ISBN: última fila gana y el ISBN cuenta como observado.

Pruebas de integración (con `TEST_DATABASE_URL`, mismo patrón que
`tests/test_stock_ventas_integration.py`):

- [ ] Primera carga: todas las filas `VIGENTE`, ninguna `OBSOLETO`.
- [ ] Segunda carga sin un ISBN: `AUSENTE`, contador 1, stock intacto, nueva versión de historial.
- [ ] Tercera carga sin el mismo ISBN: `OBSOLETO`, contador 2, versión cerrada/abierta.
- [ ] El ISBN reaparece: `VIGENTE`, contador 0, `faltante_desde = NULL`, nueva versión.
- [ ] Carga fallida: rollback, contadores y estados sin cambios.
- [ ] Archivo vacío: aborta, no marca ausencias, `stock_cargas.estado = 'VACIA'`.
- [ ] Carga sospechosa: publica filas válidas, `marca_ausencias = false`, contadores sin cambios.
- [ ] ISBN protegido por rechazo: no incrementa ausencias.
- [ ] Duplicados: una sola fila en `stock`, una sola versión vigente.
- [ ] Idempotencia: dos cargas idénticas no crean versiones nuevas para los ISBN presentes; los
      ausentes sí incrementan una vez por carga apta.
- [ ] `N=1` requiere flag explícito; `N=0` o negativo se rechaza.
- [ ] Índice único parcial: no puede haber dos versiones `is_current` por ISBN.
- [ ] Invariantes de §2.2 y §4.3 devuelven 0 filas.
- [ ] No se borra ninguna fila: `count(*)` de `stock` no disminuye.

Contrato de esquema:

- [ ] `tests/test_schema_stock_ventas.py` se actualiza con las columnas nuevas y sus tipos.
- [ ] El DDL sigue usando `{{schema}}` y no hardcodea `silver`.
- [ ] Los `CHECK` de estado, contador y coherencia existen.
- [ ] `stock_historial` incluye `estado` y el índice único parcial se mantiene.

#### Comandos de verificación

Comandos **existentes** (no se ejecutaron para esta entrega):

```powershell
.venv\Scripts\python.exe -m etl.pipelines.stock --dry-run --log-level DEBUG
.venv\Scripts\python.exe -m pytest
.venv\Scripts\python.exe -m ruff check .
```

Comandos **a crear** (no existen hoy; no documentarlos como disponibles):

- `.venv\Scripts\python.exe -m etl.pipelines.stock --dry-run --report reporte.txt`
- Script de migración/backfill de columnas de estado.

### 4.12 Reglas de stock y mensajes

| ID | Entidad | Columna | Condición | Severidad | Alcance | Mensaje en español |
|---|---|---|---|---|---|---|
| `STOCK-001` | stock | `stock_consignacion` | `< 0` o no entero | `error` | fila | `"fila rechazada: stock de consignación negativo"` |
| `STOCK-002` | stock | `stock_propio` | `< 0` o no entero | `error` | fila | `"fila rechazada: stock propio negativo"` |
| `STOCK-003` | stock | `stock_total` | `< 0` o no entero | `error` | fila | `"fila rechazada: stock negativo"` |
| `STOCK-004` | stock | `stock_total` | `stock_total != stock_consignacion + stock_propio` | `error` | fila | `"fila rechazada: stock total inconsistente"` |
| `STOCK-005` | stock | `pvp` | No entero CLP o `< 0` | `error` | fila | `"fila rechazada: PVP inválido o negativo"` |
| `STOCK-006` | stock | `ventas` | No entero o `< 0` | `error` | fila | `"fila rechazada: ventas inválidas"` |
| `STOCK-007` | stock | `estado` | Fuera de `VIGENTE/AUSENTE/OBSOLETO` | `error` | fila | `"estado de stock inválido"` |
| `STOCK-008` | stock | `ausencias_consecutivas` | `< 0` | `error` | fila | `"contador de ausencias inválido"` |
| `STOCK-009` | stock | `estado`, `ausencias_consecutivas`, `faltante_desde` | Combinación incoherente | `error` | fila/lote | `"estado de stock inconsistente"` |
| `STOCK-010` | stock | — | Primera carga intenta marcar obsoletos | `error` | lote | `"carga rechazada: no se pueden marcar obsoletos en la primera carga"` |
| `STOCK-011` | stock | — | Archivo vacío o sin filas válidas | `error` | lote | `"carga rechazada: archivo vacío o sin filas de datos"` |
| `STOCK-012` | stock | `isbn` | Fila rechazada con ISBN válido | `warning` | fila/lote | `"ISBN protegido: fila rechazada no cuenta como ausencia"` |
| `STOCK-013` | stock | — | Tasa de rechazo, caída de volumen o DV inválido por encima del umbral | `warning` | lote | `"ausencias no marcadas: carga sospechosa por tasa de rechazo o caída de volumen"` |
| `STOCK-014` | stock | `isbn` | ISBN duplicado en el archivo | `warning` | fila | `"ISBN duplicado en el archivo: se conserva la última fila"` |
| `STOCK-015` | stock | `ultima_vez_visto`, `ultima_carga_exitosa` | `ultima_vez_visto > ultima_carga_exitosa` | `error` | fila/lote | `"fechas de seguimiento inconsistentes"` |
| `STOCK-016` | stock | — | Carga no apta intenta modificar contadores | `error` | lote | `"carga no apta: no se deben marcar ausencias"` |
| `STOCK-017` | stock | — | Intento de borrado/truncado de stock | `error` | lote | `"operación destructiva prohibida sobre stock"` |
| `STOCK-018` | stock | `estado` | `OBSOLETO` sin `faltante_desde` | `error` | fila | `"estado obsoleto sin fecha de faltante"` |
| `STOCK-019` | stock | `estado` | `OBSOLETO` con stock negativo o total inconsistente | `error` | fila | `"estado obsoleto con stock inválido"` |
| `STOCK-020` | stock | — | `N < 2` sin flag explícito | `warning` | lote | `"umbral de obsolescencia menor a 2: una sola ausencia puede ser un archivo incompleto"` |

---

## 5. Cuarentena y métricas de calidad

### 5.1 Tabla de cuarentena (a crear)

Se propone una tabla genérica reutilizable por todas las entidades:

```sql
CREATE TABLE IF NOT EXISTS {{schema}}.cuarentena (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entidad         TEXT        NOT NULL,
    archivo_origen  TEXT        NOT NULL,
    sha256_origen   TEXT,
    carga_id        UUID,
    fila_numero     INTEGER     NOT NULL,
    columna         TEXT,
    valor_crudo     TEXT,
    regla           TEXT        NOT NULL,
    severidad       TEXT        NOT NULL CHECK (severidad IN ('warning', 'error')),
    motivo          TEXT        NOT NULL,
    ts_utc          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cuarentena_carga_idx
    ON {{schema}}.cuarentena (carga_id, regla);

CREATE INDEX IF NOT EXISTS cuarentena_entidad_idx
    ON {{schema}}.cuarentena (entidad, ts_utc DESC);
```

Reglas:

- `fila_numero` es el número de fila de la hoja, contando el encabezado como fila 1. La primera
  fila de datos es la 2.
- `valor_crudo` guarda el valor original de la columna que falló. En stock no hay PII; si en el
  futuro se agrega contacto de proveedor, se debe enmascarar antes de escribir.
- `sha256_origen` se calcula sobre el archivo leído. Si el archivo no se puede leer, se registra
  igualmente la carga como `FALLIDA` con `sha256_origen = NULL`; no se inventa un hash. Lo mismo
  aplica a `stock_cargas.sha256`.
- Para errores de lote (archivo vacío, esquema inválido) se usa `fila_numero = 0` y
  `columna = NULL`.
- La cuarentena de filas rechazadas se escribe en la misma transacción que el upsert.
- Un `warning` no bloquea la fila, pero se cuenta y se reporta. Un `error` rechaza la fila y, si
  aplica al lote, lo marca como `FAIL`.
- **Nunca** se descarta una fila sin una fila de cuarentena o un contador de lote que la
  explique.

### 5.2 Métricas por corrida

| Métrica | Fórmula / fuente | Uso |
|---|---|---|
| `filas_leidas` | Filas de datos del archivo | Volumen |
| `filas_aceptadas` | Filas válidas únicas | Cobertura |
| `filas_rechazadas` | Filas con al menos un `error` | Calidad |
| `tasa_rechazo` | `rechazadas / leidas` | Contrato |
| `rechazos_por_regla` | `COUNT(*) GROUP BY regla` | Priorización |
| `top_motivos` | Top 5 de motivos | Reporte |
| `filas_duplicadas` | Duplicados por ISBN | Unicidad |
| `isbn_protegidos` | Rechazos con ISBN válido | Protección de ausencias |
| `isbn_no_protegidos` | Rechazos con ISBN inválido/vacío | Riesgo |
| `isbn_dv_invalidos` | Warning/error `ISBN-004` | Calidad ISBN |
| `vigentes` | `COUNT(*) WHERE estado='VIGENTE'` | Catálogo activo |
| `ausentes` | `COUNT(*) WHERE estado='AUSENTE'` | Alerta temprana |
| `obsoletos_nuevos` | Versiones que pasaron a `OBSOLETO` en la carga | Cambio |
| `obsoletos_totales` | `COUNT(*) WHERE estado='OBSOLETO'` | Acumulado |
| `frescura_horas` | `now() - max(finalizada_en)` | Oportunidad |
| `antiguedad_archivo_horas` | `now() - mtime` | Oportunidad |
| `contrato_estado` | `PASS` / `WARN` / `FAIL` | Publicación |

### 5.3 Estado del contrato

| Estado | Condición | Acción |
|---|---|---|
| `PASS` | Carga exitosa, al menos 1 fila válida, tasa de rechazo `<= 10 %`, sin errores de lote, `marca_ausencias` según corresponda | Publicar y monitorear |
| `WARN` | Carga exitosa con warnings (DV ISBN en fase 1, duplicados, `N < 2`, frescura alta) | Publicar; revisar warnings |
| `FAIL` | Archivo vacío, 0 filas válidas, error de lectura/esquema/DB, tasa de rechazo `> 10 %`, o intento de marcar obsoletos en primera carga | No marcar ausencias; alertar; revisar antes de la próxima carga |

> En esta propuesta, un lote `FAIL` por tasa de rechazo puede publicar sus filas válidas, pero
> **no** marca ausencias y requiere revisión. La retención del lote completo en staging y la
> decisión de no publicar quedan como **a crear** y deben acordarse con negocio; el código actual
> no las implementa.

### 5.4 Reporte de rechazos por regla

```sql
SELECT
    regla,
    severidad,
    count(*) AS filas,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS porcentaje
FROM {{schema}}.cuarentena
WHERE entidad = 'stock'
  AND carga_id = %(carga_id)s
GROUP BY regla, severidad
ORDER BY filas DESC, regla;
```

```sql
SELECT
    fila_numero,
    columna,
    valor_crudo,
    regla,
    motivo
FROM {{schema}}.cuarentena
WHERE entidad = 'stock'
  AND carga_id = %(carga_id)s
ORDER BY fila_numero, columna
LIMIT 100;
```

---

## 6. Plan de adopción por fases

| Fase | Qué se hace | Dueño | Criterio de salida |
|---|---|---|---|
| **0. Acuerdo** | Validar este documento, umbrales (`N=2`, 10 %, 1 %, 30 %) y política de no borrado | `data-quality-steward` + negocio | Documento aprobado; sin cambios de código |
| **1. ISBN warning** | Crear `etl/validators/isbn.py`; warning + cuarentena; métricas; flag `--isbn-check=warning` | `silver-etl-engineer` | Vectores verdes; `--dry-run` informa `isbn_dv_invalidos`; no cambia el conteo de válidas |
| **1b. Fixtures** | Regenerar fixtures con DV válido y filas inválidas reservadas; actualizar tests | `data-testing-engineer` | Invariantes de fixtures verdes; cada regla tiene caso |
| **2. ISBN error** | Activar `--isbn-check=error` cuando la tasa real sea `<= 1 %`; rechazo + cuarentena | `silver-etl-engineer` | Tasa medida y aprobada; tests de rechazo verdes |
| **3. Stock lifecycle** | Agregar columnas, `stock_cargas`, `stock_historial.estado`, vista `stock_vigente`; migración y baseline | `silver-etl-engineer` | Migración probada en schema de test; primera carga baseline sin obsoletos |
| **4. Ausencias** | Activar `marca_ausencias` tras una carga baseline; monitorear `AUSENTE`/`OBSOLETO` | `silver-etl-engineer` + operación | Dos cargas aptas consecutivas; revisión de obsoletos nuevos |
| **5. Estabilización** | Ajustar umbrales, reporte legible y consultas de control | `data-quality-steward` | Reporte usable por negocio; sin falsos positivos graves |

---

## 7. Riesgos de negocio

| Riesgo | Impacto | Mitigación |
|---|---|---|
| **Falso positivo por archivo incompleto** | Se marcan obsoletos libros que sí existen; se pierde confianza y se distorsiona el catálogo | `marca_ausencias=false` con caída de volumen o tasa de rechazo; `N>=2`; revisión manual; nunca borrar |
| **Falso positivo por fila rechazada** | Un ISBN presente pero con PVP/stock inválido se marcaría ausente | `isbn_protegidos`: si el ISBN es válido, no cuenta como ausencia |
| **Falso negativo** | Un libro discontinuado tarda `N` cargas en marcarse obsoleto | Aceptable; `N=2` es un balance; el estado `AUSENTE` ya alerta |
| **Inventario sobreestimado** | Si los KPIs suman todo `stock` sin filtrar, los obsoletos conservan stock y parecen disponibles | Vista `stock_vigente`; contrato de marts; tests de KPI; reportar obsoletos aparte |
| **Quiebre distorsionado** | Poner stock en 0 inflaría el quiebre | Decisión de conservar stock; quiebre solo `VIGENTE` con `stock_total = 0`; obsoletos como “no observado” |
| **Precio desactualizado** | Un `pvp` obsoleto se usa para márgenes actuales | Filtrar `VIGENTE` o usar `ultima_vez_visto`; no usar precios de obsoletos para pricing |
| **ISBN-10/ISBN-13 duplicados** | El mismo título tiene dos claves; una puede quedar obsoleta y la otra vigente | Reporte de reconciliación; canonicalización a ISBN-13 **a crear**; no borrar |
| **Rechazo masivo al activar fase 2** | Se pierden filas legítimas con DV mal digitado | Fase 1 mide; umbral 1 %; cuarentena; corrección en origen; no activar si supera el umbral |
| **Fixtures regenerados cambian tests** | Se ocultan regresiones si solo se ajustan números | Semilla fija; invariantes; revisar diff; tests por regla, no solo conteos |
| **`N=1` mal configurado** | Un archivo parcial declara obsoleto medio catálogo | Default `N=2`; flag explícito; warning `STOCK-020`; revisión |
| **Auditoría de carga fallida** | El rollback borra la fila `EN_CURSO` y se pierde evidencia | Escritura de auditoría en transacción separada (mejor esfuerzo); log del error |
| **Historial con ruido** | Incrementar el contador de ausencias podría crear una versión por corrida | Comparar solo datos de negocio y `estado`; no comparar `ausencias_consecutivas` |
| **Migración de columnas** | `ALTER TABLE` en una tabla grande o con datos existentes | Migración controlada, `DEFAULT`, backfill, prueba en schema `test_*`; no recrear tabla |
| **PII futura** | Si stock incorpora contacto de proveedor, se expone en métricas | Escalar a `data-governance-steward`; enmascarar antes de loguear o persistir |

### 7.1 Riesgo específico: KPIs de quiebre y rotación

- **Quiebre operativo:** `estado = 'VIGENTE' AND stock_total = 0`. Es una observación real de
  stock 0 en la última carga.
- **No observado:** `estado IN ('AUSENTE', 'OBSOLETO')`. No es quiebre; se reporta como
  “no observado” o “último stock conocido”.
- **Rotación:** `ventas / stock_promedio` calculado solo con ISBN `VIGENTE` y períodos en que
  estuvieron vigentes. Los obsoletos no entran en el denominador.
- **Inventario:** `SUM(stock_total) WHERE estado='VIGENTE'` para disponibilidad; los obsoletos se
  reportan aparte como “último stock conocido de obsoletos”, no como inventario actual.
- Si un KPI mezcla ambos, el negocio verá inventario y quiebre inflados o deflactados. Por eso la
  vista `stock_vigente` y el contrato de marts son parte de la entrega, no un detalle opcional.

---

## 8. Qué queda “a crear” y quién lo implementa

| Artefacto | Tipo | Dueño | Criterio de aceptación |
|---|---|---|---|
| `etl/validators/isbn.py` | Código | `silver-etl-engineer` | `isbn_check_digit` y `validate_isbn` con vectores verdes; sin excepciones por dato inválido |
| `etl/validators/__init__.py` | Código | `silver-etl-engineer` | Exporta las funciones nuevas; no rompe imports existentes |
| `etl/pipelines/stock.py` | Código | `silver-etl-engineer` | Warning/error ISBN, métricas, cuarentena, ciclo de vida, `modo baseline`, `N` configurable |
| `sql/stock.sql` | SQL | `silver-etl-engineer` | Columnas de estado, `stock_cargas`, `stock_historial.estado`, `CHECK`, vista `stock_vigente` |
| `sql/calidad.sql` o sección en `sql/stock.sql` | SQL | `silver-etl-engineer` | Tabla `cuarentena` e índices; ejecutada por los pipelines |
| `etl/config.py` | Código | `silver-etl-engineer` | Umbrales `N`, tasa de rechazo, caída de volumen, frescura y modo ISBN; sin secretos |
| `etl/quarantine.py` (o helper equivalente) | Código | `silver-etl-engineer` | Escritura transaccional de cuarentena; misma transacción que el upsert |
| `tests/fixtures/build_fixtures.py` | Tests | `data-testing-engineer` | ISBN con DV válido; filas inválidas reservadas; invariantes |
| `tests/test_validators.py` | Tests | `data-testing-engineer` | Vectores ISBN válidos/inválidos; normalización; `allow_isbn10` |
| `tests/test_stock_ventas_transform.py` | Tests | `data-testing-engineer` | Fase 1 warning; fase 2 rechazo; ISBN protegido; duplicados |
| `tests/test_stock_lifecycle.py` | Tests | `data-testing-engineer` | Integración SCD2, ausencias, primera carga, carga vacía, carga sospechosa |
| `tests/test_schema_stock_ventas.py` | Tests | `data-testing-engineer` | Columnas/tipos/`CHECK` nuevos; `stock_historial.estado` |
| `tests/test_quarantine.py` | Tests | `data-testing-engineer` | Cada rechazo tiene fila con archivo, fila, columna, regla, motivo y SHA-256 |
| Migración `ALTER TABLE` / backfill | SQL | `silver-etl-engineer` | Probada en schema `test_*`; no recrea tablas; datos existentes en `VIGENTE` con backfill |
| `docs/contratos/stock.yaml` (opcional) | Docs | `data-quality-steward` | Contrato externalizado, versionado, con severidad por regla |
| `--report PATH` | Código | `silver-etl-engineer` | Reporte legible por negocio; no existe hoy |
| Canonicalización ISBN-10 → ISBN-13 | Código | `silver-etl-engineer` + catálogo | Fuera de esta entrega; evita claves duplicadas |
| Reconciliación de ISBN-10/13 en catálogo | Datos | `data-governance-steward` + negocio | Reporte de pares; no borrar; decisión de negocio |

### 8.1 Lo que **no** se debe hacer

- No cambiar las firmas de validadores existentes sin coordinación: el contrato es devolver
  `None` ante dato inválido.
- No activar la fase 2 de ISBN sin medir antes la fase 1.
- No poner `stock_* = 0` al volverse obsoleto.
- No borrar filas de `stock` ni truncar la tabla.
- No marcar ausencias en cargas fallidas, vacías, baseline o sospechosas.
- No usar datos personales reales en fixtures, reportes, ejemplos ni tests.
- No documentar comandos que no existen: `--report` y el script de migración son **a crear**.

---

## 9. Verificación y anexos

### 9.1 Verificación de esta entrega

- Documento creado en `docs/calidad-isbn-y-stock-obsoleto.md`.
- No se modificó `etl/`, `sql/`, `tests/`, configuración ni `.env`.
- No se ejecutaron cargas reales ni comandos de shell.
- Los ejemplos usan ISBN y títulos sintéticos; no hay RUT, email, teléfono ni nombres reales.
- Los vectores de §3.7 se calcularon a mano; el vector `9788478886446` se corrigió porque no
  cumple el DV.

### 9.2 Verificación futura (cuando exista implementación)

```powershell
# Dry-run existente; debe seguir sin escribir en PostgreSQL.
.venv\Scripts\python.exe -m etl.pipelines.stock --dry-run --log-level DEBUG

# Tests y lint existentes.
.venv\Scripts\python.exe -m pytest
.venv\Scripts\python.exe -m ruff check .
```

Criterios verificables:

- `--dry-run` muestra `leidas`, `validas`, `rechazadas`, `duplicadas`, `isbn_dv_invalidos`,
  `protegidas`, `marca_ausencias`, `vigentes`, `ausentes`, `obsoletos_nuevos`,
  `obsoletos_totales` y `contrato_estado`.
- Cada rechazo del dry-run tiene una fila de cuarentena equivalente en la carga real.
- Dos cargas idénticas no crean versiones nuevas para ISBN presentes.
- Un ISBN ausente dos cargas aptas consecutivas queda `OBSOLETO` y conserva su stock.
- Un ISBN que reaparece vuelve a `VIGENTE` y abre una versión nueva.
- Una carga vacía no modifica `stock` ni `stock_cargas` como exitosa.
- Las consultas de §4.9 devuelven los conteos esperados y los invariantes devuelven 0 filas.

### 9.3 Glosario

| Término | Significado |
|---|---|
| **DV** | Dígito verificador |
| **ISBN-13** | ISBN de 13 dígitos, prefijo 978/979, DV módulo 10 con pesos 1 y 3 |
| **ISBN-10** | ISBN de 10 caracteres, último puede ser `X`, DV módulo 11 con pesos 10..2 |
| **EAN-13** | Código de 13 dígitos; no todo EAN-13 es ISBN |
| **SCD2** | Slowly Changing Dimension tipo 2: versiones con `valid_from`/`valid_to` |
| **Carga apta** | Carga exitosa y confiable para marcar ausencias |
| **Ausencia** | ISBN que no apareció en una carga apta; no es stock 0 |
| **Obsoleto** | ISBN con `N` ausencias consecutivas en cargas aptas; la fila se conserva |
| **Cuarentena** | Registro persistente de una fila/regla rechazada o advertida |
| **Contrato** | Conjunto versionado de columnas, reglas, severidades y umbrales |

### 9.4 Anexo: tabla resumen de decisiones de stock

| Pregunta | Respuesta |
|---|---|
| ¿Se borra un ISBN que no aparece? | No. Se marca `AUSENTE` y luego `OBSOLETO`. |
| ¿Se pone stock 0 al volverse obsoleto? | No. Se conserva el último stock conocido. |
| ¿Qué pasa con el historial? | Se cierra la versión al cambiar el estado; se abre una nueva. |
| ¿Cuándo es obsoleto? | Tras `N=2` ausencias en cargas aptas por defecto. |
| ¿Qué carga cuenta? | Solo cargas exitosas y aptas; no fallidas, vacías, baseline o sospechosas. |
| ¿Qué pasa si la fila se rechaza? | Si el ISBN es válido, se protege y no cuenta como ausencia. |
| ¿Qué pasa si el ISBN reaparece? | Vuelve a `VIGENTE`, contador 0, nueva versión SCD2. |
| ¿Qué pasa en la primera carga? | No se marcan obsoletos; todas las filas quedan `VIGENTE`. |
| ¿Qué pasa con duplicados? | Se deduplica; última fila gana; el ISBN cuenta como observado. |
| ¿Qué pasa si el archivo llega incompleto? | Se publican filas válidas, pero no se marcan ausencias; contrato `FAIL`. |

---

**Fin del documento.**
