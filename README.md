# Data Lake Miralibros - ETL

Repositorio para el proceso de extracción, transformación y carga (ETL) de los datos de
Miralibros en PostgreSQL, capa `silver`.

## Setup

1. Crear y activar un entorno virtual.
2. Instalar dependencias:

   ```bash
   pip install -r requirements.txt
   ```

3. Opcional, para tests y lint:

   ```bash
   pip install -r requirements-dev.txt
   ```

4. Copiar `.env.example` a `.env` y configurar:

   ```text
   DATABASE_URL=postgresql://usuario:password@localhost:5432/nombre_db
   PG_SCHEMA=silver
   PII_HASH_KEY=cambia-esta-clave-en-produccion
   ```

   `PII_HASH_KEY` es la clave HMAC-SHA256 con la que `clientes.cliente_id` se deriva
   del RUT normalizado. No se registra en logs ni se versiona. **Cambiarla cambia todos
   los `cliente_id`** y desvincula los tickets ya cargados. Si falta en desarrollo se
   usa `miralibros-dev-pii-key`, el mismo valor de `.env.example`.

## Datos de entrada

Los archivos deben colocarse en `bronze/` con sus nombres originales:

```text
CLIENTES LIBRERIA.xlsx
STOCK LIBRERIA.xlsx
VENTAS LIBRERIA 2025.xlsx
VENTAS CAFETERIA 2025.xlsx
CATALOGO CAFETERIA.csv
HumedadRelativaAire2025.xls
IndiceUV2025.xls
Precipitaciones2025.xls
TemperaturaMax2025.xls
TemperaturaMedia2025.xls
TemperaturaMin2025.xls
Viento2025.xls
```

En `clientes`, la columna `Compras` se interpreta como **monto monetario histórico (CLP)**,
no como cantidad de transacciones.

Los `.xls` de clima son tablas HTML, no Excel binarios. El pipeline los lee directamente;
no hay que convertirlos.

Si los archivos de ventas no se llaman `VENTAS LIBRERIA.xlsx` / `VENTAS CAFETERIA.xlsx`,
pasarlos explícitamente:

```bash
python -m etl.pipelines.ventas \
  --input "bronze/VENTAS LIBRERIA 2025.xlsx" \
  --input "bronze/VENTAS CAFETERIA 2025.xlsx"
```

El pipeline de ventas enriquece en memoria y acepta rutas explícitas para los archivos de
apoyo:

```bash
python -m etl.pipelines.ventas \
  --clientes "bronze/CLIENTES LIBRERIA.xlsx" \
  --stock "bronze/STOCK LIBRERIA.xlsx" \
  --catalogo-cafeteria "bronze/CATALOGO CAFETERIA.csv"
```

## Ingesta Bronze

Antes de cargar, calculá el SHA-256 y registrá los Excels en un manifiesto de auditoría:

```bash
python -m etl.ingest --dry-run
python -m etl.ingest
```

El manifiesto queda en `bronze/manifest.jsonl`, es append-only y guarda una línea por
versión de archivo (nombre, ruta, tamaño, fecha, SHA-256, entidad y formato). Como
`bronze/` está en `.gitignore`, el manifiesto es local.

## Pipelines

```text
clientes -> silver.clientes
stock    -> silver.stock + silver.stock_historial
ventas   -> silver.ventas_ticket + silver.ventas_linea
clima    -> silver.clima_diario
stock/ventas (rechazos) -> silver.cuarentena
```

`silver.ventas_ticket` tiene PK `(origen, correlativo)` y `cliente_id` con FK a
`silver.clientes(cliente_id)`. `silver.ventas_linea` tiene PK `(origen, correlativo, linea)`, FK
compuesta a la cabecera con `ON DELETE CASCADE` e índices por ticket y por `codigo_interno`.
No hay FK para `codigo_interno`: apunta lógicamente a `stock.isbn` (LIBRERIA) o al catálogo
de cafetería (CAFETERIA).

## Comandos

Cada pipeline se ejecuta individualmente:

```bash
python -m etl.pipelines.clientes --dry-run
python -m etl.pipelines.stock --dry-run
python -m etl.pipelines.ventas --dry-run
python -m etl.pipelines.clima --dry-run
```

También existe un runner secuencial:

```bash
python -m etl.pipelines --dry-run
python -m etl.pipelines --only clima --dry-run
python -m etl.pipelines --only clientes --only clima --dry-run
```

Flags comunes:

- `--dry-run`: lee y transforma, sin escribir en PostgreSQL.
- `--schema-only`: crea schemas/tablas sin cargar datos.
- `--log-level DEBUG`: muestra el detalle de filas descartadas.

## Comportamiento de carga

- `clientes` reemplaza toda la tabla.
- `stock`, `ventas` y `clima` hacen upsert por su clave natural.
- `ventas` hace upsert de cabeceras por `(origen, correlativo)`. Para las líneas del lote,
  primero elimina las líneas existentes de esos tickets y después inserta las nuevas; no toca
  líneas de tickets ajenos al lote.
- `clima` usa una fila por fecha y upsert por `fecha`.
- `stock` y `ventas` registran las filas rechazadas en `silver.cuarentena`, con
  entidad, archivo, fila, columna, valor crudo, regla y motivo.
- `ventas.hora_simulada` es sintética: se genera con un hash determinístico de
  `origen|correlativo|fecha`, no proviene del POS y los domingos queda `NULL`.
- El enriquecimiento de ventas es **in-memory**: lee `clientes`, `stock` y
  `CATALOGO CAFETERIA.csv` desde los crudos, sin consultar la base para esas entidades.
  `--dry-run` no abre conexión a PostgreSQL.
- Las líneas se reconstruyen con un multiset exacto de `cant_items` precios que suma
  `total_venta`; si no existe combinación, el ticket no se carga y queda en cuarentena con la
  regla `VENTAS-LINEA-001`.
- Los `cliente_id` se asignan con subset sum exacto de montos. Un cliente sin combinación
  exacta queda sin asignación y los tickets sobrantes quedan anónimos (`cliente_id = NULL`).
  `clientes.compras` es monto monetario histórico, no cantidad de transacciones.
- `clientes` persiste 8 columnas no identificatorias: `cliente_id` (HMAC-SHA256 del RUT
  normalizado), `alta`, `canal`, `edad`, `mes_cumple`, `puntos`, `compras` y `canjes`.
  RUT, nombre, email y cumpleaños no se guardan. `Nombre` y `Email` se ignoran por completo
  y no rechazan filas. `Cumple.` solo aporta `mes_cumple` (queda `NULL` si no es parseable)
  y tampoco rechaza. La `edad` se deriva únicamente del año de nacimiento estimado desde el
  RUT; una Alta futura se guarda como `alta = NULL` con warning.

`ventas --schema-only` ejecutado de forma aislada requiere que `silver.clientes` ya exista,
porque `ventas_ticket.cliente_id` tiene FK a `silver.clientes(cliente_id)`. El runner
`clientes -> stock -> ventas -> clima` garantiza ese orden.

Las migraciones no destructivas viven en `sql/migrations/` y usan `{{schema}}`. La migración
`20261105_split_ventas.sql` crea las dos tablas nuevas y, si existe la tabla plana
`silver.ventas`, la renombra a `silver.ventas_flat_backup` como respaldo (nunca la elimina).

## Tests y lint

```bash
python -m pytest
python -m ruff check .
```
