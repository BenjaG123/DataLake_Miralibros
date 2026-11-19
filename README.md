# Data Lake Miralibros - ETL

Proceso de extracción, transformación y carga (ETL) de los crudos de Miralibros desde
`bronze/` hacia PostgreSQL, capa `silver`. El repositorio incluye los pipelines
`clientes`, `stock`, `ventas` y `clima`, la ingesta Bronze con manifiesto SHA-256 y una
base de pruebas con `pytest`.

## Requisitos y entorno local

Requiere Python 3.11. El repo espera el entorno virtual local `.venv/`; como está
en `.gitignore`, no viene en un clon limpio. Si no existe, créalo con un
intérprete Python 3.11 y comprueba que `.venv\Scripts\python.exe --version`
responda `Python 3.11`. Las dependencias **no están preinstaladas**; instálalas
con ese intérprete:

```powershell
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
```

- `requirements.txt`: dependencias de ejecución (pandas, lxml, openpyxl,
  psycopg[binary], python-dotenv, email-validator).
- `requirements-dev.txt`: solo desarrollo y CI (pytest, ruff).

### PostgreSQL local con Docker

El `docker-compose.yml` levanta únicamente PostgreSQL (`postgres:16.15-alpine`):

- Contenedor `miralibros-postgres`.
- Volumen persistente `miralibros-pgdata`.
- Healthcheck `pg_isready`.
- Publica `5434:5432` en el host (5432 y 5433 suelen estar ocupados por otros
  proyectos locales).
- Variables `POSTGRES_DB`, `POSTGRES_USER` y `POSTGRES_PASSWORD`, con valores por
  defecto de desarrollo (`miralibros`). En producción las credenciales no deben
  salir de este archivo.

```powershell
docker compose up -d
docker compose ps
```

Espera el estado `healthy` en `docker compose ps`. **El compose no crea el schema
ni las tablas**: eso lo hacen los pipelines con `--schema-only`. `docker compose down`
detiene los contenedores sin borrar el volumen de datos.

### Archivo `.env`

Copia la plantilla y ajusta los valores locales:

```powershell
Copy-Item .env.example .env
```

| Variable | Obligatoria | Valor de ejemplo / defecto | Descripción |
| --- | --- | --- | --- |
| `DATABASE_URL` | Sí | `postgresql://miralibros:miralibros@localhost:5434/miralibros` | Conexión que usa el ETL (`etl/config.py` falla si falta). |
| `PG_SCHEMA` | No | `silver` | Schema donde se cargan las tablas; se valida como identificador SQL. |
| `PII_HASH_KEY` | No | placeholder `miralibros-dev-pii-key` | Clave HMAC-SHA256 para derivar `clientes.cliente_id` desde el RUT. |

`.env` está en `.gitignore` y nunca debe versionarse. Si cambias `PII_HASH_KEY`
cambian **todos** los `cliente_id` y se desvinculan los tickets ya cargados.

## Datos de entrada

Los crudos van en `bronze/` (gitignored) y deben conservar el nombre original exacto:

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

- En `clientes`, la columna `Compras` se interpreta como **monto monetario
  histórico (CLP)**, no como cantidad de transacciones.
- Los `.xls` de clima son tablas HTML, no Excel binarios. El pipeline los lee
  directamente; no hay que convertirlos.
- Los nombres por defecto de `ventas` son `VENTAS LIBRERIA.xlsx` y
  `VENTAS CAFETERIA.xlsx`. Con los archivos anuales del listado hay que pasarlos
  explícitamente con `--input` repetible:

```powershell
.venv\Scripts\python.exe -m etl.pipelines.ventas `
  --input "bronze/VENTAS LIBRERIA 2025.xlsx" `
  --input "bronze/VENTAS CAFETERIA 2025.xlsx"
```

El enriquecimiento de `ventas` es en memoria y acepta rutas explícitas para los
archivos de apoyo (por defecto ya apunta a los nombres de arriba):

```powershell
.venv\Scripts\python.exe -m etl.pipelines.ventas `
  --clientes "bronze/CLIENTES LIBRERIA.xlsx" `
  --stock "bronze/STOCK LIBRERIA.xlsx" `
  --catalogo-cafeteria "bronze/CATALOGO CAFETERIA.csv"
```

## Ejecución de pipelines

Orden de dependencias (lo garantiza el runner `etl.pipelines.__main__`):

```text
clientes -> stock -> ventas -> clima
```

Destino de cada pipeline:

```text
clientes -> silver.clientes
stock    -> silver.stock + silver.stock_historial
ventas   -> silver.ventas_ticket + silver.ventas_linea
clima    -> silver.clima_diario
stock/ventas (rechazos) -> silver.cuarentena
```

Flags comunes a los cuatro pipelines y al runner. `--dry-run` y `--schema-only`
son mutuamente excluyentes:

- `--dry-run`: lee y transforma, sin escribir en PostgreSQL (`ventas` ni siquiera
  abre conexión).
- `--schema-only`: crea el schema y las tablas, sin cargar datos.
- `--log-level DEBUG`: muestra el motivo de rechazo fila por fila.

Flags específicos: `clientes` y `stock` aceptan `--input`; `clima` acepta
`--input-dir`; `ventas` acepta `--input` repetible, `--clientes`, `--stock` y
`--catalogo-cafeteria`.

El runner completo se ejecuta con:

```powershell
.venv\Scripts\python.exe -m etl.pipelines
```

`--only` es repetible y acepta `clientes`, `stock`, `ventas` y `clima`:

```powershell
.venv\Scripts\python.exe -m etl.pipelines --only clima --dry-run
.venv\Scripts\python.exe -m etl.pipelines --only clientes --only clima --dry-run
```

Cada pipeline también se ejecuta por separado:

```powershell
.venv\Scripts\python.exe -m etl.pipelines.clientes --dry-run
.venv\Scripts\python.exe -m etl.pipelines.stock --dry-run
.venv\Scripts\python.exe -m etl.pipelines.ventas --dry-run
.venv\Scripts\python.exe -m etl.pipelines.clima --dry-run
```

`ventas --schema-only` de forma aislada requiere que `silver.clientes` ya exista,
porque `ventas_ticket.cliente_id` tiene FK a `silver.clientes(cliente_id)`. Para
crear todo el esquema desde cero usa el runner, que respeta el orden:

```powershell
.venv\Scripts\python.exe -m etl.pipelines --schema-only
```

### Arranque reproducible desde cero

1. Preparar entorno, dependencias y `.env`:

   ```powershell
   Copy-Item .env.example .env
   .venv\Scripts\python.exe -m pip install -r requirements.txt
   .venv\Scripts\python.exe -m pip install -r requirements-dev.txt
   ```

2. Levantar PostgreSQL y esperar `healthy`:

   ```powershell
   docker compose up -d
   docker compose ps
   ```

3. Crear schema y tablas (no carga datos):

   ```powershell
   .venv\Scripts\python.exe -m etl.pipelines --schema-only
   ```

4. Verificar la lectura y transformación sin escribir, pipeline por pipeline:

   ```powershell
   .venv\Scripts\python.exe -m etl.pipelines.clientes --dry-run
   .venv\Scripts\python.exe -m etl.pipelines.stock --dry-run
   .venv\Scripts\python.exe -m etl.pipelines.ventas --dry-run
   .venv\Scripts\python.exe -m etl.pipelines.clima --dry-run
   ```

5. Cargar en el orden canónico (requiere aprobación explícita en cualquier
   base que no sea la local de desarrollo):

   ```powershell
   .venv\Scripts\python.exe -m etl.pipelines
   ```

### Modelo de tablas y migraciones

`silver.ventas_ticket` tiene PK `(origen, correlativo)` y `cliente_id` con FK a
`silver.clientes(cliente_id)`. `silver.ventas_linea` tiene PK
`(origen, correlativo, linea)`, FK compuesta a la cabecera con `ON DELETE CASCADE`
e índices por ticket y por `codigo_interno`. No hay FK para `codigo_interno`:
apunta lógicamente a `stock.isbn` (LIBRERIA) o al catálogo de cafetería
(CAFETERIA).

El DDL vive en `sql/<entidad>.sql` y usa el placeholder `{{schema}}`. Las
migraciones no destructivas están en `sql/migrations/`: la migración
`20261105_split_ventas.sql` crea las dos tablas nuevas y, si existe la tabla plana
`silver.ventas`, la renombra a `silver.ventas_flat_backup` como respaldo (nunca la
elimina).

## Ingesta Bronze

Antes de cargar, calcula el SHA-256 y registra los crudos en un manifiesto de
auditoría:

```powershell
.venv\Scripts\python.exe -m etl.ingest --dry-run
.venv\Scripts\python.exe -m etl.ingest
```

`etl.ingest` acepta `--input-dir` y `--manifest`. El manifiesto queda en
`bronze/manifest.jsonl`, es append-only y guarda una línea por versión de archivo
(nombre, ruta, tamaño, fecha, SHA-256, entidad y formato). Como `bronze/` está en
`.gitignore`, el manifiesto es local.

## Comportamiento de carga

- `clientes` reemplaza toda la tabla.
- `stock`, `ventas` y `clima` hacen upsert por su clave natural.
- `ventas` hace upsert de cabeceras por `(origen, correlativo)`. Para las líneas del
  lote, primero elimina las líneas existentes de esos tickets y después inserta las
  nuevas; no toca líneas de tickets ajenos al lote.
- `clima` usa una fila por fecha y upsert por `fecha`.
- `stock` y `ventas` registran las filas rechazadas en `silver.cuarentena`, con
  entidad, archivo, fila, columna, valor crudo, regla y motivo.
- `ventas.hora_simulada` es sintética: se genera con un hash determinístico de
  `origen|correlativo|fecha`, no proviene del POS y los domingos queda `NULL`.
- El enriquecimiento de ventas es **in-memory**: lee `clientes`, `stock` y
  `CATALOGO CAFETERIA.csv` desde los crudos, sin consultar la base para esas
  entidades. `--dry-run` no abre conexión a PostgreSQL.
- Las líneas se reconstruyen con un multiset exacto de `cant_items` precios que suma
  `total_venta`; si no existe combinación, el ticket no se carga y queda en
  cuarentena con la regla `VENTAS-LINEA-001`.
- Los `cliente_id` se asignan con subset sum exacto de montos. Un cliente sin
  combinación exacta queda sin asignación y los tickets sobrantes quedan anónimos
  (`cliente_id = NULL`). `clientes.compras` es monto monetario histórico, no
  cantidad de transacciones.
- `clientes` persiste 8 columnas no identificatorias: `cliente_id` (HMAC-SHA256 del
  RUT normalizado), `alta`, `canal`, `edad`, `mes_cumple`, `puntos`, `compras` y
  `canjes`. RUT, nombre, email y cumpleaños no se guardan. `Nombre` y `Email` se
  ignoran por completo y no rechazan filas. `Cumple.` solo aporta `mes_cumple`
  (queda `NULL` si no es parseable) y tampoco rechaza. La `edad` se deriva
  únicamente del año de nacimiento estimado desde el RUT; una Alta futura se guarda
  como `alta = NULL` con warning.

## Pruebas y calidad de código

La suite no requiere base de datos: genera XLSX/HTML sintéticos en directorios
temporales.

```powershell
.venv\Scripts\python.exe -m pytest
.venv\Scripts\python.exe -m ruff check .
```

- `pytest` usa la configuración de `pyproject.toml` (`testpaths = ["tests"]`,
  `pythonpath = ["."]`).
- `ruff check .` aplica las reglas `E`, `F` e `I` con `line-length = 88`, también
  configuradas en `pyproject.toml`.
- CI: `.github/workflows/ci.yml` ejecuta lint y `pytest` en pushes y pull requests
  hacia `develop` (Python 3.11, sin servicios ni secretos).

### Tests de integración con PostgreSQL (opcionales)

Los tests de integración de `tests/test_stock_ventas_integration.py` solo corren si
existe `TEST_DATABASE_URL`; nunca usan `DATABASE_URL` ni `PG_SCHEMA`. Sin la
variable se **omiten** (`skip`), y por eso la suite reporta algunos tests saltados.

```powershell
$env:TEST_DATABASE_URL="postgresql://usuario:password@localhost:5434/nombre_db"
.venv\Scripts\python.exe -m pytest
```

Cada test crea un schema único `test_miralibros_*`, valida que empiece con `test_`,
carga los crudos sintéticos dos veces (comprobando idempotencia) y elimina el schema
con `DROP SCHEMA ... CASCADE` en un bloque `finally`. Apunta `TEST_DATABASE_URL` a
una base desechable, no a la de desarrollo ni a producción.

### Documentación de referencia

- [`docs/catalogo_datos.md`](docs/catalogo_datos.md): catálogo Bronce/Silver, PII,
  retención y sección de verificación.
- [`docs/linaje_datos.md`](docs/linaje_datos.md): trazabilidad end-to-end de los
  datos.

## Estado

`clientes`, `stock`, `ventas` y `clima` están implementados; la capa Gold aún no
está implementada.
