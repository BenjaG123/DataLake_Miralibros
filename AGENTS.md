# AGENTS.md

Python ETL that normalizes Miralibros raw files into PostgreSQL (`silver` layer).

## Setup
- Use the provisioned venv: `.venv\Scripts\python.exe` (Python 3.11). Deps are pinned in
  `requirements.txt` but **not pre-installed**; run
  `.venv\Scripts\python.exe -m pip install -r requirements.txt` first. System `python`
  (3.12) has no deps either.
- Copy `.env.example` to `.env`; `DATABASE_URL` is required. `PG_SCHEMA` defaults to `silver`.
- `bronze/` and `.env` are gitignored. Raw source files are not in the repo and must be
  placed locally with their exact original names (e.g. `bronze/CLIENTES LIBRERIA.xlsx`).
- Dev tools live in `requirements-dev.txt` (`pytest`, `ruff`); install them with
  `.venv\Scripts\python.exe -m pip install -r requirements-dev.txt`.
- Tests (`tests/`), lint (`ruff`) and CI (`.github/workflows/ci.yml`) exist. There is still
  no formatter, typechecker nor coverage tool. Verify changes with `--dry-run` or `--help`.

## Commands
Run from the repo root (imports are absolute `etl.*`; there is no console script):
- `.venv\Scripts\python.exe -m etl.pipelines.clientes`
- `.venv\Scripts\python.exe -m etl.pipelines.clientes --dry-run` (reads/transforms, no DB writes)
- `.venv\Scripts\python.exe -m etl.pipelines.clientes --schema-only` (creates schema/table only)
- `.venv\Scripts\python.exe -m etl.pipelines.stock [--input PATH] [--dry-run] [--schema-only]`
- `.venv\Scripts\python.exe -m etl.pipelines.ventas [--input PATH ...] [--dry-run] [--schema-only]`
  (`--input` is repeatable; by default reads both `VENTAS LIBRERIA.xlsx` and `VENTAS CAFETERIA.xlsx`)
- `.venv\Scripts\python.exe -m etl.pipelines.ventas [--clientes PATH] [--stock PATH]
  [--catalogo-cafeteria PATH]` (rutas de enriquecimiento in-memory; por defecto
  `bronze/CLIENTES LIBRERIA.xlsx`, `bronze/STOCK LIBRERIA.xlsx` y
  `bronze/CATALOGO CAFETERIA.csv`)
- `.venv\Scripts\python.exe -m etl.pipelines.clima [--input-dir PATH] [--dry-run] [--schema-only]`
- `.venv\Scripts\python.exe -m etl.pipelines [--only ...] [--dry-run] [--schema-only]`
  (`--only` is repeatable; the runner executes `clientes -> stock -> ventas -> clima`)
- `.venv\Scripts\python.exe -m etl.ingest [--input-dir PATH] [--manifest PATH] [--dry-run]`
  (calcula SHA-256 y actualiza el manifiesto JSONL de bronze)
- `--log-level DEBUG` shows per-row rejection reasons.
- `.venv\Scripts\python.exe -m pytest` and `.venv\Scripts\python.exe -m ruff check .`

## Layout
- `etl/config.py` — repo-root paths, `.env` loading, `PG_SCHEMA` validation.
- `etl/db.py` — psycopg helpers: `execute_sql_file`, `truncate_table`, `copy_rows`,
  `replace_table`, `upsert_table` (COPY to a temp table + `INSERT ... ON CONFLICT DO UPDATE`).
- `etl/readers.py` — pandas readers that force `dtype=str`; `read_html_grid_as_str`
  parsea los `.xls` de clima con `lxml.html` (sin `pd.read_html`) y preserva el texto crudo.
- `etl/validators/` — `common`/`fechas`/`numeros`/`personas`/`clima`, re-exported from `etl.validators`.
- `etl/ingest.py` — checksum SHA-256 y manifiesto JSONL append-only de los Excels de bronze.
- `etl/quarantine.py` — `insert_quarantine` y `QUARANTINE_SQL` para la tabla común de rechazos.
- `etl/pipelines/<entity>.py` — argparse + `run()` per entity
  (`clientes`, `stock`, `ventas`, `clima`).
- `etl/pipelines/__main__.py` — runner secuencial de los cuatro pipelines.
- `sql/<entity>.sql` — DDL using the `{{schema}}` placeholder.
- `sql/cuarentena.sql` y `sql/migrations/` — tabla común de rechazos y migraciones no destructivas.
- `tests/` — pytest suite; `tests/fixtures/build_fixtures.py` regenerates the synthetic XLSX
  and `tests/fixtures/clima.py` the synthetic climate HTML files.
- `pyproject.toml` — pytest/ruff config; `.github/workflows/ci.yml` — CI on pushes to `develop`.

## Gotchas
- `clientes` **replaces the whole table** (`TRUNCATE` + `COPY` inside one transaction).
  `stock`, `ventas` and `clima` are **incremental**: they upsert by natural key and never
  truncate. `clima` uses `fecha` as its key. `ventas` upserts `ventas_ticket` by
  `(origen, correlativo)` and replaces only the lines of the tickets in the current batch.
  Use `--dry-run` when unsure.
- The climate `.xls` files are HTML tables. Use `read_html_grid_as_str` (parsea con
  `lxml.html` y conserva el texto crudo); `read_excel_as_str` will fail on them.
- Climate conventions: `s/p` means `0.0 mm`, `"."` in UV means `NULL`, and `CALMA` is stored
  as `viento_direccion = 'CALMA'` with speed `0` (the column is `VARCHAR(5)`). The pipeline
  creates one row per real date in 2025 (365 rows).
- Keep raw values as strings until validators run. Validators return `None` for bad data
  instead of raising; pipelines discard rejected rows and log the reasons (DEBUG per row,
  INFO/WARNING summary). `stock` y `ventas` además persisten cada rechazo en
  `{{schema}}.cuarentena` con su regla, columna y valor crudo.
- `ventas` enriquece **in-memory** leyendo `CLIENTES LIBRERIA.xlsx`, `STOCK LIBRERIA.xlsx` y
  `CATALOGO CAFETERIA.csv` (rutas configurables con `--clientes`, `--stock` y
  `--catalogo-cafeteria`). No consulta la base para clientes/stock/catálogo y `--dry-run` no
  abre conexión.
- `clientes.compras` es **monto monetario histórico en CLP**, no cantidad de transacciones.
- `ventas_ticket` (PK `(origen, correlativo)`, FK `cliente_id -> clientes(cliente_id)`) y
  `ventas_linea` (PK `(origen, correlativo, linea)`, FK compuesta `ON DELETE CASCADE`) son el
  modelo vigente. `codigo_interno` apunta lógicamente a `stock.isbn` o al catálogo de
  cafetería, sin FK.
- Las líneas se reconstruyen con bitsets determinísticos: exactamente `cant_items` precios que
  suman `total_venta`. Sin combinación exacta, el ticket no se carga y va a cuarentena con la
  regla `VENTAS-LINEA-001`. Los `cliente_id` se asignan con subset sum exacto; sin
  combinación, el cliente queda sin asignación y se registra un `WARNING` sin exponer RUT.
- `ventas.hora_simulada` es sintética: se deriva de un hash de
  `origen|correlativo|fecha`, no proviene del POS y es idempotente por ticket. Los
  domingos queda `NULL`.
- `clientes` persiste solo `cliente_id` (HMAC-SHA256 del RUT normalizado con `PII_HASH_KEY`),
  `alta`, `canal`, `edad`, `puntos`, `compras` y `canjes`. RUT, nombre, email y cumpleaños se
  validan en memoria y no se guardan. Un `"0"` textual en nombre/email no rechaza la fila; una
  `Alta` futura queda `NULL` con warning. Cambiar `PII_HASH_KEY` cambia todos los `cliente_id`.
- SQL files are read and executed whole; `{{schema}}` is substituted only after identifier
  validation. Use `psycopg.sql.Identifier` for identifiers, never f-strings.
- `ventas --schema-only` aislado requiere `{{schema}}.clientes` creado antes por la FK de
  `cliente_id`; el runner ya ejecuta `clientes` primero.
- La migración `sql/migrations/20261105_split_ventas.sql` conserva la tabla plana como
  `{{schema}}.ventas_flat_backup` (renombrado idempotente, sin `DROP`).
- All four pipelines are implemented: `clientes`, `stock`, `ventas` and `clima`.
- If the Ventas files use names like `VENTAS LIBRERIA 2025.xlsx`, pass them explicitly with
  repeated `--input`; the default paths expect the names without the year.
- Match the repo language: Spanish docstrings/log/error messages; Conventional Commits in
  Spanish (`feat:`, `refactor:`) on branch `develop`.

## Reference
- `README.md` (setup), `sql/clientes.sql`, `sql/stock.sql`, `sql/ventas.sql`,
  `sql/clima.sql` y `sql/cuarentena.sql` (target schemas/constraints) are the main docs.
