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
- `.venv\Scripts\python.exe -m etl.pipelines.clima [--input-dir PATH] [--dry-run] [--schema-only]`
- `.venv\Scripts\python.exe -m etl.pipelines [--only ...] [--dry-run] [--schema-only]`
  (`--only` is repeatable; the runner executes `clientes -> stock -> ventas -> clima`)
- `--log-level DEBUG` shows per-row rejection reasons.
- `.venv\Scripts\python.exe -m pytest` and `.venv\Scripts\python.exe -m ruff check .`

## Layout
- `etl/config.py` — repo-root paths, `.env` loading, `PG_SCHEMA` validation.
- `etl/db.py` — psycopg helpers: `execute_sql_file`, `truncate_table`, `copy_rows`,
  `replace_table`, `upsert_table` (COPY to a temp table + `INSERT ... ON CONFLICT DO UPDATE`).
- `etl/readers.py` — pandas readers that force `dtype=str`; `read_html_grid_as_str` for climate files.
- `etl/validators/` — `common`/`fechas`/`numeros`/`personas`/`clima`, re-exported from `etl.validators`.
- `etl/pipelines/<entity>.py` — argparse + `run()` per entity
  (`clientes`, `stock`, `ventas`, `clima`).
- `etl/pipelines/__main__.py` — runner secuencial de los cuatro pipelines.
- `sql/<entity>.sql` — DDL using the `{{schema}}` placeholder.
- `tests/` — pytest suite; `tests/fixtures/build_fixtures.py` regenerates the synthetic XLSX
  and `tests/fixtures/clima.py` the synthetic climate HTML files.
- `pyproject.toml` — pytest/ruff config; `.github/workflows/ci.yml` — CI on pushes to `develop`.

## Gotchas
- `clientes` **replaces the whole table** (`TRUNCATE` + `COPY` inside one transaction).
  `stock`, `ventas` and `clima` are **incremental**: they upsert by natural key and never
  truncate. `clima` uses `fecha` as its key. Use `--dry-run` when unsure.
- The climate `.xls` files are HTML tables. Use `read_html_grid_as_str` (it passes
  `thousands=None` so `"22,4"` keeps its magnitude); `read_excel_as_str` will fail on them.
- Climate conventions: `s/p` means `0.0 mm`, `"."` in UV means `NULL`, and `CALMA` is stored
  as `viento_direccion = 'CALMA'` with speed `0` (the column is `VARCHAR(5)`). The pipeline
  creates one row per real date in 2025 (365 rows).
- Keep raw values as strings until validators run. Validators return `None` for bad data
  instead of raising; pipelines discard rejected rows and log the reasons (DEBUG per row,
  INFO/WARNING summary).
- SQL files are read and executed whole; `{{schema}}` is substituted only after identifier
  validation. Use `psycopg.sql.Identifier` for identifiers, never f-strings.
- All four pipelines are implemented: `clientes`, `stock`, `ventas` and `clima`.
- If the Ventas files use names like `VENTAS LIBRERIA 2025.xlsx`, pass them explicitly with
  repeated `--input`; the default paths expect the names without the year.
- Match the repo language: Spanish docstrings/log/error messages; Conventional Commits in
  Spanish (`feat:`, `refactor:`) on branch `develop`.

## Reference
- `README.md` (setup), `sql/clientes.sql`, `sql/stock.sql`, `sql/ventas.sql` and
  `sql/clima.sql` (target schemas/constraints) are the main docs.
