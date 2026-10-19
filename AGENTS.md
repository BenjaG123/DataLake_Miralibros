# AGENTS.md

Python ETL that normalizes Miralibros raw files into PostgreSQL (`silver` layer).

## Setup
- Use the provisioned venv: `.venv/bin/python` (Python 3.14). Deps are pinned in `requirements.txt`.
- Copy `.env.example` to `.env`; `DATABASE_URL` is required. `PG_SCHEMA` defaults to `silver`.
- `bronze/` and `.env` are gitignored. Raw source files are not in the repo and must be
  placed locally with their exact original names (e.g. `bronze/CLIENTES LIBRERIA.xlsx`).
- No tests, linter, formatter, typechecker, or CI exist. Do not invent commands; verify
  changes with `--dry-run` or `--help`.

## Commands
Run from the repo root (imports are absolute `etl.*`; there is no console script):
- `.venv/bin/python -m etl.pipelines.clientes`
- `.venv/bin/python -m etl.pipelines.clientes --dry-run` (reads/transforms, no DB writes)
- `.venv/bin/python -m etl.pipelines.clientes --schema-only` (creates schema/table only)
- `--input PATH` overrides the source file; `--log-level DEBUG` shows rejection reasons.

## Layout
- `etl/config.py` — repo-root paths, `.env` loading, `PG_SCHEMA` validation.
- `etl/db.py` — psycopg helpers: `execute_sql_file`, `truncate_table`, `copy_rows`, `replace_table`.
- `etl/readers.py` — pandas readers that force `dtype=str`; `read_html_grid_as_str` for climate files.
- `etl/validators/` — `common`/`fechas`/`numeros`/`personas`, re-exported from `etl.validators`.
- `etl/pipelines/<entity>.py` — argparse + `run()` per entity, following `clientes.py`.
- `sql/<entity>.sql` — DDL using the `{{schema}}` placeholder.

## Gotchas
- Re-running a pipeline **replaces the whole table** (`TRUNCATE` + `COPY` inside one
  transaction), not appends. Use `--dry-run` when unsure.
- The climate `.xls` files are HTML tables. Use `read_html_grid_as_str`; `read_excel_as_str`
  will fail on them.
- Keep raw values as strings until validators run. Validators return `None` for bad data
  instead of raising; `run()` discards rejected rows and only logs them at DEBUG level.
- SQL files are read and executed whole; `{{schema}}` is substituted only after identifier
  validation. Use `psycopg.sql.Identifier` for identifiers, never f-strings.
- Only `clientes` is implemented. Other bronze files (`STOCK`, `VENTAS`, climate data) have
  no pipeline yet.
- Match the repo language: Spanish docstrings/log/error messages; Conventional Commits in
  Spanish (`feat:`, `refactor:`) on branch `develop`.

## Reference
- `README.md` (setup) and `sql/clientes.sql` (target schema/constraints) are the main docs.
