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
   ```

## Datos de entrada

Los archivos deben colocarse en `bronze/` con sus nombres originales:

```text
CLIENTES LIBRERIA.xlsx
STOCK LIBRERIA.xlsx
VENTAS LIBRERIA 2025.xlsx
VENTAS CAFETERIA 2025.xlsx
HumedadRelativaAire2025.xls
IndiceUV2025.xls
Precipitaciones2025.xls
TemperaturaMax2025.xls
TemperaturaMedia2025.xls
TemperaturaMin2025.xls
Viento2025.xls
```

Los `.xls` de clima son tablas HTML, no Excel binarios. El pipeline los lee directamente;
no hay que convertirlos.

Si los archivos de ventas no se llaman `VENTAS LIBRERIA.xlsx` / `VENTAS CAFETERIA.xlsx`,
pasarlos explícitamente:

```bash
python -m etl.pipelines.ventas \
  --input "bronze/VENTAS LIBRERIA 2025.xlsx" \
  --input "bronze/VENTAS CAFETERIA 2025.xlsx"
```

## Pipelines

```text
clientes -> silver.clientes
stock    -> silver.stock + silver.stock_historial
ventas   -> silver.ventas
clima    -> silver.clima_diario
```

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
- `clima` usa una fila por fecha y upsert por `fecha`.

## Tests y lint

```bash
python -m pytest
python -m ruff check .
```
