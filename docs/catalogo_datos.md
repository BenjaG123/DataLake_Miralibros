# Catálogo de Datos de Miralibros

| Campo | Valor |
|---|---|
| Título | Catálogo de Datos de Miralibros |
| Versión | 1.0.0 |
| Estado | vigente |
| Fecha | 2026-11-19 |
| Owner | `data-governance-steward` |
| Revisores | `silver-etl-engineer`, `data-quality-steward` |
| Alcance | Capa Bronce (sistema de archivos local `bronze/`), capa Silver (PostgreSQL, 7 tablas) y capa Gold (no implementada). Catálogo, linaje, clasificación PII, enmascaramiento, retención, glosario y brechas. |
| Fuera de alcance | Ejecución de pipelines, DDL, credenciales y valores de `.env`. |

> **Nota normativa.** Este documento es documentación de gobierno de datos. No modifica
> código, DDL, tests ni archivos crudos. Las reglas aquí descritas son especificaciones
> derivadas de `etl/`, `sql/` y `tests/`; cualquier cambio de comportamiento debe hacerse
> en el código por el rol correspondiente y luego reflejarse aquí con una nueva versión.
> Todo artefacto que no exista en el repositorio se marca como **a crear** con su owner.

## Control de cambios

| Versión | Fecha | Cambio | Owner |
|---|---|---|---|
| 1.0.0 | 2026-11-19 | Versión inicial del catálogo formal. Corrige la premisa: Bronce es filesystem, no PostgreSQL. | `data-governance-steward` |

---

## 1. Visión general de la arquitectura

### 1.1 Capas

| Capa | Soporte real | Contenido | Estado | Observación |
|---|---|---|---|---|
| Bronce | Sistema de archivos local `bronze/` (gitignored) | Archivos crudos con nombre exacto: 3 `.xlsx` de librería (clientes, stock, ventas), 1 `.xlsx` de ventas de cafetería, 1 `.csv` de catálogo y 7 `.xls` de clima (en realidad HTML). Más `bronze/manifest.jsonl`. | existe | **No vive en PostgreSQL.** No hay tabla Bronce ni carga de archivos a la base. |
| Silver | PostgreSQL, schema configurable (default `silver`) | 7 tablas: `clientes`, `stock`, `stock_historial`, `ventas_ticket`, `ventas_linea`, `clima_diario`, `cuarentena`. | existe | Única capa persistida en base de datos. |
| Gold | No existe | Hechos, dimensiones, marts y KPIs (rotación, quiebre, ticket promedio, margen). | **a crear** | No hay carpeta, DDL ni tabla Gold. Owner propuesto: `gold-analytics-modeler`. |

Aclaración explícita: **la capa Bronce es el directorio `bronze/` del sistema de archivos**,
inmutable por convención, con nombres exactos y sin transformación. Solo la capa Silver vive
en PostgreSQL. No existe un catálogo automatizado de metadatos ni un orquestador (Airflow,
Dagster u otro): el único ejecutor es `etl/pipelines/__main__.py`, un runner secuencial
dentro del mismo proceso.

### 1.2 Flujo end-to-end

```text
bronze/<archivo crudo>                     (filesystem, nombres exactos)
        |
        +--> etl/ingest.py                 (SHA-256 por bloques + bronze/manifest.jsonl append-only)
        |
        v
etl/pipelines/<entidad>.py                 (lectura dtype=str, require_columns, validadores)
        |
        +--> sql/<entidad>.sql             (DDL con {{schema}})
        |
        v
PostgreSQL {{schema}}.<tabla>              (Silver)
        |
        v
Gold: hechos, dimensiones y marts          (a crear)
```

El manifiesto es **trazabilidad paralela**: los pipelines no leen `manifest.jsonl` ni
verifican el SHA-256 antes de cargar. La ingesta no mueve, renombra ni transforma archivos.

### 1.3 Convenciones de nombres y SQL

| Convención | Regla |
|---|---|
| Columnas Silver | `snake_case`, español de dominio: `stock_consignacion`, `viento_velocidad_kt`, `total_venta`. |
| Columnas crudas | Se conservan con el nombre original del archivo, incluidos acentos, puntos y espacios: `Clasificación`, `Stock Consig.`, `Cumple.`, `Voucher Máquina de Pago`, `Cant. Items`, `Es Dev.`. |
| Metadatos técnicos | Prefijo `_`: `_source_file`, `_loaded_at`. |
| Vigencia SCD2 | `valid_from`, `valid_to`, `is_current`, `updated_at`. |
| DDL | Placeholder `{{schema}}`; el schema se valida con regex antes de sustituirlo. |
| SQL dinámico | Identificadores con `psycopg.sql.Identifier`; valores como parámetros. Nunca f-strings para construir SQL. |
| Archivos crudos | Nombres exactos y sensibles a mayúsculas/acentos; los archivos no están versionados en el repo. |

### 1.4 Metadatos técnicos y su presencia real por tabla

| Columna | Tipo | Significado | Tablas donde existe |
|---|---|---|---|
| `_source_file` | `TEXT NOT NULL` | Basename del archivo crudo (sin ruta). | `stock`, `stock_historial`, `ventas_ticket` |
| `_loaded_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Instante UTC de la carga. | `stock`, `stock_historial`, `ventas_ticket`, `ventas_linea`, `clima_diario`, `cuarentena` |
| `updated_at` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | Última vez que el pipeline vio o actualizó la fila. | `stock`, `ventas_ticket` |
| `valid_from` | `TIMESTAMPTZ NOT NULL DEFAULT now()` | En `stock`, primera carga del ISBN (no cambia en el upsert); en `stock_historial`, inicio de la versión. | `stock`, `stock_historial` |
| `valid_to` | `TIMESTAMPTZ` | Fin de vigencia; `NULL` si es la versión vigente. | `stock_historial` |
| `is_current` | `BOOLEAN NOT NULL DEFAULT TRUE` | Marca la versión vigente por ISBN. | `stock_historial` |
| `entidad`, `archivo_origen`, `fila`, `columna`, `valor_crudo`, `regla`, `severidad`, `motivo` | varios | Contrato de rechazo. | `cuarentena` |

| Tabla | `_source_file` | `_loaded_at` | `updated_at` | `valid_from` | `valid_to` | `is_current` |
|---|---|---|---|---|---|---|
| `clientes` | no | no | no | no | no | no |
| `stock` | sí | sí | sí | sí | no | no |
| `stock_historial` | sí | sí | no | sí | sí | sí |
| `ventas_ticket` | sí | sí | sí | no | no | no |
| `ventas_linea` | no | sí | no | no | no | no |
| `clima_diario` | no | sí | no | no | no | no |
| `cuarentena` | no (`archivo_origen`) | sí | no | no | no | no |

**Brecha documentada:** `clientes` no persiste `_source_file` ni `_loaded_at`, y
`ventas_linea` y `clima_diario` no persisten `_source_file`. No es posible reconstruir el
archivo exacto de esas filas solo con Silver.

### 1.5 Modos de carga

| Entidad | Modo | Detalle |
|---|---|---|
| `clientes` | Reemplazo total | `TRUNCATE` + `COPY` dentro de una única transacción (`db.replace_table`). No hay historial. |
| `stock` | Upsert incremental | `COPY` a tabla temporal + `INSERT ... ON CONFLICT (isbn) DO UPDATE`; `valid_from` no se actualiza; `updated_at` y `_loaded_at` sí. Luego `_refresh_history` mantiene SCD2. |
| `stock_historial` | Cierre y alta de versión | Cierra la versión vigente si cambió cualquier columna de negocio; abre una nueva si no existe una vigente idéntica. |
| `ventas_ticket` | Upsert incremental | Clave `(origen, correlativo)`; actualiza todas las columnas de negocio, `_source_file`, `_loaded_at` y `updated_at`. |
| `ventas_linea` | Reemplazo por lote | Borra las líneas de los tickets del lote actual (en chunks de 5.000 tickets) y las inserta con `COPY`. |
| `clima_diario` | Upsert incremental | Clave `fecha`; no borra años anteriores. |
| `cuarentena` | Append-only | Cada corrida vuelve a insertar los rechazos; no hay deduplicación ni clave natural. |

### 1.6 Configuración

| Variable | Obligatoria | Default | Uso | Regla de gobierno |
|---|---|---|---|---|
| `DATABASE_URL` | sí, para escribir en PostgreSQL | ninguna | Conexión a PostgreSQL. | Nunca se documenta su valor. Falla con mensaje explícito si falta. |
| `PG_SCHEMA` | no | `silver` | Schema destino. | Se valida con `^[A-Za-z_][A-Za-z0-9_]*$`. |
| `PII_HASH_KEY` | recomendada en producción | clave de desarrollo definida en `etl/config.py` | Clave HMAC-SHA256 para `cliente_id`. | No se registra en logs. Cambiarla cambia **todos** los `cliente_id` y rompe la FK de `ventas_ticket.cliente_id`. |

No se exponen valores reales de `.env`. El archivo `.env.example` solo contiene placeholders
de desarrollo.

### 1.7 Orden de ejecución y dependencias

1. `clientes`
2. `stock`
3. `ventas`
4. `clima`

Dependencias reales:

- `ventas_ticket.cliente_id` tiene FK a `clientes(cliente_id)`: `ventas` requiere que
  `clientes` exista y que los IDs coincidan (ver sección 4.3).
- `ventas_linea` tiene FK compuesta a `ventas_ticket(origen, correlativo)` con
  `ON DELETE CASCADE`.
- `ventas` requiere `stock` y `CATALOGO CAFETERIA.csv` solo **en memoria** para reconstruir
  líneas, no por FK.
- `stock` y `ventas` ejecutan además `sql/cuarentena.sql` antes de cargar.
- `clima` exige los 7 archivos: si falta uno, `FileNotFoundError` y no hay carga.

### 1.8 Resumen de las 7 tablas Silver

| # | Tabla | Grano | Clave primaria | Modo de carga | Origen |
|---|---|---|---|---|---|
| 1 | `clientes` | 1 fila por cliente seudonimizado | `cliente_id` | Reemplazo total | `bronze/CLIENTES LIBRERIA.xlsx` |
| 2 | `stock` | 1 fila por ISBN (estado actual) | `isbn` | Upsert | `bronze/STOCK LIBRERIA.xlsx` |
| 3 | `stock_historial` | 1 fila por versión SCD2 de un ISBN | `id` | Cierre y alta | Derivado de `stock` |
| 4 | `ventas_ticket` | 1 fila por ticket | `(origen, correlativo)` | Upsert | `bronze/VENTAS LIBRERIA.xlsx`, `bronze/VENTAS CAFETERIA.xlsx` |
| 5 | `ventas_linea` | 1 fila por línea reconstruida de un ticket | `(origen, correlativo, linea)` | Borrado por lote + `COPY` | Derivado del ticket y del catálogo de precios |
| 6 | `clima_diario` | 1 fila por fecha | `fecha` | Upsert | 7 archivos `.xls` de clima |
| 7 | `cuarentena` | 1 fila por detalle de rechazo | `id` | Append-only | `stock` y `ventas` |

---

## 2. Catálogo de la capa Bronce

### 2.1 Convenciones de lectura

| Lector | Implementación | Consecuencia |
|---|---|---|
| `read_excel_as_str` | `pd.read_excel(..., sheet_name=0, dtype=str, keep_default_na=False)` | Todo se lee como texto; no se infieren tipos; las celdas vacías quedan como cadena vacía, no como `NaN`. |
| `read_csv_as_str` | `pd.read_csv(..., sep=',', dtype=str, keep_default_na=False, encoding='utf-8-sig')` | Soporta BOM UTF-8; separador coma; todo texto. |
| `read_html_grid_as_str` | `lxml.html` con `itertext()` | Conserva texto crudo: ceros a la izquierda, decimales con coma, `s/p`, `CALMA` y celdas vacías. No usa `pd.read_html`. |
| `require_columns` | Compara nombres exactos | Si falta una columna obligatoria lanza `ValueError` con la lista de faltantes. **No hay carga parcial.** |

Conjunto `PLACEHOLDERS` (texto): cadena vacía, `0`, `null`, `undefined`, `none`, `n/a`,
`n.a.`, `na`, `-`, `--`, `---`, `sin nombre`, `sin dato`, `sin informacion`,
`sin información`, `no informado`, `no informa`, `no registra`, `no aplica`.

Conjunto `NUMERIC_PLACEHOLDERS` (numérico): cadena vacía, `.`, `null`, `undefined`, `none`,
`nan`, `n/a`, `n.a.`, `na`, `-`, `--`, `---`, `sin dato`, `sin informacion`,
`sin información`, `no informado`, `no informa`, `no registra`.

Nota: el valor `0` es placeholder **solo** en el conjunto de texto, no en el numérico. Por
eso `PVP = 0` no se trata como vacío: se valida y luego se rechaza por ser menor a 100.

### 2.2 Manifiesto de ingesta

| Atributo | Valor |
|---|---|
| Script | `etl/ingest.py` |
| Comando | `.venv\Scripts\python.exe -m etl.ingest [--input-dir PATH] [--manifest PATH] [--dry-run]` |
| Archivo | `bronze/manifest.jsonl` (append-only; una línea por cada contenido nuevo respecto de la última versión del mismo nombre) |
| Checksum | SHA-256 calculado por bloques de 1 MiB |
| Alcance | Solo archivos con extensión `.xlsx` y `.xls` del directorio de entrada |
| Efectos | No mueve, renombra ni transforma archivos; no toca la base de datos |

Campos exactos de cada línea:

| Campo | Contenido |
|---|---|
| `archivo` | Nombre del archivo (basename). |
| `ruta_relativa` | Ruta relativa a la raíz del repo o al directorio de entrada, con separadores `/`. |
| `tamano_bytes` | Tamaño en bytes. |
| `mtime_utc` | Fecha de modificación del archivo en ISO 8601 UTC. |
| `sha256` | Hash SHA-256 hexadecimal. |
| `ingerido_en_utc` | Instante de ingesta en ISO 8601 UTC. |
| `entidad` | Inferida del nombre: `clientes`, `stock`, `ventas_libreria`, `ventas_cafeteria`, `clima_temp_max`, `clima_temp_min`, `clima_temp_media`, `clima_humedad_relativa`, `clima_indice_uv`, `clima_precipitaciones`, `clima_viento` o `desconocido`. |
| `formato` | Detectado por firma de bytes: `xlsx` cuando empieza con `PK`, `html` cuando contiene `<html` o `<table`, `desconocido` en otro caso. |
| `version` | 1 en la primera aparición; incrementa con cada contenido nuevo. |
| `hash_anterior` | SHA-256 de la versión previa, o `null`. |
| `estado` | `nuevo` o `nueva_version`. |

Reglas: si el SHA-256 coincide con la última versión del mismo nombre, no se escribe línea
(se cuenta como duplicado); si difiere, se agrega `version + 1` con `hash_anterior`.

Brechas del manifiesto:

- **El CSV de cafetería no entra al manifiesto**: `bronze/CATALOGO CAFETERIA.csv` no tiene
  checksum ni versión registrada.
- Los 7 `.xls` de clima **sí entran** (extensión `.xls`), con `formato = html`, porque los
  archivos son tablas HTML con extensión de Excel.
- No hay validación de que el archivo cargado por un pipeline corresponda al último SHA
  registrado.

### 2.3 Ventas de librería y cafetería

| Atributo | `VENTAS LIBRERIA.xlsx` | `VENTAS CAFETERIA.xlsx` |
|---|---|---|
| Ruta | `bronze/VENTAS LIBRERIA.xlsx` | `bronze/VENTAS CAFETERIA.xlsx` |
| Formato real | XLSX | XLSX |
| Lector | `read_excel_as_str` | `read_excel_as_str` |
| Columnas en el archivo | 14 | 14 |
| Columnas obligatorias | 6 | 6 |
| Granularidad | 1 fila = 1 ticket | 1 fila = 1 ticket |
| Pipeline | `etl/pipelines/ventas.py` | `etl/pipelines/ventas.py` |
| Destino | `Silver.ventas_ticket` y `Silver.ventas_linea` | `Silver.ventas_ticket` y `Silver.ventas_linea` |
| Origen inferido | `LIBRERIA` (por nombre de archivo) | `CAFETERIA` (por nombre de archivo) |

Columnas obligatorias (`REQUIRED_COLUMNS`): `Correlativo`, `Fecha`, `Tipo Documento`,
`Folio`, `Total Venta`, `Cant. Items`.

Columnas exactas y tratamiento:

| Columna cruda | Tipo de lectura | Tipo semántico | Descripción | Limpieza / validación | Destino |
|---|---|---|---|---|---|
| `Correlativo` | `str` | entero mayor o igual a 1 | Identificador del ticket dentro del origen. | `parse_integer(minimum=1)`; inválido genera `VENTAS-VALIDACION`. | `Silver.ventas_ticket.correlativo` |
| `Orden Web` | `str` | texto | Columna leída si existe. | **No se usa, no se valida y no se persiste.** | `Excluida` |
| `Fecha` | `str` | fecha ISO | Fecha del ticket. | `parse_date_iso`: solo `YYYY-MM-DD` estricto; `dd/mm/yyyy` se rechaza. | `Silver.ventas_ticket.fecha` |
| `Medio de Pago` | `str` | texto | Medio de pago informado. | `_clean_text`: colapsa espacios y convierte placeholders en `NULL`. No rechaza. | `Silver.ventas_ticket.medio_pago` |
| `Tipo Documento` | `str` | texto | Tipo de documento tributario. | Se rechaza solo el valor literal `Sin Documento` (sin distinguir caja ni espacios) con `VENTAS-DOC-001`; vacío pasa y queda `NULL`. | `Silver.ventas_ticket.tipo_documento` |
| `Folio` | `str` | texto | Número de documento. | `VENTAS-FOLIO-001` si está vacío o si es solo ceros tras quitar `.`, `,`, `-` y espacios. Si es no vacío y distinto de cero, se acepta como texto. | `Silver.ventas_ticket.folio` |
| `Voucher Máquina de Pago` | `str` | texto | Número de voucher del POS. | **Se elimina de inmediato** (`DROPPED_COLUMNS`): no se valida, no se persiste y no va a cuarentena. | `Excluida` |
| `Total Venta` | `str` | entero CLP mayor o igual a 0 | Monto total del ticket. | `parse_non_negative_bigint`: quita `.` y `,`; solo dígitos; inválido o negativo genera `VENTAS-VALIDACION`. | `Silver.ventas_ticket.total_venta` |
| `Cant. Items` | `str` | entero mayor o igual a 1 | Cantidad total de ítems del ticket. | `parse_integer(minimum=1)`; inválido genera `VENTAS-VALIDACION`. | `Silver.ventas_ticket.cant_items` |
| `Canal` | `str` | texto | Canal de venta. | Se lee si existe, pero **no se usa ni se persiste**. `normalize_canal` existe en `ventas.py` y no se invoca en la validación. | `Excluida` |
| `Feria` | `str` | texto | Indicador de feria. | **Se elimina de inmediato** (`DROPPED_COLUMNS`): no se valida, no se persiste y no va a cuarentena. | `Excluida` |
| `Tiene Descuento` | `str` | flag SI/NO | Indica descuento. | `normalize_si_no`: `SI`, `SÍ` o `S` pasan a `SI`; `NO` o `N` pasan a `NO`; otro valor queda `NULL`, sin rechazo. | `Silver.ventas_ticket.tiene_descuento` |
| `Estado` | `str` | texto | Estado del documento. | `_clean_text`; no rechaza. | `Silver.ventas_ticket.estado` |
| `Es Dev.` | `str` | flag SI/NO | Indica devolución. | `normalize_si_no`; otro valor queda `NULL`, sin rechazo. | `Silver.ventas_ticket.es_devolucion` |

Reglas específicas de ventas:

- **`Origen` no es una columna del archivo.** Se infiere del nombre: si contiene `CAFETERIA`
  (tolera `Í` convertida en `I`) devuelve `CAFETERIA`; si contiene `LIBRERIA`, devuelve
  `LIBRERIA`; en otro caso `inferir_origen` lanza `ValueError` y **no hay carga**.
- Si el archivo trae una columna `Origen`, se acepta como extra, no se exige y no llega al
  ticket.
- Rechazos de fila: `VENTAS-VALIDACION` (Correlativo, Fecha, Total Venta, Cant. Items),
  `VENTAS-FOLIO-001` y `VENTAS-DOC-001`. Severidad `error`. Se escribe **una fila de
  cuarentena por cada detalle**, con `entidad = ventas` y `archivo_origen` igual al basename.
- **Duplicados por `(origen, correlativo)`**: se conserva la última fila, se registra
  `WARNING` en el log y **no** van a cuarentena.
- **Líneas no reconstruibles**: si el ticket es válido pero no existe una combinación exacta
  de `cant_items` precios que sumen `total_venta`, el ticket **no** se inserta en
  `ventas_ticket` y va a cuarentena con regla `VENTAS-LINEA-001`, columna `total_venta` y
  motivo `sin combinación exacta de líneas`.
- **Enriquecimiento in-memory**: lee `CLIENTES LIBRERIA.xlsx`, `STOCK LIBRERIA.xlsx` y
  `CATALOGO CAFETERIA.csv` (rutas configurables con `--clientes`, `--stock` y
  `--catalogo-cafeteria`). No consulta la base para clientes, stock ni catálogo.
  `--dry-run` no abre conexión.
- **Asignación de `cliente_id`**: solo tickets `LIBRERIA`. Son elegibles los clientes con
  `compras` mayor que 0 y `alta` no nula; se exige `fecha` mayor o igual a `alta` y
  `total_venta` menor o igual a `compras`. Se busca un subconjunto exacto de tickets cuya
  suma sea `compras`. Los clientes se ordenan por `compras` descendente y luego por
  `cliente_id`. Sin combinación o con timeout (250.000 iteraciones o 2,0 segundos) el ticket
  queda anónimo (`cliente_id NULL`) con `WARNING` que no expone RUT. **`CAFETERIA` es
  siempre anónima.**
- **`hora_simulada`**: sintética, determinística e idempotente por ticket. Se deriva de
  `SHA-256(origen|correlativo|fecha)`, **no** proviene del POS. Domingo deja `NULL` con
  `WARNING`. Ventana de 10:00 a 19:00 y sábado hasta las 18:00. Cafetería prioriza de 12:00
  a 14:00 y librería de 17:00 al cierre; pesos 60% tramo principal, 20% secundario y 20%
  resto (10:00 a 12:00 o 14:00 a 17:00). El minuto es uniforme dentro del tramo.
- **Naturaleza sintética del detalle**: la reconstrucción de líneas y la elección de
  `codigo_interno` son determinísticas, pero **no representan el mix real de productos
  vendidos**.

### 2.4 Clientes de librería

| Atributo | Valor |
|---|---|
| Ruta | `bronze/CLIENTES LIBRERIA.xlsx` |
| Formato real | XLSX |
| Lector | `read_excel_as_str` |
| Columnas | 9, todas obligatorias (`require_columns`) |
| Granularidad | 1 fila = 1 cliente |
| Pipeline | `etl/pipelines/clientes.py` |
| Destino | `Silver.clientes` (8 columnas; no se persiste identidad) |

| Columna cruda | Tipo de lectura | Tipo semántico | Descripción | Limpieza / validación | Destino |
|---|---|---|---|---|---|
| `Rut cliente` | `str` | RUT chileno | Identificador del cliente. | `normalize_rut`: mayúsculas; quita puntos, espacios y NBSP; acepta con o sin guion; cuerpo de 7 u 8 dígitos; DV módulo 11 válido. Inválido genera rechazo `RUT inválido`. | `Solo en memoria`, luego HMAC `cliente_id` |
| `Nombre` | `str` | texto | Nombre del cliente. | **Ignorado por completo**: no se valida, no rechaza y no se persiste. | `Excluida` |
| `Email` | `str` | email | Correo del cliente. | **Ignorado por completo**: no se valida, no rechaza y no se persiste. | `Excluida` |
| `Alta` | `str` | fecha | Fecha de alta del cliente. | `parse_date_dmy`: `dd/mm/yyyy` o `dd-mm-yyyy`; mínimo `2000-01-01`. Inválida genera rechazo `alta inválida`. Año posterior al actual deja `WARNING` y `alta = NULL` (fila válida). | `Silver.clientes.alta` |
| `Canal` | `str` | enumerado | Canal de alta: `Local` o `Web`. | `normalize_canal`: sin distinguir mayúsculas; placeholder o distinto genera rechazo `canal inválido`. | `Silver.clientes.canal` |
| `Cumple.` | `str` | día y mes | Cumpleaños sin año, por ejemplo `14 de junio` o `14 - junio`. | `parse_day_month_es`: tolera acentos, mayúsculas, espacios y puntuación final; valida el día contra el mes. Solo se conserva el **mes**. Inválido deja `mes_cumple = NULL` y **no rechaza**. | `Silver.clientes.mes_cumple` |
| `Puntos` | `str` | entero mayor o igual a 0 | Puntos de fidelización. | `parse_non_negative_bigint_or_zero`: vacío o placeholder pasan a 0; texto no numérico o negativo genera rechazo `puntos inválido`. | `Silver.clientes.puntos` |
| `Compras` | `str` | entero CLP mayor o igual a 0 | **Monto monetario histórico en CLP**, no cantidad de transacciones. | `parse_non_negative_bigint_or_zero`; inválido genera rechazo `compras inválido`. | `Silver.clientes.compras` |
| `Canjes` | `str` | entero mayor o igual a 0 | Canjes de puntos. | `parse_non_negative_bigint_or_zero`; inválido genera rechazo `canjes inválido`. | `Silver.clientes.canjes` |

Reglas específicas de clientes:

- **`cliente_id`**: HMAC-SHA256 del RUT normalizado (`cuerpo-DV`) usando `PII_HASH_KEY`;
  resultado hexadecimal de 64 caracteres. Es determinístico: la misma clave y el mismo RUT
  producen el mismo ID. Cambiar la clave cambia **todos** los IDs.
- **Edad estimada**: se calcula **solo desde el RUT**, nunca desde `Cumple.`. La fórmula es
  `millones = int(cuerpo[:3]) / 10 ** (9 - len(cuerpo))`; se recorre `EDAD_POR_TRAMO` y se
  asigna una edad asumida; año de nacimiento = año de referencia menos edad asumida;
  `edad = reference_year - year`. Es una **heurística**, no la edad real. Rango aceptado de
  0 a 120.
- **Rechazos**: se registran en `DEBUG` por fila y en un resumen `INFO`. **No se persisten
  en `cuarentena`**: `run()` descarta la lista de rechazos. Motivos posibles: `RUT inválido`,
  `alta inválida`, `canal inválido`, `puntos inválido`, `compras inválido`, `canjes inválido`,
  `año estimado inválido`, `edad inválida`.
- **Carga**: reemplazo total (`TRUNCATE` + `COPY`) en una transacción. No hay historial ni
  borrado lógico.

### 2.5 Stock de librería

| Atributo | Valor |
|---|---|
| Ruta | `bronze/STOCK LIBRERIA.xlsx` |
| Formato real | XLSX |
| Lector | `read_excel_as_str` |
| Columnas | 10, todas obligatorias |
| Granularidad | 1 fila = 1 ISBN |
| Pipeline | `etl/pipelines/stock.py` |
| Destino | `Silver.stock`, `Silver.stock_historial` y `Silver.cuarentena` (rechazos) |

| Columna cruda | Tipo de lectura | Tipo semántico | Descripción | Limpieza / validación | Destino |
|---|---|---|---|---|---|
| `ISBN` | `str` | ISBN-10 o ISBN-13 | Identificador del libro. | `normalize_isbn`: quita `-`, espacios y `.`; mayúsculas; solo valida forma: 13 dígitos o 9 dígitos más dígito o `X`. **No valida DV ni prefijo 978/979.** Inválido o vacío genera rechazo `ISBN inválido o vacío`. | `Silver.stock.isbn` |
| `Titulo` | `str` | texto | Título del libro. | `_clean_text`: colapsa espacios; placeholder pasa a `NULL`. Sin límite de longitud. | `Silver.stock.titulo` |
| `Autor` | `str` | texto | Autor del libro. | `_clean_text`; placeholder pasa a `NULL`. | `Silver.stock.autor` |
| `Editorial` | `str` | texto | Editorial. | `_clean_text`; placeholder pasa a `NULL`. | `Silver.stock.editorial` |
| `Clasificación` | `str` | texto | Categoría o clasificación comercial. | `_clean_text`; placeholder pasa a `NULL`. | `Silver.stock.clasificacion` |
| `PVP` | `str` | entero CLP mayor o igual a 0 | Precio de venta al público. | `parse_non_negative_bigint(default=None)`; además el pipeline exige un valor mayor o igual a 100 (más estricto que el DDL). Inválido o menor a 100 genera `STOCK-PVP-001`. | `Silver.stock.pvp` |
| `Stock Consig.` | `str` | entero mayor o igual a 0 | Unidades en consignación. | `parse_non_negative_bigint_or_zero`: vacío o placeholder pasan a 0; inválido genera `STOCK-VALIDACION`. | `Silver.stock.stock_consignacion` |
| `Stock Propio` | `str` | entero mayor o igual a 0 | Unidades propias. | `parse_non_negative_bigint_or_zero`; inválido genera `STOCK-VALIDACION`. | `Silver.stock.stock_propio` |
| `Stock Total` | `str` | entero mayor o igual a 0 | Unidades totales. | `parse_non_negative_bigint(default=None)`; inválido o negativo genera `STOCK-VALIDACION`; si no es exactamente `Stock Consig. + Stock Propio` genera `STOCK-VALIDACION` con motivo `stock total inconsistente`. | `Silver.stock.stock_total` |
| `Ventas` | `str` | entero mayor o igual a 0 | Unidades vendidas informadas por la fuente. | `parse_non_negative_bigint_or_zero`; inválido genera `STOCK-VALIDACION`. | `Silver.stock.ventas` |

Reglas específicas de stock:

- Reglas con código: `STOCK-VALIDACION` y `STOCK-PVP-001`; severidad `error`; una fila
  rechazada genera **una fila de cuarentena por cada detalle**, con `entidad = stock`.
- **Duplicados por ISBN**: se conserva la última fila, se registra `WARNING` y **no** van a
  cuarentena.
- **Carga**: upsert por ISBN. `valid_from` no se actualiza; `updated_at` y `_loaded_at` sí.
  Después, `_refresh_history` cierra y abre versiones SCD2 en `stock_historial`.
- Los textos no tienen límite de longitud en el DDL (`TEXT`).

### 2.6 Catálogo de cafetería

| Atributo | Valor |
|---|---|
| Ruta | `bronze/CATALOGO CAFETERIA.csv` |
| Formato real | CSV UTF-8 con BOM, separador coma |
| Lector | `read_csv_as_str` |
| Columnas en el archivo | 4 |
| Columnas obligatorias | 2: `codigo`, `precio` |
| Granularidad | 1 fila = 1 producto de cafetería |
| Uso | Catálogo de precio a códigos para reconstruir líneas de `CAFETERIA` |
| Destino | **Solo en memoria**. No se crea tabla en Silver. |

| Columna cruda | Tipo de lectura | Tipo semántico | Descripción | Limpieza / validación | Destino |
|---|---|---|---|---|---|
| `codigo` | `str` | texto | Código interno del producto. | `_clean_text`: colapsa espacios; placeholder pasa a `NULL`. | `Solo en memoria`, luego `ventas_linea.codigo_interno` |
| `nombre` | `str` | texto | Nombre del producto. | Se lee pero **se ignora y no se persiste**. | `Excluida` |
| `categoria` | `str` | texto | Categoría del producto. | Se lee pero **se ignora y no se persiste**. | `Excluida` |
| `precio` | `str` | entero CLP mayor o igual a 0 | Precio unitario. | `parse_non_negative_bigint(default=None)`; inválido hace que la fila se omita. | `Solo en memoria`, luego `ventas_linea.precio_unitario` |

Reglas específicas:

- Filas con `codigo` nulo o `precio` inválido: `WARNING` y se omiten **en memoria**. **No
  van a cuarentena** y no se persisten.
- Brechas: no existe dimensión de producto de cafetería; `nombre` y `categoria` se pierden;
  el archivo no entra al manifiesto.

### 2.7 Clima diario (7 archivos `.xls` que son HTML)

| Archivo | Variable | Columna Silver |
|---|---|---|
| `TemperaturaMax2025.xls` | Temperatura máxima | `temp_max` |
| `TemperaturaMin2025.xls` | Temperatura mínima | `temp_min` |
| `TemperaturaMedia2025.xls` | Temperatura media | `temp_media` |
| `HumedadRelativaAire2025.xls` | Humedad relativa | `humedad_relativa` |
| `IndiceUV2025.xls` | Índice UV | `indice_uv` |
| `Precipitaciones2025.xls` | Precipitación | `precipitacion_mm` |
| `Viento2025.xls` | Viento (dirección y velocidad) | `viento_direccion`, `viento_velocidad_kt` |

| Atributo | Valor |
|---|---|
| Directorio | `bronze/` |
| Formato real | Tablas HTML con extensión `.xls` |
| Lector | `read_html_grid_as_str` (`lxml.html`) |
| Archivos obligatorios | Los 7: si falta uno, `FileNotFoundError` y no hay carga |
| Año | `YEAR = 2025` fijo en el código |
| Grilla | Encabezado `Dia` más 12 meses (`Enero` a `Diciembre`), 31 filas |
| Contrato del lector | Exige exactamente 13 columnas y los días 1 a 31; si no, `ValueError` |
| Granularidad | 1 fila = 1 fecha |
| Destino | `Silver.clima_diario` |

Columnas de la grilla: `Dia`, `Enero`, `Febrero`, `Marzo`, `Abril`, `Mayo`, `Junio`,
`Julio`, `Agosto`, `Septiembre`, `Octubre`, `Noviembre`, `Diciembre`.

| Columna cruda | Tipo de lectura | Tipo semántico | Descripción | Limpieza / validación | Destino |
|---|---|---|---|---|---|
| `Dia` | `str` | entero de 1 a 31 | Día del mes. | `int(record['Dia'])`; la fecha se arma con `date(2025, mes, día)`. Días imposibles (29/02, 31/04, etc.) se omiten silenciosamente. | `Silver.clima_diario.fecha` |
| `Enero` a `Diciembre` (temperaturas) | `str` | decimal | Temperatura en grados. | `parse_decimal` con rango de -50 a 60; la coma se interpreta como separador decimal; inválido pasa a `NULL`. | `temp_max`, `temp_min`, `temp_media` |
| `Enero` a `Diciembre` (humedad) | `str` | decimal | Humedad relativa. | `parse_decimal` de 0 a 100; inválido pasa a `NULL`. | `humedad_relativa` |
| `Enero` a `Diciembre` (UV) | `str` | entero | Índice UV. | `parse_integer` de 0 a 20; un punto pasa a `NULL`; inválido pasa a `NULL`. | `indice_uv` |
| `Enero` a `Diciembre` (precipitación) | `str` | decimal en mm | Precipitación. | `s/p` pasa a `0.0`; el resto usa `parse_decimal` con mínimo 0; inválido pasa a `NULL`. | `precipitacion_mm` |
| `Enero` a `Diciembre` (viento) | `str` | dirección y entero en kt | Viento observado. | `CALMA` pasa a dirección `CALMA` y velocidad 0; formato `DIRECCION/velocidad` con dirección en `N`, `NE`, `E`, `SE`, `S`, `SW`, `W`, `NW`, `VRB`; malformado pasa a `NULL, NULL`. | `viento_direccion`, `viento_velocidad_kt` |

Reglas específicas de clima:

- Valores inválidos quedan `NULL`, **sin cuarentena**.
- Se inserta **una fila por cada fecha real del año**: 365 filas para 2025.
- Los `CHECK` de coherencia de temperaturas (`temp_min` menor o igual a `temp_max`, y
  `temp_media` entre ambas) **son solo de PostgreSQL**: el pipeline no los valida. Un dato
  incoherente puede abortar la carga completa del lote.
- No hay `_source_file`: no se puede saber de qué archivo provino cada valor.

### 2.8 Resumen del catálogo Bronce

| Archivo | Entidad | Owner | Consumidor | Criticidad | Estado | PII |
|---|---|---|---|---|---|---|
| `bronze/CLIENTES LIBRERIA.xlsx` | clientes | `data-governance-steward` (PII); `silver-etl-engineer` (técnico) | Fidelización y marketing; cuadre de ventas | alta | existe | alta (RUT, nombre, email) |
| `bronze/STOCK LIBRERIA.xlsx` | stock | Jefatura de librería; `silver-etl-engineer` | Compras y reposición; catálogo de precios de ventas | alta | existe | baja (`autor`) |
| `bronze/VENTAS LIBRERIA.xlsx` | ventas | Jefatura comercial; `silver-etl-engineer` | BI comercial (a crear) | alta | existe | ninguna |
| `bronze/VENTAS CAFETERIA.xlsx` | ventas | Jefatura de cafetería; `silver-etl-engineer` | BI de cafetería (a crear) | media | existe | ninguna |
| `bronze/CATALOGO CAFETERIA.csv` | catálogo cafetería | Jefatura de cafetería; `silver-etl-engineer` | `ventas`, solo en memoria | media | existe | ninguna |
| 7 archivos `.xls` de clima | clima | `gold-analytics-modeler` (consumo); `silver-etl-engineer` (técnico) | BI y operación (a crear) | baja | existe | ninguna |
| `bronze/manifest.jsonl` | metadatos de ingesta | `data-quality-steward` | Gobierno de datos y calidad | media | existe | ninguna |

---

## 3. Catálogo de la capa Silver

Convenciones de las tablas de columnas: `Nulo` indica si el DDL admite `NULL`;
`Clave / default` resume PK, FK, default y `NOT NULL`. El campo `Origen` indica la columna
cruda o el cálculo que produce el valor.

### 3.1 `ventas_ticket`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por ticket de venta |
| Clave primaria | `(origen, correlativo)` |
| Modo de carga | Upsert incremental por `(origen, correlativo)`; `updated_at` se actualiza en cada upsert |
| Origen | `VENTAS LIBRERIA.xlsx`, `VENTAS CAFETERIA.xlsx` |
| Owner | `silver-etl-engineer` (técnico); negocio: jefatura comercial |
| Consumidor | Equipo comercial y BI (marts a crear) |
| Criticidad | alta |
| Estado | existe |
| PII | baja (`cliente_id` es seudónimo) |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `origen` | `TEXT` | no | PK; `CHECK` en `LIBRERIA` o `CAFETERIA` | Nombre del archivo | `inferir_origen` | No existe como columna cruda. |
| `correlativo` | `BIGINT` | no | PK; `CHECK` mayor que 0 | `Correlativo` | `parse_integer(minimum=1)` | Único solo junto con `origen`. |
| `fecha` | `DATE` | no | sin default | `Fecha` | `parse_date_iso` estricto `YYYY-MM-DD` | `dd/mm/yyyy` se rechaza. |
| `hora_simulada` | `TIME` | sí | sin default | Derivada | SHA-256 de `origen`, `correlativo` y `fecha` | Sintética; no viene del POS; domingo deja `NULL`. |
| `cliente_id` | `VARCHAR(64)` | sí | FK a `clientes(cliente_id)` | Cálculo in-memory | HMAC-SHA256 del RUT, asignado por subset sum exacto | Solo `LIBRERIA`; `CAFETERIA` siempre `NULL`. |
| `medio_pago` | `TEXT` | sí | sin default | `Medio de Pago` | `_clean_text` | Placeholder pasa a `NULL`. |
| `tipo_documento` | `TEXT` | sí | sin default | `Tipo Documento` | `_clean_text`; `Sin Documento` rechaza | Vacío pasa a `NULL`. |
| `folio` | `TEXT` | sí | sin default | `Folio` | `_clean_text`; ceros o vacío rechazan | **No es único**; no hay unicidad de documento. |
| `total_venta` | `NUMERIC(12,0)` | no | `CHECK` mayor o igual a 0 | `Total Venta` | `parse_non_negative_bigint` | CLP sin decimales. |
| `cant_items` | `INTEGER` | no | `CHECK` mayor que 0 | `Cant. Items` | `parse_integer(minimum=1)` | Debe cuadrar con las líneas. |
| `tiene_descuento` | `TEXT` | sí | sin default | `Tiene Descuento` | `normalize_si_no`, produce `SI`, `NO` o `NULL` | Valor no reconocido no rechaza. |
| `es_devolucion` | `TEXT` | sí | sin default | `Es Dev.` | `normalize_si_no`, produce `SI`, `NO` o `NULL` | Valor no reconocido no rechaza. |
| `estado` | `TEXT` | sí | sin default | `Estado` | `_clean_text` | Sin restricción de valores. |
| `_source_file` | `TEXT` | no | sin default | Basename del archivo | — | No guarda ruta ni SHA. |
| `_loaded_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | UTC | Igual para todo el lote. |
| `updated_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | Se sobrescribe en cada upsert | Permite saber la última vez visto. |

Constraints e índices:

- PK `(origen, correlativo)`.
- FK `ventas_ticket_cliente_fk`: `cliente_id` referencia `clientes(cliente_id)`.
- `CHECK ventas_ticket_origen_valido`: `origen` en `LIBRERIA` o `CAFETERIA`.
- `CHECK ventas_ticket_correlativo_positivo`: `correlativo` mayor que 0.
- `CHECK ventas_ticket_total_no_negativo`: `total_venta` mayor o igual a 0.
- `CHECK ventas_ticket_cant_items_positiva`: `cant_items` mayor que 0.
- Índices: `ventas_ticket_fecha_idx (fecha)`, `ventas_ticket_cliente_id_idx (cliente_id)` y
  `ventas_ticket_origen_fecha_idx (origen, fecha)`.
- Comentarios DDL: `hora_simulada` es sintética; `cliente_id` es HMAC-SHA256 y `NULL` si el
  ticket es anónimo.

### 3.2 `ventas_linea`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por línea reconstruida de un ticket |
| Clave primaria | `(origen, correlativo, linea)` |
| Modo de carga | Borrado de las líneas de los tickets del lote y `COPY` |
| Origen | Derivado de `ventas_ticket` y del catálogo de precios |
| Owner | `silver-etl-engineer` |
| Consumidor | BI y análisis de productos (marts a crear) |
| Criticidad | alta |
| Estado | existe |
| PII | ninguna |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `origen` | `TEXT` | no | PK y FK compuesta | Ticket | — | `LIBRERIA` o `CAFETERIA`. |
| `correlativo` | `BIGINT` | no | PK y FK compuesta | Ticket | — | — |
| `linea` | `INTEGER` | no | PK; `CHECK` mayor que 0 | Cálculo | Enumera los precios distintos ordenados de menor a mayor | No es el número de línea del POS. |
| `codigo_interno` | `TEXT` | no | sin FK | Catálogo | `_pick_code`: SHA-256 de origen, correlativo, línea y precio | Apunta lógicamente a `stock.isbn` o al catálogo de cafetería. |
| `cantidad` | `INTEGER` | no | `CHECK` mayor que 0 | Cálculo | Multiplicidad del precio en la multiset | — |
| `precio_unitario` | `NUMERIC(12,0)` | no | `CHECK` mayor o igual a 0 | Catálogo | Precio elegido por la reconstrucción | — |
| `total_linea` | `NUMERIC(12,0)` | no | `CHECK` mayor o igual a 0 | Cálculo | `cantidad * precio_unitario` | Cuadra con `total_venta`. |
| `_loaded_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | UTC | Sin `_source_file`. |

Constraints e índices:

- PK `(origen, correlativo, linea)`.
- FK `ventas_linea_ticket_fk`: `(origen, correlativo)` referencia
  `ventas_ticket(origen, correlativo)` con `ON DELETE CASCADE`.
- `CHECK ventas_linea_linea_positiva`: `linea` mayor que 0.
- `CHECK ventas_linea_cantidad_positiva`: `cantidad` mayor que 0.
- `CHECK ventas_linea_precio_no_negativo`: `precio_unitario` mayor o igual a 0.
- `CHECK ventas_linea_total_no_negativo`: `total_linea` mayor o igual a 0.
- Índices: `ventas_linea_ticket_idx (origen, correlativo)` y
  `ventas_linea_codigo_interno_idx (codigo_interno)`.

Observaciones: en cada corrida se borran las líneas de los tickets del lote y se insertan de
nuevo. `codigo_interno` no tiene FK y no representa el producto realmente vendido: se elige
de forma determinística entre los códigos del mismo precio.

### 3.3 `clientes`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por cliente seudonimizado |
| Clave primaria | `cliente_id` |
| Modo de carga | Reemplazo total (`TRUNCATE` + `COPY` en una transacción) |
| Origen | `CLIENTES LIBRERIA.xlsx` |
| Owner | `data-governance-steward` (PII); `silver-etl-engineer` (técnico) |
| Consumidor | Fidelización, marketing y cuadre de ventas |
| Criticidad | alta |
| Estado | existe |
| PII | media (seudónimo y cuasi-identificadores) |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `cliente_id` | `VARCHAR(64)` | no | PK; `CHECK` de 64 hexadecimales | `Rut cliente` | HMAC-SHA256 con `PII_HASH_KEY` | Determinístico; rotar la clave invalida todos los IDs. |
| `alta` | `DATE` | sí | sin default | `Alta` | `dd/mm/yyyy` o `dd-mm-yyyy`; mínimo `2000-01-01`; año posterior al año actual deja `NULL` con `WARNING` | Sin hora ni zona. |
| `canal` | `VARCHAR(10)` | no | `CHECK` en `Local` o `Web` | `Canal` | Normalizado a `Local` o `Web` | Placeholder rechaza. |
| `edad` | `INTEGER` | no | `CHECK` entre 0 y 120 | RUT | Heurística `EDAD_POR_TRAMO` | **Estimación**, no edad real. |
| `mes_cumple` | `INTEGER` | sí | `CHECK NULL` o entre 1 y 12 | `Cumple.` | Solo se conserva el mes | Se pierde el día. |
| `puntos` | `BIGINT` | no | `DEFAULT 0`; `CHECK` mayor o igual a 0 | `Puntos` | Vacío o placeholder pasan a 0 | — |
| `compras` | `BIGINT` | no | `DEFAULT 0`; `CHECK` mayor o igual a 0 | `Compras` | Vacío o placeholder pasan a 0 | **Monto histórico en CLP**, no cantidad. |
| `canjes` | `BIGINT` | no | `DEFAULT 0`; `CHECK` mayor o igual a 0 | `Canjes` | Vacío o placeholder pasan a 0 | — |

Constraints e índices: PK `cliente_id`; restricciones `clientes_cliente_id_formato`,
`clientes_canal_valido`, `clientes_edad_valida`, `clientes_mes_cumple_valido`,
`clientes_puntos_no_negativo`, `clientes_compras_no_negativo` y
`clientes_canjes_no_negativo`. No hay índices adicionales.

Observaciones: **no persiste RUT, nombre, email ni fecha completa de cumpleaños**. Tampoco
persiste `_source_file` ni `_loaded_at`. La carga es destructiva y sin historial: una fila
que desaparece del Excel desaparece de Silver.

### 3.4 `stock`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por ISBN (estado actual) |
| Clave primaria | `isbn` |
| Modo de carga | Upsert incremental |
| Origen | `STOCK LIBRERIA.xlsx` |
| Owner | Jefatura de librería (negocio); `silver-etl-engineer` (técnico) |
| Consumidor | Compras, reposición y reportes de quiebre (a crear) |
| Criticidad | alta |
| Estado | existe |
| PII | baja (solo `autor` como dato profesional público) |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `isbn` | `TEXT` | no | PK; `CHECK` de formato | `ISBN` | Normalización de forma | No valida DV ni prefijo. |
| `titulo` | `TEXT` | sí | sin default | `Titulo` | `_clean_text` | Sin límite de longitud. |
| `autor` | `TEXT` | sí | sin default | `Autor` | `_clean_text` | Dato profesional, no de cliente. |
| `editorial` | `TEXT` | sí | sin default | `Editorial` | `_clean_text` | — |
| `clasificacion` | `TEXT` | sí | sin default | `Clasificación` | `_clean_text` | — |
| `pvp` | `NUMERIC(12,0)` | no | `CHECK` mayor o igual a 0 | `PVP` | El pipeline exige mayor o igual a 100 | Discrepancia pipeline y DDL. |
| `stock_consignacion` | `INTEGER` | no | `DEFAULT 0`; `CHECK` mayor o igual a 0 | `Stock Consig.` | Vacío o placeholder pasan a 0 | — |
| `stock_propio` | `INTEGER` | no | `DEFAULT 0`; `CHECK` mayor o igual a 0 | `Stock Propio` | Vacío o placeholder pasan a 0 | — |
| `stock_total` | `INTEGER` | no | `CHECK` mayor o igual a 0 y suma consistente | `Stock Total` | Debe cuadrar exactamente con consignación más propio | — |
| `ventas` | `INTEGER` | no | `DEFAULT 0`; `CHECK` mayor o igual a 0 | `Ventas` | Vacío o placeholder pasan a 0 | Unidades informadas por la fuente. |
| `valid_from` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | No se actualiza en el upsert | Primera vez que se cargó el ISBN; no marca la versión actual. |
| `updated_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | Se actualiza en cada upsert | Última vez visto. |
| `_source_file` | `TEXT` | no | sin default | Basename | — | No guarda ruta ni SHA. |
| `_loaded_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | UTC | — |

Constraints e índices: PK `isbn`; restricciones `stock_isbn_formato`,
`stock_pvp_no_negativo`, `stock_consignacion_no_negativa`, `stock_propio_no_negativo`,
`stock_total_no_negativo`, `stock_total_consistente` y `stock_ventas_no_negativas`. Sin
índices adicionales.

Observaciones: no valida el dígito verificador del ISBN; no detecta ausencias (un ISBN que
desaparece del archivo permanece en la tabla) ni marca obsolescencia.

### 3.5 `stock_historial`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por versión SCD2 de un ISBN |
| Clave primaria | `id` |
| Modo de carga | Cierre de la versión vigente y alta de versión nueva |
| Origen | Derivado de `stock` en la misma transacción |
| Owner | Jefatura de librería (negocio); `silver-etl-engineer` (técnico) |
| Consumidor | Análisis de cambios de precio y stock (a crear) |
| Criticidad | media |
| Estado | existe |
| PII | ninguna |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `id` | `BIGINT` | no | PK; `GENERATED ALWAYS AS IDENTITY` | Secuencia | — | — |
| `isbn` | `TEXT` | no | FK a `stock(isbn)`; `CHECK` de formato | `stock.isbn` | — | — |
| `titulo` | `TEXT` | sí | sin default | `stock.titulo` | — | — |
| `autor` | `TEXT` | sí | sin default | `stock.autor` | — | — |
| `editorial` | `TEXT` | sí | sin default | `stock.editorial` | — | — |
| `clasificacion` | `TEXT` | sí | sin default | `stock.clasificacion` | — | — |
| `pvp` | `NUMERIC(12,0)` | no | `CHECK` mayor o igual a 0 | `stock.pvp` | — | — |
| `stock_consignacion` | `INTEGER` | no | `CHECK` mayor o igual a 0 | `stock.stock_consignacion` | — | — |
| `stock_propio` | `INTEGER` | no | `CHECK` mayor o igual a 0 | `stock.stock_propio` | — | — |
| `stock_total` | `INTEGER` | no | `CHECK` mayor o igual a 0 y suma consistente | `stock.stock_total` | — | — |
| `ventas` | `INTEGER` | no | `CHECK` mayor o igual a 0 | `stock.ventas` | — | — |
| `valid_from` | `TIMESTAMPTZ` | no | `DEFAULT now()` | `updated_at` de la carga | Marca el inicio de la versión | — |
| `valid_to` | `TIMESTAMPTZ` | sí | `CHECK NULL` o mayor o igual a `valid_from` | `updated_at` de la carga que cierra | `NULL` si es vigente | — |
| `is_current` | `BOOLEAN` | no | `DEFAULT TRUE` | Cálculo | `TRUE` solo en la versión vigente | — |
| `_source_file` | `TEXT` | no | sin default | `stock._source_file` | — | — |
| `_loaded_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | `stock._loaded_at` | — | — |

Constraints e índices: PK `id`; FK `isbn`; restricciones `stock_historial_isbn_formato`,
`stock_historial_pvp_no_negativo`, `stock_historial_stock_no_negativo`,
`stock_historial_total_consistente`, `stock_historial_vigencia` (equivalencia entre
`is_current` y `valid_to IS NULL`) y `stock_historial_rango`; índice único parcial
`stock_historial_isbn_actual_uidx (isbn) WHERE is_current`.

Regla SCD2 implementada:

- **Cierre**: se cierra la versión vigente si **cualquier** columna de negocio cambió,
  comparando con `IS DISTINCT FROM` unidas con `OR`. Es null-safe: `NULL` a valor y valor a
  `NULL` cuentan como cambio.
- **Alta**: se abre una versión nueva solo si **no existe** una versión vigente idéntica,
  comparando con `IS NOT DISTINCT FROM` unidas con `AND`.
- `valid_from` toma el `updated_at` de la carga; `valid_to` toma el `updated_at` de la carga
  que cierra.
- No existe `updated_at` en el historial.
- **No historiza ausencias**: si un ISBN deja de venir en el archivo, no se cierra su
  versión.

### 3.6 `clima_diario`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por fecha |
| Clave primaria | `fecha` |
| Modo de carga | Upsert por `fecha`; no borra años anteriores |
| Origen | 7 archivos `.xls` de clima |
| Owner | `gold-analytics-modeler` (consumo); `silver-etl-engineer` (técnico) |
| Consumidor | BI, operación y análisis estacional (a crear) |
| Criticidad | baja |
| Estado | existe |
| PII | ninguna |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `fecha` | `DATE` | no | PK | Grilla `Dia` y mes | `date(2025, mes, dia)` | Año fijo 2025. |
| `temp_max` | `NUMERIC(4,1)` | sí | `CHECK` de -50 a 60 | `TemperaturaMax2025.xls` | `parse_decimal` | Inválido pasa a `NULL`. |
| `temp_min` | `NUMERIC(4,1)` | sí | `CHECK` de -50 a 60 | `TemperaturaMin2025.xls` | `parse_decimal` | Inválido pasa a `NULL`. |
| `temp_media` | `NUMERIC(4,1)` | sí | `CHECK` de -50 a 60 | `TemperaturaMedia2025.xls` | `parse_decimal` | Inválido pasa a `NULL`. |
| `humedad_relativa` | `NUMERIC(5,1)` | sí | `CHECK` de 0 a 100 | `HumedadRelativaAire2025.xls` | `parse_decimal` | Inválido pasa a `NULL`. |
| `indice_uv` | `SMALLINT` | sí | `CHECK` de 0 a 20 | `IndiceUV2025.xls` | `parse_integer` | Un punto pasa a `NULL`. |
| `precipitacion_mm` | `NUMERIC(6,1)` | sí | `CHECK` mayor o igual a 0 | `Precipitaciones2025.xls` | `s/p` pasa a `0.0`; el resto es decimal | Inválido pasa a `NULL`. |
| `viento_direccion` | `VARCHAR(5)` | sí | `CHECK` de lista más `CALMA` | `Viento2025.xls` | `parse_wind` | `CALMA` es valor válido. |
| `viento_velocidad_kt` | `SMALLINT` | sí | `CHECK` mayor o igual a 0 | `Viento2025.xls` | `parse_wind` | Nudos. |
| `_loaded_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | UTC | Sin `_source_file`. |

Constraints: PK `fecha`; `clima_temp_max_rango`, `clima_temp_min_rango`,
`clima_temp_media_rango`, `clima_temperaturas_coherentes`, `clima_media_en_rango`,
`clima_humedad_rango`, `clima_uv_rango`, `clima_precipitacion_no_negativa`,
`clima_viento_direccion_valida` y `clima_viento_velocidad_no_negativa`. Sin índices
adicionales.

Observaciones: todas las mediciones son nullable; solo `fecha` es obligatoria. No hay
cuarentena. Los `CHECK` de coherencia pueden abortar la carga completa.

### 3.7 `cuarentena`

| Atributo | Valor |
|---|---|
| Grano | 1 fila por detalle de rechazo |
| Clave primaria | `id` |
| Modo de carga | Append-only |
| Origen | Rechazos de `stock` y `ventas` |
| Owner | `data-quality-steward` |
| Consumidor | `data-quality-steward`, `silver-etl-engineer` y negocio para corregir el origen |
| Criticidad | media |
| Estado | existe |
| PII | media (potencial; hoy no contiene PII de clientes) |

| Columna | Tipo SQL | Nulo | Clave / default | Origen | Regla de transformación | Observaciones |
|---|---|---|---|---|---|---|
| `id` | `BIGINT` | no | PK; `GENERATED ALWAYS AS IDENTITY` | Secuencia | — | — |
| `entidad` | `TEXT` | no | sin default | Pipeline | `stock` o `ventas` | `clientes` y `clima` no escriben. |
| `archivo_origen` | `TEXT` | no | sin default | Basename | `source_file` | Agrupa por archivo en ventas. |
| `fila` | `INTEGER` | sí | sin default | `enumerate(start=2)` | Fila Excel 1-based: encabezado es 1, primer dato es 2 | — |
| `columna` | `TEXT` | sí | sin default | Detalle | Nombre de la columna cruda | — |
| `valor_crudo` | `TEXT` | sí | sin default | Detalle | `str(valor)` | Puede contener PII si una entidad futura la escribe. |
| `regla` | `TEXT` | no | sin default | Detalle | Código de regla; `SIN-REGLA` si falta | Ver sección 4.2. |
| `severidad` | `TEXT` | no | `DEFAULT error`; `CHECK` en `warning` o `error` | Detalle | Hoy siempre `error` | `warning` no se usa en la práctica. |
| `motivo` | `TEXT` | no | sin default | Detalle | Texto legible | — |
| `_loaded_at` | `TIMESTAMPTZ` | no | `DEFAULT now()` | Reloj del pipeline | UTC | — |

Constraints e índices: PK `id`; índice `cuarentena_entidad_loaded_at_idx`
`(entidad, _loaded_at DESC)`.

Observaciones: escrita por `stock` (entidad `stock`) y `ventas` (entidad `ventas`, una fila
por detalle, agrupada por archivo). **`clientes` y `clima` no escriben cuarentena.** Es
append-only: una recarga vuelve a insertar los mismos rechazos. No guarda SHA-256, `run_id`
ni la clave natural del ticket o ISBN, por lo que no se puede deduplicar ni rastrear la
corrida.

---

## 4. Linaje, reglas y limitaciones conocidas

### 4.1 Matriz de linaje

| Entidad | Archivo crudo | Checksum / manifiesto | Pipeline | DDL | Tabla o tablas Silver | Columnas producidas | Consumo final |
|---|---|---|---|---|---|---|---|
| Clientes | `bronze/CLIENTES LIBRERIA.xlsx` | `manifest.jsonl` (`.xlsx`) | `etl/pipelines/clientes.py` | `sql/clientes.sql` | `clientes` | `cliente_id`, `alta`, `canal`, `edad`, `mes_cumple`, `puntos`, `compras`, `canjes` | Cuadre de compras en `ventas`; fidelización (reportes a crear) |
| Stock | `bronze/STOCK LIBRERIA.xlsx` | `manifest.jsonl` (`.xlsx`) | `etl/pipelines/stock.py` | `sql/stock.sql` | `stock`, `stock_historial`, `cuarentena` | `isbn`, `titulo`, `autor`, `editorial`, `clasificacion`, `pvp`, `stock_consignacion`, `stock_propio`, `stock_total`, `ventas` y vigencia SCD2 | Compras y reposición; catálogo de precios de ventas; reportes (a crear) |
| Ventas librería | `bronze/VENTAS LIBRERIA.xlsx` | `manifest.jsonl` (`.xlsx`) | `etl/pipelines/ventas.py` | `sql/ventas.sql` | `ventas_ticket`, `ventas_linea`, `cuarentena` | Cabecera completa y líneas reconstruidas | BI comercial (marts a crear) |
| Ventas cafetería | `bronze/VENTAS CAFETERIA.xlsx` | `manifest.jsonl` (`.xlsx`) | `etl/pipelines/ventas.py` | `sql/ventas.sql` | `ventas_ticket`, `ventas_linea`, `cuarentena` | Cabecera completa y líneas reconstruidas; `cliente_id` siempre `NULL` | BI de cafetería (a crear) |
| Catálogo cafetería | `bronze/CATALOGO CAFETERIA.csv` | **No registrado** | `etl/pipelines/ventas.py` | no aplica | no aplica | `codigo_interno` y `precio_unitario` en `ventas_linea` | Solo en memoria; sin dimensión de producto |
| Clima | `TemperaturaMax2025.xls`, `TemperaturaMin2025.xls`, `TemperaturaMedia2025.xls`, `HumedadRelativaAire2025.xls`, `IndiceUV2025.xls`, `Precipitaciones2025.xls`, `Viento2025.xls` (todos en `bronze/`) | `manifest.jsonl` (`.xls`, formato `html`) | `etl/pipelines/clima.py` | `sql/clima.sql` | `clima_diario` | `fecha`, temperaturas, humedad, UV, precipitación y viento | Análisis estacional (a crear) |
| Rechazos | No aplica | no aplica | `etl/quarantine.py` | `sql/cuarentena.sql` | `cuarentena` | `entidad`, `archivo_origen`, `fila`, `columna`, `valor_crudo`, `regla`, `severidad` y `motivo` | Corrección de calidad en origen |

### 4.2 Reglas por código

| Código | Entidad | Condición | Severidad | Destino | Notas |
|---|---|---|---|---|---|
| `VENTAS-VALIDACION` | ventas | `Correlativo`, `Fecha`, `Total Venta` o `Cant. Items` inválido | error | `cuarentena`, una fila por columna | `entidad = ventas`. |
| `VENTAS-FOLIO-001` | ventas | `Folio` vacío o compuesto solo por ceros | error | `cuarentena` | Se rechaza la fila completa. |
| `VENTAS-DOC-001` | ventas | `Tipo Documento` igual a `Sin Documento` | error | `cuarentena` | Comparación sin caja ni espacios repetidos. |
| `VENTAS-LINEA-001` | ventas | Sin combinación exacta de `cant_items` precios que sumen `total_venta` | error | `cuarentena` | Columna `total_venta`; motivo `sin combinación exacta de líneas`; el ticket no se inserta. |
| `STOCK-VALIDACION` | stock | ISBN, stocks o ventas inválidos; `Stock Total` inconsistente | error | `cuarentena`, una fila por detalle | `entidad = stock`. |
| `STOCK-PVP-001` | stock | `PVP` inválido o menor a 100 | error | `cuarentena` | El DDL solo exige mayor o igual a 0. |

Reglas **sin código** que no van a cuarentena:

| Entidad | Motivo | Registro |
|---|---|---|
| clientes | `RUT inválido`, `alta inválida`, `canal inválido`, `puntos inválido`, `compras inválido`, `canjes inválido`, `año estimado inválido`, `edad inválida` | `DEBUG` por fila y resumen `INFO` |
| stock | duplicados por ISBN | `WARNING` |
| ventas | duplicados por `(origen, correlativo)`; cliente sin subset sum; timeout de subset sum; domingo sin hora | `WARNING` |
| clientes | alta futura | `WARNING` |
| clima | valores inválidos | pasan a `NULL`, sin registro persistente |
| cafetería | producto inválido | `WARNING` y se omite en memoria |

### 4.3 Limitaciones conocidas

1. **Bronce es filesystem, no PostgreSQL.** No existe tabla Bronce ni carga de archivos a la
   base; solo Silver está en PostgreSQL.
2. **Gold no existe.** No hay hechos, dimensiones, marts ni KPIs persistidos: a crear con
   owner `gold-analytics-modeler`.
3. **No hay orquestador ni catálogo automatizado.** `etl/pipelines/__main__.py` es un runner
   secuencial sin schedule, reintentos ni alertas. Airflow, Dagster y un catálogo de
   metadatos son a crear.
4. **CI sin base real.** `pytest` usa fixtures sintéticos y no define `DATABASE_URL`; los
   tests de esquema leen el DDL como texto. Existen tests de integración PostgreSQL en
   `tests/test_stock_ventas_integration.py`, pero se omiten si no se define
   `TEST_DATABASE_URL` y el CI no los ejecuta. No hay formatter, typechecker ni cobertura.
   `pytest`, `ruff` y CI sí existen.
5. **Sin `load_id` ni SHA en Silver.** `_source_file` solo guarda el basename; no hay forma
   de unir una fila Silver con la versión exacta del manifiesto.
6. **El manifiesto no cubre el CSV de cafetería.** Los `.xls` de clima sí entran (formato
   detectado `html`), pero el CSV no tiene checksum ni versión.
7. **`clientes` es destructivo y sin historial.** Cada corrida reemplaza todo; no hay SCD2,
   no hay borrado lógico y no se conserva `_source_file` ni `_loaded_at`.
8. **Rotar `PII_HASH_KEY` invalida IDs y FK.** Cambia todos los `cliente_id`, deja huérfana
   la FK de `ventas_ticket.cliente_id` y obliga a regenerar clientes y ventas.
9. **`clientes.edad` es una estimación**, no la edad real. `mes_cumple` solo conserva el mes;
   el día se pierde en la ingesta.
10. **Ventas no persiste `Orden Web`, `Canal`, `Voucher Máquina de Pago` ni `Feria`.** No se
    puede analizar canal web o local ni voucher; `normalize_canal` de `ventas.py` está sin
    uso en la validación.
11. **`codigo_interno` es sintético y sin FK.** Se elige por hash entre los códigos del mismo
    precio; no es el producto vendido real. No hay dimensión de producto.
12. **`CAFETERIA` es siempre anónima.** El catálogo de cafetería no se persiste y se pierden
    `nombre` y `categoria`.
13. **`folio` no es único y es texto.** No hay unicidad de documento ni validación de
    secuencia tributaria.
14. **Stock no valida el DV del ISBN** ni el prefijo 978/979, y **no detecta ausencias ni
    obsolescencia**. El pipeline exige `PVP` mayor o igual a 100 mientras el DDL permite
    mayor o igual a 0.
15. **`stock_historial` no historiza ausencias.** Si un ISBN desaparece del archivo, su
    versión vigente queda abierta.
16. **Clima tiene año fijo 2025.** Los `CHECK` de coherencia son solo de base de datos; no
    hay cuarentena y una fila incoherente puede abortar el lote completo.
17. **`cuarentena` no tiene clave natural, SHA ni `run_id`.** Las recargas duplican filas y
    no se puede deduplicar ni rastrear la corrida. `clientes` y `clima` no persisten
    rechazos.
18. **Migraciones manuales y con referencias obsoletas.** No se ejecutan automáticamente.
    `sql/migrations/20261104_clientes_columnas_nullable.sql` referencia `nombre` y `email`,
    columnas que ya no existen en el DDL vigente. `sql/migrations/20261104_add_hora_simulada.sql`
    referencia la tabla plana `{{schema}}.ventas`, que la migración de split renombra a
    `{{schema}}.ventas_flat_backup`. Ambas fallarían en una base ya migrada. En bases
    migradas puede existir `ventas_flat_backup` como respaldo histórico: ningún pipeline la
    escribe y no forma parte del modelo vigente.
19. **`ventas --schema-only` aislado requiere `clientes`** por la FK de `cliente_id`; el
    runner ya ejecuta `clientes` primero.
20. **Enriquecimiento in-memory sin verificación contra Silver.** `ventas` lee clientes y
    stock desde los archivos, no desde la base. Si `silver.clientes` se cargó con otro
    archivo o con otra `PII_HASH_KEY`, la FK de `cliente_id` puede fallar y abortar el lote.
21. **La clave HMAC tiene un valor de desarrollo por defecto.** Si `PII_HASH_KEY` no se
    define, `etl/config.py` usa una clave conocida; en producción debe configurarse un
    secreto real fuera del repositorio.
22. **`parse_non_negative_bigint` no distingue separador decimal.** Elimina todos los puntos
    y comas, de modo que un monto como `1500.50` se interpreta como `150050`. Aceptable para
    CLP entero, pero es un riesgo si la fuente cambia de formato.
23. **No hay columnas de teléfono ni dirección** en los crudos inventariados. El Anexo A
    clasifica la PII existente; no hay teléfono ni dirección que enmascarar hoy.

---

## 5. Verificación

Comandos existentes en el repositorio. No se inventan comandos ni se ejecutan pipelines
contra PostgreSQL en este documento.

| Comando | Qué verifica | Efecto secundario |
|---|---|---|
| `.venv\Scripts\python.exe -m pytest` | Suite de tests con fixtures sintéticos; no usa base de datos. | Ninguno sobre Silver. |
| `.venv\Scripts\python.exe -m ruff check .` | Lint, errores e imports (`E`, `F`, `I`). | Ninguno. |
| `.venv\Scripts\python.exe -m etl.pipelines.clientes --dry-run` | Lectura del Excel, `require_columns` y transformación de clientes. | No escribe en PostgreSQL. |
| `.venv\Scripts\python.exe -m etl.pipelines.stock --dry-run` | Lectura, validación, deduplicación y resumen de cuarentena de stock. | No escribe en PostgreSQL. |
| `.venv\Scripts\python.exe -m etl.pipelines.ventas --dry-run` | Lectura de ambos archivos, validación, reconstrucción de líneas y cuadre de clientes en memoria. | **No abre conexión** a PostgreSQL. |
| `.venv\Scripts\python.exe -m etl.pipelines.clima --dry-run` | Lectura de los 7 archivos, armado de la grilla y conteo de nulos. | No escribe en PostgreSQL. |
| `.venv\Scripts\python.exe -m etl.ingest --dry-run` | Cálculo de SHA-256 y detección de versiones nuevas. | No escribe el manifiesto. |

Aclaraciones:

- En `ventas`, `--dry-run` **no abre conexión** por diseño: el enriquecimiento es in-memory.
- En `clientes`, `stock` y `clima`, `--dry-run` **no escribe** y retorna antes de invocar la
  conexión; el efecto es solo lectura y transformación en memoria.
- `--schema-only` existe y sí abre conexión para crear schema y tablas; no forma parte de la
  verificación documental de este catálogo.
- `--log-level DEBUG` muestra el motivo por fila descartada; úsese solo en entornos locales
  porque puede exponer valores crudos.

---

## Anexo A. Clasificación PII y enmascaramiento

### A.1 Niveles de clasificación

| Nivel | Definición | Ejemplos en Miralibros |
|---|---|---|
| `alta` | Identifica directa e inequívocamente a una persona. | RUT, nombre, email. |
| `media` | Cuasi-identificador: por sí solo no identifica, pero combinado sí. | Día y mes de cumpleaños, `valor_crudo` con datos personales. |
| `baja` | Seudónimo, agregado o atributo no identificatorio. | `cliente_id`, `edad`, tramos de `compras`, `autor` como dato profesional. |
| `ninguna` | No corresponde a una persona natural. | ISBN, stock, clima, montos del ticket, `folio`. |

Marco de referencia: Ley 19.628 sobre protección de la vida privada, vigente, y Ley 21.719,
publicada en diciembre de 2024 con entrada en vigencia diferida. Este documento **describe
controles y minimización; no declara certificaciones ni cumplimiento normativo**. La base
legal de cada tratamiento debe validarse con asesoría jurídica antes de producción.

### A.2 Matriz PII por columna

| Columna | Origen | Nivel | Categoría | Base legal declarada | Persistencia | Enmascaramiento | Ejemplo ficticio antes / después |
|---|---|---|---|---|---|---|---|
| `Rut cliente` | Bronce clientes | alta | identificación | relación comercial y cuadre de compras (a validar) | no se persiste; solo HMAC | conservar últimos 3 dígitos del cuerpo y el DV | `12.345.678-5` / `*****678-5` |
| `Nombre` | Bronce clientes | alta | identificación | no aplica (minimización) | no se persiste | inicial más `***` por palabra | `Ana Pérez` / `A*** P***` |
| `Email` | Bronce clientes | alta | contacto e identificación | no aplica (minimización) | no se persiste | primera letra y dominio visible | `ana.perez@example.com` / `a***@example.com` |
| `Cumple.` | Bronce clientes | media | cuasi-identificador | fidelización (a validar) | solo el mes en Silver | mes sin día ni año | `14 - junio` / `06` |
| `Alta` | Bronce clientes | baja | cuasi-identificador | relación comercial (a validar) | `clientes.alta` | año y mes; día enmascarado en reportes | `15/03/2024` / `2024-03-**` |
| `Canal` | Bronce clientes | ninguna | no personal | no aplica | `clientes.canal` | sin máscara | `Local` / `Local` |
| `Puntos` | Bronce clientes | baja | comportamiento | relación comercial (a validar) | `clientes.puntos` | rango en reportes | `1250` / `1000-1499` |
| `Compras` | Bronce clientes | baja | comportamiento financiero | relación comercial (a validar) | `clientes.compras` | rango en reportes | `123450` / `100.000-149.999` |
| `Canjes` | Bronce clientes | baja | comportamiento | relación comercial (a validar) | `clientes.canjes` | rango en reportes | `3` / `1-5` |
| `cliente_id` | Derivado del RUT | baja | seudónimo | anonimización técnica | `clientes.cliente_id` y `ventas_ticket.cliente_id` | completo solo en joins internos; externo primeros 8 caracteres | `a1b2c3d4e5f6...` / `a1b2c3d4...` |
| `edad` | Derivado del RUT | baja | cuasi-identificador | estimación técnica (a validar) | `clientes.edad` | rango de 10 años | `68` / `65-74` |
| `mes_cumple` | Bronce clientes | media | cuasi-identificador | fidelización (a validar) | `clientes.mes_cumple` | solo mes; nunca día | `6` / `06` |
| `valor_crudo` | Cuarentena | media | variable; PII potencial | control de calidad (a validar) | `cuarentena.valor_crudo` | truncar y enmascarar si contiene PII | `12.345.678-5` / `*****678-5` |
| `autor` | Bronce stock | baja | dato profesional público | interés editorial (a validar) | `stock.autor` | sin máscara; no se usa para identificar personas | `Autora Ficticia` / `Autora Ficticia` |
| ISBN, stock, clima, montos y `folio` | Bronce stock, ventas y clima | ninguna | no personal | no aplica | tablas Silver | sin máscara | `9788412345678` / `9788412345678` |
| Teléfono | No existe | media | contacto | a definir | **no existe columna (a crear)** | `+56 9 **** 5678` | `+56 9 1234 5678` / `+56 9 **** 5678` |
| Dirección | No existe | media | contacto y localización | a definir | **no existe columna (a crear)** | conservar calle y comuna; enmascarar numeración | `Av. Ficticia 123, Temuco` / `Av. Ficticia ***, Temuco` |

Todos los valores de ejemplo son ficticios. El RUT `12.345.678-5` tiene dígito verificador
válido; no corresponde a una persona real. `example.com` es un dominio reservado para
documentación.

### A.3 Reglas de enmascaramiento propuestas por destino

| Destino | Regla |
|---|---|
| Reportes y marts (a crear) | RUT enmascarado, nombre abreviado, email con dominio visible, edad y montos por rango. Nunca RUT completo ni email completo. |
| `cliente_id` en reportes | Se permite el seudónimo completo en cruces internos. En compartición externa, solo los primeros 8 caracteres. |
| Logs de pipeline | El código no registra RUT, nombre, email ni teléfono. Registra `cliente_id` o `sin_id`. `--log-level DEBUG` puede mostrar valores crudos de ventas, stock y clima; no de clientes. |
| Cuarentena | `valor_crudo` se conserva para corrección en origen. Si una entidad futura escribe PII de clientes, debe aplicarse la misma máscara antes de insertar. |
| Exportaciones y respaldos | Mismo criterio que reportes. Un respaldo con PII se trata como dato `alta` y se cifra en reposo (a crear). |

### A.4 Verificación de clasificación

| Revisión | Resultado |
|---|---|
| Columnas PII en `sql/clientes.sql` | `cliente_id` (baja), `alta` (baja), `edad` (baja), `mes_cumple` (media), `puntos` (baja), `compras` (baja), `canjes` (baja): todas clasificadas. |
| Columnas PII en `sql/ventas.sql` | `cliente_id` (baja): clasificada. |
| Columnas PII en `sql/stock.sql` | `autor` (baja): clasificada como dato profesional. |
| Columnas PII en `sql/clima.sql` | ninguna. |
| Columnas PII en `sql/cuarentena.sql` | `valor_crudo` (media): clasificada. |
| Marts Gold propuestos | No existen; cuando se creen, esta matriz es el contrato de enmascaramiento obligatorio. |
| Columna con PII sin clasificar | **No se detectó ninguna** en `sql/` ni en los artefactos propuestos. |

---

## Anexo B. Retención y borrado: estado actual y procedimiento propuesto

La retención se define por capa y por dataset. **No hay plazos aprobados en el repositorio**
ni mecanismos automatizados de purga; esta tabla solo identifica qué dato existe, quién es el
owner y qué falta decidir. Ningún borrado se ejecuta sin política aprobada, respaldo
verificado y autorización del owner.

### B.1 Retención por capa y dataset

| Capa / dataset | Qué se conserva | Plazo | Al vencer | Owner | Estado |
|---|---|---|---|---|---|
| Bronce clientes | `CLIENTES LIBRERIA.xlsx` con RUT, nombre y email | a definir por el owner | Revisión, anonimización del archivo o borrado con aprobación y respaldo | `data-governance-steward` | no implementada |
| Bronce ventas | `VENTAS LIBRERIA.xlsx`, `VENTAS CAFETERIA.xlsx` | a definir por el owner | Archivo o agregado sin identificadores, con aprobación | `silver-etl-engineer` | no implementada |
| Bronce stock | `STOCK LIBRERIA.xlsx` | a definir por el owner | Archivo histórico o borrado con respaldo | Jefatura de librería | no implementada |
| Bronce catálogo cafetería | `CATALOGO CAFETERIA.csv` | a definir por el owner | Archivo histórico o borrado con respaldo | `silver-etl-engineer` | no implementada |
| Bronce clima | 7 `.xls` de clima | a definir por el owner | no aplica (sin PII) | `gold-analytics-modeler` (consumo) | no implementada |
| Manifiesto | `bronze/manifest.jsonl` | a definir por el owner | no aplica (sin PII) | `data-quality-steward` | no implementada |
| Silver `clientes` | Seudónimo y atributos de fidelización | a definir por el owner | Anonimizar o eliminar la fila; el historial de ventas queda con `cliente_id NULL` | `data-governance-steward` | no implementada |
| Silver `ventas_ticket` | Cabecera de ticket | a definir por el owner | Eliminar `cliente_id` y conservar el agregado | Jefatura comercial | no implementada |
| Silver `ventas_linea` | Detalle reconstruido | a definir por el owner | Conservar o eliminar con el ticket asociado | `silver-etl-engineer` | no implementada |
| Silver `stock` | Estado actual por ISBN | a definir por el owner | Marcar obsoleto o eliminar con respaldo (a crear) | Jefatura de librería | no implementada |
| Silver `stock_historial` | Versiones SCD2 | a definir por el owner | Purgar versiones cerradas más antiguas con respaldo | Jefatura de librería | no implementada |
| Silver `clima_diario` | Serie diaria | a definir por el owner | no aplica (sin PII) | `gold-analytics-modeler` (consumo) | no implementada |
| Silver `cuarentena` | Rechazos con `valor_crudo` | a definir por el owner | Borrado por lote con aprobación | `data-quality-steward` | no implementada |
| Logs de pipeline | Mensajes operativos | a definir por el owner | Rotación y borrado | `silver-etl-engineer` | no implementada |
| Respaldos de PostgreSQL | Copia completa | a definir por el owner | Rotación | `data-platform-devops` y `data-governance-steward` | no implementada |

### B.2 Cuarentena y manifiesto

| Artefacto | Regla de retención | Justificación |
|---|---|---|
| `cuarentena` | a definir por el owner; append-only mientras no se purgue. | Permite corregir el origen, pero acumula `valor_crudo` potencialmente sensible. |
| `manifest.jsonl` | a definir por el owner. | Auditoría de integridad sin PII; solo nombres, tamaños, fechas y hashes. |
| Rechazos de `clientes` | No se persisten. | Minimización: el RUT nunca se escribe en cuarentena ni en logs. |
| Rechazos de `clima` | No se persisten. | Los valores inválidos quedan `NULL`. |

### B.3 Procedimiento propuesto de borrado o anonimización

> Procedimiento **propuesto**: no está implementado ni automatizado en el repositorio.

1. Recibir la solicitud y verificar la identidad del titular por un canal aprobado.
2. Calcular el `cliente_id` con la `PII_HASH_KEY` vigente y localizar la fila en
   `clientes`. No registrar el RUT en la solicitud ni en el ticket de atención.
3. Con aprobación del owner y respaldo verificado, eliminar o anonimizar la fila de
   `clientes` y poner `cliente_id = NULL` en `ventas_ticket`.
4. **Corregir el archivo crudo o agregar una lista de exclusión.** `clientes` se recarga
   completo en cada corrida: si el archivo de `bronze/` sigue conteniendo el RUT, la fila
   reaparece en la siguiente carga. La lista de exclusión es **a crear**.
5. Registrar la evidencia del borrado sin conservar el dato personal.
6. Verificar que respaldos y cuarentena no reintroduzcan el dato al restaurar.

Limitación de gobierno: hoy **no existe un mecanismo técnico de supresión efectiva** mientras
`clientes` sea un reemplazo total desde un archivo que no se corrige. Es un riesgo alto de la
sección 4.3 (limitación 7) y de la brecha G-04.

### B.4 Respaldos

- Un respaldo que contenga `clientes` o `ventas_ticket` se clasifica como dato `alta` y debe
  cifrarse en reposo y en tránsito.
- La restauración debe probarse antes de declarar la política operativa; hoy es a crear.
- La rotación de `PII_HASH_KEY` debe coordinarse con respaldos y con la regeneración de IDs.

---

## Anexo C. Glosario de negocio

| Término | Definición en Miralibros | Dónde vive |
|---|---|---|
| Cliente | Persona que compra en la librería o participa del programa de fidelización. En Silver se representa solo por un seudónimo. | `clientes` |
| Título | Libro o producto editorial identificado por un ISBN. Es la unidad de catálogo y de stock. | `stock.titulo` |
| ISBN | Identificador internacional del libro. Miralibros acepta ISBN-13 de 13 dígitos e ISBN-10 de 9 dígitos más dígito o `X`; no valida el dígito verificador. | `stock.isbn` |
| Editorial | Empresa que publica el título. | `stock.editorial` |
| Clasificación | Categoría comercial o temática del título usada para ordenar el catálogo. | `stock.clasificacion` |
| PVP | Precio de venta al público, en CLP entero. El pipeline exige un mínimo de 100. | `stock.pvp` |
| Boleta | Documento tributario de venta al consumidor. En los crudos aparece como valor de `Tipo Documento`; no se valida su secuencia. | `ventas_ticket.tipo_documento` |
| Folio | Número identificatorio del documento tributario. Es texto, no es único en Silver y no se valida contra el SII. | `ventas_ticket.folio` |
| Correlativo | Identificador interno del ticket dentro de un origen (`LIBRERIA` o `CAFETERIA`). | `ventas_ticket.correlativo` |
| Ticket | Cabecera de una venta: fecha, origen, cliente, documento, monto y cantidad de ítems. | `ventas_ticket` |
| Línea de venta | Detalle reconstruido de un ticket: precio unitario, cantidad y total. No representa el producto real vendido. | `ventas_linea` |
| Stock | Unidades disponibles de un ISBN. Se descompone en stock propio y stock en consignación. | `stock` |
| Stock propio | Unidades financiadas y administradas por Miralibros. | `stock.stock_propio` |
| Stock en consignación | Unidades entregadas por un tercero que se pagan al venderse. | `stock.stock_consignacion` |
| Quiebre | Situación en que un ISBN no tiene unidades disponibles. No existe columna ni alerta implementada: es un KPI a crear. | no implementado |
| Temporada escolar | Período de mayor demanda de textos escolares y útiles. Es una convención de análisis; no hay columna ni calendario implementado. | no implementado |
| Temporada navideña | Período de mayor demanda de libros y regalos de fin de año. Es una convención de análisis; no hay columna ni calendario implementado. | no implementado |
| Punto | Unidad del programa de fidelización acumulada por compras. No es dinero. | `clientes.puntos` |
| Canje | Uso de puntos para obtener un beneficio. | `clientes.canjes` |
| Compra histórica | Monto monetario acumulado en CLP por el cliente, no la cantidad de transacciones. | `clientes.compras` |
| Canal | Origen de la venta o del alta: `Local` o `Web`. En ventas de POS no se persiste. | `clientes.canal` |
| Origen | Fuente del ticket: `LIBRERIA` o `CAFETERIA`. Se infiere del nombre del archivo. | `ventas_ticket.origen` |
| Cafetería | Línea de venta de café y pastelería. Sus tickets son siempre anónimos y su catálogo no se persiste. | `ventas_ticket` con `origen = CAFETERIA` |
| Devolución | Ticket marcado como devolución mediante `Es Dev.`. No genera movimientos de stock ni de dinero. | `ventas_ticket.es_devolucion` |
| RUT | Rol Único Tributario chileno. Dato personal de nivel alto; se transforma en HMAC y nunca se persiste ni se registra en logs. | solo en memoria |
| CLP | Peso chileno, moneda sin decimales usada en precios y montos. | `pvp`, `total_venta`, `compras` |
| SCD2 | Modelo de historial por versiones con `valid_from`, `valid_to` e `is_current`. Se aplica solo a stock. | `stock_historial` |

---

## Anexo D. Brechas y plan de acción

| ID | Brecha | Impacto | Acción propuesta | Owner | Prioridad | Estado |
|---|---|---|---|---|---|---|
| G-01 | Gold no existe: no hay hechos, dimensiones ni marts. | Sin KPIs de rotación, quiebre, ticket promedio ni margen. | Diseñar y crear la capa Gold con contratos de granularidad y PII enmascarada. | `gold-analytics-modeler` | alta | a crear |
| G-02 | No hay orquestador ni schedule. | Dependencias y reintentos dependen de ejecución manual. | Implementar DAG `clientes -> stock -> ventas -> clima` con sensores y alertas. | `pipeline-orchestration-engineer` | alta | a crear |
| G-03 | No hay catálogo automatizado de metadatos. | Este documento se desactualiza si cambia el código. | Publicar el catálogo desde un repositorio de metadatos o generar esta matriz en CI. | `data-governance-steward` | media | a crear |
| G-04 | La supresión de un cliente no es efectiva: `clientes` se recarga desde el archivo. | Un titular borrado reaparece en la siguiente carga. | Crear lista de exclusión por `cliente_id` o corregir el archivo antes de cada carga; documentar el procedimiento. | `data-governance-steward` | alta | a crear |
| G-05 | `PII_HASH_KEY` tiene valor de desarrollo por defecto. | Riesgo de IDs reproducibles si falta el secreto en producción. | Exigir la variable en producción y fallar si no está definida; usar gestor de secretos. | `silver-etl-engineer` | alta | a crear |
| G-06 | Rotar la clave HMAC invalida IDs y FK. | Ruptura de integridad entre clientes y ventas. | Documentar y automatizar la regeneración coordinada de ambas tablas. | `data-governance-steward` | alta | a crear |
| G-07 | `clientes` no tiene historial ni auditoría de archivo. | No se puede reconstruir la evolución ni el origen de una fila. | Evaluar SCD2 o al menos persistir `_source_file` y `_loaded_at`. | `silver-etl-engineer` | media | a crear |
| G-08 | El CSV de cafetería no entra al manifiesto. | Sin checksum ni versión para un insumo de ventas. | Incluir `.csv` en `etl/ingest.py` con formato detectado. | `bronze-ingestion-engineer` | media | a crear |
| G-09 | Ventas descarta `Orden Web`, `Canal`, `Voucher` y `Feria`. | No se puede analizar canal web o local ni conciliar vouchers. | Definir si se persisten en un modelo ampliado; retirar `normalize_canal` si no se usará. | Jefatura comercial y `silver-etl-engineer` | media | a crear |
| G-10 | No hay dimensión de producto ni de cafetería. | `codigo_interno` no es el producto real y se pierden nombre y categoría. | Crear dimensión de producto para librería y cafetería; persistir el catálogo. | `gold-analytics-modeler` | alta | a crear |
| G-11 | `folio` es texto y no es único. | No hay unicidad de documento ni control tributario. | Definir clave de documento y validación de folio con negocio. | Jefatura comercial | media | a crear |
| G-12 | Stock no valida DV ni detecta ausencias. | ISBN mal formados y títulos obsoletos permanecen como vigentes. | Agregar validación de DV y un proceso de obsolescencia por ausencia. | Jefatura de librería y `data-quality-steward` | media | a crear |
| G-13 | Discrepancia `PVP`: pipeline exige 100, DDL permite 0. | Un precio bajo puede rechazarse sin explicación en el DDL. | Alinear la regla y documentar la decisión de negocio. | `data-quality-steward` | baja | a crear |
| G-14 | `stock_historial` no historiza ausencias. | Versiones vigentes quedan abiertas para títulos retirados. | Extender `_refresh_history` para cerrar ISBN ausentes con un corte aprobado. | `silver-etl-engineer` | media | a crear |
| G-15 | Clima tiene año fijo 2025 y sin cuarentena. | No escala a 2026 y una fila incoherente aborta el lote. | Parametrizar el año y validar coherencia antes de insertar. | `silver-etl-engineer` | media | a crear |
| G-16 | `cuarentena` no tiene clave natural, SHA ni `run_id`. | Recargas duplican rechazos y no se puede auditar la corrida. | Agregar `run_id`, `sha256` y clave natural; definir deduplicación. | `data-quality-steward` | media | a crear |
| G-17 | `clientes` y `clima` no persisten rechazos. | Se pierden motivos y no se puede medir calidad en origen. | Evaluar cuarentena con PII enmascarada para clientes y para clima. | `data-quality-steward` | media | a crear |
| G-18 | Migraciones manuales con referencias obsoletas: `clientes_columnas_nullable` usa `nombre` y `email`; `add_hora_simulada` usa la tabla plana `ventas`. | Ambas fallarían en una base ya migrada. | Corregir o retirar las migraciones y documentar el orden de ejecución. | `silver-etl-engineer` | alta | a crear |
| G-19 | CI no prueba contra PostgreSQL. | Los `CHECK` y las FK no se validan en integración. | Agregar un servicio de base en CI con migraciones y pruebas de contrato. | `data-platform-devops` y `data-testing-engineer` | media | a crear |
| G-20 | Sin `load_id` ni SHA en Silver. | No hay linaje fila a archivo. | Propagar `load_id`, `sha256` y `run_id` a las tablas Silver. | `silver-etl-engineer` | media | a crear |
| G-21 | Enriquecimiento de ventas no verifica Silver. | La FK puede fallar si clientes y ventas no comparten archivo y clave. | Validar que los `cliente_id` asignados existan en `silver.clientes` antes del upsert. | `silver-etl-engineer` | alta | a crear |
| G-22 | No hay columnas de teléfono ni dirección. | La política de enmascaramiento no tiene aplicación real. | Si se incorporan, clasificarlas como `media` y aplicar el Anexo A. | `data-governance-steward` | baja | a crear |
| G-23 | No hay retención automatizada ni respaldos verificados. | La política existe solo en documentación. | Implementar jobs de purga, respaldo cifrado y prueba de restauración. | `data-platform-devops` y `data-governance-steward` | alta | a crear |
| G-24 | No hay infraestructura de logs con retención. | Los logs operativos no se conservan ni se purgan de forma controlada. | Definir destino, retención definida por el owner y prohibición de PII en logs. | `silver-etl-engineer` | media | a crear |

### D.1 Lectura de prioridades

- **Alta**: afecta integridad, PII o la capacidad de operar la plataforma. Requiere decisión
  y responsable antes de producción.
- **Media**: afecta calidad analítica o auditabilidad; se planifica en el corto plazo.
- **Baja**: mejora documental o de alineación; se resuelve junto con otros cambios.

Ninguna acción de este anexo autoriza borrar datos, cambiar DDL ni ejecutar migraciones por
sí sola. Cada cambio debe pasar por revisión de `silver-etl-engineer`, validación de
`data-quality-steward` y aprobación del owner del dataset.
