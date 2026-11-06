-- Migración no destructiva: separa la tabla plana {{schema}}.ventas en
-- {{schema}}.ventas_ticket (cabecera) y {{schema}}.ventas_linea (detalle).
--
-- La migración es idempotente:
--   1. crea las dos tablas nuevas si no existen;
--   2. si la tabla plana vieja todavía existe y las nuevas ya están creadas,
--      la renombra a {{schema}}.ventas_flat_backup para conservarla como
--      respaldo, sin eliminar información.
--
-- La transacción la controla quien ejecute el archivo.
-- La FK de ventas_ticket requiere que {{schema}}.clientes exista.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.ventas_ticket (
    origen            TEXT          NOT NULL,
    correlativo       BIGINT        NOT NULL,
    fecha             DATE          NOT NULL,
    hora_simulada     TIME          NULL,
    rut_cliente       VARCHAR(10)   NULL,
    medio_pago        TEXT,
    tipo_documento    TEXT,
    folio             TEXT,
    total_venta       NUMERIC(12,0) NOT NULL,
    cant_items        INTEGER       NOT NULL,
    tiene_descuento   TEXT,
    es_devolucion     TEXT,
    estado            TEXT,
    _source_file      TEXT          NOT NULL,
    _loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (origen, correlativo),

    CONSTRAINT ventas_ticket_rut_fk
        FOREIGN KEY (rut_cliente)
        REFERENCES {{schema}}.clientes (rut),

    CONSTRAINT ventas_ticket_origen_valido
        CHECK (origen IN ('LIBRERIA', 'CAFETERIA')),

    CONSTRAINT ventas_ticket_correlativo_positivo
        CHECK (correlativo > 0),

    CONSTRAINT ventas_ticket_total_no_negativo
        CHECK (total_venta >= 0),

    CONSTRAINT ventas_ticket_cant_items_positiva
        CHECK (cant_items > 0)
);

CREATE INDEX IF NOT EXISTS ventas_ticket_fecha_idx
    ON {{schema}}.ventas_ticket (fecha);

CREATE INDEX IF NOT EXISTS ventas_ticket_rut_cliente_idx
    ON {{schema}}.ventas_ticket (rut_cliente);

CREATE INDEX IF NOT EXISTS ventas_ticket_origen_fecha_idx
    ON {{schema}}.ventas_ticket (origen, fecha);

CREATE TABLE IF NOT EXISTS {{schema}}.ventas_linea (
    origen            TEXT          NOT NULL,
    correlativo       BIGINT        NOT NULL,
    linea             INTEGER       NOT NULL,
    codigo_interno    TEXT          NOT NULL,
    cantidad          INTEGER       NOT NULL,
    precio_unitario   NUMERIC(12,0) NOT NULL,
    total_linea       NUMERIC(12,0) NOT NULL,
    _loaded_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (origen, correlativo, linea),

    CONSTRAINT ventas_linea_ticket_fk
        FOREIGN KEY (origen, correlativo)
        REFERENCES {{schema}}.ventas_ticket (origen, correlativo)
        ON DELETE CASCADE,

    CONSTRAINT ventas_linea_linea_positiva
        CHECK (linea > 0),

    CONSTRAINT ventas_linea_cantidad_positiva
        CHECK (cantidad > 0),

    CONSTRAINT ventas_linea_precio_no_negativo
        CHECK (precio_unitario >= 0),

    CONSTRAINT ventas_linea_total_no_negativo
        CHECK (total_linea >= 0)
);

CREATE INDEX IF NOT EXISTS ventas_linea_ticket_idx
    ON {{schema}}.ventas_linea (origen, correlativo);

CREATE INDEX IF NOT EXISTS ventas_linea_codigo_interno_idx
    ON {{schema}}.ventas_linea (codigo_interno);

DO $$
BEGIN
    IF to_regclass('{{schema}}.ventas') IS NOT NULL
        AND to_regclass('{{schema}}.ventas_ticket') IS NOT NULL
        AND to_regclass('{{schema}}.ventas_linea') IS NOT NULL
        AND to_regclass('{{schema}}.ventas_flat_backup') IS NULL
    THEN
        ALTER TABLE {{schema}}.ventas RENAME TO ventas_flat_backup;
    END IF;
END
$$;
