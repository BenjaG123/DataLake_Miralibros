-- Ventas unificadas de librería y cafetería.
-- La clave de negocio es (origen, correlativo): el correlativo es único
-- dentro de cada archivo de origen. El pipeline hace upsert incremental.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.ventas (
    origen              TEXT          NOT NULL,
    correlativo         BIGINT        NOT NULL,
    orden_web           TEXT,
    fecha               DATE          NOT NULL,
    medio_pago          TEXT,
    tipo_documento      TEXT,
    folio               TEXT,
    voucher_maquina_f   TEXT,
    total_venta         NUMERIC(12,0) NOT NULL,
    cant_items          INTEGER       NOT NULL,
    canal               TEXT,
    feria               TEXT,
    tiene_descuento     TEXT,
    es_devolucion       TEXT,
    estado              TEXT,
    _source_file        TEXT          NOT NULL,
    _loaded_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),

    PRIMARY KEY (origen, correlativo),

    CONSTRAINT ventas_origen_valido
        CHECK (origen IN ('LIBRERIA', 'CAFETERIA')),

    CONSTRAINT ventas_correlativo_positivo
        CHECK (correlativo > 0),

    CONSTRAINT ventas_total_no_negativo
        CHECK (total_venta >= 0),

    CONSTRAINT ventas_cant_items_positiva
        CHECK (cant_items > 0),

    CONSTRAINT ventas_canal_valido
        CHECK (canal IN ('Local', 'Web')),

    CONSTRAINT ventas_tiene_descuento_valido
        CHECK (tiene_descuento IN ('SI', 'NO')),

    CONSTRAINT ventas_es_devolucion_valida
        CHECK (es_devolucion IN ('SI', 'NO'))
);

CREATE INDEX IF NOT EXISTS ventas_fecha_idx
    ON {{schema}}.ventas (fecha);

CREATE INDEX IF NOT EXISTS ventas_origen_fecha_idx
    ON {{schema}}.ventas (origen, fecha);
