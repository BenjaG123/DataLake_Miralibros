-- Ventas de librería y cafetería en modelo cabecera/detalle.
--
-- ``ventas_ticket`` guarda la cabecera con clave de negocio
-- (origen, correlativo). ``ventas_linea`` guarda el detalle reconstruido de
-- forma determinística a partir del total y la cantidad de ítems del ticket.
--
-- ``codigo_interno`` no tiene FK porque apunta lógicamente a
-- ``{{schema}}.stock.isbn`` para LIBRERIA y al catálogo de cafetería para
-- CAFETERIA. La FK de ``cliente_id`` exige que ``{{schema}}.clientes`` ya
-- exista; el runner ejecuta clientes antes de ventas.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.ventas_ticket (
    origen            TEXT          NOT NULL,
    correlativo       BIGINT        NOT NULL,
    fecha             DATE          NOT NULL,
    hora_simulada     TIME          NULL,
    cliente_id        VARCHAR(64)   NULL,
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

    CONSTRAINT ventas_ticket_cliente_fk
        FOREIGN KEY (cliente_id)
        REFERENCES {{schema}}.clientes (cliente_id),

    CONSTRAINT ventas_ticket_origen_valido
        CHECK (origen IN ('LIBRERIA', 'CAFETERIA')),

    CONSTRAINT ventas_ticket_correlativo_positivo
        CHECK (correlativo > 0),

    CONSTRAINT ventas_ticket_total_no_negativo
        CHECK (total_venta >= 0),

    CONSTRAINT ventas_ticket_cant_items_positiva
        CHECK (cant_items > 0)
);

COMMENT ON COLUMN {{schema}}.ventas_ticket.hora_simulada IS
    'Hora sintética generada por el ETL; no proviene del POS.';

COMMENT ON COLUMN {{schema}}.ventas_ticket.cliente_id IS
    'Identificador anonimizado (HMAC-SHA256) asignado por cuadre exacto de montos; NULL si el ticket es anónimo.';

CREATE INDEX IF NOT EXISTS ventas_ticket_fecha_idx
    ON {{schema}}.ventas_ticket (fecha);

CREATE INDEX IF NOT EXISTS ventas_ticket_cliente_id_idx
    ON {{schema}}.ventas_ticket (cliente_id);

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
