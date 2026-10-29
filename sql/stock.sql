-- Stock actual por ISBN y su historial de cambios de precio/stock.
-- El pipeline hace upsert por ISBN sobre {{schema}}.stock y, en la misma
-- transacción, cierra/abre versiones en {{schema}}.stock_historial.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.stock (
    isbn                TEXT          PRIMARY KEY,
    titulo              TEXT,
    autor               TEXT,
    editorial           TEXT,
    clasificacion       TEXT,
    pvp                 NUMERIC(12,0) NOT NULL,
    stock_consignacion  INTEGER       NOT NULL DEFAULT 0,
    stock_propio        INTEGER       NOT NULL DEFAULT 0,
    stock_total         INTEGER       NOT NULL,
    ventas              INTEGER       NOT NULL DEFAULT 0,
    valid_from          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _source_file        TEXT          NOT NULL,
    _loaded_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT stock_isbn_formato
        CHECK (isbn ~ '^(?:[0-9]{13}|[0-9]{9}[0-9X])$'),

    CONSTRAINT stock_pvp_no_negativo
        CHECK (pvp >= 0),

    CONSTRAINT stock_consignacion_no_negativa
        CHECK (stock_consignacion >= 0),

    CONSTRAINT stock_propio_no_negativo
        CHECK (stock_propio >= 0),

    CONSTRAINT stock_total_no_negativo
        CHECK (stock_total >= 0),

    CONSTRAINT stock_total_consistente
        CHECK (stock_total = stock_consignacion + stock_propio),

    CONSTRAINT stock_ventas_no_negativas
        CHECK (ventas >= 0)
);

CREATE TABLE IF NOT EXISTS {{schema}}.stock_historial (
    id                  BIGINT        GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    isbn                TEXT          NOT NULL
        REFERENCES {{schema}}.stock (isbn),
    titulo              TEXT,
    autor               TEXT,
    editorial           TEXT,
    clasificacion       TEXT,
    pvp                 NUMERIC(12,0) NOT NULL,
    stock_consignacion  INTEGER       NOT NULL,
    stock_propio        INTEGER       NOT NULL,
    stock_total         INTEGER       NOT NULL,
    ventas              INTEGER       NOT NULL,
    valid_from          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    valid_to            TIMESTAMPTZ,
    is_current          BOOLEAN       NOT NULL DEFAULT TRUE,
    _source_file        TEXT          NOT NULL,
    _loaded_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT stock_historial_isbn_formato
        CHECK (isbn ~ '^(?:[0-9]{13}|[0-9]{9}[0-9X])$'),

    CONSTRAINT stock_historial_pvp_no_negativo
        CHECK (pvp >= 0),

    CONSTRAINT stock_historial_stock_no_negativo
        CHECK (
            stock_consignacion >= 0
            AND stock_propio >= 0
            AND stock_total >= 0
            AND ventas >= 0
        ),

    CONSTRAINT stock_historial_total_consistente
        CHECK (stock_total = stock_consignacion + stock_propio),

    CONSTRAINT stock_historial_vigencia
        CHECK (
            (is_current AND valid_to IS NULL)
            OR (NOT is_current AND valid_to IS NOT NULL)
        ),

    CONSTRAINT stock_historial_rango
        CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS stock_historial_isbn_actual_uidx
    ON {{schema}}.stock_historial (isbn)
    WHERE is_current;
