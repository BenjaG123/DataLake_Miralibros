-- Cuarentena común para filas rechazadas por los pipelines incrementales.
-- Cada fila conserva la regla, el motivo y el valor crudo que la originó.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.cuarentena (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    entidad         TEXT        NOT NULL,
    archivo_origen  TEXT        NOT NULL,
    fila            INTEGER,
    columna         TEXT,
    valor_crudo     TEXT,
    regla           TEXT        NOT NULL,
    severidad       TEXT        NOT NULL DEFAULT 'error'
        CHECK (severidad IN ('warning','error')),
    motivo          TEXT        NOT NULL,
    _loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cuarentena_entidad_loaded_at_idx
    ON {{schema}}.cuarentena (entidad, _loaded_at DESC);
