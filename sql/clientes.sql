-- Tabla final de clientes normalizados.
-- El pipeline crea la tabla si no existe y luego reemplaza su contenido.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.clientes (
    rut       VARCHAR(10)  PRIMARY KEY,
    nombre    VARCHAR(150) NOT NULL,
    email     VARCHAR(254) NOT NULL,
    alta      DATE         NOT NULL,
    canal     VARCHAR(10)  NOT NULL,
    cumple    DATE         NOT NULL,
    puntos    BIGINT       DEFAULT 0 NOT NULL,
    compras   BIGINT       DEFAULT 0 NOT NULL,
    canjes    BIGINT       DEFAULT 0 NOT NULL,

    CONSTRAINT clientes_rut_formato
        CHECK (rut ~ '^[0-9]{7,8}-[0-9K]$'),

    CONSTRAINT clientes_canal_valido
        CHECK (canal IN ('Local', 'Web')),

    CONSTRAINT clientes_puntos_no_negativo
        CHECK (puntos >= 0),

    CONSTRAINT clientes_compras_no_negativo
        CHECK (compras >= 0),

    CONSTRAINT clientes_canjes_no_negativo
        CHECK (canjes >= 0)
);
