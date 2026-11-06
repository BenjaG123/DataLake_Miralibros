-- Tabla final de clientes anonimizados.
-- El pipeline crea la tabla si no existe y luego reemplaza su contenido.
--
-- No se persiste ningún dato identificatorio. ``cliente_id`` es un
-- HMAC-SHA256 determinístico y ``edad`` se deriva del año de nacimiento
-- estimado; ambos son datos no identificatorios.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.clientes (
    cliente_id  VARCHAR(64) PRIMARY KEY,
    alta        DATE,
    canal       VARCHAR(10) NOT NULL,
    edad        INTEGER     NOT NULL,
    puntos      BIGINT      DEFAULT 0 NOT NULL,
    compras     BIGINT      DEFAULT 0 NOT NULL,
    canjes      BIGINT      DEFAULT 0 NOT NULL,

    CONSTRAINT clientes_cliente_id_formato
        CHECK (cliente_id ~ '^[0-9a-f]{64}$'),

    CONSTRAINT clientes_canal_valido
        CHECK (canal IN ('Local', 'Web')),

    CONSTRAINT clientes_edad_valida
        CHECK (edad BETWEEN 0 AND 120),

    CONSTRAINT clientes_puntos_no_negativo
        CHECK (puntos >= 0),

    CONSTRAINT clientes_compras_no_negativo
        CHECK (compras >= 0),

    CONSTRAINT clientes_canjes_no_negativo
        CHECK (canjes >= 0)
);
