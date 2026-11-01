-- Clima diario de Miralibros, una fila por fecha.
-- Las mediciones son opcionales: la única columna obligatoria es la fecha.
-- El pipeline hace upsert por fecha para no borrar años anteriores.

CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE IF NOT EXISTS {{schema}}.clima_diario (
    fecha                DATE          PRIMARY KEY,

    temp_max             NUMERIC(4,1),
    temp_min             NUMERIC(4,1),
    temp_media           NUMERIC(4,1),
    humedad_relativa     NUMERIC(5,1),
    indice_uv            SMALLINT,
    precipitacion_mm     NUMERIC(6,1),
    viento_direccion     VARCHAR(5),
    viento_velocidad_kt  SMALLINT,

    _loaded_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),

    CONSTRAINT clima_temp_max_rango
        CHECK (temp_max IS NULL OR temp_max BETWEEN -50 AND 60),

    CONSTRAINT clima_temp_min_rango
        CHECK (temp_min IS NULL OR temp_min BETWEEN -50 AND 60),

    CONSTRAINT clima_temp_media_rango
        CHECK (temp_media IS NULL OR temp_media BETWEEN -50 AND 60),

    CONSTRAINT clima_temperaturas_coherentes
        CHECK (
            temp_min IS NULL
            OR temp_max IS NULL
            OR temp_min <= temp_max
        ),

    CONSTRAINT clima_media_en_rango
        CHECK (
            temp_media IS NULL
            OR temp_min IS NULL
            OR temp_max IS NULL
            OR temp_media BETWEEN temp_min AND temp_max
        ),

    CONSTRAINT clima_humedad_rango
        CHECK (
            humedad_relativa IS NULL
            OR humedad_relativa BETWEEN 0 AND 100
        ),

    CONSTRAINT clima_uv_rango
        CHECK (indice_uv IS NULL OR indice_uv BETWEEN 0 AND 20),

    CONSTRAINT clima_precipitacion_no_negativa
        CHECK (precipitacion_mm IS NULL OR precipitacion_mm >= 0),

    CONSTRAINT clima_viento_direccion_valida
        CHECK (
            viento_direccion IS NULL
            OR viento_direccion IN (
                'N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'VRB', 'CALMA'
            )
        ),

    CONSTRAINT clima_viento_velocidad_no_negativa
        CHECK (
            viento_velocidad_kt IS NULL
            OR viento_velocidad_kt >= 0
        )
);
