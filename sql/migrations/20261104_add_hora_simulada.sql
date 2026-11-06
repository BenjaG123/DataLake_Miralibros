-- Agrega la hora sintética de venta a las tablas ya existentes.
-- No usa BEGIN/COMMIT: la transacción la controla el ejecutor.

ALTER TABLE {{schema}}.ventas ADD COLUMN IF NOT EXISTS hora_simulada TIME;
