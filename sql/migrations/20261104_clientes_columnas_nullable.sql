-- Permite guardar NULL en nombre, email y alta de clientes.
-- No usa BEGIN/COMMIT: la transacción la controla el ejecutor.

ALTER TABLE {{schema}}.clientes ALTER COLUMN nombre DROP NOT NULL;
ALTER TABLE {{schema}}.clientes ALTER COLUMN email DROP NOT NULL;
ALTER TABLE {{schema}}.clientes ALTER COLUMN alta DROP NOT NULL;
