-- Create schemas and grant least-privilege per tenant
CREATE SCHEMA IF NOT EXISTS t001 AUTHORIZATION t001;
CREATE SCHEMA IF NOT EXISTS t002 AUTHORIZATION t002;

-- Restrict cross-tenant access
REVOKE ALL ON SCHEMA t001 FROM PUBLIC;
REVOKE ALL ON SCHEMA t002 FROM PUBLIC;
GRANT USAGE ON SCHEMA t001 TO t001;
GRANT USAGE ON SCHEMA t002 TO t002;

-- Set default privileges to allow DML for each tenant on their schema
ALTER DEFAULT PRIVILEGES IN SCHEMA t001 GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO t001;
ALTER DEFAULT PRIVILEGES IN SCHEMA t001 GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO t001;
ALTER DEFAULT PRIVILEGES IN SCHEMA t002 GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO t002;
ALTER DEFAULT PRIVILEGES IN SCHEMA t002 GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO t002;

-- Drop and recreate tables for idempotent reloads
DROP TABLE IF EXISTS t001.payments CASCADE;
DROP TABLE IF EXISTS t001.customers CASCADE;
DROP TABLE IF EXISTS t002.payments CASCADE;
DROP TABLE IF EXISTS t002.customers CASCADE;

-- Customers
CREATE TABLE t001.customers (
  id SERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT NOT NULL,
  phone TEXT,
  pan TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE t002.customers (
  id SERIAL PRIMARY KEY,
  full_name TEXT NOT NULL,
  email TEXT NOT NULL,
  phone TEXT,
  pan TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Payments
CREATE TABLE t001.payments (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES t001.customers(id),
  amount NUMERIC(12,2) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  status TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE t002.payments (
  id SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES t002.customers(id),
  amount NUMERIC(12,2) NOT NULL,
  currency CHAR(3) NOT NULL DEFAULT 'USD',
  status TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed data
INSERT INTO t001.customers(full_name, email, phone, pan) VALUES
('Alice Johnson','alice@example.com','+1-555-1001','AAAAA9999A'),
('Bob Smith','bob@example.com','+1-555-1002','BBBBB9999B');

INSERT INTO t002.customers(full_name, email, phone, pan) VALUES
('Charlie Brown','charlie@example.com','+1-555-2001','CCCCC9999C'),
('Dana White','dana@example.com','+1-555-2002','DDDDD9999D');

INSERT INTO t001.payments(customer_id, amount, currency, status) VALUES
(1, 120.50, 'USD', 'CAPTURED'),
(2, 89.99, 'USD', 'PENDING');

INSERT INTO t002.payments(customer_id, amount, currency, status) VALUES
(1, 240.00, 'USD', 'FAILED'),
(2, 35.75, 'USD', 'CAPTURED');

-- Final grants (ensure tenants can DML their own objects; no cross-schema grants)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA t001 TO t001;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA t001 TO t001;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA t002 TO t002;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA t002 TO t002;
