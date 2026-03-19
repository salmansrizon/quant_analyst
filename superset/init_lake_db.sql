-- init_lake_db.sql
-- Runs once when postgres container first starts.
-- Creates a dedicated read-only user + lake schema for Superset.

-- Lake schema (separate from Dagster internal tables)
CREATE SCHEMA IF NOT EXISTS lake;

-- Read-only Superset user
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'superset_reader') THEN
    CREATE ROLE superset_reader LOGIN PASSWORD 'superset_reader_pass';
  END IF;
END
$$;

-- Grant access
GRANT CONNECT ON DATABASE dagster TO superset_reader;
GRANT USAGE  ON SCHEMA lake       TO superset_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lake TO superset_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA lake
  GRANT SELECT ON TABLES TO superset_reader;

-- Placeholder tables (real data loaded by pg_loader.py after scrapers run)
CREATE TABLE IF NOT EXISTS lake.sectors (
    symbol      TEXT,
    sector      TEXT,
    company     TEXT
);

CREATE TABLE IF NOT EXISTS lake.prices (
    symbol      TEXT,
    date        TEXT,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      BIGINT
);

CREATE TABLE IF NOT EXISTS lake.announcements (
    symbol      TEXT,
    date        TEXT,
    text        TEXT,
    link        TEXT
);
