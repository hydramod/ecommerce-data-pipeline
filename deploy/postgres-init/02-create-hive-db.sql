-- deploy/postgres-init/01-create-airflow-db.sql
\set ON_ERROR_STOP on

-- Create DB only if it doesn't exist
SELECT 'CREATE DATABASE hive OWNER postgres'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'hive'
)\gexec
