-- Create the data_engineering database only if it does not already exist.
-- PostgreSQL has no "CREATE DATABASE IF NOT EXISTS", so we generate the
-- statement conditionally and execute it with psql's \gexec meta-command.
SELECT 'CREATE DATABASE data_engineering'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'data_engineering'
)\gexec
