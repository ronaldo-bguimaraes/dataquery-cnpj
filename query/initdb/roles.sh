#!/usr/bin/env bash
set -euo pipefail

# Creates the read-only application role. The loader role is the database owner
# (POSTGRES_USER) and is allowed to write during the load.
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${CNPJ_QUERY_APP_PASSWORD:?CNPJ_QUERY_APP_PASSWORD is required}"

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v app_password="$CNPJ_QUERY_APP_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE cnpj_query_app LOGIN PASSWORD %L', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cnpj_query_app')\gexec

ALTER ROLE cnpj_query_app WITH LOGIN PASSWORD :'app_password';
SELECT format('GRANT CONNECT ON DATABASE %I TO cnpj_query_app', current_database())\gexec
GRANT USAGE ON SCHEMA public TO cnpj_query_app;
SQL
