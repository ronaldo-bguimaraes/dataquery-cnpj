#!/usr/bin/env bash
set -euo pipefail

# Optional test database for the compose query-tests service. It is separate from
# the serving database so tests never truncate the loaded competence.
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_QUERY_TEST_DB:?POSTGRES_QUERY_TEST_DB is required}"

if ! psql -U "$POSTGRES_USER" -d postgres -tAc \
  "SELECT 1 FROM pg_database WHERE datname = '$POSTGRES_QUERY_TEST_DB'" | grep -q 1; then
  createdb -U "$POSTGRES_USER" "$POSTGRES_QUERY_TEST_DB"
fi
