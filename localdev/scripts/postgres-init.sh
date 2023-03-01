#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER docker;
    CREATE DATABASE postgresv2;
    GRANT ALL PRIVILEGES ON DATABASE postgresv2 TO docker;
EOSQL