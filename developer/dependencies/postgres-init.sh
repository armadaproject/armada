#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER docker;
    CREATE DATABASE lookout;
    GRANT ALL PRIVILEGES ON DATABASE lookout TO docker;
    CREATE DATABASE scheduler;
    GRANT ALL PRIVILEGES ON DATABASE scheduler TO docker;
EOSQL
