-- Creates the databases Armada's components expect. Mounted into the postgres
-- container's /docker-entrypoint-initdb.d/, so it runs once on first init.
-- The goreman flow does the equivalent in _local/scripts/init.sh.
CREATE DATABASE scheduler;
CREATE DATABASE lookout;
CREATE DATABASE lookouthc;
