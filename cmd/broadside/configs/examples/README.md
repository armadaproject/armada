# Broadside Example Configurations

This directory contains example YAML configuration files for Broadside load
tests.

These examples demonstrate various load testing scenarios and configuration
options.

You may find it useful to copy one of these example files to
`cmd/broadside/configs/`, modify it as needed, then run Broadside with the
configuration file.

## In-memory database example

This is useful for smoke-testing changes to this testing framework:

```bash
go run ./cmd/broadside --config ./cmd/broadside/configs/examples/test-inmemory.yaml
```

## Postgres in Docker example

You can run Broadside against a Postgres database in a Docker cotainer:
```bash
# Create the container
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml up postgres --wait -d

# Run the test
go run ./cmd/broadside --config ./cmd/broadside/configs/examples/test-postgres.yaml

# Clear up the container
docker compose -f ./cmd/broadside/configs/examples/docker-compose-postgres.yaml down postgres -v
```
