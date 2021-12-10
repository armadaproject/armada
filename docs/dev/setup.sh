#!/bin/sh

stream_backend="$1"

if [ -z "$stream_backend" ] || (echo "stan jetstream" | grep -v -q "$stream_backend"); then
  stream_backend="stan"
fi
echo "Using $stream_backend"

kind create cluster --name demo-a --config ./docs/dev/kind.yaml

docker-compose -f ./docs/dev/docker-compose.yaml up -d
sleep 1s

go run ./cmd/lookout/main.go --migrateDatabase

echo "go run ./cmd/armada/main.go --config ./docs/dev/config/armada/auth.yaml --config ./docs/dev/config/armada/$stream_backend.yaml"
echo "go run ./cmd/lookout/main.go --config ./docs/dev/config/lookout/$stream_backend.yaml"
echo 'ARMADA_APPLICATION_CLUSTERID=demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go'
