#!/bin/sh

stream_backend="$1"

if [ -z "$stream_backend" ] || (echo "stan jetstream" | grep -v -q "$stream_backend"); then
  stream_backend="stan"
fi
echo "Using $stream_backend"

COMPOSE_CMD='docker-compose'

DCLIENT_VERSION=$(docker version -f '{{.Client.Version}}')
if echo $DCLIENT_VERSION | grep -q '^2' ; then
  COMPOSE_CMD='docker compose'
fi

kind create cluster --name demo-a --config ./docs/dev/kind.yaml

OSTYPE=$(uname -s)
if [ $OSTYPE == "Linux" ]; then
  $COMPOSE_CMD --profile linux -f ./docs/dev/docker-compose.yaml up -d
else
  $COMPOSE_CMD -f ./docs/dev/docker-compose.yaml up -d
fi

sleep 10

go run ./cmd/lookout/main.go --migrateDatabase

echo "go run ./cmd/armada/main.go --config ./docs/dev/config/armada/base.yaml --config ./docs/dev/config/armada/$stream_backend.yaml"
echo "go run ./cmd/lookout/main.go --config ./docs/dev/config/lookout/$stream_backend.yaml"
echo 'ARMADA_APPLICATION_CLUSTERID=demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go'
echo "go run ./cmd/binoculars/main.go --config ./docs/dev/config/binoculars/base.yaml"
echo "go run ./cmd/jobservice/main.go run"
