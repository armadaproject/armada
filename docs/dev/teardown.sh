#!/bin/sh

# Ensure armada, armada-executor, and armada-lookout are not running

kind delete cluster --name demo-a

COMPOSE_CMD='docker-compose'

DCLIENT_VERSION=$(docker version -f '{{.Client.Version}}')
if echo $DCLIENT_VERSION | grep -q '^2' ; then
  COMPOSE_CMD='docker compose'
fi

OSTYPE=$(uname -s)
if [ $OSTYPE == "Linux" ]; then
  $COMPOSE_CMD -f ./docs/dev/docker-compose.yaml --profile linux down
else
  $COMPOSE_CMD -f ./docs/dev/docker-compose.yaml down
fi
