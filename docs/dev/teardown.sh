#!/bin/sh

CLUSTER_NAME="demo-a"
COMPOSE_FILE="./docs/dev/armada-compose.yaml"

# Ensure armada, armada-executor, and armada-lookout are not running

COMPOSE_CMD='docker-compose'

docker compose > /dev/null 2>&1
if [ $? -eq 0 ] ; then
  COMPOSE_CMD='docker compose'
fi

$COMPOSE_CMD -f $COMPOSE_FILE down

echo -n "Looking for running Kind cluster $CLUSTER_NAME ..."
running_clusters=$(kind get clusters)
if [ "$running_clusters" != "$CLUSTER_NAME" ] ; then
  echo "not found - exiting now."
else
  echo "found it - deleting it."
  kind delete cluster --name $CLUSTER_NAME
fi
