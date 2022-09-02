#!/bin/sh

OSTYPE=$(uname -s)
COMPOSE_CMD='docker-compose'
CLUSTER_NAME="demo-a"
COMPOSE_FILE="./docs/dev/armada-compose.yaml"

docker compose > /dev/null 2>&1
if [ $? -eq 0 ] ; then
  COMPOSE_CMD='docker compose'
fi

CMD="$COMPOSE_CMD -f $COMPOSE_FILE"

# First argument is service-name (e.g. 'postgres'), second argument is 'up' or 'down'
run_service() {
  if [ $2 == "up" ]; then
    $COMPOSE_CMD -f $COMPOSE_FILE up -d $1
  else
    $COMPOSE_CMD -f $COMPOSE_FILE stop $1
  fi
}

echo -n "Looking for running Kind cluster $CLUSTER_NAME ..."
running_clusters=$(kind get clusters)
if [ "$running_clusters" != "$CLUSTER_NAME" ] ; then
  echo "not found.  Creating Kind cluster $CLUSTER_NAME"
  kind create cluster --name $CLUSTER_NAME --config ./docs/dev/kind.yaml
else
  echo "found it."
fi

sleep 3

echo "Starting Postgres and running database migration"
run_service postgres up
sleep 3
go run ./cmd/lookout/main.go --migrateDatabase
run_service postgres down

echo -n Creating a .env file in Armada project root...
my_uid=$(id -u)
my_gid=$(id -g)

rm -f .env
echo "APP_UID=$my_uid" >> .env
echo "APP_GID=$my_gid" >> .env
echo " done"


echo "To start the Armada services, please run (press <ctrl>-C to stop)"
echo "    $CMD up"
echo
echo "If the Lookout UI Service starts successfully, you should be able to"
echo "access the Lookout UI at http://hostname-of-this-system:8089"
echo
