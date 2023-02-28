#!/bin/bash

INFRA_SVCS="redis postgres pulsar"
ARMADA_SVCS="server lookout lookout-ingester lookoutv2 lookout-ingesterv2 executor binoculars jobservice event-ingester"
COMPOSE_FILE=""

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit
# get the first command-line argument, or 'default' if not available
command=${1:-default}

# Switch compose files if requested by command-line arg
case "$command" in
  "debug")
    echo "Starting debug compose environment"
    COMPOSE_FILE="-f docker-compose.yaml -f docker-compose.debug.yaml"
    # make golang image with delve
    docker build -t golang:1.18-delve .
    ;;
  "demo")
    NON_INTERACTIVE=1
    COMPOSE_FILE="-f ../.devcontainer/demo/docker-compose.yaml --project-directory ."
    ;;
  *)
    echo "Starting compose environment"
    # default action
    ;;
esac

# Rewrite the above to only read if NON_INTERACTIVE is not set
if [ -z "$NON_INTERACTIVE" ]; then
  read -p "Build Lookout UI? [y/N] " -n 1 -r
  echo
else
  # Set REPLY to 'y' so that the if statement below will run
  REPLY=y
fi

if [[ $REPLY =~ ^[Yy]$ ]]
then
  echo "Building Lookout UI from Yarn..."
  cd ../internal/lookout/ui
  yarn install
  yarn run openapi
  yarn run build
  cd ../../../localdev
fi

# check if kind exists, otherwise run scripts/kind-start.sh
kind --help &> /dev/null
if [ $? -ne 0 ];
then
  echo "kind not found, installing ..."
  scripts/kind-start.sh
fi

# start the kubernetes cluster if needed
kind get clusters | grep armada-test &> /dev/null
if [ $? -ne 0 ];
then
  scripts/kind-start.sh
fi

# select arm64 image for pulsar if needed
uname -a | grep "arm64" &> /dev/null
if [ $? -eq 0 ];
then
    export PULSAR_IMAGE="kezhenxu94/pulsar"
fi

# see if pulsar is already up, in which case we don't need to sleep
SLEEP_TIME=0
docker-compose ps | grep -E "pulsar.+running" &> /dev/null
if [ $? -ne 0 ];
then
    echo "Pausing for pulsar start up ..."
    SLEEP_TIME=50
fi

docker-compose $COMPOSE_FILE up -d $INFRA_SVCS
sleep $SLEEP_TIME
docker-compose $COMPOSE_FILE up -d $ARMADA_SVCS

# Give a note to users that it might take a long time to
# compile the golang code

echo "NOTE: it may take a while for the golang code to compile!"

# Apply the same logic as above to only read if NON_INTERACTIVE is not set
if [ -z "$NON_INTERACTIVE" ]; then
  read -p "View logs? [y/N] " -n 1 -r
  echo
else
  echo "Viewing logs..."
  # Set REPLY to 'y' so that the if statement below will run
  REPLY=y
fi

if [[ $REPLY =~ ^[Yy]$ ]]
then
  docker-compose $COMPOSE_FILE logs -f --tail=20
fi