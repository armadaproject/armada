#!/bin/bash

ARMADA_SVCS="server lookout lookout-ingester lookoutv2 lookout-ingesterv2 executor binoculars event-ingester"
COMPOSE_FILE=""

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit
# get the first command-line argument, or 'default' if not available
command=${1:-default}

# Switch compose files if requested by command-line arg
case "$command" in
  "debug")
    echo "starting debug compose environment"
    COMPOSE_FILE="-f docker-compose.yaml -f docker-compose.debug.yaml"
    # make golang image with delve
    docker build -t golang:1.18-delve .
    ;;
  *)
    echo "starting compose environment"
    # default action
    ;;
esac

# Check that mage is installed
mage --help &> /dev/null
if [ $? -ne 0 ];
then
  echo "mage not found, installing ..."
  go install github.com/magefile/mage@v1.14.0
fi

cd ../

mage buildlookoutui "ask"
mage kind
mage startdependencies

# see if pulsar is already up, in which case we don't need to sleep
SLEEP_TIME=0
docker-compose ps | grep -E "pulsar.+running" &> /dev/null
if [ $? -ne 0 ];
then
    echo "Pausing for pulsar start up ..."
    SLEEP_TIME=50
fi

cd localdev

sleep $SLEEP_TIME
docker-compose $COMPOSE_FILE up -d $ARMADA_SVCS

# Give a note to users that it might take a long time to
# compile the golang code

echo "NOTE: it may take a while for the golang code to compile!"

# Ask if user would like to view logs
read -p "View logs? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
  docker-compose $COMPOSE_FILE logs -f --tail=20
fi
