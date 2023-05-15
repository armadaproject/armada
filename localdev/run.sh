#!/bin/bash

ARMADA_SVCS="server lookout lookout-ingester lookoutv2 lookout-ingesterv2 executor binoculars event-ingester"
COMPOSE_FILE="-f docker-compose.debug.yaml"

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit
# get the first command-line argument, or 'default' if not available
command=${1:-default}

echo "Starting debug compose environment"

docker build -t golang:1.20-delve .

# Check that mage is installed
mage --help &> /dev/null
if [ $? -ne 0 ];
then
  echo "mage not found, installing ..."
  go install github.com/magefile/mage@v1.14.0
fi

cd ../

# Offer the user to build the Lookout UI
read -p "Build Lookout UI? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    mage buildlookoutui
fi

mage kind
mage startdependencies
mage checkForPulsarRunning

cd localdev

docker-compose $COMPOSE_FILE up -d $ARMADA_SVCS

# Give a note to users that it might take a long time to
# compile the golang code

echo "NOTE: It may take a while for Armada to start."

# Ask if user would like to view logs
read -p "View logs? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
  docker-compose $COMPOSE_FILE logs -f --tail=20
fi
