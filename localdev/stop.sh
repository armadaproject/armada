#!/bin/bash
COMPOSE_FILE="-f docker-compose.debug.yaml"

# Check that mage is installed
mage --help &> /dev/null
if [ $? -ne 0 ];
then
  echo "mage not found, installing ..."
  go install github.com/magefile/mage@v1.14.0
fi

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit

# Stop services.
docker-compose $COMPOSE_FILE down

cd ../

mage stopDependencies
# stop the k8s cluster.
mage KindTeardown
