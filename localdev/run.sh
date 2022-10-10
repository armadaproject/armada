#! /bin/bash

# make the dir containing this file the CWD
cd "$(dirname "$0")"

# start the kubernetes cluster if needed
kind get clusters | grep demo-a
if [ $? -ne 0 ];
then
kind create cluster --name demo-a --config ../docs/dev/kind.yaml
fi

INFRA_SVCS="redis postgres pulsar stan"
ARMADA_SVCS="armada-server lookout lookout-ingester executor binoculars jobservice"
START_SVCS=""

# Start all services mentioned in the compose file we use, with the
# exception of the service(s) given as arguments to this script
for SVC in $INFRA_SVCS $ARMADA_SVCS ; do
  echo $* | grep -q $SVC
  if [ $? -ne 0 ] ; then
    START_SVCS="$START_SVCS $SVC"
  fi
done

docker-compose up $START_SVCS
