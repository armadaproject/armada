#! /bin/bash
INFRA_SVCS="redis postgres pulsar stan"
ARMADA_SVCS="armada-server lookout lookout-ingester executor binoculars jobservice"
START_SVCS=""

# make the dir containing this file the CWD
cd "$(dirname "$0")"

# start the kubernetes cluster if needed
kind get clusters | grep demo-a
if [ $? -ne 0 ];
then
kind create cluster --name demo-a --config ../docs/dev/kind.yaml
fi

docker-compose up -d $INFRA_SVCS

sleep 30


# Start all services mentioned in the compose file we use, with the
# exception of the service(s) given as arguments to this script
for SVC in $ARMADA_SVCS ; do
  echo $* | grep -q $SVC
  if [ $? -ne 0 ] ; then
    START_SVCS="$START_SVCS $SVC"
  fi
done

docker-compose up -d $START_SVCS
