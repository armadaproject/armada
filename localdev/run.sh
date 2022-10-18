#! /bin/bash
INFRA_SVCS="redis postgres pulsar stan"
ARMADA_SVCS="armada-server lookout lookout-ingester executor binoculars jobservice"

# make the dir containing this file the CWD
cd "$(dirname "$0")"

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

docker-compose up -d $INFRA_SVCS
sleep $SLEEP_TIME
docker-compose up -d $ARMADA_SVCS
