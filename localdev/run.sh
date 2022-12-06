#! /bin/bash

INFRA_SVCS="redis postgres pulsar stan"
ARMADA_SVCS="armada-server lookout lookout-ingester executor binoculars jobservice event-ingester"

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit

# start the kubernetes cluster if needed
kind_output="$(kind get clusters)"
if [[ $kind_output == *"armada-test"* ]]; then
    echo "Using existing armada-test cluster"
else
    scripts/kind-start.sh
fi

# select arm64 image for pulsar if needed
os_system="$(uname -a)"
#arm_found=$(grep -c arm64 <<< $os_system)
#echo $arm_found
if [[ $arm_found == *"arm64"* ]]; then
    echo "Detecting Arm64 so will use custom pulsar image"
    PULSAR_IMAGE="kezhenxu94/pulsar"
else
    echo "Detecting Linux so will use normal pulsar image."
fi

# see if pulsar is already up, in which case we don't need to sleep
SLEEP_TIME=50
compose_list="$(docker-compose ps)"
if [[ $compose_list == *"pulsar.+running"* ]];
then
    echo "Pulsar is up, wait shortly ..."
    SLEEP_TIME=2
fi

docker-compose up --verbose -d $INFRA_SVCS
sleep $SLEEP_TIME
docker-compose up --verbose -d $ARMADA_SVCS

docker-compose ps
