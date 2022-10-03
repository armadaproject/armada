#! /bin/sh

# make sure that this is the CWD
cd "$(dirname "$0")"

INFRA_SVCS="redis postgres pulsar stan"
ARMADA_SVCS="armada-server lookout executor binoculars job-service"
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
