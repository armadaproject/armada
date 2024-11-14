#!/bin/bash
# This script is intended to be run under the docker container at $ARMADADIR/build/python-api-client/

# make the python package armada.client, not pkg.api
mkdir -p proto/armada
cp pkg/api/event.proto pkg/api/submit.proto pkg/api/health.proto pkg/api/job.proto pkg/api/binoculars/binoculars.proto proto/armada
sed -i 's/\([^\/]\)pkg\/api/\1armada/g' proto/armada/*.proto
cp -rf proto/* client/python/armada_client/proto/
