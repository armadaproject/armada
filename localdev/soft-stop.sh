#!/bin/bash

ARMADA_SVCS="server lookout lookout-ingester lookoutv2 lookout-ingesterv2 executor binoculars jobservice event-ingester"

# make the dir containing this file the CWD
cd "$(dirname "${0}")" || exit

# Stop services.
docker-compose stop $ARMADA_SVCS

