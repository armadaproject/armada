#!/bin/bash

../../docs/local/armadactl.sh

cd ../../

export ARMADA_IMAGE=armada-full-bundle-dev
export ARMADA_IMAGE_TAG=a08830b4911c6784d67d1227367f8505243fd167

# Run the demo
mage localdev

