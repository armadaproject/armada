#!/bin/bash

sh ../../docs/local/armadactl.sh

cd ../../

export ARMADA_IMAGE=gresearchdev/armada-full-bundle-dev
export ARMADA_IMAGE_TAG=a08830b4911c6784d67d1227367f8505243fd167

# Install mage
go install github.com/magefile/mage@latest

# Run the demo
mage localdev no-build

