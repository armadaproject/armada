#!/bin/bash

cd ../../

echo "Downloading armadactl for your platform"

ARCHIVE_TYPE=tar.gz
UNARCHIVE="tar xzf -"

SYSTEM=$(uname | sed 's/MINGW.*/windows/' | tr A-Z a-z)

# Find the latest Armada version
LATEST_GH_URL=$(curl -fsSLI -o /dev/null -w %{url_effective} https://github.com/armadaproject/armada/releases/latest)
ARMADA_VERSION=${LATEST_GH_URL##*/}
ARMADACTL_URL="https://github.com/armadaproject/armada/releases/download/$ARMADA_VERSION/armadactl-$ARMADA_VERSION-$SYSTEM-amd64.$ARCHIVE_TYPE"

# Download and untar/unzip armadactl
if curl -sL $ARMADACTL_URL | sh -c "$UNARCHIVE" ; then
	echo "armadactl downloaded successfully"
else
	echo "Something is amiss!"
	echo "Please visit:"
	echo "  - https://github.com/armadaproject/armada/releases/latest"
	echo "to find the latest armadactl binary for your platform"
fi

# Run the demo
./localdev/run.sh demo