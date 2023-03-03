#!/bin/sh -ex

echo "Downloading armadactl for your platform"

# Determine Platform
SYSTEM=$(uname | sed 's/MINGW.*/windows/' | tr A-Z a-z)
if [ $SYSTEM == "windows" ]; then
  ARCHIVE_TYPE=zip
  UNARCHIVE="zcat > armadactl.exe"
else
  ARCHIVE_TYPE=tar.gz
  UNARCHIVE="tar xzf -"
fi

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