#!/bin/sh -ex

echo "Downloading armadactl for your platform"

# Determine Platform
SYSTEM=$(uname | sed 's/MINGW.*/windows/' | tr A-Z a-z)
if [ "$SYSTEM" = "windows" ]; then
  ARCHIVE_TYPE=zip
  UNARCHIVE="zcat > armadactl.exe"
else
  ARCHIVE_TYPE=tar.gz
  UNARCHIVE="tar xzf -"
fi

ARCHITECTURE=arm64
[ $(uname -m) = x86_64 ] && ARCHITECTURE=amd64
[ $(uname -m) = aarch64 ] && ARCHITECTURE=arm64
# if $SYSTEM equals "darwin" then set ARCHITECTURE=all
if [ "$SYSTEM" = "darwin" ]; then
  ARCHITECTURE=all
fi

# Find the latest Armada version
LATEST_GH_URL=$(curl -fsSLI -o /dev/null -w %{url_effective} https://github.com/armadaproject/armada/releases/latest)

ARMADA_VERSION=${LATEST_GH_URL##*/}
ARMADA_VERSION_NO_V=${ARMADA_VERSION#v}
ARMADACTL_URL="https://github.com/armadaproject/armada/releases/download/$ARMADA_VERSION/armadactl_${ARMADA_VERSION_NO_V}_${SYSTEM}_$ARCHITECTURE.$ARCHIVE_TYPE"

# Download and untar/unzip armadactl
if curl -sL $ARMADACTL_URL | sh -c "$UNARCHIVE" ; then
	echo "armadactl downloaded successfully"

  # Move armadactl binary to a directory in user's PATH
    TARGET_DIR="$HOME/bin" # Change this to the desired target directory in your user's home
    mkdir -p "$TARGET_DIR"
    cp armadactl "$TARGET_DIR/"
    export PATH="$TARGET_DIR:$PATH"

    echo "armadactl is now available on your PATH at $TARGET_DIR/armadactl"

else
	echo "Something is amiss!"
	echo "Please visit:"
	echo "  - https://github.com/armadaproject/armada/releases/latest"
	echo "to find the latest armadactl binary for your platform"
fi
