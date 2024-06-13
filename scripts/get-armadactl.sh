#!/bin/sh -e

# Determine Platform
SYSTEM=$(uname | sed 's/MINGW.*/windows/' | tr A-Z a-z)
if [ "$SYSTEM" = "windows" ]; then
  ARCHIVE_TYPE=zip
  UNARCHIVE="zcat > armadactl.exe"
else
  ARCHIVE_TYPE=tar.gz
  UNARCHIVE="tar xzf -"
fi

# Determine architecture
get_arch() {
    case $(uname -m) in
        "x86_64" | "amd64" ) echo "amd64" ;;
        "i386" | "i486" | "i586") echo "386" ;;
        "aarch64" | "arm64" | "arm") echo "arm64" ;;
        "mips64el") echo "mips64el" ;;
        "mips64") echo "mips64" ;;
        "mips") echo "mips" ;;
        *) echo "unknown" ;;
    esac
}

ARCH=$(get_arch)
if [ "$SYSTEM" = "darwin" ]; then
    ARCH="all"
fi

# Get latest release
get_latest_release() {
    curl --silent "https://api.github.com/repos/armadaproject/armada/releases/latest" | \
    grep '"tag_name":' | \
    sed -E 's/.*"([^"]+)".*/\1/'
}

VERSION=$(get_latest_release)

ARMADACTL_URL="https://github.com/armadaproject/armada/releases/download/$VERSION/armadactl_${VERSION#v}_${SYSTEM}_${ARCH}.${ARCHIVE_TYPE}"

echo "Downloading armadactl $VERSION for $SYSTEM/$ARCH"

# Download and untar/unzip armadactl
if curl -sL $ARMADACTL_URL | sh -c "$UNARCHIVE" ; then
	echo "armadactl downloaded successfully"

  # Move armadactl binary to a directory in user's PATH
  TARGET_DIR="$HOME/bin" # Change this to the desired target directory in your user's home
  mkdir -p "$TARGET_DIR"
  cp armadactl "$TARGET_DIR/"
  export PATH="$TARGET_DIR:$PATH"
  echo "armadactl copied to $TARGET_DIR/armadactl"
  echo "armadactl is now available on your PATH"

else
	echo "Something is amiss!"
	echo "Please visit:"
	echo "  - https://github.com/armadaproject/armada/releases/latest"
	echo "to find the latest armadactl binary for your platform"
fi
