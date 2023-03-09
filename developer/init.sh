#!/bin/bash

# cd to the directory of this script
cd "$(dirname "$0")"
cd ../

mage BootstrapTools
mage proto
mage buildDockers "bundle"
mage LocalDev

echo ""
echo "Completed! Run 'mage LocalDev' to start the server in future."