#!/bin/bash

# cd to the directory of this script
cd "$(dirname "$0")"
cd ../

mage BootstrapTools
mage proto
mage buildDockers "bundle"

# Offer the user to build the Lookout UI
read -p "Build Lookout UI? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    mage buildlookoutui
fi

mage LocalDev

echo ""
echo "Completed! Run 'mage LocalDev' to start Armada in future."