#!/bin/bash
##################################################################################
##### Copy markdown files from root directory and docs/ to current directory #####
##################################################################################

# Copy CODE_OF_CONDUCT.md and CONTRIBUTING.md to _includes/ and overwrite existing files
rsync -av ../CODE_OF_CONDUCT.md ../CONTRIBUTING.md _includes/

# Copy everything from docs/ to current directory and overwrite existing files
rsync -av ../docs/* ./
