#!/bin/sh
# Starts an Armada component in a container of _local/compose/full.yaml. The component reads its base config. When the
# testsuite config hook writes a file for the component, the component also reads that file.
#
# Usage: run-component.sh <component> <command> [args...]
set -eu

component=$1
shift

overrides=/test-config/$component.yaml
if [ -f "$overrides" ]; then
  exec "$@" --config /config/config.yaml --config "$overrides"
fi
exec "$@" --config /config/config.yaml
