#!/bin/bash

set -euo pipefail

REPO_ROOT="$(dirname "${BASH_SOURCE[0]}" &>/dev/null && pwd)"

YARN_LOCK_FILES=(
    "$REPO_ROOT/internal/lookoutui/yarn.lock"
)
DESIRED_PUBLIC_REGISTRY="https://registry.yarnpkg.com"

ORIGINAL_NPM_CONFIG_REGISTRY="$(npm config get registry)"
ORIGINAL_YARN_CONFIG_REGISTRY="$(yarn config get registry)"
REGISTRY_TO_USE="${ARMADA_NPM_REGISTRY:=$DESIRED_PUBLIC_REGISTRY}"

function reset {
    npm config set registry "$ORIGINAL_NPM_CONFIG_REGISTRY"
    yarn config set registry "$ORIGINAL_YARN_CONFIG_REGISTRY"
    for YARN_LOCK_FILE in "${YARN_LOCK_FILES[@]}"; do
        perl -pi -e s,"$REGISTRY_TO_USE","$DESIRED_PUBLIC_REGISTRY",g "$YARN_LOCK_FILE"
    done
}

trap reset EXIT

npm config set registry "$REGISTRY_TO_USE"
yarn config set registry "$REGISTRY_TO_USE"

yarn "$@"
