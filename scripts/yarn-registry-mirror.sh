#!/bin/bash

# Yarn registry mirror

# It may be necessary to use a mirror of the Yarn registry when you are
# developing. This script provides a wrapper around the `yarn` command to
# support this, while ensuring `yarn.lock` files in the repository keep using
# the public Yarn registry.

# It sets Yarn to use your desired registry mirror from your
# `ARMADA_NPM_REGISTRY` environment variable, then in `yarn.lock` files,
# replaces this mirror with `https://registry.yarnpkg.com`.

# You can use this script in place of the `yarn` command - for example,
# `scripts/yarn-registry-mirror.sh --cwd internal/lookoutui add is-thirteen`.


set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"

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
