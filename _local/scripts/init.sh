#!/bin/bash

# Armada Local Development Initialization Script
# ===============================================
#
# This script initializes the local development environment for Armada.
# It performs the following operations:
#
# 1. Waits for PostgreSQL container to be ready (up to 30 attempts)
# 2. Runs database migrations for scheduler and lookout components
# 3. Applies Kubernetes priority classes for Armada workloads
#
# The scheduler and lookout databases are created by _local/compose/postgres-init.sql
# when the postgres container first initialises.
#
# Prerequisites:
# - Docker must be running with PostgreSQL container named 'postgres'
# - Go toolchain must be installed (or GO_BIN environment variable set)
# - kubectl must be configured with access to target Kubernetes cluster
#
# Configuration files are loaded from:
# - _local/scheduler/config.yaml - Scheduler configuration
# - _local/lookout/config.yaml - Lookout configuration
# - _local/kind/priorityclasses.yaml - Kubernetes priority class definitions
#
# Environment variables:
# - POSTGRES_CONTAINER: Name of the PostgreSQL Docker container (default: postgres)
# - GO_BIN: Path to Go binary (default: go)
#
# Flags:
# - --hotCold: Also migrate the lookouthc database (default: false)

set -euo pipefail # Exit on error, undefined variable, and pipe failure

# Configuration
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres}"
GO_BIN="${GO_BIN:-go}"
HOT_COLD=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --hotCold)
      HOT_COLD=true
      shift
      ;;
    *)
      print_error "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1"; }
print_info() { echo -e "${YELLOW}→${NC} $1"; }

print_info "Waiting for PostgreSQL container '${POSTGRES_CONTAINER}' to be ready..."
for i in {1..30}; do
  if docker exec "${POSTGRES_CONTAINER}" pg_isready -U postgres >/dev/null 2>&1; then
    print_success "PostgreSQL is ready!"
    break
  fi
  if [ "$i" -eq 30 ]; then
    print_error "PostgreSQL failed to start after 30 attempts"
    exit 1
  fi
  print_info "PostgreSQL not ready yet, retrying in 2s... (attempt $i/30)"
  sleep 2
done

# run_migration runs a migration command, logging a labelled result and aborting on failure.
run_migration() {
  local label="$1"
  shift
  print_info "Running ${label} database migrations..."
  if "$@"; then
    print_success "${label} database migrations completed"
  else
    print_error "${label} database migrations failed"
    exit 1
  fi
}

run_migration Scheduler $GO_BIN run ./cmd/scheduler/main.go migrateDatabase --config ./_local/scheduler/config.yaml
run_migration Lookout $GO_BIN run ./cmd/lookout/main.go --migrateDatabase --config ./_local/lookout/config.yaml

if [ "${HOT_COLD}" = true ]; then
  run_migration LookoutHC $GO_BIN run ./cmd/lookouthc/main.go --migrateDatabase --config ./_local/lookouthc/config.yaml
fi

print_success "All migrations completed successfully!"

if kubectl cluster-info >/dev/null 2>&1; then
  print_info "Applying Kubernetes priority classes..."
  if kubectl apply -f _local/kind/priorityclasses.yaml; then
    print_success "Priority classes applied"
  else
    # A PriorityClass's value is immutable, so re-applying with a different value fails. When that
    # happens the classes already exist, which is all the dev setup needs, so this is not fatal.
    print_info "Could not update priority classes (they already exist) - leaving them as-is"
  fi
else
  print_info "No Kubernetes cluster reachable, skipping priority classes (fine for fake-executor or pure dependency setup)"
fi

print_success "Armada local development environment initialized successfully!"
