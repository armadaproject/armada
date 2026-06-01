#!/bin/bash

# Armada Local Development Initialization Script
# ===============================================
#
# This script initializes the local development environment for Armada.
# It performs the following operations:
#
# 1. Waits for PostgreSQL container to be ready (up to 30 attempts)
# 2. Creates required databases (scheduler, lookout) if they don't exist
# 3. Runs database migrations for scheduler and lookout components
# 4. Applies Kubernetes priority class for Armada workloads
#
# Prerequisites:
# - Docker must be running with PostgreSQL container named 'postgres'
# - Go toolchain must be installed (or GO_BIN environment variable set)
# - kubectl must be configured with access to target Kubernetes cluster
#
# Configuration files are loaded from:
# - _local/scheduler/config.yaml - Scheduler configuration
# - _local/lookout/config.yaml - Lookout configuration
# - _local/priority-class.yaml - Kubernetes priority class definition
#
# Environment variables:
# - POSTGRES_CONTAINER: Name of the PostgreSQL Docker container (default: postgres)
# - GO_BIN: Path to Go binary (default: go)

set -euo pipefail # Exit on error, undefined variable, and pipe failure

# Configuration
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-postgres}"
GO_BIN="${GO_BIN:-go}"

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
  if [ $i -eq 30 ]; then
    print_error "PostgreSQL failed to start after 30 attempts"
    exit 1
  fi
  print_info "PostgreSQL not ready yet, retrying in 2s... (attempt $i/30)"
  sleep 2
done

# db_exists returns 0 if the named database exists, 1 if not. If psql itself fails
# (auth error, connection hiccup after pg_isready passed), it exits the script rather
# than letting an empty result be misread as "database missing" and triggering a
# CREATE DATABASE against an unhealthy server. -tA gives a bare "1" or empty output.
db_exists() {
  local name="$1" out
  out=$(docker exec "${POSTGRES_CONTAINER}" psql -U postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '${name}'") \
    || { print_error "psql failed while checking for database '${name}'"; exit 1; }
  [ "${out}" = "1" ]
}

create_db_if_missing() {
  local name="$1"
  if db_exists "${name}"; then
    print_info "${name} database already exists"
  else
    docker exec "${POSTGRES_CONTAINER}" psql -U postgres -c "CREATE DATABASE ${name};"
    print_success "Created ${name} database"
  fi
}

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

print_info "Creating databases if needed..."
create_db_if_missing scheduler
create_db_if_missing lookout

print_success "Databases ready!"

run_migration Scheduler $GO_BIN run ./cmd/scheduler/main.go migrateDatabase --config ./_local/scheduler/config.yaml
run_migration Lookout $GO_BIN run ./cmd/lookout/main.go --migrateDatabase --config ./_local/lookout/config.yaml

print_success "All migrations completed successfully!"

if kubectl cluster-info >/dev/null 2>&1; then
  print_info "Applying Kubernetes priority class..."
  if kubectl apply -f _local/priority-class.yaml; then
    print_success "Priority class 'armada-default' applied"
  else
    print_error "Failed to apply priority class"
    exit 1
  fi
else
  print_info "No Kubernetes cluster reachable, skipping priority class (fine for fake-executor or pure dependency setup)"
fi

print_success "Armada local development environment initialized successfully!"
