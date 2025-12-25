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

set -eu # Exit on error and undefined variable

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

print_info "Creating databases if needed..."
if ! docker exec "${POSTGRES_CONTAINER}" psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'scheduler'" | grep -q 1; then
  docker exec "${POSTGRES_CONTAINER}" psql -U postgres -c "CREATE DATABASE scheduler;"
  print_success "Created scheduler database"
else
  print_info "Scheduler database already exists"
fi

if ! docker exec "${POSTGRES_CONTAINER}" psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'lookout'" | grep -q 1; then
  docker exec "${POSTGRES_CONTAINER}" psql -U postgres -c "CREATE DATABASE lookout;"
  print_success "Created lookout database"
else
  print_info "Lookout database already exists"
fi

print_success "Databases ready!"

print_info "Running scheduler database migrations..."
if $GO_BIN run ./cmd/scheduler/main.go migrateDatabase --config ./_local/scheduler/config.yaml; then
  print_success "Scheduler database migrations completed"
else
  print_error "Scheduler database migrations failed"
  exit 1
fi

print_info "Running lookout database migrations..."
if $GO_BIN run ./cmd/lookout/main.go --migrateDatabase --config ./_local/lookout/config.yaml; then
  print_success "Lookout database migrations completed"
else
  print_error "Lookout database migrations failed"
  exit 1
fi

print_success "All migrations completed successfully!"

print_info "Applying Kubernetes priority class..."
if kubectl apply -f _local/priority-class.yaml; then
  print_success "Priority class 'armada-preemptible' applied"
else
  print_error "Failed to apply priority class"
  exit 1
fi

print_success "Armada local development environment initialized successfully!"
