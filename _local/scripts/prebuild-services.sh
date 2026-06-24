#!/usr/bin/env bash
# Pre-build Go services in parallel with debug flags 
# Inital build can take several minutes so report progress to user
set -euo pipefail

SERVICES=(
  ./cmd/server
  ./cmd/scheduler
  ./cmd/scheduleringester
  ./cmd/eventingester
  ./cmd/executor
  ./cmd/lookout
  ./cmd/lookoutingester
  ./cmd/binoculars
  ./cmd/fakeexecutor
)

CACHE_DIR="$(go env GOCACHE)/armada-prebuild"
mkdir -p "$CACHE_DIR"

build_service() {
  local pkg="$1"
  local name
  name=$(basename "$pkg")
  local stamp="$CACHE_DIR/$name.stamp"
  local start=$SECONDS

  local search_paths=("$pkg")
  [[ -d "./internal/$name" ]] && search_paths+=("./internal/$name")
  [[ -d "./internal/common" ]] && search_paths+=("./internal/common")

  local newest
  newest=$(find "${search_paths[@]}" -name '*.go' -newer "$stamp" 2>/dev/null | head -1) || true

  if [[ -z "$newest" && -f "$stamp" ]]; then
    echo "  ↷ $name (cached)"
    return 0
  fi

  local output
  if output=$(go build -gcflags="all=-N -l" -o /dev/null "$pkg/main.go" 2>&1); then
    touch "$stamp"
    echo "  ✓ $name ($((SECONDS - start))s)"
  else
    echo "  ✗ $name failed:"
    echo "$output"
    return 1
  fi
}

heartbeat() {
  local start=$SECONDS
  while true; do
    sleep 10
    echo "still building (${#SERVICES[@]} services, $((SECONDS - start))s elapsed — first build can take a couple minutes)"
  done
}

echo "Pre-building ${#SERVICES[@]} services in parallel ..."
heartbeat &
heartbeat_pid=$!

pids=()
for pkg in "${SERVICES[@]}"; do
  build_service "$pkg" &
  pids+=($!)
done

trap 'kill "${pids[@]}" "$heartbeat_pid" 2>/dev/null' EXIT
failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed=$((failed + 1))
done
kill "$heartbeat_pid" 2>/dev/null
trap - EXIT

if [ "$failed" -gt 0 ]; then
  echo "Pre-build finished with $failed failure(s)."
  exit 1
fi

echo "Pre-build complete (all ${#SERVICES[@]} services)."
