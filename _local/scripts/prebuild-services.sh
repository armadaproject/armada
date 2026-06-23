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

build_service() {
  local name
  name=$(basename "$1")
  local start=$SECONDS
  local output
  if output=$(go build -gcflags="all=-N -l" -o /dev/null "$1/main.go" 2>&1); then
    echo "  ✓ $name ($((SECONDS - start))s)"
  else
    echo "  ✗ $name failed: $output"
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
