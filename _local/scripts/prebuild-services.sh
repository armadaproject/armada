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

HC_SERVICES=(
  ./cmd/lookout:lookouthc
  ./cmd/lookoutingester:lookouthcingester
)

HOT_COLD=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --hotCold) HOT_COLD=true; shift ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

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

build_hc_service() {
  local spec="$1"
  local pkg="${spec%%:*}"
  local outname="${spec##*:}"
  local stamp="$CACHE_DIR/$outname.stamp"
  local start=$SECONDS

  local search_paths=("$pkg")
  [[ -d "./internal/common" ]] && search_paths+=("./internal/common")

  local newest
  newest=$(find "${search_paths[@]}" -name '*.go' -newer "$stamp" 2>/dev/null | head -1) || true

  if [[ -z "$newest" && -f "$stamp" && -f "./dist/armada-$outname" ]]; then
    echo "  ↷ $outname (cached)"
    return 0
  fi

  local output
  if output=$(go build -gcflags="all=-N -l" -o "./dist/armada-$outname" "$pkg/main.go" 2>&1); then
    touch "$stamp"
    echo "  ✓ $outname ($((SECONDS - start))s)"
  else
    echo "  ✗ $outname failed:"
    echo "$output"
    return 1
  fi
}

total=${#SERVICES[@]}
[ "$HOT_COLD" = true ] && total=$((total + ${#HC_SERVICES[@]}))

heartbeat() {
  local start=$SECONDS
  while true; do
    sleep 10
    echo "still building ($total services, $((SECONDS - start))s elapsed — first build can take a couple minutes)"
  done
}

echo "Pre-building $total services in parallel ..."
heartbeat &
heartbeat_pid=$!

pids=()
for pkg in "${SERVICES[@]}"; do
  build_service "$pkg" &
  pids+=($!)
done
if [ "$HOT_COLD" = true ]; then
  for spec in "${HC_SERVICES[@]}"; do
    build_hc_service "$spec" &
    pids+=($!)
  done
fi

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

total=${#SERVICES[@]}
[ "$HOT_COLD" = true ] && total=$((total + ${#HC_SERVICES[@]}))
echo "Pre-build complete (all $total services)."
