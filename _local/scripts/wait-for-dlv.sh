#!/usr/bin/env bash
# Usage: wait-for-dlv.sh standard|fake-executor
set -euo pipefail

MODE="${1:-standard}"

COMMON_SERVICES=(
  2345:server
  2346:scheduler
  2347:scheduleringester
  2348:eventingester
  2350:lookout
  2351:lookoutingester
  2352:binoculars
)

case "$MODE" in
  standard)      MODE_SERVICE=2349:executor ;;
  fake-executor) MODE_SERVICE=2353:fakeexecutor ;;
  hot-cold)      MODE_SERVICE=2349:executor ;;
  *)
    echo "Unknown mode: $MODE (expected standard or fake-executor)"
    exit 1
    ;;
esac

ENTRIES=("${COMMON_SERVICES[@]}" "$MODE_SERVICE")

case "$MODE" in
  hot-cold)
    ENTRIES+=("2354:lookouthc" "2355:lookouthcingester")
    ;;
esac

for entry in "${ENTRIES[@]}"; do
  port="${entry%%:*}"
  name="${entry##*:}"
  elapsed=0
  until lsof -i :"$port" -sTCP:LISTEN -t >/dev/null 2>&1; do
    if [ "$elapsed" -ge 60 ]; then
      echo "✗ $name timed out after 60s"
      exit 1
    fi
    echo "Waiting for $name (dlv :$port)..."
    sleep 1
    elapsed=$((elapsed + 1))
  done
  echo "✓ $name ready"
done

echo "All dlv servers ready"
