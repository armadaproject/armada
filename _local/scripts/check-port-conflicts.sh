#!/usr/bin/env bash
set -euo pipefail

PORTS=(2345 2346 2347 2348 2349 2350 2351 2352 2353 3000 8080 8081 8082 8083 8084 8089 9000 9001 9002 9003 9004 9005 9006 9007 9008 50051 50052 50053)

for port in "${PORTS[@]}"; do
  proc=$(lsof -i :"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR>1{print $1}' | sort -u || true)
  [ -n "$proc" ] && echo "WARNING: port $port already in use by $proc"
done

true
