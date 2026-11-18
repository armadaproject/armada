#!/usr/bin/env bash
#
# Sequential pod create throughput on the dev cluster: one create at a time
# (-concurrency=1), very small vs very large pod objects.
#
# Answers "how fast can a single writer insert a pod, and what does object size
# cost?" — not the parallel ceiling (use dev-cluster-insert-bench.sh for that).
#
#   CONFIRM=yes ./_local/poc/dev-cluster-sequential-bench.sh <context>
#   CONFIRM=yes SIZES="small 200000" ./_local/poc/dev-cluster-sequential-bench.sh <context>
#
# Runs the matrix and cleanup from dev-cluster-insert-bench.sh; see
# dev-cluster-insert-bench.md for the tunables and how to read the result.
# Results: $OUT/summary.csv plus $OUT/benchmark.md (table + verdict).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# One create per round trip: 500 is plenty to average, and the batch stays short.
export COUNT=${COUNT:-500}
# small ≈ 0.4 KB encoded. large is the 32 KB preset; pass raw annotation bytes
# (SIZES="small 200000") for a pod near the 200 KB padding limit.
export SIZES=${SIZES:-"small large"}
export CONCURRENCY=1
export RATES=""
export PACE_SIZES=""
export TARGET_RATE=0 # no rate target: the achieved sequential rate is the result

exec ./_local/poc/dev-cluster-insert-bench.sh "$@"
