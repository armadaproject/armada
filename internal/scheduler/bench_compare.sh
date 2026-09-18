#!/usr/bin/env bash
# Benchmark comparison tool for scheduler performance.
#
# Usage:
#   # Compare current branch against a ref (default: origin/master):
#   ./bench_compare.sh
#   ./bench_compare.sh origin/master
#   ./bench_compare.sh main
#
#   # Run with profiling (generates CPU + memory + GC traces):
#   PROFILE=1 ./bench_compare.sh
#
# Prerequisites:
#   go install golang.org/x/perf/cmd/benchstat@latest

set -euo pipefail

BASE_REF="${1:-origin/master}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTDIR="/tmp/scheduler-bench-$(date +%Y%m%d-%H%M%S)"
BENCHSTAT=$(command -v benchstat)
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BENCHTIME="${BENCHTIME:-2s}"
COUNT="${COUNT:-5}"
PROFILE="${PROFILE:-0}"

if [ -z "$BENCHSTAT" ]; then
  echo "benchstat not found. Install: go install golang.org/x/perf/cmd/benchstat@latest"
  exit 1
fi

mkdir -p "$OUTDIR"

BENCH_REGEXP="BenchmarkScheduleMany|BenchmarkNodeDbConstruction"
SCHED_PACKAGES="./internal/scheduler/nodedb/... ./internal/scheduler/scheduling/..."

echo "=== Scheduler Benchmark Comparison ==="
echo "  Base:       $BASE_REF"
echo "  Branch:     $CURRENT_BRANCH"
echo "  Output:     $OUTDIR"
echo "  Benchtime:  $BENCHTIME"
echo "  Count:      $COUNT"
echo "  Profile:    $PROFILE"
echo ""

cd "$REPO_ROOT"

# Restore original branch on exit/interrupt
cleanup() {
  echo ""
  echo "Restoring branch: $CURRENT_BRANCH"
  git checkout "$CURRENT_BRANCH" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# --- Helper: run benchmarks on a ref ---
run_benchmarks() {
  local ref="$1"
  local label="$2"
  local outfile="$OUTDIR/${label}.txt"
  local profdir="$OUTDIR/profile/${label}"
  mkdir -p "$profdir"

  echo "--- Benchmarking $ref ($label) ---"
  git checkout "$ref" 2>/dev/null
  go build $SCHED_PACKAGES 2>/dev/null

  if [ "$PROFILE" = "1" ]; then
    # CPU profile (single package — cpuprofile doesn't support multiple packages)
    echo "  Running CPU profile (5s)..."
    go test -bench="BenchmarkScheduleMany1000CpuNodes32000SmallJobs" \
      -benchmem -benchtime=5s -count=1 -timeout=120s \
      -cpuprofile="$profdir/cpu.prof" ./internal/scheduler/nodedb/... 2>&1 | grep -E "^Benchmark|^ok"
    echo "  CPU profile: $profdir/cpu.prof"

    # Memory profile (single package — memprofile doesn't support multiple packages)
    echo "  Running memory profile (5s)..."
    go test -bench="BenchmarkScheduleMany1000CpuNodes32000SmallJobs" \
      -benchmem -benchtime=5s -count=1 -timeout=120s \
      -memprofile="$profdir/mem.prof" ./internal/scheduler/nodedb/... 2>&1 | grep -E "^Benchmark|^ok"
    echo "  Memory profile: $profdir/mem.prof"

    # GC trace
    echo "  Running GC trace (3s)..."
    GODEBUG=gctrace=1 go test -bench="BenchmarkScheduleMany100CpuNodes3200SmallJobs" \
      -benchmem -benchtime=3s -count=1 -timeout=120s $SCHED_PACKAGES 2>&1 \
      | grep -E "^gc|^Benchmark" | tee "$profdir/gc_trace.txt"

    echo "  All profiles saved to $profdir/"
  fi

  echo "  Running benchmarks..."
  go test -bench="$BENCH_REGEXP" -benchmem -benchtime="$BENCHTIME" -count="$COUNT" -timeout=600s $SCHED_PACKAGES 2>&1 | grep -E "^Benchmark|^ok" | tee "$outfile"
  echo "  Results: $outfile"
  echo ""
}

# --- Phase 1: Benchmark base ---
run_benchmarks "$BASE_REF" "base"

# --- Phase 2: Benchmark current branch ---
run_benchmarks "$CURRENT_BRANCH" "branch"

# --- Phase 3: Compare ---
echo ""
echo "==========================================="
echo "  RESULTS: $CURRENT_BRANCH vs $BASE_REF"
echo "==========================================="
echo ""

benchstat "$OUTDIR/base.txt" "$OUTDIR/branch.txt"

echo ""
echo "==========================================="
echo "  SUMMARY"
echo "==========================================="
echo ""
echo "Raw data: $OUTDIR/"
echo "  base.txt   - benchmark results for $BASE_REF"
echo "  branch.txt - benchmark results for $CURRENT_BRANCH"
if [ "$PROFILE" = "1" ]; then
  echo "  profile/   - CPU, memory, and GC profiles"
  echo ""
  echo "View profiles:"
  echo "  go tool pprof -http=:8080 $OUTDIR/profile/branch/cpu.prof"
  echo "  go tool pprof -http=:8080 $OUTDIR/profile/branch/mem.prof"
fi
