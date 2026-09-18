# Scheduler Performance Optimizations

Reduces heap allocations and GC pressure in scheduler hot paths by reusing buffers, pre-allocating maps, and eliminating redundant computation.

## How to Run Benchmarks

```bash
# Compare against origin/master:
bash internal/scheduler/bench_compare.sh

# Compare against a specific ref:
bash internal/scheduler/bench_compare.sh origin/master

# With CPU/memory profiling:
PROFILE=1 bash internal/scheduler/bench_compare.sh

# More iterations for better accuracy:
COUNT=6 BENCHTIME=3s bash internal/scheduler/bench_compare.sh
```

Output goes to `/tmp/scheduler-bench-*/`.
