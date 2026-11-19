# Compare local Armada simulator candidates

Use `simulator compare` to replay one small, fixed workload through two
configurations of the same local Armada simulator executable. The command uses the production scheduler with a
simulated policy clock and writes a terminal table and JSON report.

## Run the capacity example

From the Armada checkout, set `example` to the checked-in fixture directory and
build the simulator beside the experiment file. Choose a new output directory.

```sh
example=internal/scheduler/simulator/replay/testdata/compare
go build -o "$example/simulator" ./cmd/simulator
"$example/simulator" compare --experiment "$example/capacity.json" --output capacity-results
```

Both jobs complete. With one 4-CPU node, `second` waits 60 seconds. With two
4-CPU nodes, it starts immediately. Both sides allocate 480 CPU-seconds over
the 180-second window. The capacity increases from 720 to 1,440 CPU-seconds,
so utilisation falls from 2/3 to 1/3.

## Define an experiment

Paths in the experiment are relative to that file or absolute. Use the same local
simulator executable for both candidates. The experiment and workload
files use version 1 and whole seconds for timestamps and durations.
The command runs the supplied executable with your local permissions. Use
a build you trust. Set `--timeout` to the wall-clock deadline per candidate
(default 30 seconds). Both runs use the same measurement window and fixed
offered jobs. The simulator starts empty at warm-up and schedules at the
specified cycle. Times and durations in the JSON files use integer
seconds. Admission and measurement exclude events at the end timestamp.

Choose one value for `change`.

| Value | Candidate difference |
| --- | --- |
| `none` | Identical inputs and binary. The command checks that logical outcomes and CPU accounting match. |
| `capacity` | Farm definition. |
| `queue_cordon` | One or more queue cordon flags. |

Both sides must supply byte-identical scheduler configurations. The runner
calls the scheduler synchronously on a fixed farm of healthy nodes. It
disables round-duration budgets. Set `acknowledge_unbounded_rounds` to true
to acknowledge that behavior. The adapter rejects unsupported settings such as
asynchronous scheduling, short-job penalties, away pools, and floating resources.

Use a version 1 JSON workload with a work estimation method and at most 10,000
jobs. Record how the estimates were made once in the workload’s
`work_estimation_method`. Job IDs are unique. Resource quantities are strings. Useful duration
excludes startup and discarded work. Optional `startup_s` applies on each
allocation. A positive `checkpoint_s` saves progress at that interval with
no write delay; zero gives full restart on preemption. The simulator applies
the production queue, priority class, resource and placement checks.

## Read the results

`comparison.json` contains the table values, including B−A deltas,
first-allocation wait percentiles and per-queue/priority counts. Report
version 1 summarises waits at p50, p95 and p99 by queue and priority class,
using exact nearest-rank percentiles. The sample is arrivals during the
measurement window that received an allocation before cutoff. Allocated and
unallocated arrival counts show the sample size and the excluded waits.
An empty sample gives null percentiles and null deltas for that group. Completions and cutoff
counts also include warm-up jobs still present at measurement start. CPU utilisation divides the integral
of allocated CPU requests by the integral of farm CPU capacity during the
measurement window. A farm with zero CPU capacity reports null utilisation.
These are modelled quantities for a static healthy farm.

The output directory contains the experiment, each candidate's input snapshot,
its process log, and its successful run manifest. The manifest records the
workload, executable, configuration and farm hashes, the worker protocol
version, the window and logical outcomes. A failed or incompatible run leaves its
available diagnostics under the output directory. Use a fresh directory to retry.

Each candidate also writes two raw Parquet files. Join their
`logical_job_id` to the job outcomes in `run.json` and to the supplied jobs
in `input.json`.

| File | Rows |
| --- | --- |
| `attempts.parquet` | One row per allocation attempt, including placement, resource requests, timing, end state and useful-work accounting. A running attempt is observed at cutoff. |
| `queue_intervals.parquet` | One row per period a job waited for an allocation, including immediate allocations and waits still in progress at cutoff (`censored=true`). |

Timestamp columns ending in `_at_s` are UTC epoch seconds on the replay
clock. The records include warm-up. Clip the intervals to the measurement
window to reconstruct its CPU-seconds.
