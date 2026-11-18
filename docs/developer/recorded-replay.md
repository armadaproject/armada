# Recorded workload replay

The simulator's `replay` subcommand takes a local trace of logical jobs, a fixed
farm, and an Armada scheduler configuration. It runs the production scheduling
algorithm on a simulated clock. Each result directory contains a single run.

## Run the synthetic example

From the Armada checkout, build and run the simulator with the checked-in
CPU/GPU fixture.

```sh
go build -o /tmp/armada-simulator ./cmd/simulator
/tmp/armada-simulator replay \
  --scenario internal/scheduler/simulator/replay/testdata/mixed/scenario.json \
  --output /tmp/armada-mixed-replay \
  --timeout 5m
```

Choose a new output directory on each invocation. Its parent must exist and
the directory must be outside the input workload. `--timeout` sets a host
deadline. Use a process supervisor or container memory limit when running
large inputs and give the output filesystem room for scratch and artifacts.
If interrupted, inspect `run.json` and start the next run in a fresh directory.

The fixture has a restart-only CPU job and a checkpointing GPU job. Both
allocate at warm-up, lose work to a higher-priority job at +30 minutes, and
allocate again at +40 minutes. The GPU job completes at +205 minutes.
The CPU job remains running at the +210-minute cutoff. The event times are
relative to the start of measurement.

## Scenario and farm

The example scenario is a JSON file with the following version 1 fields.

| Field | Meaning |
| --- | --- |
| `version`, `candidate_id` | Version 1 and a name for the run's candidate |
| `workload`, `farm`, `scheduler_config` | Dataset directory and YAML file paths, relative to the scenario file or absolute |
| `seed` | Recorded experiment seed. The v1 execution profile uses fixed durations and has no random delay |
| `window` | UTC epoch microseconds in `warmup_start_us`, `measurement_start_us` and `end_us`, plus the positive `cycle_us` period |
| `bucket_us`, `scratch_bytes` | Positive series bucket width in microseconds and the byte limit for each sequential scratch phase |
| `acknowledge_unbounded_rounds` | Must be true. The run validates the source scheduling config and disables its host-time hard and soft round budgets |
| `queues` | Static queue names and positive fairness weights, with optional `cordoned` and `priority_class_limits` |

Use `priority_class_limits` for overrides keyed by priority class. Each object
accepts Armada's `maximumResourceFraction` and `maximumResourceFractionByPool`,
where each pool has its own `maximumResourceFraction`. Fractions range from
zero to one. A larger scenario weight increases relative fair share.
The scheduler config supplies the priority classes, preemptibility, scheduling
rates, node indexes and pool settings. Set a positive `executorTimeout` so a
heartbeat at the round time establishes a healthy executor. `scheduler_config` contains the fields
of Armada's `SchedulingConfig` directly.

The farm YAML has `schemaVersion: 1` and `clusters`. Each cluster has a unique
`name`, a configured `pool`, and `nodeTemplates`. A template has a positive
`number` of nodes, `totalResources.resources` with Kubernetes quantity strings,
and optional `labels` and `taints`. Quantities must be representable at the
scheduler's resource resolution. The farm's resource quantities represent
allocatable capacity. Replay assigns stable node IDs in cluster/template order
and supplies the configured node ID label and the Armada cluster name label.

State starts empty at warm-up. Rows before warm-up are counted and excluded.
Rows at warm-up and before the end keep their original submission times.
Each scheduling instant processes completions and admissions before scheduling.
The end is exclusive. An attempt completing exactly at the end is observed
running, with only checkpoints reached before the cutoff treated as durable. Run a longer
warm-up to measure sensitivity to empty initialization.

The initial profile models static healthy executors, immediate acknowledgment,
placement, queue/priority resource caps and cordons, fairness and preemption,
and round token limits. Backlogged jobs remain eligible for a later round
after tokens refill. Offered jobs that cannot fit any modeled pool remain
queued with an eligibility reason. The run excludes away placement, gangs,
short-job penalties, market pricing, the optimiser, async scheduling, dynamic
executor state, pod-slot limits, floating resources, timed tolerations, and the
production retry engine. Replay implements preemption restart from the offered
job's recorded policy. Host process time and the simulated window are
independent. The result measures modeled placement and requested resources.

## Converter format

A dataset directory contains `manifest.json` and the listed Parquet files.
The JSON manifest includes `schema_version: 1`,
`execution_model: "fixed-useful-work-v1"`, `dataset_id`,
`converter_version`, `work_estimation_method`, `quality_counts` and
`partitions`. List partitions in replay order. Each entry contains a relative
`path`, SHA-256 hex `sha256`, `rows` and the actual inclusive
`min_submitted_at_us` and `max_submitted_at_us`. Empty partitions omit the
timestamps. Use a separate output directory for conversion.

All times and durations are integer microseconds. Work is the useful duration
of the logical job at its original arrival. Exclude startup and work discarded
by historical preemptions when collapsing attempts. Each job appears once in
order by `(submitted_at_us, logical_job_id)`, including ties across partitions.
Record the estimation method and counts for missing or estimated work.

| Parquet column | Type and rule |
| --- | --- |
| `logical_job_id` | UTF-8, unique and nonempty, at most 256 bytes |
| `submitted_at_us` | Signed INT64 UTC epoch microseconds |
| `queue`, `job_set`, `priority_class` | UTF-8. Queue and class must exist in the scenario. A job set may be empty |
| `queue_priority` | INT64 in the uint32 range |
| `useful_work_us` | Positive INT64 |
| `startup_us` | Nonnegative INT64. Null or absent means zero |
| `checkpoint_interval_us` | Positive INT64 or null. Null or absent means all useful progress is lost on preemption |
| `requirements_json` | UTF-8 JSON, at most 16 KiB. The object below is the supported projection |
| `gang_id`, `gang_cardinality`, `gang_node_uniformity_label` | Optional UTF-8, INT64 and UTF-8. Leave them null or absent for the initial profile |
| `work_estimation_method` | Optional UTF-8 override of the manifest method |

`resourceRequirements.requests` holds exact quantity strings, such as
`"cpu": "750m"` or `"memory": "9007199254740993"`. Optional scheduling inputs are
`nodeSelector`, `affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution`
with label expressions, and static Kubernetes `tolerations`. Carry preemptibility
via the named priority class. Preferred affinity, field expressions and job
payload fields are outside the projection. Unknown columns and JSON fields
cause validation errors.

Write flat, nonrepeated UTF-8 and signed INT64 columns. Use uncompressed or Snappy
Parquet and PLAIN or dictionary-encoded data pages. Each partition allows up to
1,024 row groups and an 8 MiB footer. A row group allows up to 65,536 rows,
32 MiB compressed and 64 MiB uncompressed column data. Compressed and uncompressed
page sizes are capped at 8 MiB with at most 1,024 values per page. A logical row
is capped at 64 KiB. With DuckDB, export at most 1,024 sorted rows per partition
to keep page and dictionary counts within the profile. The manifest supports up
to 4,096 partitions. Writers with configurable page counts can put many
conforming row groups in each partition. The reader validates
all listed data first, checks identity uniqueness with bounded sorted scratch
runs, and reads one partition and one row of lookahead during simulation.
An actual running or queued backlog still occupies memory.

## Read results

`run.json` records artifact hashes, workload provenance, source scheduler
configuration and effective budget overrides, binary identity, Go build info
and host costs. Supply `--source-patch` with the patch for a dirty build to record
its SHA-256. The run status changes to `complete` after successful output close
and hashing. An intentional cutoff produces a complete run with censored rows.
In a failed or interrupted run inspect `failure` and treat any artifacts as partial.

- `logical_jobs.parquet` has one row per admitted job, including offered demand,
  allocation and observation times, durable and provisional progress, attempt
  count, retry wait, and the arrival, carry-in or warm-up-only cohort.
- `attempts.parquet` links every allocation to a logical ID and attempt ordinal.
  It records pool/node, request quantities, timestamps, preemption type and
  durations in startup, retained, lost and provisional execution. Production
  run IDs are opaque. Use logical ID plus attempt ordinal for run comparisons.
- `series.parquet` has sparse rows in measurement buckets. Queue/priority rows
  have arrival, completion and preemption counts and the state immediately
  before each bucket end. Rows with a resource also carry the physical pool.
  Resource-time values are exact decimal strings in resource units multiplied
  by microseconds. Each interval contributes to the buckets when it executed.
  Missing rows represent zero activity.
- `summary.json` contains pool capacity, window resource-time totals and group
  counts. First allocation delay is a base-2 microsecond histogram of observed
  allocations for the measurement arrival cohort. Index 0 counts zero delay.
  Index `i > 0` counts delays from `2^(i-1)` through `2^i-1` microseconds.
  Use the denominator and censored counts when comparing delay tails.

Scratch accounting retains compact execution intervals on disk. Series
aggregation scans that file in bounded batches of buckets, so finer bucket
widths increase processing cost. Configure the scratch cap and measure host
usage on representative synthetic inputs before scheduling large experiments.
