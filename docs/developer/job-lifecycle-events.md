# Job lifecycle: states, transitions, and events

Armada's job and run lifecycle is event-driven. This doc covers the state machines and the events that drive transitions between states, for the four terminal flows: succeeded, failed, preempted, cancelled.

For higher-level context on scheduling and preemption mechanics, see [scheduling_and_preempting_jobs.md](../scheduling_and_preempting_jobs.md).

## Topology

Armada is a multi-cluster system. The control plane runs once, centrally. Each worker Kubernetes cluster runs its own executor process that connects the cluster to the control plane.

```
                                Worker clusters
                              +-----------------+
                              | Executor + k8s  |
                              +-----------------+
+----------------+   gRPC     +-----------------+
| Control plane  | <--------> | Executor + k8s  |
| (server,       |   Pulsar   +-----------------+
|  scheduler,    | <--------> +-----------------+
|  ingesters)    |            | Executor + k8s  |
+----------------+            +-----------------+
```

Two transports carry information between the control plane and an executor:

- **Pulsar.** Event bus, persistent log. All lifecycle events flow through Pulsar. Both the control-plane components and the executors publish to and subscribe from Pulsar.
- **gRPC lease stream.** Bidirectional connection initiated by the executor. The scheduler uses it to push lease, preempt, and cancel instructions to a specific executor; the executor uses it to ack and to request more work.

A consequence relevant to the rest of this doc: a single logical action (for example, preempting a run) can trigger both a Pulsar publication and a gRPC message, and the two paths are not synchronised with each other. The scheduler's Pulsar emission and the executor's subsequent Pulsar emission for the same logical action have no guaranteed ordering relative to each other, only a typical-case ordering driven by the gRPC round trip.

## Cast

A job has at most one active run at a time. A run is one attempt to actually start the pod.

Control-plane components:

- **Server.** Accepts job submissions and cancel requests over its gRPC API. Validates them and publishes the resulting events to Pulsar.
- **Scheduler.** Owns scheduling decisions. Maintains an in-memory job database (`jobdb`) periodically reconciled from the scheduler Postgres database. Decides which jobs to lease, which to preempt, which to mark terminal. Emits both run-level and job-level decision events to Pulsar. State transitions are computed in the scheduler's cycle from the current run and job flags, then persisted via emitted events that the scheduler ingester writes back to Postgres.
- **Scheduler ingester.** Consumes events from Pulsar, writes to the scheduler Postgres database. The database is the canonical state store; the scheduler's `jobdb` is a cached view.
- **Lookout ingester.** Consumes the same events independently, writes to the Lookout Postgres database. Drives what users see in the Lookout UI.
- **Event ingester.** Consumes events into Redis to back the external event-stream API that external watchers consume.
- **Conversion layer.** Code inside the API server (the same server that accepts submissions). Translates internal proto events into the external API event vocabulary when serving event-stream requests. Many internal events have no external counterpart.

Worker-cluster components:

- **Executor.** One per worker cluster, running inside that cluster. Holds the gRPC lease stream to the control plane's scheduler. Creates pods in its local Kubernetes API, watches them via a pod informer. Emits run-level observation events to Pulsar.

## State machines

Armada tracks state at two levels: per job and per run. Lookout's enums capture the public-facing state vocabulary, defined in `internal/common/database/lookout/jobstates.go`.

### Job state

```
Queued ---> Leased ---> Pending ---> Running ---> Succeeded
              |            |            |     |
              |            |            |     +--> Failed
              |            |            |     |
              |            |            |     +--> Cancelled
              |            |            |     |
              |            |            |     +--> Preempted
              |            |            |
              |            |            +-------> Failed / Cancelled / Preempted
              |            |
              |            +--------------------> Failed / Cancelled
              |
              +---------------------------------> Cancelled / Rejected
```

The scheduler's in-memory job model is a bit richer than the public enum: it tracks flags (queued, failed, succeeded, cancelled, rejected) and timestamps rather than a single state value, and computes the public state on demand. The transitions above are the user-visible ones.

### Run state

```
Leased ---> Pending ---> Running ---> Succeeded
   |           |             |    |
   |           |             |    +--> Failed
   |           |             |    |
   |           |             |    +--> Preempted
   |           |             |    |
   |           |             |    +--> Cancelled
   |           |             |
   |           |             +--------> Failed / Preempted / Cancelled
   |           |
   |           +----------------------> Failed / LeaseReturned / UnableToSchedule
   |
   +----------------------------------> LeaseReturned / LeaseExpired
```

The lookout run-state enum also defines `MaxRunsExceeded` and `Terminated`.

## Event vocabulary

Three layers of events show up in this system, and conflating them causes confusion.

**Run-level internal events** are emitted by the executor (mostly) and the scheduler (some). They describe what happened to a single run.

- `JobRunLeased`, `JobRunAssigned`, `JobRunRunning`: progress events. Not covered here.
- `JobRunSucceeded`: the executor observed `PodSucceeded`. Carries resource-usage info collected during the run.
- `JobRunErrors`: a failure occurred. Holds a discriminated union of reasons (`PodError`, `PodLeaseReturned`, `LeaseExpired`, `MaxRunsExceeded`, `JobRunPreemptedError`, `GangJobUnschedulable`, `JobRejected`, `ReconciliationError`, and others), plus optional `failure_category` and `failure_subcategory` strings assigned by the executor's classifier.
- `JobRunPreempted`: a run was preempted. Carries the preemption reason text.
- `JobRunCancelled`: a run was cancelled.

**Job-level internal events** are emitted by the scheduler when it concludes the job itself reaches a terminal state, usually one scheduling cycle after the corresponding run-level event lands in the database.

- `JobSucceeded`: the job's last run succeeded.
- `JobErrors`: the job has failed terminally (no requeue). Carries one or more `Error` values like `JobRunErrors` does.
- `CancelledJob`: the job was cancelled.
- `JobRequeued`: the job's run failed but the scheduler will re-lease it for another attempt.

**External API events** are what watchers of the event-stream API see. The conversion layer maps internal events to external events. Some internal events have no external counterpart and are silently ignored.

| Internal event             | External event                                    | Notes                                                                                                       |
| -------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `JobSucceeded` (job-level) | `JobSucceededEvent`                               |                                                                                                             |
| `JobErrors` (job-level)    | `JobFailedEvent` (for terminal)                   | Carries reason, category, subcategory.                                                                      |
| `CancelledJob`             | `JobCancelledEvent`                               |                                                                                                             |
| `JobRunPreempted`          | `JobPreemptedEvent`                               |                                                                                                             |
| `JobRunErrors`             | `JobLeaseReturnedEvent` or `JobLeaseExpiredEvent` | Only for `PodLeaseReturned` and `LeaseExpired` reasons. All other reasons ignored at the conversion layer.  |
| `JobRunSucceeded`          | (none, ignored)                                   | API watchers see success via the job-level `JobSucceeded`.                                                  |
| `JobRunCancelled`          | (none, ignored)                                   | API watchers see cancellation via the job-level `CancelledJob`.                                             |

The two-layer split has an important consequence: API watchers see the success or failure of a job only after the scheduler has emitted its job-level event, which is one scheduling cycle later than when the run actually finished. Lookout, which consumes the run-level events directly via its own ingester, updates the run-state column promptly and the job-state column with the same lag.

## Job succeeded

Run state: `Running` to `Succeeded`. Job state: `Running` to `Succeeded`.

Two phases.

Phase 1, observation, the executor:

```
container exits 0
    |
    v
kubelet sets PodSucceeded
    |
    v
executor informer fires
    |
    +-> emits JobRunSucceeded   (with resource-usage info)
```

Phase 2, conclusion, the scheduler:

```
scheduler ingester writes runs.succeeded = true, succeeded_timestamp
    |
    v
scheduler reconciles jobdb from database
    |
    v
scheduler's cycle sees the run terminal and the job has runs
    |
    +-> emits JobSucceeded      (job-level)
```

Downstream: the scheduler ingester marks the job succeeded. The lookout ingester writes both the run state and the job state to its DB. The conversion layer ignores `JobRunSucceeded` and converts `JobSucceeded` to `JobSucceededEvent` for the external API stream.

## Job failed

Two emission paths reach this flow, depending on who detected the failure.

### Path A: pod reaches terminal phase

Run state: `Running` to `Failed`. Job state: `Running` to `Failed`.

Container exits non-zero, or Kubernetes evicts the pod, or the OOM killer fires.

```
container exits non-zero (or k8s evicts)
    |
    v
kubelet sets PodFailed
    |
    v
executor informer fires
    |
    +-> executor classifier categorises the failure
    |
    +-> emits JobRunErrors      (PodError reason, failure_category, failure_subcategory)
```

Then phase 2:

```
scheduler ingester writes runs.failed = true, terminated_timestamp,
                          and inserts a row into job_run_errors
    |
    v
scheduler reconciles jobdb
    |
    v
scheduler's cycle: requeue or terminal?
    |
    +-> if requeueing: emits JobRequeued, no terminal job-level event
    +-> if terminal:   emits JobErrors (job-level, Terminal=true)
```

"Requeue" here is conditional. A failed run is only requeue-eligible if it was *returned*, meaning the run carried a `PodLeaseReturned` error (the executor never successfully ran the pod, typically because the cluster could not honour the lease). Failures with a `PodError` reason (pod ran and exited non-zero, OOM, evicted) are not requeued; the job is marked failed on the next cycle. Returned runs are additionally gated on the per-job `failFast` annotation being unset and the job's attempt count being below the scheduler's `maxAttemptedRuns` cap.

Downstream: lookout writes the run state and (when the job-level event arrives) the job state. The conversion layer converts the scheduler's `JobErrors` to `JobFailedEvent` for the API stream. The executor's `JobRunErrors` itself does not produce an external API event for `PodError`-reason failures; it only does so for `LeaseExpired` and `PodLeaseReturned`.

### Path B: executor's issue handler detects a problem first

The issue handler watches managed pods for known problem patterns: pods stuck pending too long, containers that hit pre-startup error patterns, and similar cases. When it decides a pod is broken, it emits the failure event at detection time, then deletes the pod.

```
issue handler decides the pod is broken
    |
    +-> classifier produces category and subcategory
    +-> emits JobRunErrors      (PodError reason, category, subcategory)
    |
    v
executor marks the pod for deletion, issues delete to k8s
    |
    v
pod drains
    |
    v
kubelet sets PodFailed
    |
    v
executor informer fires
    |
    +-> phase-report skipped: pod carries deletion-marked annotation
```

The state reporter skips re-emitting on the terminal-phase tick because the pod is marked for deletion. The downstream effects from there are the same as path A: the scheduler ingester writes the run as failed, the scheduler concludes the job's fate on a later cycle and emits `JobRequeued` or `JobErrors`. The conversion layer converts the latter to `JobFailedEvent`.

The shape of this path differs from path A in timing: `JobRunErrors` is emitted at detection time, not at pod-drain time, so the gap between event-emit and pod-actually-stopped can be tens of seconds for pods with a long graceful shutdown.

## Job preempted

Run state: `Running` to `Preempted` (the lookout state, driven by `JobRunPreempted`). Job state: terminal `Preempted` once the scheduler concludes.

This is the busiest flow. It also emits duplicate events to external watchers on every preemption: each preempted run produces two `JobPreemptedEvent` messages on the event-stream API, and a row-overwrite in the scheduler database that loses the scheduler's preemption description.

```
scheduler decides to preempt a run
    |
    +-> emits JobRunPreempted   (with the scheduler's preemption reason)
    +-> emits JobRunErrors      (Terminal=true, JobRunPreemptedError, scheduler's reason)
    +-> emits JobErrors         (job-level)
    |
    v
scheduler sends PreemptRuns to the executor via lease stream
    |
    v
executor's preempt processor fires
    |
    +-> emits JobRunPreempted   (a second one)
    +-> emits JobRunErrors      (a second one, generic "Run preempted" PodError, KubernetesReason=AppError)
    |
    v
executor marks the pod for deletion, issues delete to k8s
    |
    v
pod drains over terminationGracePeriodSeconds
    |
    v
kubelet sets PodFailed
    |
    v
executor informer fires
    |
    +-> phase-report skipped: pod carries deletion-marked annotation
```

Downstream conversion: each `JobRunPreempted` becomes a `JobPreemptedEvent` on the external API, so watchers see two preempted events per preemption. The executor's `JobRunErrors` is silently dropped by the conversion layer (`PodError` reason is not in the run-level handler's whitelist). The scheduler's `JobErrors` becomes the single `JobFailedEvent`.

### Known issues

Two of them.

First, `JobRunPreempted` and `JobRunErrors` are each emitted twice: once by the scheduler at decision time, once by the executor when it processes the preempt instruction. External API watchers see two `JobPreemptedEvent` messages per preemption. They do not see two `JobFailedEvent`, because run-level `JobRunErrors` with `PodError` and `JobRunPreemptedError` reasons are both dropped at the conversion layer; only the scheduler's job-level `JobErrors` becomes a `JobFailedEvent`.

Second, the scheduler ingester upserts `job_run_errors` keyed on `run_id`. Whichever `JobRunErrors` emission lands second overwrites the first.

The executor's emission typically arrives second because of the two-transport flow described in [Topology](#topology): the scheduler publishes its `JobRunErrors` to Pulsar immediately upon deciding to preempt, then issues a `PreemptRuns` gRPC message to the executor; the executor only emits its own `JobRunErrors` after receiving that gRPC message and processing it. The gRPC round-trip plus the executor's processing time put its publish strictly after the scheduler's in typical operation. Pulsar does not guarantee a global ordering across publishers, only within a single partition for a single producer, so the "typically second" claim is timing not guarantee.

The executor's `JobRunErrors` is built from a generic helper that produces a `PodError` with the literal text "Run preempted" and a misleading `KubernetesReason: AppError`. When it overwrites the scheduler's row, the specific preemption description (which job caused the eviction, which pool, what fairness violation) is replaced with this generic content. The `MarkRunsFailed` update statement also has no IS NULL guard, so the second emission overwrites `runs.terminated_timestamp` as well.

The lookout ingester is more defensive: it has an explicit `continue` for `JobRunErrors` with `JobRunPreemptedError` reason, with a comment noting that case is handled by `JobRunPreempted`. So the lookout DB does not develop duplicate rows from that specific reason combo, although it does store the executor's later `PodError`-reason emission.

## Job cancelled

Run state: `Running` to `Cancelled`. Job state: `Running` to `Cancelled`.

```
user submits cancel via API
    |
    v
server emits CancelJob to Pulsar
    |
    v
scheduler reads it in its cycle
    |
    +-> if the job has an active run, emits JobRunCancelled
    +-> emits CancelledJob      (job-level)
    |
    v
scheduler sends CancelRuns to the executor via lease stream
    |
    v
executor's remove-runs processor marks the pod for deletion
    and issues delete
    |
    v
pod drains
    |
    v
kubelet sets PodFailed (or PodSucceeded if the cancel raced container exit)
    |
    v
executor informer fires
    |
    +-> phase-report skipped: pod is marked for deletion
```

The executor's cancel path does not emit any events. Cancellation is a clean teardown driven entirely by the scheduler.

Downstream: the lookout ingester writes the run and job as cancelled. The conversion layer ignores `JobRunCancelled` and converts `CancelledJob` to `JobCancelledEvent` for the API stream.

## Summary

Per flow, internal Pulsar events emitted, grouped by run-level (about a single attempt) and job-level (about the job as a whole):

| Flow            | Run-level events                                                                                                      | Job-level events                                                                         |
| --------------- | --------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Succeeded       | `JobRunSucceeded` (executor)                                                                                          | `JobSucceeded` (scheduler)                                                               |
| Failed (path A) | `JobRunErrors` (executor)                                                                                             | `JobErrors` (scheduler, terminal) or `JobRequeued` (scheduler, if returned and eligible) |
| Failed (path B) | `JobRunErrors` (executor, at detection time)                                                                          | `JobErrors` (scheduler, terminal) or `JobRequeued`                                       |
| Preempted       | `JobRunPreempted` (scheduler) + `JobRunErrors` (scheduler) + `JobRunPreempted` (executor) + `JobRunErrors` (executor) | `JobErrors` (scheduler)                                                                  |
| Cancelled       | `JobRunCancelled` (scheduler)                                                                                         | `CancelledJob` (scheduler)                                                               |

And what the external API stream sees, per flow:

| Flow      | External API events                                                                          |
| --------- | -------------------------------------------------------------------------------------------- |
| Succeeded | `JobSucceededEvent` (from `JobSucceeded`)                                                    |
| Failed    | `JobFailedEvent` (from `JobErrors`)                                                          |
| Preempted | `JobFailedEvent` (from `JobErrors`) and 2x `JobPreemptedEvent` (from both `JobRunPreempted`) |
| Cancelled | `JobCancelledEvent` (from `CancelledJob`)                                                    |

