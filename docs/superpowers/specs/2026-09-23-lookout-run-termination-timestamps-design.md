# Lookout Run Termination Timestamps

## Purpose

Make `job_run.started` and `job_run.finished` represent Kubernetes container
lifecycle timestamps rather than timestamps from scheduler or executor event
creation. This prevents negative runtimes and makes Lookout's run history
reliable for the UI, zombie reconciliation, reporting, capacity analysis, and
Drake.

Existing rows are not repaired. The authoritative container timestamps were not
retained, so a historical backfill could not restore the correct values.

## Current Failure

The executor reads Kubernetes container status when it observes a terminal pod,
but drops the status timestamps when constructing Armada events. Lookout uses
the enclosing event's `Created` timestamp for `finished` instead.

For successful and failed runs this is the executor's event-emission time. For
cancelled and preempted runs it is the scheduler's decision time, which occurs
before the executor deletes the pod. The executor suppresses its normal
terminal status report for a pod marked for deletion, so it never supplies the
real termination timestamp for these paths.

`started` is similarly based on an executor-created event timestamp. Since the
scheduler and executor clocks are independent, the current values can produce a
negative runtime. `job_run.finished` is also last-write-wins, so its value can
depend on arrival order from the two Pulsar producers.

An independent defect compresses an empty `JobRunTerminatedDebugInfo`
`DebugMessage` into a non-nil empty byte slice. The Lookout database update
then overwrites an existing debug message.

## Event Contract

Add these optional timestamps to executor-originated events in
`pkg/armadaevents/events.proto`:

- `JobRunRunning.started_at`: the earliest non-zero `StartedAt` among
  application-container statuses.
- `JobRunSucceeded.finished_at`: the latest non-zero `FinishedAt` among
  application-container statuses.
- `JobRunErrors.finished_at`: the latest non-zero `FinishedAt` among
  application-container statuses when reporting a terminal pod failure.

Add a new `JobRunTerminated` event type to the `EventSequence` oneof. It
contains `job_id`, `run_id`, and `finished_at`. It is executor-originated and
updates only the run's completion timestamp; it does not set a run state,
failure reason, or debug data.

The executor emits `JobRunTerminated` for a marked-for-deletion pod only after
Kubernetes has reported a non-zero application-container `FinishedAt`. This
covers cancelled and preempted runs without converting their scheduler-owned
terminal state into a failed state. Repeated delivery is safe.

Application containers are the normal `PodStatus.ContainerStatuses` entries.
Init-container statuses are excluded. For multi-container pods, `started_at`
is the earliest valid application-container start and `finished_at` is the
latest valid application-container termination.

Generated protobuf outputs are regenerated using the repository's Mage proto
generation target.

## Executor Behaviour

Add focused pod-status helpers that return the earliest application-container
start and latest application-container termination timestamps. They inspect
the current state first and use `LastTerminationState` only when the current
state does not contain the relevant terminal data.

`CreateEventForCurrentState` populates the new timing fields for running,
succeeded, and failed pods. It continues to use event `Created` for event
ordering and compatibility, but that timestamp is no longer a lifecycle time.

The job-state reporter continues suppressing state-transition events for pods
marked for deletion. Separately, when a deletion-marked update exposes a
container termination timestamp, it emits the new `JobRunTerminated` event.
The emission is idempotent for duplicate pod updates; duplicate events are
also harmless at ingestion.

## Lookout Ingestion And Storage

Lookout maps the new executor timing fields into `UpdateJobRunInstruction`.
`JobRunTerminated` produces an instruction containing only `RunId` and
`Finished`.

Lookout no longer sets `Finished` for `JobRunCancelled` or `JobRunPreempted`.
It also no longer falls back to `EventSequence_Event.Created` for terminal
executor events that lack the new fields. During a rolling upgrade this leaves
`finished` null rather than storing a known-inaccurate value.

The database update is made monotonic and order-independent:

- A stored `finished` value is replaced only by a later non-null executor
  termination timestamp.
- A stored or incoming `started` value cannot be later than the resulting
  `finished` value. If stale or out-of-order data would violate that invariant,
  storage clamps the resulting completion timestamp to `started`.
- The equivalent batch and scalar fallback SQL paths apply the same rule.

This guarantees `finished >= started` for all newly written rows without
rewriting existing historical rows. A schema check constraint is not added:
adding one without history repair would reject otherwise unrelated updates to
existing invalid rows.

`handleJobRunTerminatedDebugInfo` leaves `Debug` nil when `DebugMessage` is
empty. Only non-empty debug payloads are compressed and persisted.

## Consumers

The Lookout UI and the in-memory repository clamp displayed runtime to zero as
defence in depth. Corrected stored timestamps make the normal runtime accurate;
the floor prevents an impossible negative value from an old row or external
data.

The zombie reconciler remains unchanged. Once `job_run.finished` is based on
container termination, its copy to `job.last_transition_time` also becomes
correct.

## Tests

Add or update tests for:

- extraction of application-container start and termination timestamps,
  including multi-container and init-container cases;
- executor running, succeeded, and failed events carrying Kubernetes timing;
- deletion-marked cancelled and preempted pods emitting `JobRunTerminated`
  only once a real termination timestamp is available;
- Lookout conversion of all new fields and absence of scheduler-derived
  `Finished` values;
- batch and scalar database writes with reversed event arrival order,
  preserving `finished >= started` and the latest completion timestamp;
- runtime calculation floors in both PostgreSQL-backed and in-memory Lookout
  paths; and
- empty termination-debug messages preserving an existing stored debug value.

## Rollout And Compatibility

The protobuf fields and the new oneof member are additive. Older consumers
ignore them. Older executors do not provide authoritative timestamps, so
Lookout intentionally leaves the corresponding lifecycle value null instead
of falling back to event creation time. New executors and Lookout can be
deployed independently; correctness begins once both are present.
