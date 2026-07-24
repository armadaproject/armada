# queueConfig

Test cases in this folder exercise queue lifecycle behavior (create, update,
assert, delete) via `queueConfig` on the `TestSpec` (see
`pkg/api/testspec.proto`). `queueConfig` has four sections, run in this order:

1. `setup` — create queue(s) before the rest of the test runs.
2. `update` — apply an update to the created queue(s).
3. `assertions` — check queue state.
4. `teardown` — delete the queue(s) (runs last, always, even on failure).

## setup

```yaml
queueConfig:
  setup:
    create: true       # create the queue(s) named by TestSpec.queue
    randomSuffix: true  # append a random suffix to avoid name collisions
    numBatches: 5        # number of batches of queues to create (default 1)
    batchSize: 5         # queues created per batch (default 1)
    interval: "1s"        # time between batches (default: as fast as possible)
    queueSpecs:            # optional templates (see below)
      - priorityFactor: 2.0
```

If `numBatches == 1` and `batchSize == 1` (the default), exactly one queue is
created, named `TestSpec.queue` (or `<queue>-<randomSuffix>` if
`randomSuffix` is set). Otherwise queues are named `<queue>-<index>`.

`queueSpecs` lets a test create multiple queues with different properties
(anything on `api.Queue` — priority factor, permissions, labels, etc.) in one
batch. One copy of each spec is created per queue slot in the batch. When
more than one queue is created in a batch, the batched
`POST /v1/batched/create_queues` endpoint is used instead of individual
`CreateQueue` calls.

## update

```yaml
queueConfig:
  update:
    priorityFactor: 2.0
```

`update` is an `api.Queue` template applied to every queue created by
`setup` (or to `TestSpec.queue` if `setup` didn't run). Only set the fields
you want to change — unset `priorityFactor` defaults to `1.0` to satisfy
server-side validation. When more than one queue is being updated, the
batched `PUT /v1/batched/update_queues` endpoint is used instead of
individual `UpdateQueue` calls.

## assertions

A list of checks, run in order after `update`:

```yaml
queueConfig:
  assertions:
    - activeInPool: "default"       # queue appears in GetActiveQueues for this pool
    - notActiveInPool: "default"    # queue does NOT appear in GetActiveQueues for this pool
    - appearsInStream: true          # queue appears in the GetQueues streaming response
    - matches:                        # GetQueue's result matches this template
        priorityFactor: 2.0           # (useful for verifying an `update` was applied)
    - deleted: true                    # after teardown deletes the queue(s), GetQueue returns NOT_FOUND
```

`deleted` only takes effect once `teardown` actually deletes the queue(s) —
it's evaluated as part of teardown, not before it.

## teardown

```yaml
queueConfig:
  teardown:
    skipDelete: true      # don't delete the queue(s) after the test (default: delete)
    expectNotFound: true   # expect DeleteQueue to return NOT_FOUND (tests the error path)
```

Queues are deleted by default at the end of every test that uses
`queueConfig`; set `skipDelete: true` to opt out.

## Examples

- `crud_1x1.yaml` — create, update, assert, delete a single queue.
- `crud_batch_5x5.yaml` — create 25 queues (5 batches of 5) via `queueSpecs`,
  update and assert them, exercising the batched endpoints.
- `active_list_1x1.yaml` — create a queue, submit jobs to it, assert it
  appears in `GetActiveQueues`.
