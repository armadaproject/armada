# queueConfig

Test cases in this folder exercise queue lifecycle behavior (create, update,
assert, delete) via `queueConfig` on the `TestSpec` (see
`pkg/api/testspec.proto`). `queueConfig` has three sections, run in this order:

1. `setup` — create queue(s) before the rest of the test runs.
2. `update` — apply an update to the created queue(s).
3. `assertions` — check queue state.

Presence of `queueConfig` creates the queue(s) named by `TestSpec.queue` (via
`setup`, defaulted if omitted or empty). Queues created this way are always
deleted at the end of the test, even on failure.

## setup

```yaml
queueConfig:
    setup: # optional -- omit or leave empty to create with defaults
        numBatches: 5 # number of batches of queues to create (default 1)
        batchSize: 5 # queues created per batch (default 1)
        interval: "1s" # time between batches (default: as fast as possible)
        queueSpec: # optional template (see below)
            priorityFactor: 2.0
```

`queueSpec` lets a test create queues with non-default properties (anything
on `api.Queue` — priority factor, permissions, labels, etc.). One copy of the
spec is created per queue slot in the batch. When more than one queue is
created in a batch, the batched `POST /v1/batched/create_queues` endpoint is
used instead of individual `CreateQueue` calls.

## update

```yaml
queueConfig:
    update:
        priorityFactor: 2.0
```

`update` is an `api.Queue` template applied to every queue created by
`setup`. Only set the fields you want to change — unset `priorityFactor`
defaults to `1.0` to satisfy server-side validation. When more than one queue
is being updated, the batched `PUT /v1/batched/update_queues` endpoint is
used instead of individual `UpdateQueue` calls.

## assertions

A list of checks, run in order after `update`:

```yaml
queueConfig:
    assertions:
        - activeInPool: "default" # queue appears in GetActiveQueues for this pool
        - appearsInStream: true # queue appears in the GetQueues streaming response
        - matches: # GetQueue's result matches this template
              priorityFactor: 2.0 # (useful for verifying an `update` was applied)
        - deleted: true # after the queue(s) are deleted at the end of the test, GetQueue returns NOT_FOUND
```

`deleted` is evaluated after the queue(s) are deleted at the end of the test,
not before.

## Examples

- `crud_1x1.yaml` — create, update, assert, delete a single queue.
- `crud_batch_5x5.yaml` — create 25 queues (5 batches of 5) via `queueSpec`,
  update and assert them, exercising the batched endpoints.
- `active_list_1x1.yaml` — create a queue, submit jobs to it, assert it
  appears in `GetActiveQueues`.
