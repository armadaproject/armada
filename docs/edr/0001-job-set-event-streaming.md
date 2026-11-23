# EDR 0001: Job Set Event Streaming at Scale

**Status:** Proposed
**Date:** 2026-11-23
**Deciders:** Armada platform team
**Out of scope:** Event message size and encoding (tracked separately)

## Context

Today, producers (the submit API and the scheduler) publish compressed
`EventSequence` batches to the Pulsar `events` topic. Three independent consumers
attach to it:

- `eventingester` — appends compressed event batches to Redis streams.
- `lookoutingester` — builds a relational job/job-run state view in Postgres.
- `scheduleringester` — builds scheduler state in Postgres.

The Event API (`GetJobSetEvents`) serves both event history and live watch by reading
Redis streams directly, keyed as `Events:<queue>:<jobSetId>`.

This design has three structural problems:

1. **Cardinality.** Streams are per job set. We run 500k+ live job sets; at 10x this
   becomes roughly 5M Redis streams, most containing only a handful of events. Redis
   pays stream-shaped costs (key, stream struct, TTL bookkeeping) for row-shaped data.
2. **Retention is TTL-only.** The 14-day key TTL is the only cleanup mechanism. Job
   sets whose history nobody needs still hold a stream for 14 days, and the many
   short-lived job sets that dominate the cardinality are exactly this case.
   Conversely, long-lived job sets accumulate unbounded history, since no
   `MAXLEN`/`XTRIM` is applied.
3. **Recovery.** Redis is the durable event store. A Redis failure means slow,
   load-sensitive rehydration of a large keyspace; recovery time is a real outage risk.

The per-job-set stream is also a de-facto public API: external consumers attach
directly to the deterministic stream keys, and the Event API cursor encodes Redis
stream IDs (with legacy sub-message index conversion). Any design that removes or
renames these keys is an API break, not a refactor.

## Use cases

Two dominant, structurally different access patterns:

| | Few long-lived job sets per queue | Many short-lived job sets per queue |
|---|---|---|
| Job sets per queue | ~5–10 | most of the cardinality |
| Events per job set | many, detailed progress | few |
| Watch (live tail) | yes — users follow progress | typically no |
| History | yes, long | often none beyond terminal state |
| Sensitive to | watch latency, detail | stream count, cleanup cost |

The per-job-set stream is the right model for few long-lived job sets and the wrong
one for many short-lived job sets, which supply nearly all of the cardinality.

## Decision

**Durable event history moves to a Postgres `events` table (reusing the existing
Postgres infrastructure). Redis streams keep their current role — one per job set,
created for every job set — but become a short-lived disposable cache covering only
the live window. Pulsar remains the transport. The Event API serves history from
Postgres and live events from Redis streams. Retention is per job set, configurable
per queue.**

Note on cardinality: the stream count is bounded by the retention window (roughly
jobsets in flight plus recently finished ones) instead of the current 14-day tail.
Eliminating streams for some queue classes is a possible follow-up, not part of this
decision; see Open Questions.

ClickHouse is recorded as the alternative if Postgres expiry machinery proves
insufficient at 10x; see Alternatives and Open Questions for the trigger condition.

### Data flow

1. Producers: unchanged (Pulsar `events` topic).
2. An ingester writes each event batch to both stores: the Postgres `events` table
   (one row per event; batched inserts matching the existing batch pipeline) and the
   Redis stream, as today, for every job set.
3. Event API:
   - History: paginated query over the `events` table, cursor = `(jobSetId, sequence position)`.
   - Live: unchanged `XREAD` against Redis streams.
   - Watch and poll stay separate API verbs: poll history, stream for live.

Lookout is not reused for this. It is a lossy projection of events onto state (the
ingester deliberately drops events it cannot apply to a job's state) and has no
event table or event API; it cannot serve as an event log.

### Retention

- **Postgres:** per-job-set expiry. The ingester stamps every row with the job set's
  expiry and a background job deletes rows for job sets that have expired or reached
  a terminal event under a "delete on completion" policy. Policy is configurable per
  queue and applied per job set. Example policies:
  - Short-lived job set: delete shortly after the terminal event.
  - Long-running: delete N days after the last event.
- **Redis:** retention shrinks from 14 days to hours — only enough to cover attached
  watchers. A stream is still created for every job set, and is evicted promptly on
  the terminal event. Streams are no longer the history store, so short retention
  loses nothing: clients backfill from Postgres.

Implementation note: unlike a columnar TTL, Postgres has no row-level TTL. Expiry is
a delete job keyed on `(jobSetId)`, ideally batched and rate-limited to avoid vacuum
pressure from delete storms driven by many short-lived job sets. See Open Questions
for the validation this needs.

### Recovery posture

Redis no longer holds durable state. History is durable in Postgres; Redis can run
without persistence (AOF/RDB disabled). A Redis crash recovers as an empty restart:
clients re-query history from the API and reattach to the live stream. Existing
external stream consumers keep working, with the documented understanding that the
stream only covers a recent window.

### Handoff contract (history to live)

A client reads history up to point T, then attaches to the Redis stream. The API
mints a resume token derived from T. Because the two stores never align exactly:

- A short overlap window is kept in both stores.
- Clients deduplicate by event sequence ID (events already carry IDs).
- Once attached, the stream is authoritative; history may briefly lag because
  Postgres inserts are batched.

### Client impact

The seam is intended to live inside the server: `GetJobSetEvents` reads history from
Postgres and switches to the Redis tail, so the watch loop's shape does not change.
Client-visible changes are limited to:

- Cursors become opaque positions rather than Redis stream IDs. SDKs treating cursors
  as tokens are unaffected; anything that parses or manufactures stream IDs breaks.
  `GetLastMessageId` semantics must be redefined (last known position in history).
- Clients stitching history and live themselves deduplicate by event sequence ID.

The bulk of the work is server-side: dual write, the Postgres history reader, the
expiry delete job, policy configuration, and the seam itself.

## Alternatives considered

- **A. ClickHouse as the durable history store.** Closest rival. Strictly better
  storage behavior: row-level TTL (`TTL expires_at DELETE`) expresses per-job-set
  expiry natively, and it is unbothered at 10x. Rejected for now on operational cost:
  it introduces a new cluster. Revisit if Postgres expiry or vacuum behavior fails at
  scale (see Open Questions).
- **B. Per-queue streams with client-side filtering.** Solves cardinality (streams =
  queue count) but every watcher consumes the whole queue's traffic, per-job-set
  cursors break, and filtered history requires scans. Weak as a history store.
- **C. Pulsar-only.** Partitioned per-queue topics, independent readers, retention
  policies. Fewest components and fast recovery, but history is unindexed (time-seek
  plus filter over busy partitions) and watch fan-out reads the full partition per
  watcher. Rejected at 10x.
- **D. ClickHouse-only with an in-memory watch relay.** Removes Redis entirely; the
  relay is fed by a Pulsar reader. Cleanest end state, but breaks the existing watch
  protocol and all external stream consumers in one step. Rejected as a first move;
  the chosen design can converge here later if external consumers migrate off Redis
  streams.

## Consequences

**Positive**

- Durable cardinality moves from streams to rows: ~25M event rows is manageable for
  a partitioned Postgres table, and payload size is a separate work item.
- Redis stream count drops from a 14-day TTL tail to the live window (in-flight plus
  recently finished job sets), and memory drops with it.
- TTL waste is eliminated: short-lived job sets' events expire with the job set, by policy.
- Redis recovery risk is removed; Redis becomes an ephemeral watch cache.
- Producers are unchanged; external stream consumers are unbroken; the migration is
  additive.
- Retention becomes policy-driven and observable (a stamped expiry, not a key TTL).
- No new infrastructure: the store is the Postgres already operated for Armada state.

**Negative / costs**

- Dual-write fan-out adds load to the ingester pipeline.
- Postgres has no row TTL; per-job-set expiry needs explicit delete machinery and
  vacuum care when many short-lived job sets expire at once.
- Event write load shares infrastructure with existing state stores unless isolated
  (separate database, or separate tablespace/cluster).
- Two storage systems for events; the history-to-live handoff must be documented and
  deduplicated client-side.
- History query patterns (indexing, partitioning) must be designed for the
  `(queue, jobSetId, time)` access pattern.

## Open questions

1. Sizing: current Redis memory and stream count, event throughput per queue class,
   and peak concurrent watchers; these drive both stores' sizing.
2. Delete strategy: batched delete-by-job-set vs. time partitioning plus partition
   drops. Benchmark vacuum behavior at arrival rates driven by many short-lived job
   sets before committing to the delete job. If it cannot keep up, ClickHouse
   (Alternative A) becomes the decision.
3. Deferred option, not adopted here: suppressing stream creation for high-cardinality
   queue classes (poll-only), which would cut Redis stream count to zero for those
   classes. Requires a policy model and an API answer for "live unavailable". Decide
   separately if the hours-bounded stream count from item 1 is still too high at 10x.
4. Isolation: does the events table live in the Lookout database, a separate database
   on the same cluster, or its own cluster? Shared-cluster event writes must not
   degrade UI queries.
5. Cursor versioning: the history cursor format changes. Version the API
   (`GetJobSetEvents` v2) or break in place for SDK consumers?
6. Where per-queue event-retention policy is configured (queue annotations vs. server
   config), and who owns changing it.
