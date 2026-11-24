# Job Set Event Streaming at Scale: Options Under Consideration

**Status:** exploration. Nothing is decided, and no benchmarking has been done yet.
Six candidate ideas are laid out below, with a common problem statement and evaluation criteria.

## Problem

Producers (the submit API and the scheduler) publish compressed `EventSequence` batches to the
Pulsar `events` topic. Three consumers attach: `eventingester` (Redis streams), `lookoutingester`
and `scheduleringester` (Postgres state). The Event API (`GetJobSetEvents`) serves both history and
live watch from Redis streams keyed `Events:<queue>:<jobSetId>`.

Three issues with the current design:

1. **Cardinality.** Streams are per job set. 500k+ live job sets today; at 10x that is ~5M streams,
   most holding only a handful of events.
2. **Retention is TTL-only.** The 14-day key TTL is the only cleanup. Job sets with no wanted
   history still hold a stream for 14 days; active job sets accumulate unbounded history (no
   `MAXLEN`/`XTRIM`).
3. **Recovery.** Redis is the durable event store, so a Redis failure means slow, load-sensitive
   rehydration of a large keyspace.

Two constraints shape every idea:

- The per-job-set stream is a de-facto public API. External consumers attach to the deterministic
  keys, and the API cursor encodes Redis stream IDs. Changing this is an API break, not a refactor.
- `lookoutingester`'s Postgres tables are *not* an event log: the ingest path is a lossy projection
  onto job/job-run state and drops events it cannot apply. Reusing Lookout as-is is not an option.

## Workload shapes

Different queues have structurally different event patterns. Any solution must serve both:

| | Few long-lived job sets per queue | Many short-lived job sets per queue |
|---|---|---|
| Job sets per queue | ~5–10 | most of the cardinality |
| Events per job set | many, detailed progress | few |
| Watch (live tail) | yes — users follow progress | typically no |
| History | yes, long | often none beyond terminal state |
| Sensitive to | watch latency, detail | stream count, cleanup cost |

## Evaluation criteria

1. Recovery: what happens when the store fails and how fast service resumes.
2. Cardinality: cost of 5M job sets, and what bounds it.
3. Retention: can it be set per job set, and does it clean up unwanted history.
4. Client/API impact: cursor changes, watch protocol, external consumers.
5. Operational cost: new infrastructure, skills, failure modes.
6. Migration: can it land incrementally, and is it reversible.

## Ideas at a glance

| # | Idea | How it works | Pros | Cons |
|---|---|---|---|---|
| 1 | Status quo plus retention policy | Keep Redis as the history store; add per-job-set expiry on terminal events and a stream length cap | Smallest change; no new infra; API untouched | Redis still the durable store, so recovery risk stays; no queryable history after expiry; stream count only drops for finished job sets |
| 2 | Postgres history, Redis live tail | Events written to a Postgres `events` table for history; Redis streams keep just the live window | Reuses existing infrastructure; durable history; Redis becomes disposable; streams stay for every job set so live watch is unchanged | Dual writes; Postgres has no row TTL, so expiry is a delete job; event writes compete with state DB unless isolated |
| 3 | ClickHouse history, Redis live tail | Same split as idea 2, with ClickHouse as the history store | Native per-job-set TTL; unbothered at 10x; good for many-row history | New cluster to operate; a second storage technology for events |
| 4 | Pulsar-only | Partitioned per-queue topics; readers for watch; retention for history | Removes Redis entirely; fast recovery; reuses the existing transport | History queries become scans; per-job-set cursors break; watch fan-out reads whole partitions |
| 5 | Per-queue streams with client filtering | One stream per queue; clients filter for their job set | Stream count equals queue count; simple mental model | Every watcher consumes the whole queue's traffic; noisy queues; per-job-set cursors break |
| 6 | ClickHouse-only with in-memory live relay | History in ClickHouse; live events pushed by a relay inside the API, no Redis | Cleanest end state; Redis gone; live is genuinely ephemeral | Breaks the watch protocol and external stream consumers in one step; most code to build |

## Idea 1: Status quo plus retention policy

**One line:** keep Redis streams as the event store, but clean them up by policy instead of relying
solely on the 14-day TTL.

**How it would work.** Continue writing every event batch to `Events:<queue>:<jobSetId>`. Add a
per-job-set retention policy configurable per queue: expire short-lived job sets shortly after the
terminal event; expire long-lived ones N days after the last event, with a stream length cap. API,
cursors and keys stay the same.

**Workload fit.** Both shapes work without change: long-lived job sets keep their history,
short-lived ones release their stream as soon as they finish.

**Pros.** Smallest possible change — a policy decision plus cleanup logic, not a new storage path.
No new infrastructure or failure modes. Reversible and independently useful whatever is chosen
later.

**Cons.** Redis remains the durable event store, so recovery risk stays. No queryable history after
expiry. Stream count only drops for *finished* job sets; many concurrently in-flight short-lived
job sets still produce a high count. Retention stays implicit key deletion.

**Open questions.** How many of today's streams belong to terminal job sets? Where does the policy
live (queue annotations vs. server config) and who owns it? Is a stream length cap acceptable to
clients that replay full history?

**Migration.** None needed. Can land and be measured before any other idea is chosen; every other
idea benefits from it.

## Idea 2: Postgres history, Redis live tail

**One line:** events are written to a Postgres `events` table as the durable history; Redis streams
keep only a short live window and stop being the system of record.

**How it would work.** Producers are unchanged. The ingester writes each batch to both stores: the
`events` table (one row per event, batched inserts matching the existing pipeline) and the Redis
stream, as today, for every job set. The API serves history from a paginated query with cursor
`(jobSetId, sequence position)`, and live from unchanged `XREAD`; watch and poll stay separate
verbs.

**Handoff from history to live.** The client reads history up to point T, then attaches to the
stream using a resume token minted from T. A short overlap window covers stores never aligning
exactly; clients deduplicate by event sequence ID. Once attached, the stream is authoritative and
history may lag slightly.

**Retention and recovery.** Postgres: per-job-set expiry (row stamped with the job set's expiry; a
background job deletes rows per queue policy). Redis: retention drops from 14 days to hours, with
prompt eviction on the terminal event and no persistence — a Redis crash becomes a reconnect, not a
rehydration.

**Workload fit.** Few long-lived job sets: full history in Postgres, watch unchanged. Many
short-lived: events expire with the job set; streams exist only for the live window. If that window
is still too large at 10x, suppressing streams for some queue classes is a separate later decision.

**Pros.** Durable, queryable history in a store the team already operates. Redis recovery risk
removed. Streams and watch keys unchanged, so migration is additive and reversible. Retention is an
explicit per-job-set property.

**Cons.** Dual writes: every batch goes to two stores, with two places to fail. Postgres has no
row-level TTL, so expiry needs a batched, rate-limited delete job and delete storms pressure
vacuum. Event writes compete with state tables unless isolated. History cursor format changes,
which is client-visible for anything parsing stream IDs. Two storage systems plus a documented
history-to-live seam.

**Open questions.** Can batched delete-by-job-set keep up without vacuum problems? If not, idea 3
is the natural replacement. Does the events table live with Lookout's database, a separate database
on the same cluster, or its own cluster? Version the cursor change or break in place? How is the
per-queue retention policy configured, and who owns it?

## Idea 3: ClickHouse history, Redis live tail

**One line:** the same history/live split as idea 2, but ClickHouse is the durable history store.

**How it would work.** The ingester writes event rows to ClickHouse and keeps appending to Redis
streams for the live window. History reads become ClickHouse queries; live reads stay on Redis.
Each row carries the job set's expiry and the table declares `TTL expires_at DELETE`, with
per-queue policy applied per job set at write time. Redis retention is hours, without persistence.

**Workload fit.** ClickHouse fits "many rows, small payloads, time-ordered, cheap range scans": 5M
job sets with a few events each is 25M rows, which is unremarkable for it. Per-job-set expiry is
native rather than bolted on.

**Pros.** Row-level TTL expresses per-job-set expiry directly, with no delete job or vacuum concern.
Scales past 10x with little tuning; compression keeps payloads cheap. History queries are the
store's bread and butter. Redis recovery risk removed.

**Cons.** A new cluster to operate, monitor, back up and secure. A second event storage technology
on top of Postgres for state and Redis for live. ClickHouse is poor at frequent small point updates,
so the design must stay append-only. Still a dual write and still a cursor-format break. Batched
inserts must be sized well.

**Open questions.** What would operating ClickHouse involve for this team (size, backup, upgrade
cadence)? Pin the version supporting column-referencing row TTL. Does a new cluster cost more than
idea 2's delete machinery? Same cursor and policy questions as idea 2.

**Migration.** Identical client shape to idea 2, so the two are interchangeable: idea 2 can be built
first and the history store swapped later without touching the watch path.

## Idea 4: Pulsar-only

**One line:** drop Redis from the event path entirely; keep events in Pulsar, partitioned per queue,
and serve history and live from the existing transport.

**How it would work.** Producers keep publishing to Pulsar `events`, but the layout becomes one
partitioned topic per queue (or a small number of shared partitions keyed by queue). Topic retention
defines history; a history request seeks to a timestamp or key and filters for its job set. Live
watch uses Pulsar readers or a broker-side subscription. The `eventingester`'s current role goes
away.

**Workload fit.** Many short-lived job sets: cardinality becomes partitioning and filtering, which
suits it. Few long-lived: every watcher reads and filters the queue's partition, so a busy queue
means seeing everyone's events.

**Pros.** Fewest components — one transport for streaming and retention. Fast recovery is native.
No new storage technology; the team already runs Pulsar. Retention configurable per topic.

**Cons.** History is unindexed: a seek plus scan and filter per request is expensive on busy queues.
Fan-out cost is per watcher per partition. Per-job-set cursors and direct Redis consumers break.
Retention is per topic, so a long tail of short-lived job sets inside one queue cannot expire
individually. Reading beyond the topic's window is impossible.

**Open questions.** What does a history read cost in practice on a busy queue for a job set that
finished days ago? How many concurrent readers per partition is Pulsar comfortable with? Is a
compatibility shim worth it, or does this become a v2 API?

**Migration.** Hardest to migrate incrementally: storage and cursor model change together. A shim
can preserve the API shape, but then the old complexity remains.

## Idea 5: Per-queue streams with client filtering

**One line:** store one stream per queue instead of per job set; clients read the queue's stream and
filter for their job set.

**How it would work.** Replace `Events:<queue>:<jobSetId>` with `Events:<queue>` (Redis or Pulsar).
Events are tagged with the job set ID in the payload. Watch reads the queue stream from a cursor and
filters; history can be the queue stream with filtering or a history table from idea 2/3 alongside.

**Workload fit.** Many short-lived: stream count drops to the number of queues — the cardinality
win. Few long-lived: watch works, but each watcher consumes all other job sets' traffic on that
queue.

**Pros.** Directly attacks the 5M-stream problem. Simple model. Composes with other ideas: it could
be the topic layout for idea 4 or the live layer for idea 2. Retention per queue fits the "queue owns
its policy" instinct.

**Cons.** Fan-out cost moves to the client or a relay: every watcher sees every event. Per-job-set
cursors disappear. Filtering cost grows with queue event rate, not with the client's own job set.
No isolation — one noisy queue loads all its watchers. History is unindexed unless combined with a
history store. External consumers break.

**Open questions.** What is the realistic watcher count per queue? Should filtering happen
client-side, in the API, or in a relay? Does any client need to watch more than one job set at a
time?

**Migration.** Requires a new key layout, so external consumers break. Can run alongside the old
layout during a transition at the cost of writing both.

## Idea 6: ClickHouse-only with in-memory live relay

**One line:** durable history lives in ClickHouse; live events are pushed from a relay inside the
API server, so there is no Redis in the event path at all.

**How it would work.** The ingester writes events only to ClickHouse. A live relay inside the Event
API subscribes to the Pulsar `events` topic with a reader (not an acking subscription) and holds a
short in-memory buffer per recently active job set. A watcher attaches to the relay; on a miss it
falls back to ClickHouse history and reattaches. In-memory state is purely ephemeral.

**Workload fit.** Both shapes fit: long-lived job sets have few watchers and hot entities; short-lived
ones live entirely as rows and never create a per-entity storage object.

**Pros.** One durable store plus an ephemeral cache with no recovery obligation. No Redis at all.
Live and history are cleanly separated by nature. Live features are in-process code rather than
storage design. History gets ClickHouse's TTL and scale.

**Cons.** Largest change: removes the watch protocol and the direct Redis stream contract at once.
A new ClickHouse cluster plus a relay to build and operate. The relay needs per-job-set buffering,
backpressure and restart design. Reconnects depend on history being caught up. External consumers
must move to the API or lose access.

**Open questions.** Can the relay be stateless per watcher rather than buffering per job set? What
is the reconnect story in detail (buffer window, cursor semantics, duplicate suppression)? Should
this be the target with idea 2 or 3 as an intermediate step rather than a first delivery?

**Migration.** Not incremental unless the old stream path runs in parallel. The realistic path is
idea 2 or 3 first, then converge here once external consumers have moved off Redis.

## What we would like to do next

Before choosing anything: measure. The numbers that decide between these ideas are current Redis
memory and stream count, in-flight job sets per queue class, event throughput, and peak concurrent
watchers. Without those, the ranking above is opinion.

## Common open questions

1. How many of the ~500k job sets are in flight versus finished but still retained?
2. Do any published SDKs parse Redis stream IDs out of cursors, or are cursors opaque?
3. How much isolation do event writes need from the existing state databases?
4. Is a stream-per-job-set contract something we must preserve for external consumers, or is the
   API the only supported interface?
