# Job Set Event Streaming at Scale: Options Under Consideration

**Status:** exploration. Nothing is decided, and no benchmarking has been done yet.
Seven candidate ideas and one cross-cutting enabler are laid out below, with a common problem
statement and evaluation criteria.

## Problem

Producers (the submit API and the scheduler) publish `EventSequence` messages to the Pulsar `events`
topic. Three consumers attach: `eventingester` (Redis streams), `lookoutingester` and
`scheduleringester` (Postgres state). The eventingester is the only writer of the Redis streams. It
compresses, filters and chunks each event sequence before writing it to
`Events:<queue>:<jobSetId>`, and can optionally write a second Redis (`RedisReplica`, used for
control-plane migration) with the same stream IDs. The Event API (`GetJobSetEvents`) serves both
history and live watch from these streams.

The Redis stream is a *watcher-facing projection*, not the full event log. Before storing, the
eventingester merges sequences per job set, splits them at 1 MB, removes `Groups`, clears
cancellation reasons, `StartUpInfo` and debug messages, drops `JobRunSucceeded`,
`JobRunTerminatedDebugInfo` and `JobValidated` entirely, and keeps only `LeaseExpired` and
`PodLeaseReturned` reasons inside `JobRunErrors`. There is therefore no complete event log anywhere
today: Pulsar retention is transient, Redis holds this projection, and Lookout holds state. Ideas 2
and 3 have to decide explicitly whether the new history store ingests the raw Pulsar events or the
same projection.

Three issues with the current design:

1. **Cardinality.** Streams are per job set. 500k+ retained job sets today; at 10x that is ~5M
   streams, most holding only a few events (one stream entry can carry several events).
2. **Retention is TTL-only.** The 14-day key TTL is the only cleanup, and it is reset on every
   write, so it runs from the last event. Job sets with no wanted history still hold a stream for
   14 days; active job sets accumulate unbounded history (no `MAXLEN`/`XTRIM`).
3. **Recovery.** Redis is the durable event store, so a Redis failure means slow, load-sensitive
   rehydration of a large keyspace.

Two constraints shape every idea:

- The per-job-set stream is a de-facto public API. The cursor is `time:seq:subSeq:last`, where
  `time:seq` is the Redis stream entry ID and `subSeq` indexes the event inside the decompressed
  chunk; legacy `time-seq` IDs are still accepted. The API also uses key existence for
  `ErrorIfMissing` (the `Watch` RPC always sets it) and the last stream entry as the one-shot end
  position, so shorter-lived streams change behaviour even when the cursor format is preserved.
  Changing any of this is an API break, not a refactor.
- `lookoutingester`'s Postgres tables are *not* an event log: `filterEventsForTerminalJobs` drops
  events for jobs it has already seen terminate, and the ingest path is a lossy projection onto
  job/job-run state. Reusing Lookout as-is is not an option.

This analysis does not assume an event size. That problem is independent of cardinality: it should
be solved, but it changes sizing and cost, not which of these architectures is viable.

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
| 1 | Status quo plus retention policy | Keep Redis as the history store; add per-job-set expiry and a stream length cap | Smallest change; no new infra; API untouched | Needs a job-set-finished signal that does not exist yet; Redis still the durable store, so recovery risk stays; stream count only drops for finished job sets |
| 2 | Postgres history, Redis live tail | Events written to a Postgres `events` table for history; Redis streams keep only a delivery window of minutes | Reuses existing infrastructure; durable history; Redis becomes disposable; streams stay for every job set, so watch is unchanged once existence and last-ID checks come from history | Dual writes; Postgres has no row TTL, so expiry is a delete job; event writes compete with state DB unless isolated |
| 3 | ClickHouse history, Redis live tail | Same split as idea 2, with ClickHouse as the history store | Per-job-set TTL applied at merge time; unbothered at 10x; good for many-row history | New cluster to operate; a second store for events |
| 4 | Pulsar-only | Partitioned per-queue topics; readers for watch; retention for history | Removes Redis entirely; fast recovery; reuses the existing transport | History queries become scans; per-job-set cursors break; watch fan-out reads whole partitions |
| 5 | Per-queue streams with client filtering | One stream per queue; the API or the client filters for a job set | Stream count equals queue count; simple mental model; can hide the layout behind the watch API | Every watcher path carries the whole queue's traffic; noisy queues; per-job-set cursors and per-job-set retention both disappear |
| 6 | ClickHouse-only with in-memory live relay | History in ClickHouse; live events pushed by a relay inside the API, no Redis | Cleanest end state; Redis gone; live is genuinely ephemeral | Breaks the watch protocol and external stream consumers in one step; most code to build |
| 7 | Pulsar per-job-set topics | Each job set gets its own Pulsar topic instead of a Redis stream | Per-job-set retention and cursors from the transport; no filtering or fan-out amplification; durable and fast to recover | Millions of short-lived topics means metadata churn and per-topic overhead; existing consumers must fan in across all topics |

## Cross-cutting enabler: watch through the API

Independent of which storage wins, the watch surface can be separated from the storage entirely. The
public contract becomes the Event API's server-streaming call — `GetJobSetEvents` today, with
optional WebSocket or Connect RPC front-ends over the same semantics for browser and non-gRPC
clients. Clients watch a job set through the API; they never attach to `Events:<queue>:<jobSetId>`
keys.

Why this is worth doing on its own:

- It is the one change that makes the history-store ideas (2, 3, 5, 6, 7) non-breaking: the cursor
  becomes opaque, and existence and last-ID are resolved server-side, so storage can move without a
  client release. Where the cons below say "the API contract changes", this enabler turns that into
  an internal change.
- The history-to-live handoff and dedupe live in one place, where the store choice is known, instead
  of being reimplemented in every client.
- Existing gRPC clients are already on this path; only components reading Redis keys directly have
  to move, and that is a one-time change with a clear owner.
- WebSocket or Connect is a transport choice for the same call, not a second event system: one
  event schema, one cursor semantics, one auth model.

The longer-term payoff is that the stream layout stops being part of the contract. If a later change
moves from per-job-set streams to per-queue streams (Idea 5), from Redis to a history store (Ideas 2
and 3), or from Redis streams to Pulsar topics (Idea 7), users keep calling the same API and
receiving the same events. They do not need to know which key layout exists, whether their events
are filtered server-side, or which store answered the read. The server owns that mapping; the API is
the only thing clients depend on.

What it costs:

- The API owns watch fan-out, backpressure, slow consumers and reconnects, so it scales with
  concurrent watchers rather than with storage.
- A WebSocket/Connect front-end is extra surface to secure, version and test, even if it proxies the
  same logic.
- Cursors must be treated as opaque from the start of the new API version; anything that parses
  stream IDs needs a compatibility window.

This does not fix cardinality or retention by itself, and it does not replace any idea below. It is
the migration seam that lets a storage change ship without an API break.

## Idea 1: Status quo plus retention policy

**One line:** keep Redis streams as the event store, but clean them up by policy instead of relying
solely on the 14-day TTL.

**How it would work.** Continue writing every event batch to `Events:<queue>:<jobSetId>`. Add a
retention policy configurable per queue: expire short-lived job sets shortly after they finish;
expire long-lived ones N days after the last event, with a stream length cap. API, cursors and keys
stay the same.

There is no "job set finished" event today: producers emit per-job events, and `CancelJobSet` is a
request, not an announcement. "Expire on terminal" therefore needs a signal the stateless ingester
does not have — per-job state tracking, a query against Lookout/scheduler state, or a new event. The
state-free fallback is expiry N days after the last event.

**Workload fit.** Both shapes work without change: long-lived job sets keep their history,
short-lived ones release their stream once they are detected as finished.

**Pros.**

- Smallest possible change — a policy decision plus cleanup logic, not a new storage path.
- No new infrastructure or failure modes.
- No client-visible change unless a stream length cap is used; keys, cursors, watch semantics and
  SDKs stay untouched.
- No dual write and no history-to-live seam, so there is no consistency or dedupe logic to build.
- Reversible and independently useful whatever is chosen later; ideas 2 and 3 benefit too, because
  the same policy can shrink their live window.
- Fastest way to get real numbers: how much of the 500k is finished-but-retained.

**Cons.**

- Redis remains the durable event store, so recovery risk stays and grows with the retained
  keyspace.
- Needs the job-set-finished signal described above; without it, per-job-set expiry collapses to
  "N days after the last event".
- No queryable history after expiry: no filtering by event type or time beyond the stream key.
- Stream count only drops for *finished* job sets; many concurrently in-flight short-lived job sets
  still produce a high count at 10x.
- Retention stays implicit key deletion, not an explicit queryable property.
- A stream length cap bounds active history but changes replay semantics for clients that expect
  the full event sequence.
- Per-queue policy × finished/live state is more config surface than it looks; misconfiguration can
  delete history users need or retain far too much.
- Leaves the API contract tied to Redis stream IDs untouched, so that break is still outstanding.

**Open questions.** How do we detect that a job set is finished — ingester-side state, a query
against Lookout/scheduler state, or a new event? How many of today's streams belong to finished job
sets? Where does the policy live (queue annotations vs. server config) and who owns it? Is a stream
length cap acceptable to clients that replay full history?

**Migration.** None needed. Can land and be measured before any other idea is chosen; every other
idea benefits from it.

## Idea 2: Postgres history, Redis live tail

**One line:** events are written to a Postgres `events` table as the durable history; Redis streams
keep only a delivery window of minutes and stop being the system of record.

**How it would work.** Producers are unchanged. The ingester writes each event to both stores: an
`events` table (one row per event, batched inserts) and the Redis stream, as today, for every job
set. Today's sink receives compressed per-job-set chunks *after* the watcher projection, so a
row-per-event sink needs a converter fed from the raw Pulsar message; storing the projection instead
would inherit its dropped events and fields. The API serves history from a paginated query with
cursor `(jobSetId, sequence position)` and live from unchanged `XREAD`; watch and one-shot reads stay
separate modes.

The API must also move `ErrorIfMissing` and the one-shot end position to the history store. Both
currently come from the Redis key (`EXISTS`, last stream entry), so with a minutes-long window a
watcher of a finished job set would get `NotFound` and a one-shot replay would return nothing.

**Handoff from history to live.** The client attaches to the stream first and buffers
incoming events, then backfills from history up to the point where live delivery began.
Events present on both sides are deduplicated by sequence ID, and the buffer is replayed
over the backfill. This ordering closes the gap that "read history, then attach" would
leave open. Because the stream holds only a delivery window, an attach can find it
trimmed or missing (after a restart or a quiet period); that is not an error — the
client falls back to history and attaches at the current position. A client that
disconnects mid-watch does the same on reconnect, so no cursor-based stream replay is
required. If the key expires while a watcher is still attached, `XREAD` keeps returning
empty; the server needs an explicit "no live stream, history exhausted" condition instead
of spinning.

**Retention and recovery.** Postgres: per-job-set expiry (row stamped with the job set's expiry; a
background job deletes rows per queue policy). A short-lived policy needs the job-set-finished
signal from Idea 1; "N days after the last event" needs no state. Redis: retention drops from 14
days to a delivery window of minutes — just long enough for attached clients to drain the stream,
because anything missed is recovered from history. Eviction is by idle time and/or stream length,
with a short grace period after the last event so attached watchers get it. No persistence: a Redis
crash becomes a reconnect, not a rehydration. The number of streams created is unchanged; only their
lifetime shrinks.

**Workload fit.** Few long-lived job sets: full history in Postgres, watch unchanged. Many
short-lived: events expire with the job set; streams exist only for the live window. If that window
is still too large at 10x, suppressing streams for some queue classes is a separate later decision.

**Pros.**

- Durable, queryable history in a store the team already operates; no new technology to learn.
- Redis recovery risk removed.
- Streams and watch keys unchanged; migration is additive and reversible. The only API-visible
  changes are the cursor format and existence/last-ID reads.
- Retention is an explicit per-job-set property.
- History can be indexed by job set, sequence, event type and time, enabling queries the stream API
  cannot offer (debugging, admin tools, filtered history) — provided the store ingests raw events,
  not the watcher projection.
- Shadow writes let us validate the new sink before switching history reads over.
- The two stores can be reconciled against each other, which is a correctness check neither has
  today (only for the fields the projection keeps).

**Cons.**

- Dual writes: every batch goes to two stores, with two places to fail and partial-write windows.
- Postgres has no row-level TTL, so expiry needs a batched, rate-limited delete job and delete
  storms pressure vacuum.
- High append volume means WAL growth and table/index bloat; partitioning and archival need
  designing up front.
- Per-job-set expiry does not line up with time-based partitions, which is the cheapest way to drop
  old data.
- Event writes compete with state tables unless isolated; isolation adds a database or cluster.
- History cursor format changes, and the current cursor carries a stream entry ID plus a sub-index.
  This is client-visible for anything parsing Redis stream IDs; legacy `time-seq` IDs would need a
  compatibility path.
- Existence and last-ID checks move to the history store; today both read the Redis key, so this is
  a server change, not just a client change.
- A short-lived expiry policy needs the job-set-finished signal that does not exist yet.
- Two storage systems plus a history-to-live seam: buffer window and client-side dedupe all have to
  be correct.
- Batched inserts make history lag live; reconnecting clients can see a gap if the seam is wrong.
- A single large events table can make long-lived job set history queries slow without careful
  indexing.

**Open questions.** Does the history store ingest raw Pulsar events or the watcher projection (which
decides whether dropped events and fields exist in history)? Can batched delete-by-job-set keep up
without vacuum problems? If not, idea 3 is the natural replacement. Does the events table live with
Lookout's database, a separate database on the same cluster, or its own cluster? Version the cursor
change or break in place? How is the per-queue retention policy configured, and who owns it?

## Idea 3: ClickHouse history, Redis live tail

**One line:** the same history/live split as idea 2, but ClickHouse is the durable history store.

**How it would work.** The ingester writes event rows to ClickHouse and keeps appending to Redis
streams for the live window. History reads become ClickHouse queries; live reads stay on Redis.
Each row carries the job set's expiry and the table declares `TTL expires_at DELETE`, with
per-queue policy applied per job set at write time. TTL is applied during merges, not at write
time: `merge_with_ttl_timeout` defaults to 4 hours, and expired rows can still be returned between
merges. A per-job-set expiry timestamp also does not line up with time partitions, so parts get
rewritten; partition-aligned TTL is the pattern ClickHouse recommends. Redis retention is a delivery
window of minutes, without persistence.

**Workload fit.** ClickHouse fits "many rows, small-ish payloads, time-ordered, cheap range scans":
5M job sets with a few events each is 25M rows, which is unremarkable for it. Row count is not the
whole cost — resource infos and error payloads are large, and compression depends on the sort key.
Per-job-set expiry is expressible directly, subject to the merge-time caveat above.

**Pros.**

- TTL is expressed per row, with no delete job or vacuum concern (applied at merge time).
- Scales past 10x without sharding by job set, though it needs its own tuning.
- History queries are the store's bread and butter, including filters and aggregations.
- Redis recovery risk removed.
- Append-only event logs are exactly what a MergeTree-style store is built for: high ingest
  throughput with columnar compression.
- Scaling out is straightforward; no complex sharding scheme keyed by job set.
- Same client shape as idea 2, so the history store can be swapped later without touching watch.

**Cons.**

- A new cluster to operate, monitor, back up and secure, plus on-call expertise.
- A third storage system to operate, on top of Postgres for state and Redis for live — and a second
  one for events.
- ClickHouse's TTL is asynchronous — rows may survive past expiry until a merge (4 hours by default),
  which matters if retention has a compliance meaning.
- No multi-statement transactions for MergeTree and no real upserts; `ReplacingMergeTree` dedupe is
  eventual, so late or out-of-order events need sequence-based dedupe.
- Small inserts are inefficient; the ingester needs batching, buffering and retry logic.
- Query performance depends on the chosen sort key; the wrong `ORDER BY` makes per-job-set history
  scans expensive.
- Still a dual write, still a cursor-format break, and still the history/live seam.
- Can become the easy answer that skips measuring whether idea 2's delete path is actually a
  problem.

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
watch uses Pulsar readers or a broker-side subscription. One subscription carries one cursor, so
fan-out to N independent watchers means N subscriptions or readers; a shared subscription
load-balances instead of broadcasting. The `eventingester`'s current role goes away.

**Workload fit.** Many short-lived job sets: cardinality becomes partitioning and filtering, which
suits it. Few long-lived: every watcher reads and filters the queue's partition, so a busy queue
means seeing everyone's events.

**Pros.**

- Fewest components — one transport for streaming and retention; Redis and its ingester go away.
- Fast recovery is native: data lives in BookKeeper with replication, so a failed broker is replaced
  and clients reconnect without replaying from the producer.
- No new storage technology; the team already runs Pulsar.
- Retention configurable per topic, so policy can differ by queue.
- Tiered storage can move old history to cheaper storage without a separate archive pipeline.
- Producers are unchanged — they already publish to Pulsar.

**Cons.**

- History is unindexed: a seek plus scan and filter per request is expensive on busy queues.
- Fan-out cost is per watcher per partition; N watchers read the same traffic N times.
- Per-job-set cursors and direct Redis consumers break — a larger API change than the history-store
  ideas.
- Retention is per topic, so a long tail of short-lived job sets inside one queue cannot expire
  individually.
- Reading beyond the topic's window is impossible; the window must cover the slowest reader.
- Pulsar compaction is topic-wide and periodic: it can drop keys via tombstones, but it cannot
  delete one job set's history on demand without compacting the whole topic. That constrains future
  data-minimization requirements.
- Topic and partition counts still need tuning; too many partitions carry their own overhead.
- Live watch latency and ordering follow Pulsar rather than the current `XREAD` path — measure it.
- A client coming back after the retention window has no history story without tiered storage.

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
With the watch-API enabler, the API does the filtering and the per-queue layout stays invisible to
users; without it, clients read the queue stream and filter themselves.

**Workload fit.** Many short-lived: stream count drops to the number of queues — the cardinality
win. Few long-lived: watch works, but each watcher consumes all other job sets' traffic on that
queue.

**Pros.**

- Directly attacks the 5M-stream problem: stream count equals queue count.
- Simple model, easy to explain and reason about.
- Works with either Redis or Pulsar underneath, so it composes with other ideas.
- One retention policy per queue, which is simple to operate.
- No per-job-set key management or TTL bookkeeping.
- Predictable capacity planning: memory and key count scale with queues, not job sets.
- Can act as the live layer for ideas 2 and 3 without rewriting the whole watch path.

**Cons.**

- Fan-out cost moves to the client, a relay, or the API: every watcher path carries every event on
  the queue, even though each client only needs one job set.
- Per-job-set cursors disappear; the API contract changes for every client.
- Filtering cost grows with queue event rate, not with the client's own job set.
- No isolation — one noisy job set loads every watcher on that queue.
- History is unindexed unless combined with a history store; filtering millions of events per
  request is slow.
- External consumers break, since their keys disappear.
- Network bandwidth is consumed even when no watcher cares about most of the events.
- Stream-level length caps apply to the whole queue, so one job set cannot keep longer history than
  another — this idea cannot satisfy per-job-set retention at all, only per-queue retention.
- Events for different job sets are interleaved, making inspection and debugging harder.

**Open questions.** What is the realistic watcher count per queue? Should filtering happen
client-side, in the API, or in a relay? Does any client need to watch more than one job set at a
time?

**Migration.** Requires a new key layout, so external consumers break. Can run alongside the old
layout during a transition at the cost of writing both.

## Idea 6: ClickHouse-only with in-memory live relay

**One line:** durable history lives in ClickHouse; live events are pushed from a relay inside the
API server, so there is no Redis in the event path at all.

**How it would work.** The ingester writes events only to ClickHouse. A live relay inside the Event
API subscribes to the Pulsar `events` topic with a reader (an exclusive, non-durable subscription
with a random name) and holds a short in-memory buffer per recently active job set. A Pulsar reader
does not prevent topic deletion and does not retain data, so the relay depends on the topic's
retention policy for its catch-up window. A watcher attaches to the relay; on a miss it falls back
to ClickHouse history and reattaches. In-memory state is purely ephemeral. Each API replica runs its
own reader, so relay cost scales with replica count.

**Workload fit.** Both shapes fit: long-lived job sets have few watchers and hot entities; short-lived
ones live entirely as rows and never create a per-entity storage object.

**Pros.**

- One durable store for events, plus an ephemeral cache with no recovery obligation.
- No Redis at all: no stream cardinality, no persistence tuning, no rehydration, no stream-key API
  contract.
- Live and history are cleanly separated by nature, matching the two workload shapes.
- Adding live features (filters, fan-out) is in-process code rather than storage design.
- History gets ClickHouse's TTL and scale.
- Losing live state is correct behaviour, not a failure — there is no durability promise to keep.
- Fewer storage systems to back up and monitor once the relay is proven.

**Cons.**

- Largest overall change: removes the watch protocol and the direct Redis stream contract in one
  step.
- A new ClickHouse cluster plus a relay to build and operate.
- The relay is stateful-ish: per-job-set buffering, backpressure and restart behaviour all need
  design.
- Relay cost is replicas × topic event rate, since every API instance reads the whole topic.
- Reconnects depend on history being fully caught up; the seam still exists, just inside one
  service.
- With multiple API replicas, a watcher can land where the job set is not buffered, forcing a
  history fallback on reconnect.
- External consumers must move to the API or lose access entirely.
- History reads have higher latency than an in-memory live read, so the API has two latency
  profiles.
- Tempting to build the relay before the history store is proven, which inverts the risk order.

**Open questions.** Can the relay be stateless per watcher rather than buffering per job set? What
is the reconnect story in detail (buffer window, cursor semantics, duplicate suppression)? Should
this be the target with idea 2 or 3 as an intermediate step rather than a first delivery?

**Migration.** Not incremental unless the old stream path runs in parallel. The realistic path is
idea 2 or 3 first, then converge here once external consumers have moved off Redis.

## Idea 7: Pulsar per-job-set topics

**One line:** keep the Pulsar transport but give each job set its own topic
(`Events:<queue>:<jobSetId>`), replacing Redis streams as the client-facing store.

**How it would work.** Producers publish each job set's events to its own topic. A watcher opens a
reader on that topic; history is a seek within the topic's retention window and a read. Per-topic
retention is set from the job set's policy at creation, and idle topics are removed by the broker's
inactive-topic policy (`delete_when_no_subscriptions` is on by default; the broker checks every 60
seconds). Retention itself defaults to off and must be configured, or history disappears with the
topic. The three existing ingesters either subscribe by regex across the namespace (`TopicsPattern`
is supported by the Go client) or continue to consume a duplicate copy of the single `events` topic.

**Workload fit.** Few long-lived job sets: a natural fit — long retention, one reader
per watcher, no filtering. Many short-lived job sets: one topic per job set, deleted
after it finishes, so cardinality becomes topic churn — which is the main risk, not
the access pattern.

**Pros.**

- Per-job-set retention, cursors and cleanup semantics come from the existing
  transport rather than a second store.
- Durable, replicated storage with fast broker recovery; Redis and its rehydration go
  away.
- No filtering and no fan-out amplification: a watcher reads only its own job set.
- Tiered storage can extend history to cheap storage without a separate archive.
- Inactive-topic deletion approximates "delete this job set's history when it is done", and
  unlike ideas 1–3 it needs no job-state knowledge to do it.
- Producers already speak Pulsar, so the protocol is not new to the platform.

**Cons.**

- "Millions of topics" is a metadata-scale claim for mostly-idle topics; millions of
  short-lived topics is a churn workload — create/delete writes in the metadata store
  and broker cache churn per job set.
- Per-topic fixed cost (managed ledger, metadata, broker memory, BookKeeper state)
  dominates when a topic holds only a few events.
- Tiny ledgers are inefficient on BookKeeper; a few messages per ledger is the worst
  case for that storage model.
- Fan-in: the ingesters must regex-subscribe across millions of topics or producers
  must write both a shared topic and a per-job-set topic — both add significant cost
  and operational complexity.
- Per-topic retention is a metadata write per job set; namespace defaults are too
  coarse for per-job-set policy. Retention is off by default, so every topic needs it
  set explicitly.
- Topic deletion is asynchronous and blocked by attached consumers or retention, so
  expiry becomes deletion orchestration rather than a TTL; physical reclaim waits for
  segment/entry-log rollover and garbage collection.
- Producer pressure: a producer per job set from the submit API and scheduler, or a
  pooled publisher with its own connection management.
- A large API break: cursors, watch protocol and direct stream consumers all change.
- Regex-subscribing across millions of topics is its own scale problem: the client must
  match and track the whole namespace, not just the ingesters' own topics.
- Monitoring, alerting and admin tooling at millions of topics is unproven for this
  team.

**Open questions.** What is real topic create/delete rate at CI volumes, and what does
the metadata store tolerate? What is actual broker memory per idle topic in our version
and config — measure it rather than trusting the marketing? How does a regex
subscription over millions of topics behave for the three ingesters? Can per-topic
retention plus inactive-topic deletion express "expire shortly after finished" in
practice?

**Migration.** Requires changing the publish path and every consumer at once unless
events are dual-published; the hardest Pulsar idea to stage.

## References

- Pulsar subscriptions and cursors: <https://pulsar.apache.org/docs/concepts-messaging/#subscriptions>
- Pulsar retention, TTL, and inactive-topic deletion: <https://pulsar.apache.org/docs/cookbooks-retention-expiry/>
- Pulsar topic compaction and tombstones: <https://pulsar.apache.org/docs/concepts-topic-compaction/>
- Pulsar Go client `TopicsPattern`: <https://pulsar.apache.org/docs/client-libraries-go/>
- ClickHouse MergeTree TTL (`merge_with_ttl_timeout`): <https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree>
- ClickHouse `ReplacingMergeTree` (eventual dedupe): <https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree>
