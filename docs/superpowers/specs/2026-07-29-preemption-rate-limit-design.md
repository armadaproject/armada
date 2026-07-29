# Per-pool fairshare preemption rate limit — Design

Date: 2026-07-29
Branch: `preemption_rate_limit`

## Goal

Add a per-pool rate limit that throttles **fairshare** preemptions, analogous to the
existing job scheduling rate limit. The limit:

- Is configured **per pool**.
- **Defaults to off** (no limit) when unconfigured.
- Counts **one token per preempted job** (mirrors how the scheduling limiter counts job
  cardinality).
- Is **retrospective**: preemptions are counted after the fact (we only know how many
  jobs were preempted once the NodeDb reports them). The token bucket is therefore
  allowed to go (minorly) negative. Fairshare preemption stays disabled until the bucket
  refills to a positive balance.

## Background — how the existing pieces work

Facts established from the current codebase (`internal/scheduler`):

- **Scheduling rate limit** uses `golang.org/x/time/rate` token buckets. A global limiter
  lives on `FairSchedulingAlgo.limiter` and a per-queue map on
  `FairSchedulingAlgo.limiterByQueue`. Both persist across scheduling rounds. They are
  checked with `TokensAt(sctx.Started)` (frozen at round start) in
  `constraints.CheckJobConstraints`, and consumed with `ReserveN(sctx.Started, cardinality)`
  in `gang_scheduler.go` (deferred block, ~line 117-123).

- **Fairshare preemptions are marked** in `GangScheduler.applyPreemptions`
  (`gang_scheduler.go` ~line 264-273), which iterates the `[]*nodedb.JobPreemptionInfo`
  returned by the NodeDb and calls `sch.schedulingContext.MarkJobPreempted(jobId)`.
  This path is **exclusively fairshare**: urgency preemption returns `nil` preempted jobs,
  and the optimiser uses a separate `PreemptJob` path. So counting here isolates fairshare
  preemptions with no extra classification needed.

- **The NodeDb already has a `disableFairshareScheduling` flag** (`nodedb.go` ~line 166),
  which gates the entire fairshare preemption branch in `selectNodeForJobWithTxnAtPriority`
  (~line 650). It is currently set once per pool per round via
  `ConfigureScheduling(SchedulingOptions{...})` in `scheduling_algo.go` (~line 241-248),
  sourced from `PoolConfig.DisableFairshareScheduling`. `ConfigureScheduling` overwrites
  **all** disable flags at once.

- **`SchedulingContext` is per-pool per-round**, holds `Started time.Time` and the
  scheduling `Limiter`, and is constructed in `constructSchedulingContext`
  (`scheduling_algo.go` ~line 700) via `NewSchedulingContext(...)`.

- **The `QueueScheduler`** (`queue_scheduler.go`) holds `schedulingContext` and
  `gangScheduler`; it reaches the NodeDb indirectly via `sch.gangScheduler.nodeDb`
  (same package). Its `Schedule` loop (~line 100-244) peeks candidate gangs and delegates
  to `sch.gangScheduler.Schedule(ctx, gctx)`.

- **Per-pool config** lives in `PoolConfig` (`configuration.go` ~line 384), with a
  `Get<X>(poolName)` getter idiom on `SchedulingConfig` (e.g.
  `GetProtectedFractionOfFairShare`). No preemption rate limit exists anywhere today.

## Design

### 1. Config surface

Add nested per-pool config, scoped to fairshare (leaving room for other preemption limits
later). In `internal/scheduler/configuration/configuration.go`:

```go
// New types
type PoolPreemptionConfig struct {
    // Fairshare, when non-nil, rate-limits fairshare preemptions for this pool.
    // nil => no limit (default / off).
    Fairshare *PreemptionRateLimitConfig
}

type PreemptionRateLimitConfig struct {
    // Sustained preemptions per second (tokens/sec).
    MaximumRate  float64 `validate:"gte=0"`
    // Bucket capacity: max preemptions that can bunch up in a single round.
    MaximumBurst int     `validate:"gte=0"`
}

// On PoolConfig, add:
Preemption PoolPreemptionConfig
```

YAML shape:

```yaml
pools:
  - name: cpu
    preemption:
      fairshare:
        maximumRate: 10
        maximumBurst: 20
```

Notes:
- `Preemption` is a plain (non-pointer) struct; its zero value has `Fairshare == nil`,
  i.e. off. So default-off requires no config and no `config.yaml` change (a commented
  example may be added for discoverability).
- Validation tags are `gte=0`, **not** `gt=0` — the scheduling fields use `gt=0`, but we
  must permit zero.

Add a getter mirroring `GetProtectedFractionOfFairShare`:

```go
// GetFairsharePreemptionRateLimit returns the configured fairshare preemption rate and
// burst for the pool, and whether a limit is configured at all. When enabled is false,
// callers should treat preemption as unlimited.
func (sc *SchedulingConfig) GetFairsharePreemptionRateLimit(poolName string) (rate float64, burst int, enabled bool)
```

### 2. Limiter storage & lifecycle

The token bucket must persist **across rounds** so it refills over wall-clock time (like
the scheduling `limiter`). It therefore lives on `FairSchedulingAlgo`, not the per-round
context.

- Add to `FairSchedulingAlgo` (`scheduling_algo.go` ~line 45):

  ```go
  // Per-pool fairshare preemption rate-limiters (persist across rounds).
  preemptionLimiterByPool map[string]*rate.Limiter
  ```

  Initialize `make(map[string]*rate.Limiter)` in `NewFairSchedulingAlgo`.

- Add a `PreemptionLimiter *rate.Limiter` field to `SchedulingContext`
  (`context/scheduling.go`), mirroring `Limiter`. To avoid churning every
  `NewSchedulingContext` call site (simulator, tests), **default it in the constructor** to
  a no-op unlimited limiter:

  ```go
  // In NewSchedulingContext:
  PreemptionLimiter: rate.NewLimiter(rate.Inf, math.MaxInt),
  ```

  This means simulator/dry-run/test contexts get unlimited preemption by default —
  consistent with how they already treat the scheduling limiter.

- In `constructSchedulingContext` (`scheduling_algo.go` ~line 713), after creating `sctx`,
  look up / lazily create the pool's persistent limiter and assign it:

  ```go
  sctx.PreemptionLimiter = l.getOrCreatePreemptionLimiter(pool)
  ```

  where `getOrCreatePreemptionLimiter`:
  - returns the cached limiter for the pool if present **and** its rate/burst still match
    config;
  - otherwise builds one: if `enabled`, `rate.NewLimiter(rate.Limit(rate), burst)`;
    if not enabled, `rate.NewLimiter(rate.Inf, math.MaxInt)` (the no-op idiom already used
    by `noOpRateLimiter` in `idealised_value.go`);
  - caches it (keyed by pool) alongside the rate/burst it was built with, so a config
    reload that changes rate/burst rebuilds the limiter rather than serving a stale one.

  Note: `constructSchedulingContext` takes `pool string`; the getter takes a pool name, so
  this is a straightforward lookup.

### 3. Consuming tokens (retrospective count)

In `GangScheduler.applyPreemptions` (`gang_scheduler.go`), after marking preempted jobs,
consume one token per preempted job:

```go
func (sch *GangScheduler) applyPreemptions(preemptedJobs []*nodedb.JobPreemptionInfo) {
    for _, preemption := range preemptedJobs {
        preemption.PreemptedJob.PreemptingJob = preemption.PreemptingJob
        sch.schedulingContext.MarkJobPreempted(preemption.PreemptedJob.JobId)
    }
    if n := len(preemptedJobs); n > 0 {
        sch.schedulingContext.PreemptionLimiter.ReserveN(sch.schedulingContext.Started, n)
    }
}
```

`ReserveN` always succeeds and allows the bucket to go negative when a round preempts more
than the remaining tokens — matching the "happy for it to go minorly negative" requirement.
Because the reservation mutates the bucket state, subsequent `TokensAt(sctx.Started)` calls
within the same round reflect the consumed tokens immediately.

`applyPreemptions` is called from all three commit sites in `gang_scheduler.go` (the two
node-uniformity paths ~line 199/208 and the common path ~line 241), always after the NodeDb
transaction commits, so aborted attempts never consume tokens.

### 4. Checking & disabling (queue scheduler → NodeDb)

Add a targeted setter on `NodeDb` so we don't clobber the other `disable*` flags that
`ConfigureScheduling` sets:

```go
// nodedb.go
func (nodeDb *NodeDb) SetDisableFairshareScheduling(disable bool) {
    nodeDb.disableFairshareScheduling = disable
}
```

In `QueueScheduler.Schedule` (`queue_scheduler.go`), inside the main loop, before
delegating to the gang scheduler, check the pool limiter and disable fairshare preemption
when exhausted:

```go
if sctx.PreemptionLimiter.TokensAt(sctx.Started) < 1 {
    sch.gangScheduler.nodeDb.SetDisableFairshareScheduling(true)
}
```

(If reaching `sch.gangScheduler.nodeDb` directly reads poorly, add a small accessor on
`GangScheduler`, e.g. `NodeDb()`; both types are in package `scheduling`.)

Semantics:
- We only ever **disable** (set `true`), never re-enable within a round. Tokens are frozen
  at `sctx.Started`, so they cannot refill mid-round; once exhausted, fairshare preemption
  stays disabled for the remainder of the round. A pool configured with
  `DisableFairshareScheduling = true` is never accidentally re-enabled.
- Each new round constructs a fresh `SchedulingContext` (new `Started`) and calls
  `ConfigureScheduling(...)` in `scheduling_algo.go`, which resets
  `disableFairshareScheduling` from `PoolConfig` before the round begins. So the disable is
  scoped to the round in which the budget was exhausted; the next round re-evaluates against
  the refilled bucket.
- When the pool has no configured limit, `PreemptionLimiter` is the `rate.Inf` no-op:
  `TokensAt` returns effectively infinite, so the branch never fires and behaviour is
  unchanged.

This is the deliberately simple ("naive") approach described in the task: toggle the NodeDb
flag rather than threading per-attempt fairshare-permission into the NodeDb's per-job
selection logic.

### Trade-off (accepted by design)

Because the check is per-gang and consumption is retrospective, a single gang can overshoot
the limit (drive the bucket negative) before the next check disables fairshare preemption.
The limit is a throttle, not a hard cap. This is intentional per the requirement.

## Testing

- **Config getter** (`configuration_test.go` or equivalent): nil `Fairshare` → `enabled=false`;
  configured → returns the rate/burst.
- **`applyPreemptions` token consumption**: after preempting N jobs, the pool limiter's
  `TokensAt(Started)` has dropped by N; preempting more than the balance drives it negative
  and does not error.
- **Limiter lifecycle**: `getOrCreatePreemptionLimiter` returns the same instance across
  rounds for an unchanged pool config, and rebuilds when rate/burst change; returns a no-op
  `rate.Inf` limiter when unconfigured.
- **Queue-scheduler integration**: a pool with a low fairshare preemption limit disables
  fairshare preemption once the per-round budget is spent (later gangs in the round schedule
  without fairshare preemption), while a pool with no configured limit preempts freely.
  Reuse the `TestFairSharePreemption_*` style in `nodedb_test.go` and the existing
  queue/preempting scheduler test fixtures.
- **Default-off**: unconfigured pools behave exactly as today (no disabling, unlimited
  preemption).

## Files touched (anticipated)

- `internal/scheduler/configuration/configuration.go` — new config types + getter.
- `internal/scheduler/scheduling/context/scheduling.go` — `PreemptionLimiter` field +
  no-op default in `NewSchedulingContext`.
- `internal/scheduler/scheduling/scheduling_algo.go` — `preemptionLimiterByPool` field,
  init, `getOrCreatePreemptionLimiter`, assignment in `constructSchedulingContext`.
- `internal/scheduler/scheduling/gang_scheduler.go` — `ReserveN` in `applyPreemptions`.
- `internal/scheduler/scheduling/queue_scheduler.go` — limiter check + disable call.
- `internal/scheduler/nodedb/nodedb.go` — `SetDisableFairshareScheduling` setter.
- `config/scheduler/config.yaml` — optional commented example (no functional default).
- Tests as above.

## Out of scope

- Per-queue or global preemption rate limits (only per-pool requested).
- Resource-weighted counting (per-job counting chosen).
- Threading per-attempt fairshare permission into the NodeDb (kept simple per task).
- Urgency-based and optimiser preemption (untouched; only fairshare is limited).
