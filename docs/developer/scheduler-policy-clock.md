# Scheduler policy clock

`scheduling.NewFairSchedulingAlgo` uses a real clock by default. A caller that
models elapsed policy time can pass `scheduling.WithPolicyClock(clk)` as its final
constructor argument. `clk` implements the existing `k8s.io/utils/clock.Clock`
interface. Keep one algorithm instance across rounds so its global, per-queue,
and per-pool fairshare-preemption limiters retain their reservations. Advance the
clock monotonically before each round; do not move a live policy clock backwards.

The supplied clock controls token refill, executor expiry, optimiser cadence,
reconciliation timestamps, and scheduling result timestamps. Each pool's
`SchedulingContext.Started`, `Finished`, and queue `Created` timestamps use this
same policy-time domain. The context report's `Duration` is elapsed **policy**
time; it can be zero when the policy clock is frozen for a round.

`SchedulingContext.ComputationDuration` measures host elapsed time inside
`PreemptingQueueScheduler.Schedule`. Queue scheduling duration accounting,
hard context deadlines, and operational timeout checks also use host time.
Job and gang context `Created` values and timing log messages remain host-time
diagnostics. A supplied policy clock does not simulate scheduler computation
cost or change the wall-time budgets. A later replay with unbounded rounds
must state that budget fidelity is outside its model.

Clock injection changes policy timing only. The caller still supplies
consistent executor and job lifecycle state for each round; this option does
not replay completions or restore production scheduler state.
