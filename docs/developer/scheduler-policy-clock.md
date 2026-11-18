# Scheduler policy clock

`NewFairSchedulingAlgo` uses a real clock by default. To control policy time, pass `scheduling.WithPolicyClock(clk)` as a constructor option, where `clk` is a non-nil `k8s.io/utils/clock.Clock`. A nil clock returns an error; passing a nil option panics.

The policy clock governs token refill, executor expiry, optimiser cadence, the preempted time of runs preempted during scheduling, and scheduling context `Started`, `Finished`, and queue `Created`. Reuse one algorithm instance across rounds so rate limiters keep their reservations. The short-job penalty is not driven by this clock: call `ShortJobPenalty.SetNow` with the policy time before each round. Advance the policy clock monotonically, and pass a job database whose lifecycle state matches the policy time. The option only changes the clock: it does not simulate job completions.

Also on policy time: scheduler and pool result `StartTime`/`EndTime`, the `scheduling_cycle_times` and `pool_scheduling_cycle_times` metrics, cycle metric event timestamps, and the `ReportString` `Duration` line. With a frozen clock these durations are zero. `schedule_cycle_times` uses the `Scheduler`'s own clock, so this option doesn't affect it.

Host time still governs the new-job scheduling soft budgets, context deadlines, operational timeouts, and job and gang context `Created` timestamps. The `Scheduled on pool ... in` log line reports host time per pool. Advancing the policy clock does not simulate scheduler computation time.
