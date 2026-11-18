# Scheduler policy clock

`NewFairSchedulingAlgo` uses a real clock by default. To control policy time, pass `scheduling.WithPolicyClock(clk)` as a constructor option, where `clk` is a non-nil `k8s.io/utils/clock.Clock`. A nil clock returns an error; passing a nil option panics.

The policy clock governs token refill, executor expiry, optimiser cadence, the preempted time of runs preempted during scheduling, and scheduling context `Started`, `Finished`, and queue `Created`. Reuse one algorithm instance across rounds so rate limiters keep their reservations. Advance the policy clock monotonically, and pass a job database whose lifecycle state matches the policy time. The option only changes the clock: it does not simulate job completions.

The scheduler and pool result `StartTime` and `EndTime`, and the scheduling cycle duration metrics derived from them, are also policy time. So is the `Duration` line of `SchedulingContext.ReportString`. All of these are zero with a frozen clock. No field records host computation time; the `Scheduled on pool ... in` log line reports it.

Host time still governs the new-job scheduling soft budgets (global and per queue), context deadlines and operational timeouts, and job and gang context `Created` timestamps. Advancing the policy clock does not simulate scheduler computation time or use up host-time budgets.
