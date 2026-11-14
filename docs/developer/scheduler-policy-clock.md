# Scheduler policy clock

`NewFairSchedulingAlgo` uses a real clock by default. To control policy time, pass `scheduling.WithPolicyClock(clk)` as a constructor option, where `clk` is a non-nil `k8s.io/utils/clock.Clock`. Constructor options must also be non-nil.

The policy clock governs token refill, executor expiry, optimiser cadence, reconciliation timestamps, and scheduling context `Started`, `Finished`, and queue `Created`. Keep one algorithm instance across rounds so limiters retain reservations, advance its policy clock monotonically, and supply consistent lifecycle state. This option does not replay job completions.

`SchedulingContext` report `Duration` is policy elapsed time, which may be zero with a frozen clock. `ComputationDuration` and queue soft-budget accounting use host elapsed time. Context deadlines and operational timeouts remain real. Job and gang diagnostic timestamps also remain host based. Advancing the policy clock does not simulate scheduler computation time or advance host-time budgets.
