# Scheduler policy clock

Production scheduling behavior is unchanged: `NewFairSchedulingAlgo` uses real time by default. `WithPolicyClock(clk)` is currently used only by tests and provides a seam for planned replay. No production caller supplies this option. Pass a non-nil `k8s.io/utils/clock.Clock` (and non-nil constructor options).

The policy clock governs token refill, executor expiry, optimiser cadence, reconciliation timestamps, and scheduling context `Started`, `Finished`, and queue `Created`. Keep one algorithm instance across rounds so limiters retain reservations, advance its policy clock monotonically, and supply consistent lifecycle state. This option does not replay job completions.

`SchedulingContext` report `Duration` is policy elapsed time, which may be zero with a frozen clock. `ComputationDuration` and queue soft-budget accounting use host elapsed time. Context deadlines and operational timeouts remain real. Job and gang diagnostic timestamps also remain host based. Clock injection does not model computation cost; a later replay with unbounded rounds must disclose that budget fidelity is outside its model.

The legacy simulator constructs contexts at its simulated time and supplies that time directly to the core scheduler. It does not use `WithPolicyClock`, and its scheduling, execution, and CLI behavior are unchanged.
