# Regatta docs

Additional detail beyond the top-level [README](../README.md) quickstart:

- [Writing a scenario file](scenario-files.md) — the `executionTargets`/`nodeGroups`/`load` sections, node profiles, job specs, readiness/rate-limit tuning, ramp-up load.
- [Metrics report](metrics-report.md) — how `regatta run` detects drain, what the generated JSON report contains, and how to configure it.
- [Teardown](teardown.md) — tearing down by scenario file or by a single target.
- [Scaling beyond two clusters, and real clusters](scaling-and-real-clusters.md) — the 10-cluster example, writing your own N-cluster example, and pointing regatta at a real (non-kind) cluster.
