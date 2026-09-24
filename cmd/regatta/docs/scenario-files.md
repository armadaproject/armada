# Writing a scenario file

See `cmd/regatta/config/two-cluster.example.yaml` and `ramp-up.example.yaml` for the full format.

A scenario file has three sibling sections:

- **`executionTargets`** — where simulated capacity is stood up. Each target is a **`cluster`** target: real `v1.Node` objects created via a kwok controller in a live cluster, exercising Armada's real executor/scheduler path end to end against simulated hardware. A scenario may declare any number of targets.
- **`nodeGroups`** — a top-level map of named node groups, each an arbitrary number of `(nodeProfile, count)` members. Every `executionTargets[]` entry installs the node groups named in its own `nodeGroups` list; if a target omits `nodeGroups`, it installs *all* of them.
- **`load`** — the submission batch: `queue`, `jobSetId`, and a list of job-spec files with a count per reference (mirroring how `nodeGroups` reference node-profile files by path + count). `mode: one-shot` (the default) submits everything at once. `mode: ramp-up` spreads the total count evenly across `rampUp.rampDuration` in steps of `rampUp.stepInterval`.

Details worth knowing when writing your own:

- Node profiles (`cmd/regatta/config/node-profiles/*.yaml`) describe the shape of a single simulated node — allocatable resources, labels, taints — so a hardware shape only needs to be described once. A top-level `nodeGroups` entry names a set of `(nodeProfile, count)` members; an `executionTargets[]` entry installs the named groups it lists, or every group if it lists none.
- Job specs (`cmd/regatta/config/job-specs/*.yaml`) are plain pod specs — no queue, no count. `load.jobs` references them by path with a count per reference; `load.queue`/`load.jobSetId` apply to the whole submission batch.
- `executionTargets[].name` is optional; unset targets get an auto-generated name (`cluster-0`, `cluster-1`, ...) in file order. Names must be unique and are used to namespace per-target resources (kwok controller container/kubeconfig).
- `cluster.evaluateReadiness` controls whether a canary job is submitted to confirm the fake nodes are actually schedulable before load is submitted. Defaults to `true` when `cluster.kind` is `true` (a kind-provisioned target, environment known/controlled, the probe is cheap and meaningful) and `false` otherwise (an externally-provided cluster is assumed already schedulable rather than probed). Set it explicitly to override either default.
- `cluster.kubernetes.qps`/`cluster.kubernetes.burst` control the rate limit regatta's own Kubernetes client applies against this target's API server (default `100`/`200`). Fake-node setup/teardown creates or deletes every node concurrently, so a large `nodeGroups` count (several hundred+) may need these raised further if you see it stall out waiting for nodes to go `Ready`.

When Regatta runs, it stands up every execution target and submits the configured load, then exits — it never polls job state, since regatta is a load generator, not a job-completion tracker, and polling doesn't scale to large submission batches.
Execution targets are **not** torn down automatically: they stay up so you can keep collecting metrics after submission finishes, and must be torn down explicitly with `regatta teardown`.
If setting up an execution target fails partway through a run, whatever did come up in that same run is still torn down automatically — only the normal-exit path leaves things running.

## Ramp-up load

`cmd/regatta/config/ramp-up.example.yaml` submits its jobs gradually instead of all at once — `load.mode: ramp-up` with `rampUp.rampDuration: 30s`/`rampUp.stepInterval: 5s` spreads 100 jobs across six ~5-second steps of ~17 jobs each, rather than one burst of 100.

```bash
go run ./cmd/regatta run cmd/regatta/config/ramp-up.example.yaml
```
