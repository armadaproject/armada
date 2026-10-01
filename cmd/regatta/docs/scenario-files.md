# Writing a scenario file

See `cmd/regatta/config/two-cluster.example.yaml` and `ramp-up.example.yaml` for the full format.

A scenario file has three sibling sections:

- **`executionTargets`** — where simulated capacity is stood up. Each target is a **`cluster`** target: real `v1.Node` objects created via a kwok controller in a live cluster, exercising Armada's real executor/scheduler path end to end against simulated hardware. A scenario may declare any number of targets.
- **`nodeGroups`** — a top-level map of named node groups, each an arbitrary number of `(nodeProfile, count)` members. Every `executionTargets[]` entry installs the node groups named in its own `nodeGroups` list; if a target omits `nodeGroups`, it installs *all* of them.
- **`load`** — the submission batch: `queue`, `jobSetId`, and a list of job-spec files with a count per reference (mirroring how `nodeGroups` reference node-profile files by path + count). `mode: one-shot` (the default) submits everything at once. `mode: ramp-up` spreads the total count evenly across `rampUp.rampDuration` in steps of `rampUp.stepInterval`.

Details worth knowing when writing your own:

- `armadactl` is the path to an `.armadactl.yaml` (default `$HOME/.armadactl.yaml`) and `authContext` picks which of its `contexts` to run against — the same names `armadactl config get-contexts` lists. Unset, the file's own `currentContext` is used, so one file can hold a local context and a shared-instance context and each scenario names the one it wants. `regatta run --context <name>` overrides the scenario's `authContext`. An unknown name fails before anything is created, and the error lists the available contexts. Regatta logs the Armada URL and context it resolved at the start of a run.
- Node profiles (`cmd/regatta/config/node-profiles/*.yaml`) describe the shape of a single simulated node — allocatable resources, labels, taints — so a hardware shape only needs to be described once. A top-level `nodeGroups` entry names a set of `(nodeProfile, count)` members; an `executionTargets[]` entry installs the named groups it lists, or every group if it lists none.
- Job specs (`cmd/regatta/config/job-specs/*.yaml`) are plain pod specs — no queue, no count. `load.jobs` references them by path with a count per reference; `load.queue`/`load.jobSetId` apply to the whole submission batch.
- `executionTargets[].name` is optional; unset targets get an auto-generated name (`cluster-0`, `cluster-1`, ...) in file order. Names must be unique and are used to namespace per-target resources (kwok controller container/kubeconfig).
- `cluster.evaluateReadiness` controls whether regatta waits for the scheduler to learn about the fake nodes before submitting load. It defaults to `true` for every target; set it to `false` to skip the wait. The scheduler takes a while (about a minute on a shared instance) to register freshly created nodes, so without the wait the start of the run measures that warm-up. The wait submits a small canary job to the `regatta` queue, in a job set of its own, and watches that job's lifecycle events (the stream `armadactl watch` uses) until it is running. Each attempt waits up to `cluster.readinessDelay` (default `5s`) for the canary and doubles that on every retry, for up to `cluster.readinessRetries` attempts (default `5`, about 2.5 minutes in total). If the wait gives up, the error names the last event seen and includes the scheduler's own job report when it has one, e.g. `label kwok.x-k8s.io/node not set`.
- `cluster.continueOnReadinessFailure` (default `false`) lets the run carry on when the readiness check above gives up, instead of failing setup. It is for environments known to be faulty where you still want the test to run. The failure is logged as a warning when it happens and again at the end of the run, and recorded under `readinessFailures` in the metrics report (one `{target, error}` entry per affected target), so a report from such a run says so. The fake nodes are kept, and load is submitted straight away. Only a failed readiness check is tolerated: a failure to create the nodes, or any other setup error, still stops the run. It has no effect when `evaluateReadiness` is `false`.
- `cluster.readinessSelectsTarget` controls whether that canary also selects on this target's `armadaproject.io/regatta-target` node label, which pins it to this target's own fake nodes. Defaults to `true` when `cluster.kind` is `true` and `false` otherwise. Turn it on for an external cluster only if several targets share one Armada *and* the executors report that label (`kubernetes.trackedNodeLabels`); otherwise the canary never finds a node. Without it the canary selects on the KWOK annotation alone.
- `cluster.kubernetes.qps`/`cluster.kubernetes.burst` control the rate limit regatta's own Kubernetes client applies against this target's API server (default `100`/`200`). Fake-node setup/teardown creates or deletes every node concurrently, so a large `nodeGroups` count (several hundred+) may need these raised further if you see it stall out waiting for nodes to go `Ready`.

When Regatta runs, it stands up every execution target and submits the configured load, then exits — it never polls job state, since regatta is a load generator, not a job-completion tracker, and polling doesn't scale to large submission batches.
Execution targets are **not** torn down automatically: they stay up so you can keep collecting metrics after submission finishes, and must be torn down explicitly with `regatta teardown`.
If setting up an execution target fails partway through a run, whatever did come up in that same run is still torn down automatically — only the normal-exit path leaves things running.

## Ramp-up load

`cmd/regatta/config/ramp-up.example.yaml` submits its jobs gradually instead of all at once — `load.mode: ramp-up` with `rampUp.rampDuration: 30s`/`rampUp.stepInterval: 5s` spreads 100 jobs across six ~5-second steps of ~17 jobs each, rather than one burst of 100.

```bash
go run ./cmd/regatta run cmd/regatta/config/ramp-up.example.yaml
```
