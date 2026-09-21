# Regatta

Regatta tests scenarios against Armada with simulated hardware.

A scenario file has three sibling sections:

- **`executionTargets`** — where simulated capacity is stood up. Each target is either a **`cluster`** target (real `v1.Node` objects created via a kwok controller in a live cluster, exercising Armada's real executor/scheduler path end to end against simulated hardware) or a **`fake-executor`** target (`armada-fakeexecutor` registers as a real executor against the Armada scheduler over gRPC, but simulates nodes and pods entirely in-process — no cluster, no kubeconfig). A scenario may declare any number of targets, but they must all share the same type: any number of `cluster` targets, or any number of `fake-executor` targets, never a mix of both in one run.
- **`nodeGroups`** — a top-level map of named node groups, each an arbitrary number of `(nodeProfile, count)` members. Every `executionTargets[]` entry installs the node groups named in its own `nodeGroups` list; if a target omits `nodeGroups`, it installs *all* of them.
- **`load`** — the submission batch: `queue`, `jobSetId`, and a list of job-spec files with a count per reference (mirroring how `nodeGroups` reference node-profile files by path + count). `mode: one-shot` (the default) submits everything at once. `mode: ramp-up` spreads the total count evenly across `rampUp.rampDuration` in steps of `rampUp.stepInterval`.

When Regatta runs, it stands up every execution target and submits the configured load, then exits — it never polls job state, since regatta is a load generator, not a job-completion tracker, and polling doesn't scale to large submission batches.
Execution targets are **not** torn down automatically: cluster targets stay up so you can keep collecting metrics after submission finishes, and must be torn down explicitly with `regatta teardown` (see below); fake-executor targets are a local process you'll need to stop manually (e.g. `ps aux | grep fakeexecutor`) until a future Prometheus-based signal can trigger teardown automatically.
If setting up an execution target fails partway through a run, whatever did come up in that same run is still torn down automatically — only the normal-exit path leaves things running.

The primary use cases are benchmarking and performance testing Armada.

# Prerequisites

You have a k8s cluster with armada and prometheus deployed, as `cluster` targets need a kubeconfig and every target needs an `.armadactl` config. The cluster isn't needed for fake-executor-only scenarios, see below.

You can run the local clusters with:
```bash
mage kindRegatta cmd/regatta/config/armada/kind/two-cluster
mage dev:up regatta,prometheus # mind the lack of space between regatta, comma, and prometheus
```

Both `mage dev:up` profiles above and below start the scheduler with `cmd/regatta/config/armada/scheduler/config-regatta.yaml`, which indexes the node labels regatta's fake nodes carry (`kwok.x-k8s.io/node`, `armadaproject.io/fake-executor`). The default `_local/scheduler/config.yaml` doesn't index them, so a scheduler started via a plain `mage dev:up no-auth` won't schedule onto regatta's fake nodes.

# Quick Start: cluster (kwok)

`mage kindRegatta cmd/regatta/config/armada/kind/two-cluster` stands up two dedicated kind clusters (`armada-regatta-1`/`armada-regatta-2`), separate from `mage kind` (which serves other, non-regatta local-dev workflows). `cmd/regatta/config/two-cluster.example.yaml` runs a `cluster` target against each simultaneously, each with a different named node group — exceeding what benchmarking harnesses that only support N *identical* clusters can express.

Run regatta:
```bash
go run ./cmd/regatta run cmd/regatta/config/two-cluster.example.yaml
```

This creates the kwok fake nodes described by the scenario file's `nodeGroups` (profiles under `cmd/regatta/config/node-profiles/`) on each target's own cluster and submits the jobs referenced in its `load.jobs` (job-spec files under `cmd/regatta/config/job-specs/`), then exits — the fake nodes are left up on both clusters so you can keep collecting metrics.

When you're done, tear the fake nodes back down with:

```bash
go run ./cmd/regatta teardown cmd/regatta/config/two-cluster.example.yaml
```

This tears down every cluster target the scenario file declares. To tear down a single target by hand instead:

```bash
go run ./cmd/regatta teardown --kubeconfig .kube/external/regatta/regatta-1 --name gpu-cluster
```

`--name` identifies which execution target's kwok controller/kubeconfig to remove — it must match the target's `name` in the scenario file that was running (or the auto-generated `cluster-0`, `cluster-1`, ... if the target left `name` unset).

Tear the clusters themselves down with `mage kindTeardownRegatta`.

# 10 clusters

Besides the 2-cluster quickstart above, `cmd/regatta/config/ten-cluster.example.yaml` is a second checked-in example demonstrating scale: 10 `cluster`-type `executionTargets[]`, all sharing the same `cpu-only` node-group shape, submitting 1000 jobs across all ten.

Bring up all ten kind clusters, then run regatta against it:

```bash
mage kindRegatta cmd/regatta/config/armada/kind/ten-cluster
mage dev:up regatta-ten-cluster,prometheus  # or use the matching Procfile directly:
goreman -f cmd/regatta/config/armada/procfiles/ten-cluster.Procfile start
go run ./cmd/regatta run cmd/regatta/config/ten-cluster.example.yaml
```

Tear down with:

```bash
go run ./cmd/regatta teardown cmd/regatta/config/ten-cluster.example.yaml
mage kindTeardownRegatta cmd/regatta/config/armada/kind/ten-cluster
```

Both examples are hand-authored, checked-in scenario files, kind-cluster configs, executor configs, and Procfiles — there's no code-generation/templating step. Writing your own scenario at some other N follows the same pattern: pick a target count, then write one kind-cluster config per target (`cmd/regatta/config/armada/kind/<your-example>/<name>.yaml`, `name:` field must match) and one executor config per target (`cmd/regatta/config/armada/executor/<your-example>/<name>.yaml`; `httpPort`/`metric.port`/`application.clusterId` each offset by index — copy the ten-cluster example's values as a reference), then one Procfile with one line per cluster target (`export KUBECONFIG=<path> && ...`, following `ten-cluster.Procfile`'s pattern) — a directory per example once there's more than one file (as with `kind/`/`executor/` above), otherwise the example name goes in the filename instead (as with `ten-cluster.Procfile`, a single file). Finally, a scenario file whose `executionTargets[].cluster.kubeconfig` fields point at `.kube/external/regatta/<config-file-basename>` (relative to the scenario file's own location) — the path `mage kindRegatta <dir>` writes each cluster's external kubeconfig to.
A target's cluster name (used to name the kind cluster and, by default, to derive its network-internal API server address for the kwok-controller container) comes from `cluster.name` if set, otherwise defaults to the target's own `name`. A non-kind cluster must set `cluster.internalApiServerAddress` explicitly, since the kind-based default doesn't apply.

# Quick Start: fake-executor

No kind cluster and no kubeconfig needed, `armada-fakeexecutor` never touches the Kubernetes API.

`regatta run` starts its own `armada-fakeexecutor` process(es), so bring up the rest of the stack with the `fake-executor-regatta` profile, not `fake-executor`. The plain `fake-executor` profile also starts its own goreman-managed `armada-fakeexecutor`, which would register a second fake executor alongside regatta's.

```bash
mage dev:up fake-executor-regatta,prometheus
```

In another terminal:

```bash
go run ./cmd/regatta run cmd/regatta/config/fakeexecutor.example.yaml
```

This starts `armada-fakeexecutor` with the node shapes from `cmd/regatta/config/fakeexecutor.example.yaml`'s `nodeGroups`, submits the jobs referenced in its `load.jobs`, then exits — `armada-fakeexecutor` is left running in the background. When you're done, stop it manually (e.g. `ps aux | grep fakeexecutor`); there's currently no `regatta teardown` support for fake-executor targets.

# Quick Start: ramp-up load

`cmd/regatta/config/ramp-up.example.yaml` submits its jobs gradually instead of all at once — `load.mode: ramp-up` with `rampUp.rampDuration: 30s`/`rampUp.stepInterval: 5s` spreads 100 jobs across six ~5-second steps of ~17 jobs each, rather than one burst of 100.

```bash
go run ./cmd/regatta run cmd/regatta/config/ramp-up.example.yaml
```

# Writing a scenario file

See `cmd/regatta/config/two-cluster.example.yaml`, `fakeexecutor.example.yaml`, and `ramp-up.example.yaml` for the full format.

- Node profiles (`cmd/regatta/config/node-profiles/*.yaml`) describe the shape of a single simulated node — allocatable resources, labels, taints — and are shared between both target types so a hardware shape only needs to be described once. A top-level `nodeGroups` entry names a set of `(nodeProfile, count)` members; an `executionTargets[]` entry installs the named groups it lists, or every group if it lists none.
- Job specs (`cmd/regatta/config/job-specs/*.yaml`) are plain pod specs — no queue, no count. `load.jobs` references them by path with a count per reference; `load.queue`/`load.jobSetId` apply to the whole submission batch.
- `executionTargets[].name` is optional; unset targets get an auto-generated name (`cluster-0`, `fake-executor-1`, ...) counted per type in file order. Names must be unique and are used to namespace per-target resources (kwok controller container/kubeconfig, fake-executor ports).
- `cluster.evaluateReadiness` controls whether a canary job is submitted to confirm the fake nodes are actually schedulable before load is submitted. Defaults to `true` when `cluster.name` is set (a kind-provisioned target, environment known/controlled, the probe is cheap and meaningful) and `false` otherwise (an externally-provided cluster is assumed already schedulable rather than probed). Set it explicitly to override either default.
