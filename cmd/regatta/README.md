# Regatta

Regatta tests scenarios against Armada with simulated hardware.

A scenario file has three sibling sections:

- **`executionTargets`** — where simulated capacity is stood up. Each target is either a
  **`cluster`** target (real `v1.Node` objects created via a kwok controller in a live cluster,
  exercising Armada's real executor/scheduler path end to end against simulated hardware) or a
  **`fake-executor`** target (`armada-fakeexecutor` registers as a real executor against the
  Armada scheduler over gRPC, but simulates nodes and pods entirely in-process — no cluster, no
  kubeconfig). A scenario may declare any number of targets, but they must all share the same
  type: any number of `cluster` targets, or any number of `fake-executor` targets, never a mix of
  both in one run.
- **`nodeGroups`** — a top-level map of named node groups, each an arbitrary number of
  `(nodeProfile, count)` members. Every `executionTargets[]` entry installs the node groups named
  in its own `nodeGroups` list; if a target omits `nodeGroups`, it installs *all* of them.
- **`load`** — the submission batch: `queue`, `jobSetId`, and a list of job-spec files with a
  count per reference (mirroring how `nodeGroups` reference node-profile files by path + count).
  `mode: one-shot` (the default) submits everything at once and waits for the last job to reach a
  terminal state. `mode: ramp-up` spreads the total count evenly across `rampUp.rampDuration` in
  steps of `rampUp.stepInterval`, and waits for every submitted job to reach a terminal state.

When Regatta runs, it stands up every execution target, submits the configured load, waits for
completion, and tears every target back down — best-effort, even if a later target failed to
start.

The primary use cases are benchmarking and performance testing Armada.

# Prerequisites

You have a k8s cluster with armada and prometheus deployed, as `cluster` targets need a
kubeconfig and every target needs an `.armadactl` config. The cluster isn't needed for
fake-executor-only scenarios, see below.

You can run the local clusters with:
```bash
mage kindRegatta
mage dev:up kwok,prometheus # mind the lack of space between kwok, comma, and prometheus
```

Both `mage dev:up` profiles above and below start the scheduler with `_local/scheduler/config-regatta.yaml`, which indexes the node labels regatta's fake nodes carry (`kwok.x-k8s.io/node`, `armadaproject.io/fake-executor`). The default `_local/scheduler/config.yaml` doesn't index them, so a scheduler started via a plain `mage dev:up no-auth` won't schedule onto regatta's fake nodes.

# Quick Start: cluster (kwok)

`mage kindRegatta` stands up two dedicated kind clusters (`armada-regatta-1`/`armada-regatta-2`),
separate from `mage kind`/`mage kindSecondCluster` (which serve other, non-regatta local-dev
workflows). `cmd/regatta/config/multi-cluster.example.yaml` runs a `cluster` target against each
simultaneously, each with a different named node group — exceeding what benchmarking harnesses
that only support N *identical* clusters can express.

Run regatta:
```bash
go run ./cmd/regatta run cmd/regatta/config/multi-cluster.example.yaml
```

This creates the kwok fake nodes described by the scenario file's `nodeGroups` (profiles under
`cmd/regatta/config/node-profiles/`) on each target's own cluster, submits the jobs referenced in
its `load.jobs` (job-spec files under `cmd/regatta/config/job-specs/`), waits for them to reach a
terminal state, and tears the fake nodes back down on both clusters.

Tear the clusters themselves down with `mage kindTeardownRegatta`.

Regatta should teardown any kwok resources it adds to your clusters as part of a normal run.
If regatta exits unexpectedly and kwok nodes need to be removed from a cluster, run:

```bash
go run ./cmd/regatta teardown --kubeconfig .kube/external/config-regatta-1 --kind-cluster-name armada-regatta-1 --name gpu-cluster
```

`--name` identifies which execution target's kwok controller/kubeconfig to remove — it must match
the target's `name` in the scenario file that was running (or the auto-generated `cluster-0`,
`cluster-1`, ... if the target left `name` unset).

# Quick Start: fake-executor

No kind cluster and no kubeconfig needed, `armada-fakeexecutor` never touches the Kubernetes API.

`regatta run` starts and stops its own `armada-fakeexecutor` process(es), so bring up the rest of the stack with the `fake-executor-regatta` profile, not `fake-executor`. The plain `fake-executor` profile also starts its own goreman-managed `armada-fakeexecutor`, which would register a second fake executor alongside regatta's.

```bash
mage dev:up fake-executor-regatta,prometheus
```

In another terminal:

```bash
go run ./cmd/regatta run cmd/regatta/config/fakeexecutor.example.yaml
```

This starts `armada-fakeexecutor` with the node shapes from `cmd/regatta/config/fakeexecutor.example.yaml`'s `nodeGroups`, submits the jobs referenced in its `load.jobs`, waits for the submitted jobs to reach a terminal state, and stops the process.

# Quick Start: ramp-up load

`cmd/regatta/config/ramp-up.example.yaml` submits its jobs gradually instead of all at once —
`load.mode: ramp-up` with `rampUp.rampDuration: 30s`/`rampUp.stepInterval: 5s` spreads 100 jobs
across six ~5-second steps of ~17 jobs each, rather than one burst of 100.

```bash
go run ./cmd/regatta run cmd/regatta/config/ramp-up.example.yaml
```

# Writing a scenario file

See `cmd/regatta/config/multi-cluster.example.yaml`, `fakeexecutor.example.yaml`, and `ramp-up.example.yaml` for the full format.

- Node profiles (`cmd/regatta/config/node-profiles/*.yaml`) describe the shape of a single
  simulated node — allocatable resources, labels, taints — and are shared between both target
  types so a hardware shape only needs to be described once. A top-level `nodeGroups` entry
  names a set of `(nodeProfile, count)` members; an `executionTargets[]` entry installs the named
  groups it lists, or every group if it lists none.
- Job specs (`cmd/regatta/config/job-specs/*.yaml`) are plain pod specs — no queue, no count.
  `load.jobs` references them by path with a count per reference; `load.queue`/`load.jobSetId`
  apply to the whole submission batch.
- `executionTargets[].name` is optional; unset targets get an auto-generated name
  (`cluster-0`, `fake-executor-1`, ...) counted per type in file order. Names must be unique and
  are used to namespace per-target resources (kwok controller container/kubeconfig,
  fake-executor ports).

# Architecture
