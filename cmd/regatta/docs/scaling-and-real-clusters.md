# Scaling beyond two clusters, and real (non-kind) clusters

## Prerequisites for any cluster target

You need a k8s cluster with armada and prometheus deployed, as `cluster` targets need a kubeconfig and every target needs an `.armadactl` config.

Both `mage dev:up` profiles used by the quickstarts (`regatta`, `regatta-ten-cluster`) start the scheduler with `cmd/regatta/config/armada/scheduler/config-regatta.yaml`, which indexes the node labels regatta's fake nodes carry (`kwok.x-k8s.io/node`, `armadaproject.io/fake-executor`). The default `_local/scheduler/config.yaml` doesn't index them, so a scheduler started via a plain `mage dev:up no-auth` won't schedule onto regatta's fake nodes.

## 10 clusters

Besides the 2-cluster quickstart, `cmd/regatta/config/ten-cluster.example.yaml` is a second checked-in example demonstrating scale: 10 `cluster`-type `executionTargets[]`, all sharing the same `cpu-only` node-group shape, submitting 1000 jobs across all ten.

Bring up all ten kind clusters, then run regatta against it:

```bash
mage kindRegatta cmd/regatta/config/armada/kind/ten-cluster
mage dev:up regatta-ten-cluster,prometheus
go run ./cmd/regatta run cmd/regatta/config/ten-cluster.example.yaml
```

Tear down with:

```bash
go run ./cmd/regatta teardown cmd/regatta/config/ten-cluster.example.yaml
mage kindTeardownRegatta cmd/regatta/config/armada/kind/ten-cluster
```

## Writing your own N-cluster example

Both examples are hand-authored, checked-in scenario files, kind-cluster configs, executor configs, and Procfiles — there's no code-generation/templating step. Writing your own scenario at some other N follows the same pattern:

1. Pick a target count.
2. Write one kind-cluster config per target: `cmd/regatta/config/armada/kind/<your-example>/<name>.yaml` (the `name:` field must match the filename).
3. Write one executor config per target: `cmd/regatta/config/armada/executor/<your-example>/<name>.yaml`. `httpPort`/`metric.port`/`application.clusterId` each need to be offset by index — copy the ten-cluster example's values as a reference.
4. Write one Procfile with one line per cluster target (`export KUBECONFIG=<path> && ...`, following `ten-cluster.Procfile`'s pattern). Use a directory per example once there's more than one file (as with `kind/`/`executor/` above); otherwise the example name goes in the filename instead (as with `ten-cluster.Procfile`, a single file).
5. Write a scenario file whose `executionTargets[].cluster.kubeconfig` fields point at `.kube/external/regatta/<config-file-basename>` (relative to the scenario file's own location) — the path `mage kindRegatta <dir>` writes each cluster's external kubeconfig to.

A target's cluster name (used to name the kind cluster, and, when `cluster.kind: true`, to derive its network-internal API server address for the kwok-controller container) comes from `cluster.name` if set, otherwise defaults to the target's own `name`.

## Real (non-kind) clusters

Set `cluster.kind: true` for any kind-provisioned target (the checked-in examples all do) — it opts into two kind-only defaults: deriving `cluster.internalApiServerAddress` from `cluster.name` (kind's own internal-DNS convention) and joining the kwok-controller container to the `kind` docker network.

A real cluster (e.g. EKS, reached via `~/.kube/config`) leaves `cluster.kind` unset — the kwok-controller container then reuses `cluster.kubeconfig`'s own server address and docker's default network, no special config needed unless that address is wrong for the container's network path, in which case set `cluster.internalApiServerAddress` explicitly.

### Exec credential plugins

If `cluster.kubeconfig`'s current-context user authenticates via an exec credential plugin (e.g. `aws eks get-token`, or similar — anything under a kubeconfig `user.exec:` block), regatta runs that plugin once on the host when starting the kwok-controller container and bakes the resulting client certificate/key into the kubeconfig it bind-mounts into the container — the container itself never needs the plugin binary or any local session state (AWS credentials, etc.) that only exists on the host.

This credential is as short-lived as the plugin issues it; if the kwok-controller container later fails auth with an expired-certificate error, rerun the target's setup (`regatta run`/`teardown`) to re-resolve it.
