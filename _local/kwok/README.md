# KWOK all-in-one cluster

Mirrors `mage kind` — a single mage target stands up a fake Kubernetes API
server (no real kubelets, no real containers) backed by
[KWOK](https://kwok.sigs.k8s.io/)'s all-in-one image. Used for HPCX-286's
executor perf-testing spike to compare against Base (kind) and the Fake
executor at higher node/pod scale than a real cluster can practically host.

## Setup

```sh
mage kwok
```

This is idempotent — running it again while `armada-kwok` is already up just
no-ops the container-create step and re-applies nodes.

It will:
1. Start the `armada-kwok` container (`registry.k8s.io/kwok/cluster:v0.7.0-k8s.v1.28.15`, see `cluster.yaml`) on port 8888 (not 8080 — that collides with Armada's own scheduler/server/executor http ports when running side by side).
2. Write a static kubeconfig to `.kube/kwok/config` (no certs/tokens — the image serves plain HTTP with no auth).
3. Apply `KWOK_NODE_COUNT` (500, matching `internal/executor/fake/context/context.go`'s `DefaultNodeSpec`) fake nodes, generated from the single-node shape documented in `nodes.yaml`.
4. Apply `priorityclasses.yaml` and `namespace.yaml` (mirroring `_local/kind/priorityclasses.yaml`/`namespace.yaml`) — the `personal-anonymous` namespace and `job-submitter` RBAC needed to submit jobs under the no-auth profile, and the `armada-default`/`armada-preemptible` priority classes.
5. Wait for all nodes to report `Ready`.

Check the result:

```sh
KUBECONFIG=.kube/kwok/config kubectl get nodes -o wide
```

## Standing up the whole stack against KWOK

The easiest path is the `kwok` `dev:up` profile, which brings up KWOK and
points the real executor at it, in place of kind:

```sh
mage dev:up kwok
```

This is mutually exclusive with `fake-executor` (which needs no Kubernetes
cluster at all). It combines with other profiles/flags, e.g. `mage dev:up
kwok,auth -dap`.

Prometheus is **not** brought up by default under any profile — it's its own
opt-in compose profile. If you need metrics (e.g. for perf-testing), add it
explicitly:

```sh
mage dev:up kwok,prometheus
```

## Pointing a standalone executor at it

For running the executor on its own (e.g. against a `dev:up`-brought-up
control plane started without the `kwok` profile), the real executor needs
`kubernetes.toleratedTaints` to include `kwok.x-k8s.io/node` (already set in
`_local/executor/config.yaml`) — every fake node carries that taint, so
without the toleration every node is permanently unschedulable.

Run the executor with:

```sh
KUBECONFIG=.kube/kwok/config go run ./cmd/executor --config _local/executor/config.yaml
```

## Pod lifecycle simulation (Stage resources)

KWOK has no real kubelet/container runtime — pod phase transitions
(`Pending → Running → Succeeded`) are simulated by `kwok-controller` reading
`Stage` resources. `Stage` is **not a CRD registered in the apiserver** (it
can't be `kubectl apply`-ed); it's a `kwok-controller`-native config construct
read from a local YAML file, referenced via `kwokctl create cluster -c
<file>`. `kwokInitCluster` in `magefiles/kwok.go` bind-mounts
`_local/kwok/stages.yaml` into the container and passes it via `-c` for this
reason.

The all-in-one image's built-in `pod-complete` stage only fires for pods
owned by a `Job` (`metadata.ownerReferences[].kind == Job`). Armada's
executor submits bare `Pod` objects with no owner references, so without an
override, jobs reach `Running` but then sit there forever — nothing ever
simulates completion.

**Gotcha:** `-c` does not merge user-supplied stages with the built-in
defaults per resource kind — supplying *any* `Pod`-kind stage suppresses
*all* built-in `Pod` stages (`pod-ready`, `pod-delete`), not just the one
being overridden. (Confirmed by counting `Stage` docs in the container's
generated `kwok.yaml`: supplying only a custom `pod-complete` override left
a single `Stage` doc total, instead of the normal five.) `Node`-kind stages
have a working default-fallback (`kwok-controller` logs `"No node stages
found, using default node stages"` at startup when none are supplied) but
`Pod`-kind stages do not get an equivalent fallback log line or behavior.

Because of this, `_local/kwok/stages.yaml` carries the **complete** `Pod`
stage set: `pod-ready` and `pod-delete` copied verbatim from the built-ins,
plus `pod-complete-armada` (same as the built-in `pod-complete`, minus the
Job-ownership selector, plus a randomized delay so concurrently-running
pods don't all complete in the same instant). If this file is ever edited to
add another override, keep all three stages in it — don't trim back to just
the one being changed.

## Teardown

```sh
mage kwokTeardown
```

The all-in-one image has no persistent volume, so removing the container is a
complete teardown — nodes and any cluster state are gone, and the next
`mage kwok` starts clean.
