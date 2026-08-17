# KWOK-simulated fake nodes (out-of-cluster)

Joins [KWOK](https://kwok.sigs.k8s.io/)-simulated fake nodes into the existing `kind` cluster, via a standalone `kwok-controller` running against kind's own kubeconfig. Used for executor performance testing to compare against the Fake executor at higher node/pod scale than a real cluster can practically host.

## Setup

Requires `kind` to already be running (`mage kind`). This is applied on top.

```sh
mage kind
mage kwok
```

Idempotent, running `mage kwok` again while `armada-kwok-controller` is already up just no-ops the controller-start step and re-applies fake nodes.

It will:
1. Apply the `Stage` CRD (`stage-crd.yaml`) and `Stage` resources (`stages.yaml`) into kind's cluster.
2. Start the `armada-kwok-controller` container (`registry.k8s.io/kwok/kwok:v0.7.0`) against kind's own kubeconfig, restricted to nodes annotated `kwok.x-k8s.io/node=fake` — kind's real node(s) are never touched.
3. Apply `KWOK_NODE_COUNT` (default 40, override via env var) fake `v1.Node` objects, shaped by a `NodeProfile` (see `magefiles/kwok.go`).
4. Wait for all fake nodes to report `Ready`.

Check the result:

```sh
kubectl --context kind-armada get nodes -o wide
```

## Standing up the whole stack against KWOK

```sh
mage dev:up kwok
```

This brings up `kind` first, then joins KWOK fake nodes into it (equivalent to running `mage kind && mage kwok` manually), then starts the real executor pointed at kind's own kubeconfig — same kubeconfig, running alongside kind's real node(s). Mutually exclusive with `fake-executor` (which needs no Kubernetes cluster at all). Combines with other profiles/flags, e.g. `mage dev:up kwok,auth -dap`.

The `kwok` profile also layers `_local/scheduler/kwok_config.yaml` and `_local/executor/kwok_config.yaml` on top of whichever base Procfile is selected (see `writeKwokProcfile` in `magefiles/dev.go`) — no separate KWOK-specific Procfile is needed.

With Prometheus:

```sh
mage dev:up kwok,prometheus
```

## Pod lifecycle simulation (Stage resources)

KWOK has no real kubelet/container runtime, so pod phase transitions (`Pending → Running → Succeeded`) are simulated by `kwok-controller` reading `Stage` resources, applied as real cluster objects (`stage-crd.yaml`, `stages.yaml`) rather than passed via a config file. The controller must be started with `--enable-crds=Stage`, or it silently ignores these and falls back to its built-in pod stages, which have no completion behavior for Armada's bare (ownerless) pods — see `stages.yaml`'s comments.

## Teardown

```sh
mage kwokTeardown
```

Removes the `armada-kwok-controller` container and deletes the fake `v1.Node` objects, leaving kind's own cluster and real node(s) untouched.
