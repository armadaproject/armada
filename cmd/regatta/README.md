# Regatta

Regatta tests scenarios against Armada with simulated hardware.

Scenarios are bundles of an `.armadactl` config file, a `.kubectl` config file, a group of nodes and Armada jobs.

Groups of nodes (nodegroups) are node profiles (an Armada-internal abstraction) and a count of those nodes. Node profiles are read into kwok nodes, fake-executor config, or both.

When Regatta runs, it adds fake nodes via the fake executor or the kwok controller, so that the Armada scheduler can schedule fake workloads on it. When it exits, these fake nodes are removed.

Regatta supports two mutually exclusive targets for simulated capacity:

- **kwok**, real `v1.Node` objects created in a live cluster, exercising Armada's real executor/scheduler path end to end against simulated hardware.
- **fake-executor**, `armada-fakeexecutor` registers as a real executor against the Armada scheduler over gRPC, but simulates nodes and pods entirely in-process. No cluster, no kubeconfig.

Exactly one of `kwok.enabled` or `fakeExecutor.enabled` must be true in a regatta file.

The primary use cases are benchmarking and performance testing Armada.

# Prerequisites

You have a k8s cluster with armada and prometheus deployed, as regatta will need a `.kubeconfig` and `.armadactl`. This isn't needed for the fake-executor path, see below.

You can run a local cluster with:
```bash
mage kind
mage dev:up kwok,prometheus # mind the lack of space between kwok, comma, and prometheus
```

Both `mage dev:up` profiles above and below start the scheduler with `_local/scheduler/config-regatta.yaml`, which indexes the node labels regatta's fake nodes carry (`kwok.x-k8s.io/node`, `armadaproject.io/fake-executor`). The default `_local/scheduler/config.yaml` doesn't index them, so a scheduler started via a plain `mage dev:up no-auth` won't schedule onto regatta's fake nodes.

# Quick Start: kwok

Run regatta:
```bash
go run ./cmd/regatta run cmd/regatta/config/kwok.example.yaml
```

This creates the kwok fake nodes described by `cmd/regatta/config/kwok.example.yaml`'s `nodeGroup` (profiles under `cmd/regatta/config/node-profiles/`), submits the jobs in `cmd/regatta/config/submissions/basic.yaml`, waits for them to reach a terminal state, and tears the fake nodes back down.

Regatta should teardown any resources it adds to your cluster.
If regatta exits unexpectedly and kwok nodes need to be removed from the cluster, run:

```bash
go run ./cmd/regatta teardown --kubeconfig ~/.kube/config
```

# Quick Start: fake-executor

No kind cluster and no kubeconfig needed, `armada-fakeexecutor` never touches the Kubernetes API.

`regatta run` starts and stops its own `armada-fakeexecutor` process, so bring up the rest of the stack with the `fake-executor-regatta` profile, not `fake-executor`. The plain `fake-executor` profile also starts its own goreman-managed `armada-fakeexecutor`, which would register a second fake executor alongside regatta's.

```bash
mage dev:up fake-executor-regatta,prometheus
```

In another terminal:

```bash
go run ./cmd/regatta run cmd/regatta/config/fakeexecutor.example.yaml
```

This starts `armada-fakeexecutor` with the node shapes from `cmd/regatta/config/fakeexecutor.example.yaml`'s `nodeGroup`, submits the jobs in `cmd/regatta/config/submissions/fakeexecutor.yaml`, waits for the last submitted job to reach a terminal state, and stops the process.

# Writing a regatta file

See `cmd/regatta/config/kwok.example.yaml` and `fakeexecutor.example.yaml` for the full format. Node profiles (`cmd/regatta/config/node-profiles/*.yaml`) describe the shape of a single simulated node, allocatable resources, labels, taints, and are shared between both targets so a hardware shape only needs to be described once. Submission specs (`cmd/regatta/config/submissions/*.yaml`) describe the queue, job count, and pod template to submit.

# Architecture
