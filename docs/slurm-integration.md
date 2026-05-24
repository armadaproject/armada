# Slurm Integration via SkipNodeBinding

This document describes how to integrate Armada with a [Slurm](https://slurm.schedmd.com/)
cluster using the [slurm-bridge](https://github.com/SlinkyProject/slurm-bridge) scheduler
plugin and the `SkipNodeBinding` pool configuration option.

## Background

By default, the Armada scheduler binds every job to a specific node before the executor
sees it, injecting an `armadaproject.io/nodeId` nodeSelector into the pod spec. This is
incompatible with slurm-bridge, which needs to intercept pods before node binding and
delegate placement to Slurm via its own scheduler framework plugins (PreFilter, Filter,
PostFilter, PreBind).

The `SkipNodeBinding` flag disables node selector injection for a named pool. Armada
still uses its NodeDB for cluster-level capacity accounting and multi-cluster routing —
it just stops short of baking in the final node assignment, leaving that to the
in-cluster scheduler.

## How it works

1. Armada scheduler evaluates capacity across all executors in the pool using its NodeDB.
2. It selects a node for accounting purposes (determining which executor cluster gets the
   job) but does **not** inject the `armadaproject.io/nodeId` nodeSelector.
3. The executor submits the pod with `schedulerName: slurm-bridge-scheduler`.
4. slurm-bridge intercepts the pod, submits an external job to Slurm, and binds the pod
   to the node Slurm allocates.

Armada handles multi-cluster routing, fair-share scheduling, queue management, and
preemption between clusters. Slurm handles within-cluster node selection, GRES/GPU
allocation, and job management on its managed nodes.

## Configuration

### Armada scheduler — pool config

Add a pool entry with `skipNodeBinding: true` to the scheduler's `applicationConfig`:

```yaml
scheduling:
  pools:
    - name: slurm
      skipNodeBinding: true
```

### Armada executor — executor config

On the child cluster, configure the executor to use slurm-bridge as the pod scheduler:

```yaml
application:
  clusterId: slurm-cluster-1
  pool: slurm
executorApiConnection:
  armadaUrl: <armada-scheduler-address>:50051
  forceNoTls: true
metric:
  port: 9001
kubernetes:
  podDefaults:
    schedulerName: slurm-bridge-scheduler
```

### slurm-bridge

Deploy slurm-bridge on the child cluster following the
[SlinkyProject documentation](https://github.com/SlinkyProject/slurm-bridge). The key
requirements are:

- Kubernetes ≥ 1.35 with `DynamicResourceAllocation` feature gates enabled
- Nodes that Slurm manages must be labeled `scheduler.slinky.slurm.net/slurm-bridge: worker`
  and tainted `slinky.slurm.net/managed-node=slurm-bridge-scheduler:NoExecute`
- A running Slurm cluster with a partition that covers those nodes (the default partition
  name expected by slurm-bridge is `slurm-bridge`)
- cert-manager, scheduler-plugins (CoScheduling), JobSet, and LeaderWorkerSet installed

## Multiple Slurm clusters

Multiple executor clusters can all register to the same pool. Armada's scheduler will
distribute jobs across them using its standard fair-share algorithm: it bin-packs by
default, filling one cluster before spilling over to another, and rebalances between
queues based on usage history. Jobs are not round-robined — the scheduler makes an
informed placement decision based on available capacity in each cluster's NodeDB entries.

## Race conditions and retry behaviour

Armada's NodeDB is updated on each executor heartbeat and is therefore a slightly stale
view of cluster capacity. If slurm-bridge rejects a pod because Slurm's actual available
capacity differs from what Armada's NodeDB showed, the executor reports the run as failed,
Armada reschedules the job, and the NodeDB is corrected on the next heartbeat. This is the
same retry loop used for standard Kubernetes scheduling failures and requires no additional
handling.
