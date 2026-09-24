# Teardown

`regatta teardown` removes a scenario's kwok fake nodes and controller from a cluster.

## By scenario file

Tears down every cluster target the scenario file declares:

```bash
go run ./cmd/regatta teardown cmd/regatta/config/two-cluster.example.yaml
```

## By target, without a scenario file

To tear down a single target by hand instead, omit the scenario file and use `--kubeconfig`/`--name`:

```bash
go run ./cmd/regatta teardown --kubeconfig .kube/external/regatta/regatta-1 --name gpu-cluster
```

`--name` identifies which execution target's kwok controller/kubeconfig to remove — it must match the target's `name` in the scenario file that was running (or the auto-generated `cluster-0`, `cluster-1`, ... if the target left `name` unset).

Only touches nodes tagged `kwok.x-k8s.io/node=fake`, so real nodes are never affected. Safe to run even if there's nothing to tear down.
