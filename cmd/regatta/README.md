# Regatta

Regatta is a benchmarking tool that tests scenarios against a deployment of Armada.

It drives job submissions, installs simulated nodes, scrapes metrics, and outputs results to a results file.

## Quickstart

### Setup two kind clusters

Run from the repo root. This command stands up two kind clusters which regatta will install kwok nodes on.

```
mage kindRegatta cmd/regatta/config/armada/kind/two-cluster
```

### Run armada (with regatta-specific configs) and prometheus

This command will build and start armada processes with configs used for this quickstart, namely a modified scheduler config and executor configs to tolerate kwok nodes.

```
mage dev:up regatta,prometheus
```

### Run Regatta

Validates the scenario file, installs 300 kwok nodes on each cluster, tests the cluster for readiness, submits jobs, and writes a json report.

While regatta is running, you can view queue metrics on [prometheus](http://localhost:9090/) and use this PromQL (`sum by (cluster) (armada_queue_leased_pod_count{phase="Running"})`).

```
go run ./cmd/regatta run cmd/regatta/config/two-cluster.example.yaml
```

## Scenarios

A scenario is a yaml file that specifies which clusters to target, which nodegroups to install on them, which jobs to submit, and where to scrape metrics from and write them to.

Take a look at this example in the repo (cmd/regatta/config/two-cluster.example.yaml).

## Cleanup

Using the quickstart, we created kind clusters, started armada, ran kwok controllers, and installed nodes. 

In reverse:

Removes nodes, deletes the kwok controllers.
```
go run ./cmd/regatta teardown cmd/regatta/config/two-cluster.example.yaml
```

Shuts down armada processes and prometheus.
```
mage dev:down
```

Stops the kind clusters.
```
mage kindTeardownRegatta cmd/regatta/config/armada/kind/two-cluster
```