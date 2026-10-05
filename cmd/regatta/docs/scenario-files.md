# Writing a scenario file

See `cmd/regatta/config/scenarios/two-cluster.example.yaml` for the full format.

A scenario file has three sibling sections:

- **`executionTargets`** — where simulated capacity is stood up. Each target is a **`cluster`** target: real `v1.Node` objects created via a kwok controller in a live cluster, exercising Armada's real executor/scheduler path end to end against simulated hardware. A scenario may declare any number of targets.
- **`nodeGroups`** — a top-level map of named node groups, each an arbitrary number of `(nodeProfile, count)` members. Every `executionTargets[]` entry installs the node groups named in its own `nodeGroups` list; if a target omits `nodeGroups`, it installs *all* of them.
- **`load`** — what is submitted: a list of `queues`. Each entry is `count` queues (one, by default) sharing `totalJobs`, with an optional `distribution` over the queues and an optional `arrival` over time. See "Queues and arrival" below.

Details worth knowing when writing your own:

- `armadactl` is the path to an `.armadactl.yaml` (default `$HOME/.armadactl.yaml`) and `authContext` picks which of its `contexts` to run against — the same names `armadactl config get-contexts` lists. Unset, the file's own `currentContext` is used, so one file can hold a local context and a shared-instance context and each scenario names the one it wants. `regatta run --context <name>` overrides the scenario's `authContext`. An unknown name fails before anything is created, and the error lists the available contexts. Regatta logs the Armada URL and context it resolved at the start of a run.
- Node profiles (`cmd/regatta/config/node-profiles/*.yaml`) describe the shape of a single simulated node — allocatable resources, labels, taints — so a hardware shape only needs to be described once. A top-level `nodeGroups` entry names a set of `(nodeProfile, count)` members; an `executionTargets[]` entry installs the named groups it lists, or every group if it lists none.
- Job specs (`cmd/regatta/config/job-specs/*.yaml`) are plain pod specs — no queue, no count. A queue entry's `jobs` references them by path, with a `share` of each queue's jobs.
- `executionTargets[].name` is optional; unset targets get an auto-generated name (`cluster-0`, `cluster-1`, ...) in file order. Names must be unique and are used to namespace per-target resources (kwok controller container/kubeconfig).
- `cluster.evaluateReadiness` controls whether regatta waits for the scheduler to learn about the fake nodes before submitting load. It defaults to `true` for every target; set it to `false` to skip the wait. The scheduler takes a while (about a minute on a shared instance) to register freshly created nodes, so without the wait the start of the run measures that warm-up. The wait submits a small canary job to the `regatta` queue, in a job set of its own, and watches that job's lifecycle events (the stream `armadactl watch` uses) until it is running. Each attempt waits up to `cluster.readinessDelay` (default `5s`) for the canary and doubles that on every retry, for up to `cluster.readinessRetries` attempts (default `5`, about 2.5 minutes in total). If the wait gives up, the error names the last event seen and includes the scheduler's own job report when it has one, e.g. `label kwok.x-k8s.io/node not set`.
- `cluster.continueOnReadinessFailure` (default `false`) lets the run carry on when the readiness check above gives up, instead of failing setup. It is for environments known to be faulty where you still want the test to run. The failure is logged as a warning when it happens and again at the end of the run, and recorded under `readinessFailures` in the metrics report (one `{target, error}` entry per affected target), so a report from such a run says so. The fake nodes are kept, and load is submitted straight away. Only a failed readiness check is tolerated: a failure to create the nodes, or any other setup error, still stops the run. It has no effect when `evaluateReadiness` is `false`.
- `cluster.readinessSelectsTarget` controls whether that canary also selects on this target's `armadaproject.io/regatta-target` node label, which pins it to this target's own fake nodes. Defaults to `true` when `cluster.kind` is `true` and `false` otherwise. Turn it on for an external cluster only if several targets share one Armada *and* the executors report that label (`kubernetes.trackedNodeLabels`); otherwise the canary never finds a node. Without it the canary selects on the KWOK annotation alone.
- `cluster.kubernetes.qps`/`cluster.kubernetes.burst` control the rate limit regatta's own Kubernetes client applies against this target's API server (default `100`/`200`). Fake-node setup/teardown creates or deletes every node concurrently, so a large `nodeGroups` count (several hundred+) may need these raised further if you see it stall out waiting for nodes to go `Ready`.

When Regatta runs, it stands up every execution target and submits the configured load, then exits — it never polls job state, since regatta is a load generator, not a job-completion tracker, and polling doesn't scale to large submission batches.
Execution targets are **not** torn down automatically: they stay up so you can keep collecting metrics after submission finishes, and must be torn down explicitly with `regatta teardown`.
If setting up an execution target fails partway through a run, whatever did come up in that same run is still torn down automatically — only the normal-exit path leaves things running.

## Queues and arrival

```yaml
load:
  queues:
    - prefix: big-tenant-                   # one queue, called big-tenant-1
      priorityFactor: 1                     # used only if the queue has to be created (default 1)
      totalJobs: 600
      targets:                              # optional: targets its jobs may land on (default all)
        - gpu-cluster
      arrival:
        shape: uniform
        duration: 2m
        step: 10s
      jobs:
        - jobSpec: job-specs/gpu-job.yaml
          share: 1
    - prefix: tenant-                       # 50 queues, tenant-01 to tenant-50
      count: 50
      totalJobs: 1500                       # across the 50 queues
      minJobsPerQueue: 1                    # default 1
      distribution:
        type: lognormal
        mu: 2.3
        sigma: 0.8
      arrival:
        shape: gaussian
        duration: 2m
      jobs:
        - jobSpec: job-specs/kwok-sleep.yaml
          share: 0.9
        - jobSpec: job-specs/gpu-job.yaml
          share: 0.1
```

- **One kind of entry.** An entry is `count` queues (default 1) named `prefix` plus a zero-padded number: `tenant-` with `count: 50` gives `tenant-01` to `tenant-50`, and with the default count gives `tenant-1`. The job set of each queue is `regatta-<queue name>`. Queue names must be unique across all entries.
- **Jobs are shares.** `jobs` lists job-spec files with the `share` (a fraction, summing to 1) of each queue's jobs that have that shape. `totalJobs` is the number of jobs across all of an entry's queues; the counts are exact and the same on every run.
- **Queues** are created if they do not exist and never changed if they do (regatta runs against shared instances). If an existing queue has a different `priorityFactor` than declared, that is logged as a warning.
- **`distribution`** splits `totalJobs` over the entry's queues, so it needs `count` above 1. It is `uniform` (the default), `gaussian` (`mean`, `stddev`: queue numbers, defaulting to the middle queue and a sixth of the count) or `lognormal` (`mu`, `sigma`: the mean and standard deviation of ln of the queue number, so `e^mu` is the median queue). A bare string (`distribution: uniform`) uses the defaults. `minJobsPerQueue` lifts any queue the curve would leave below it. For 1000 queues sharing 100000 jobs, `uniform` gives every queue 100, `lognormal` with `mu: 4.6, sigma: 1` peaks around queue 37 with about 665, and `gaussian` peaks at queue 500 with about 240.
- **`arrival`** says when a queue's jobs are submitted: `shape` `uniform` (constant rate, the default) or `gaussian` (ramps up, peaks, ramps down; `mean` and `stddev` are durations, defaulting to the middle of `duration` and a sixth of it), over `duration`, in batches every `step` (default 10s). Without an `arrival`, or with `duration: 0`, everything is submitted at once. For continuous submission see below. Every queue's clock starts together, after the fake nodes are ready.
- **Continuous submission** keeps submitting until you stop the run. Set `totalJobs: -1` and `arrival.duration: -1` (both, or neither), and give the rate as `jobsPerStep`:

  ```yaml
  load:
    queues:
      - prefix: steady-
        count: 20
        totalJobs: -1
        jobsPerStep: 600                # jobs per step across all 20 queues
        distribution:
          type: lognormal
          mu: 2.0
          sigma: 0.8
        arrival:
          duration: -1
          step: 60s                     # default for continuous submission; 10s otherwise
        jobs:
          - jobSpec: job-specs/kwok-sleep.yaml
            share: 1
  ```

  `jobsPerStep` is split between the entry's queues by `distribution`, and between the job shapes by their `share`. Rates are fractional (a queue in the tail might get 0.3 jobs per step); the remainder is carried from step to step, so the long-run rate is exact and no job shape starves. Continuous submission is `uniform` only, since a curve needs an end, and `minJobsPerQueue`, `mean` and `stddev` do not apply. It can share a file with bounded entries. The run never drains, so it writes a report every `metrics.reportInterval` (default `120s`), each covering the time since the previous one, and a final report when you press Ctrl+C. See `metrics-report.md`.
- `targets` limits which execution targets a queue's jobs may land on; it defaults to all of them. The readiness canary for a target runs in the first queue that can land on it.
- The metrics report covers all the declared queues together.
