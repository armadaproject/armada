# Dev-cluster pod insert-rate benchmark

Answers one question: **can this cluster really take ~1.6k pod inserts per
second through the Kubernetes API, and if not, which stage caps it?**

It creates `COUNT` (default 20k) pods per batch, records the achieved insert
rate and client-side latency, and samples the kube-apiserver's own metrics
during the run so the control-plane cost is visible rather than inferred.

This is the same measurement as the `-mode=api` sweep in
[bulk-pod-creation-benchmark.md](./bulk-pod-creation-benchmark.md), but made
runnable against a real dev cluster: pod object size is a variable, batches are
large enough to mean something, and apiserver impact is collected instead of
assumed.

## What is measured

Per run, written to a JSON file and printed as two lines:

| Signal | Source | What it tells you |
|---|---|---|
| `podsPerSecond` | client wall clock | achieved insert rate (created pods only) |
| `clientP50Ms`, `clientP99Ms`, `clientMaxMs` | per-create round trip | end-to-end latency seen by the writer |
| `postRatePerSecond` | `apiserver_request_total` | creates the apiserver actually served |
| `postMeanMs` | `apiserver_request_duration_seconds` | server-side create latency |
| `etcdCreateMeanMs` | `etcd_request_duration_seconds` | apiserver→etcd write latency |
| `webhookAdmitMeanMs` | `apiserver_admission_webhook_admission_duration_seconds` | admission webhook cost |
| `code201`, `code429`, `code5xx` | `apiserver_request_total` | outcomes; 429 means server-side throttling |
| `apfRejected` | `apiserver_flowcontrol_rejected_requests_total` | API Priority & Fairness shedding |
| `inflightMutatingPeak` | `apiserver_current_inflight_requests` | queuing/saturation peak (0 if the run was shorter than the sampling interval) |
| `cpuCoresMean`, `rssAfterBytes` | `process_*` | apiserver CPU and memory during the run |
| `podsInStorageBefore/After` | `apiserver_resource_objects` | stored pod count (alpha metric, refreshed periodically; `code201` is authoritative) |
| `dbSizeBytesBefore/After` | `apiserver_storage_size_bytes` | etcd space growth |

## How the load is shaped

- **Batches of 20k.** `COUNT=20000` per run, then the pods are force-deleted in
  parallel (`bulk-bench -mode=delete`, a few hundred per second) and the empty
  namespace is removed before the next run starts. Going through the namespace
  controller instead deletes pods at roughly 10/s, which would make a 20k
  cleanup take half an hour. Deletes are as expensive as creates, so they are
  never in flight during a measured window.
- **Pods stay Pending.** Each pod is bound to a node name that does not exist
  (`-node-name=bench-nonexistent`). The scheduler skips bound pods and no
  kubelet sees them, so the numbers isolate the create path. Real Armada inserts
  would additionally pay scheduling and kubelet load; this benchmark
  deliberately does not.
- **Pod size is a variable.** `-pod-annotations-bytes` pads one annotation, so
  object size can be swept without changing anything else. `small` / `medium` /
  `large` map to 0 / 4096 / 32768 annotation bytes (≈0.4 KB / 4.5 KB / 33 KB
  encoded). Check what you are writing with:

  ```sh
  go run ./cmd/bulk-bench -dry-run -count=5 -pod-annotations-bytes=32768
  ```

- **Client limiter is raised.** `bulk-bench` sets client-go QPS to 5000; the
  default 5 QPS would measure the client, not the cluster. Anything above
  ~5k/s needs that raised.

## Running it

Prerequisites: `kubectl` pointing at the dev cluster, Go toolchain, and a
reasonable expectation that nobody else is doing something heavy on the control
plane for the duration.

Everything is one script. It builds the tool, runs a canary, sweeps the matrix,
prints the table, and ends with a verdict:

```sh
# 1. Smoke test: 500 pods, ~1 minute. Proves the path works end to end.
SMOKE=1 CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh

# 2. Full sweep: 3 sizes x 2 concurrencies + a paced 1600/s run.
CONFIRM=yes ./_local/poc/dev-cluster-insert-bench.sh
```

The script refuses to run without `CONFIRM=yes` and prints the context, the
server URL and the total number of pods it is about to create first.

Tunables (`CONFIRM=yes SIZES=... ./script`):

| Variable | Default | Meaning |
|---|---|---|
| `COUNT` | `20000` | pods per batch |
| `SIZES` | `small medium large` | pod object sizes, or raw annotation byte counts |
| `CONCURRENCY` | `32 64` | concurrent creates, unpaced runs |
| `RATES` | `1600` | sustained target rate, one run per value |
| `PACE_SIZES` | `medium` | which size the paced runs use |
| `PACED_CONCURRENCY` | `64` | concurrency for paced runs |
| `COOLDOWN` | `20` | seconds between runs |
| `DELETE_CONCURRENCY` | `64` | parallel deletes during cleanup |
| `TARGET_RATE` | `1600` | rate the final verdict judges against |
| `NODE` | `bench-nonexistent` | node name bound into each pod |
| `OUT` | `/tmp/armada-insert-bench-<ts>` | results directory |

Each run leaves `<tag>.json` (full metrics), `<tag>.log` (console output) and,
after the sweep, `summary.csv`.

## Reading the result

`summary.csv` is the table to look at:

```
run,pods,conc,rate,annotBytes,podsPerSecond,clientP50Ms,clientP99Ms,postRate,postMeanMs,etcdCreateMeanMs,webhookMs,inflightPeak,cpuCores,code201,code429,code5xx,apfRejected
```

Sanity checks before believing a number:

- `code201` should equal `count`. If not, creates were dropped or the metrics are
  incomplete.
- `podsPerSecond` should be close to `postRate`. A large gap means client-side
  time (pacing, serialization, connection setup), not server work.
- Re-run the winning configuration once; ~10% run-to-run spread was normal on
  kind.

## Deciding whether 1.6k/s is real

The script ends with a `=== verdict ===` block that applies steps 1–2 below and
names the likely limit; the text here explains what it is telling you.

1. Find the highest unpaced `podsPerSecond` across the concurrency sweep.
   **Below ~1600 with no throttling → no**: the cluster cannot do it at all.
2. Take the best config and look at the paced `rate=1600` run. **1600 achieved
   with flat latency and zero 429/5xx → yes, sustainable.**
3. If the paced run degrades (429s, latency climbing, inflight pinned), the
   ceiling is below 1600 and the paced run shows you how it fails.

Diagnosis when it caps below target:

| Symptom | Likely limit |
|---|---|
| `code429`/`apfRejected` above zero | API Priority & Fairness or a per-client server limit, not raw capacity |
| `etcdCreateMeanMs` rising with load | etcd is the bottleneck (fsync/WAL, disk, single member) |
| `webhookAdmitMeanMs` large or rising | admission webhooks are the cost; they run on every create |
| `postMeanMs` rising while etcd stays flat | apiserver CPU/admission/encoding |
| `cpuCoresMean` flat at the apiserver's CPU limit | apiserver is CPU-bound; check its requests/limits |
| client `p99` rising but `postMeanMs` flat | client or network: run the benchmark from inside the cluster or raise concurrency |
| `inflightMutatingPeak` pinned while rate plateaus | requests are queuing; find what they queue behind |

## Caveats worth stating in the write-up

- **One apiserver's metrics.** `/metrics` is scraped through the API endpoint,
  so on an HA control plane you see whichever instance answered. Create rates
  are still what the client achieved overall.
- **No server-side p99.** Server latency is reported as a mean; the tail comes
  from the client-side percentiles.
- **Metrics scrapes are not free.** Every poll fetches the full `/metrics`
  document (a few MB) from the apiserver, every 2s by default. It is small next
  to 1.6k creates/s, but raise `-metrics-interval` if the control plane is
  already tight, and do not read the CPU figure as "load excluding the
  benchmark".
- **Watch fan-out is not measured.** Real consumers (Armada's scheduler and
  executor watches) will feel 20k creates as watch churn. The apiserver metrics
  here show request latency, not watch delivery lag.
- **Scheduler and kubelet are excluded** by design. Add those back only if the
  question is end-to-end `lease→Running`, which needs real nodes.
- **Object size is one padded annotation.** Real pod specs grow through many
  fields; byte size is the dominant factor for the write path, but validation
  cost is not perfectly linear in bytes.
- **Throughput covers creates only.** The delete storm that cleans up each batch
  is deliberately outside the measured window.
- **Benchmark pods are force-deleted** (grace period 0): they are bound to a
  node that does not exist, so no kubelet would ever confirm a graceful delete.
- **Cluster must be quiet.** Other tenants' apiserver and etcd load lands in the
  same numbers.

## Cleanup

The script force-deletes each batch's pods in parallel, then deletes the
namespace and waits for it to disappear. If a run is interrupted, clean up with:

```sh
kubectl get namespaces | grep ins-bench
kubectl delete namespace <name> --wait=false
```

Deleting through the namespace controller is slow (~10 pods/s) but correct; for
tens of thousands of leftovers use the binary built by the script:

```sh
$OUT/bulk-bench -mode=delete -count=20000 -concurrency=64 \
  -namespace=<name> -name-prefix=bench-<tag>
```

Deleting 20k pods writes to etcd once per pod; if the cluster has a small etcd
quota, watch `dbSizeBytesAfter` and let it compact between batches rather than
running the sweep back to back with `COOLDOWN=0`.
