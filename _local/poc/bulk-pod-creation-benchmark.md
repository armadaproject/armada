# Direct etcd pod writer — POC benchmark report

Results of the POC described in [bulk-pod-creation.md](./bulk-pod-creation.md),
measured 2026-11-06 on the `poc/direct-etcd-pod-writer` branch.
Everything here is from a disposable kind cluster; treat the numbers as
relative ratios, not capacity figures for production.

## How the testing was performed

Reproduction procedure, written to be runnable as-is by a person or an agent.
Distilled requirements and machine-specific commands are pointed out inline.
The whole suite takes about 10 minutes, most of it the API-path runs; the etcd
sweeps run in seconds.

### Prerequisites

- Go toolchain, Docker, kubectl.
- This repository checked out on branch `poc/direct-etcd-pod-writer` — the
  writer package, `cmd/bulk-bench`, and its `-txn-concurrency` flag exist only
  there.
- A disposable kind cluster with ≥1 control-plane node and ≥1 worker
  ("disposable" is a hard requirement: the API-path run creates real Kubernetes
  pods and the etcd sweep writes thousands of keys). Any cluster works;
  node names below are auto-discovered, not assumed to be `kind-control-plane`.
- A quiet control plane: no other benchmark or delete storm in flight.
- Commands assume Linux (`ss` for the port check, `docker exec` for kind
  nodes). On macOS substitute `lsof -iTCP:2379 -sTCP:LISTEN`.

### 1. Discover the cluster and pick scratch paths

All machine-specific names are resolved once into variables, so agents and
humans paste the same commands on any cluster. Run from the repo root on the
POC branch. The measured cluster was kind with 1 control plane + 2 workers
(16 CPUs, 110 max pods per node each); etcd runs as a static pod on the
control plane. Names differ between clusters — that is why they are
discovered, not hardcoded.

```sh
SCRATCH=/tmp/podwriter-bench   # results, certs, logs; deleted in step 9
CERTS=$SCRATCH/certs
OUT=$SCRATCH/results
mkdir -p "$CERTS" "$OUT"

# Control-plane node (same string as the docker container name on kind)
CP_NODE=$(kubectl get nodes -l node-role.kubernetes.io/control-plane \
  -o jsonpath='{.items[0].metadata.name}')
# etcd static pod
ETCD_POD=$(kubectl -n kube-system get pods -o name | grep etcd | head -1 | cut -d/ -f2)
echo "CP_NODE=$CP_NODE ETCD_POD=$ETCD_POD"
```

### 2. Extract etcd client certificates

The apiserver's etcd client certificate is at the pki root of the
control-plane node, not under `etcd/`:

```sh
docker exec "$CP_NODE" cat /etc/kubernetes/pki/etcd/ca.crt           > "$CERTS/ca.crt"
docker exec "$CP_NODE" cat /etc/kubernetes/pki/apiserver-etcd-client.crt > "$CERTS/client.crt"
docker exec "$CP_NODE" cat /etc/kubernetes/pki/apiserver-etcd-client.key > "$CERTS/client.key"
ls "$CERTS"   # must show 3 non-empty files
```

### 3. Expose etcd

Host-process access needs a port-forward; keep it running for all etcd-mode
runs. The PID is recorded because cleanup happens in a later step, possibly
from a different shell (this matters for agents, which run each command in a
fresh shell):

```sh
kubectl -n kube-system port-forward "pod/$ETCD_POD" 2379:2379 >"$SCRATCH/portforward.log" 2>&1 &
echo $! > "$SCRATCH/portforward.pid"
sleep 2
ss -ltn | grep 2379    # must be listening before continuing
```

### 4. Verify the writer before measuring

First a canary write that becomes a real pod, verified through the Kubernetes
API, so no numbers are collected against a broken path:

```sh
go build -o "$SCRATCH/bulk-bench" ./cmd/bulk-bench
"$SCRATCH/bulk-bench" -mode=etcd -count=1 -concurrency=1 \
  -ca="$CERTS/ca.crt" -cert="$CERTS/client.crt" -key="$CERTS/client.key" \
  -verify          # waits for the pod to be visible via the API server
kubectl get pod armada-bulk-bench-00000 -o wide   # phase/schedulable; delete it after
```

Then correctness tests for the writer code itself:

```sh
go test -race ./internal/podwriter   # includes parallel-chunk and idempotency tests
```

### 5. Run the sweep

Every measured run uses a unique `-name-prefix` so all pods are fresh creates
(no `AlreadyExists` contamination); `-prefix=/registry-bench` keeps the written
keys out of the real Kubernetes registry, so no scheduler/kubelet interference
contaminates write-throughput numbers. One run = one process measuring
wall-clock around all writes (JSON via `-output`; the directory exists since
step 1).

Tuning sweep — pods per etcd transaction fixed at 64 (etcd's `maxTxnOps` 128,
one compare + one put each), parallel transactions swept 1→16:

```sh
for tc in 1 2 4 8 16; do
  "$SCRATCH/bulk-bench" -mode=etcd -count=1000 -concurrency=1 -batch-size=64 \
    -txn-concurrency=$tc -prefix=/registry-bench -name-prefix=bench-tc$tc \
    -ca="$CERTS/ca.crt" -cert="$CERTS/client.crt" -key="$CERTS/client.key" \
    -output="$OUT/etcd-1000-tc$tc.json"
done
```

Scale run at the best setting (5000 pods, plus 4 concurrent batches):

```sh
"$SCRATCH/bulk-bench" -mode=etcd -count=5000 -concurrency=4 -batch-size=64 \
  -txn-concurrency=8 -prefix=/registry-bench -name-prefix=bench-5k \
  -ca="$CERTS/ca.crt" -cert="$CERTS/client.crt" -key="$CERTS/client.key" \
  -output="$OUT/etcd-5000-tc8-bc4.json"
```

Repeat runs at one config for variance+: `-count=1000 -txn-concurrency=8`
run 3×; spread was 22.5–27.6k pods/s (~10%).

### 6. API-path baseline

The `api` mode creates real Pod objects through the Kubernetes API (validation,
admission, watch fan-out). Two correctness notes that change the numbers:

- `-node-name=bench-nonexistent` binds pods to a node that doesn't exist, so
  they stay Pending forever: no kubelet or scheduler churn distorts create
  latency, and no cleanup storm afterwards.
- The client-side rate limiter is raised to `QPS=5000` **in `bulk-bench`'s
  kube config on this branch** — no patching needed. Without it, client-go's
  default **5 QPS burst 10** caps every run: 1000 pods take a suspiciously
  exact ~198s regardless of concurrency — a wasted measurement that looks
  like a slow apiserver. If rebasing, keep that change.

```sh
kubectl create ns benchapi
for c in 8 32 128; do
  "$SCRATCH/bulk-bench" -mode=api -count=1000 -concurrency=$c \
    -namespace=benchapi -name-prefix=bench-api$c \
    -node-name=bench-nonexistent -output="$OUT/api-1000-c$c.json"
done
"$SCRATCH/bulk-bench" -mode=api -count=5000 -concurrency=32 -namespace=benchapi \
  -name-prefix=bench-api-5k -node-name=bench-nonexistent \
  -output="$OUT/api-5000-c32.json"
```

### 7. How durations are computed

Throughput = pods / wall-clock around the write call loop in the process. For
`-mode=etcd` this includes prepare, validation, protobuf encoding and the
etcd commits (that is the component being measured). gRPC dial cost is paid
once via a warm-up `Get` before timing. For `-mode=api` it includes the API
client's create round trips.

### 8. Sanity checks on results

- Every run printed `created=<count> rejected=0 failed=0` (`bulk-bench` exits
  non-zero otherwise); `-verify` confirmed API visibility of etcd-written pods
  in step 4.
- Repeated runs at identical settings bounded run-to-run variance at ~10%.
- Every run's timestamp spread was checked to exclude runs where a coincident
  background process (kubelet churn, deletes) distorted the window.

### 9. Cleanup

```sh
kubectl delete ns benchapi --wait=false   # API-path pods (stayed Pending; quick)
kill "$(cat "$SCRATCH/portforward.pid")"  # stop the etcd port-forward
rm -rf "$SCRATCH"                         # certs are sensitive; results included
# /registry-bench keys (tens of thousands) stay in the kind etcd; invisible to
# Kubernetes. etcdctl is not in the kind container to delete them; they vanish
# on cluster rebuild or `mage kind reset`.
```

## Environment

- kind v1.36.1: 1 control plane (apiserver + single-member etcd), 2 workers, 16 CPUs each.
- etcd reached through `kubectl port-forward` (loopback TCP, no mTLS path used by apiserver).
- Benchmark tool: `cmd/bulk-bench` (`-mode=etcd` writes via the shared writer core;
  `-mode=api` calls the Kubernetes Pod create API as the baseline).
- Workload: pause containers, 100m CPU / 64Mi each (~2.4 KB encoded pod).
- etcd-mode runs used the isolated prefix `/registry-bench` so written keys are not
  real Kubernetes objects (no scheduler/kubelet interference); the API baseline
  creates real pods bound to a nonexistent node so they stay Pending and the
  measurement covers only the create path.
- JSON summaries per run: session scratch directory, deleted afterwards
  (not preserved).

## A benchmark-client bug to know about

The first API-path measurements were invalid: client-go defaults to a **5 QPS
client-side rate limiter** (burst 10), which silently throttled the benchmark
client — 20 pods took exactly 2.0s, 1000 pods exactly ~198s, independent of
concurrency. `bulk-bench` now sets a high client QPS/burst. Any other benchmark
that measures raw Kubernetes create throughput must do the same or it measures
the client, not the server.

## Writer tuning

Original writer: one batch's etcd transactions committed strictly sequentially,
each paying a full round trip. Change (Env level; no semantic change):

- New `podwriter.Config.Concurrency`: chunks of one batch (≤64 pods, etcd's
  `maxTxnOps` cap) commit **in parallel**. Chunks hold disjoint keys; each is
  still a single compare-and-put transaction, so per-pod create semantics are
  unchanged. Verified with `go test -race` (see "How the testing was performed").
- New executor setting `podWrite.etcd.txnConcurrency`, set to `8` in the POC
  config (`_local/executor/config-etcd-poc.yaml`).
- `bulk-bench` gained `-txn-concurrency` to sweep this.

## Direct write throughput (etcd path)

| Config | Pods | Throughput | Relative |
|---|---:|---:|---:|
| txn-concurrency=1 (pre-tuning) | 1000 | 10.1k pods/s | 1.0× |
| txn-concurrency=2 | 1000 | 14.7k pods/s | 1.5× |
| txn-concurrency=4 | 1000 | 19.2k pods/s | 1.9× |
| **txn-concurrency=8 (best)** | 1000 | **27.6k pods/s** | **2.7×** |
| txn-concurrency=16 | 1000 | 24.4k pods/s | 2.4× |
| txn-concurrency=1 | 5000 | 9.6k pods/s | 1.0× |
| txn-concurrency=8 | 5000 | 28.1k pods/s | 2.9× |
| txn-concurrency=8 + 4 concurrent batches | 5000 | **38.5k pods/s** | 4.0× |

- Repeats at the same config vary ~10% (22.5–27.6k pods/s at 1000 pods):
  the 2.7× tuning gain is well outside noise.
- txn-concurrency=16 is worse than 8 at this batch size — contention on tiny
  batches. 8 is the setting, not a law; production hardware may shift it.
- Sweet spot: txn size 64 (etcd `maxTxnOps` 128 / 2 for compare+put),
  txn-concurrency 8, batch-level concurrency ~4.

## API-path baseline (same cluster, same objects via the Pod create API)

| Config | Pods | Throughput |
|---|---:|---:|
| concurrency=8 | 1000 | 885 pods/s |
| concurrency=32 (best) | 1000 | 1253 pods/s |
| concurrency=128 (saturated) | 1000 | 677 pods/s |
| concurrency=32 | 5000 | 1639 pods/s |

The API path is the full Kubernetes create cost — validation, admission
plugins, watch fan-out, event publication — against the same etcd.

## Relative values

- **Tuning gain: ~2.7–4×** over the sequential writer (27.6–38.5k vs 10.1k pods/s).
- **Direct etcd vs the API-path baseline: ~20–25×** (22–38k vs 1.3–1.6k pods/s at
  1000–5000 pods), using the tuned writer.

These ratios are consistent with the design doc's premise: a single apiserver
serializes bulk creates (~1.6k pods/s ceiling here), while chunked parallel
etcd transactions scale with round-trip pipelining.

## Caveats

- Single etcd member via port-forward. Production would use 3 members over
  mTLS with raft replication: lower absolute throughput, similar relative gain
  (the win is round-trip pipelining, not transport).
- `/registry-bench` writes bypass apiserver observability entirely; that is the
  point of the design, so the vs-API ratio is not overstated, but a real
  /registry write additionally feeds the watch cache and any etcd quota pressure.
- The API baseline ran on a quiet control plane (pods unschedulable, kubelet idle).
  A busy cluster would slow the API path further, making ~20× conservative.
- End-to-end Armada results (previous measurement session, default and tuned
  executor settings) showed the executor's 5s lease/report loop and kubelet
  startup dominate `lease→Running` time, so these write-path gains do not
  translate 1:1 into end-to-end improvement. See the gate discussion in
  [bulk-pod-creation.md](./bulk-pod-creation.md): the ≥2–3× end-to-end gate remains
  the binding decision criterion.

## Cleanup note

The benchmark left ~35k keys under `/registry-bench` in the kind etcd
(invisible to Kubernetes; `etcdctl` is not available in the kind container).
`mage kind reset` or a cluster rebuild removes them.
