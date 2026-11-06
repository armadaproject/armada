# Bulk pod creation for Armada: feasibility and proposed design

- [Bulk pod creation for Armada: feasibility and proposed design](#bulk-pod-creation-for-armada-feasibility-and-proposed-design)
  - [Status](#status)
  - [1. Summary](#1-summary)
  - [2. Problem and scope](#2-problem-and-scope)
  - [3. Kubernetes today](#3-kubernetes-today)
  - [4. Peer practice](#4-peer-practice)
  - [5. How Armada creates pods today](#5-how-armada-creates-pods-today)
  - [6. What the Kubernetes API does on create](#6-what-the-kubernetes-api-does-on-create)
  - [7. Designs A and B](#7-designs-a-and-b)
    - [7.1 Shared writer core](#71-shared-writer-core)
    - [7.2 Design A: executor-integrated writer](#72-design-a-executor-integrated-writer)
    - [7.3 Design B: per-cluster bulk writer service](#73-design-b-per-cluster-bulk-writer-service)
    - [7.4 A versus B](#74-a-versus-b)
  - [8. Implementation plan](#8-implementation-plan)
    - [8.1 Package layout](#81-package-layout)
    - [8.2 Batch algorithm](#82-batch-algorithm)
    - [8.3 Object preparation](#83-object-preparation)
    - [8.4 Encoding and transactions](#84-encoding-and-transactions)
    - [8.5 Wiring](#85-wiring)
  - [9. Cluster upgrades and component tracking](#9-cluster-upgrades-and-component-tracking)
    - [9.1 Component identity and inventory](#91-component-identity-and-inventory)
    - [9.2 Compatibility contract](#92-compatibility-contract)
    - [9.3 Upgrade order](#93-upgrade-order)
    - [9.4 Rollout phases](#94-rollout-phases)
    - [9.5 Rollback](#95-rollback)
    - [9.6 Upgrade checklist](#96-upgrade-checklist)
  - [10. Risks and limitations](#10-risks-and-limitations)
  - [11. Recommendation](#11-recommendation)
  - [12. Open questions](#12-open-questions)
  - [References](#references)

## Status

Draft for review. This is a feasibility study, not an accepted design.

| Field | Value |
|---|---|
| Status | Draft for review |
| Date | 2026-11-04 |
| Owners | TBD |
| Reviewers | TBD |

POC implementation results and benchmark measurements: see the repo-only note
`_local/poc/bulk-pod-creation-benchmark.md` (not part of the published docs).

## 1. Summary

Kubernetes has no bulk API. Every pod is created through an individual API
request, and every write goes through the API server's validation, admission and
etcd commit path. For Armada's dispatch path, that is one create plus one scheduler
bind per pod, which becomes the bottleneck at tens of thousands of pods.

The research findings:

- **A Kubernetes bulk API is unlikely.** The 2015 bulk-create request
  ([#9633](https://github.com/kubernetes/kubernetes/issues/9633)) has been open
  since 2015 without implementation.
- **Peer systems do not write bulk to etcd.** Volcano and YuniKorn are capped by
  the API server and etcd. The failure
  modes are write storms, request-size limits and webhook timeouts, not a missing
  bulk verb.
- **Direct etcd writes are technically feasible but not fully correct.** The API
  server's watch cache observes correctly encoded writes automatically. What is
  lost is defaulting, validation, admission, quota, RBAC, audit and a stable
  storage contract.
- **The gain is bounded by the write-path share.** If create and bind are less than
  about 30% of time from lease to `Running`, the ceiling is roughly 1.43x.

Two designs are worth considering:

- **A:** an executor-integrated writer.
- **B:** a per-cluster writer service.

They share the same writer core. A is the prototype; B is the production shape if
the experiment succeeds.

**Recommendation:** measure the bottleneck first. If the
create and bind path is the dominant cost and the cluster is dedicated and
self-hosted, a narrow Design A prototype is justified. Do not ship this as a
general Armada feature.

## 2. Problem and scope

A training job may need hundreds or thousands of worker pods, often starting
together. The current dispatch path costs, per pod:

1. one API request and response;
2. API server decoding, defaulting, validation and admission;
3. one etcd transaction and fsync;
4. one watch event fan-out;
5. one scheduler bind write.

The goal is to reduce the time from lease to all pods `Running`, and to reduce
partial failures under burst load.

In scope: the pod creation path from the executor to etcd, and the two direct-etcd
designs. Out of scope: job ingestion, queueing, scheduling architecture, broad
pod-spec support, and managed clusters without etcd access.

## 3. Kubernetes today

**No bulk create endpoint.** The API conventions define single-resource verbs.
`kubectl create -f` sends one request per object.

**SIG API Machinery work is elsewhere.** KEP-5598 batches scheduling decisions, not
object creation.

**A bulk API is unlikely.** Every write is deliberately funnelled through
authentication, authorization, admission, validation, defaulting and optimistic
concurrency. A bulk API would either serialize those checks, offering little gain,
or relax them, creating correctness and security risks.

## 4. Peer practice

**Volcano** sits directly on the API-server/etcd path and documents the failure
modes:

- CREATE versus SCHEDULE contention produces a "stair-step" stall; increasing the
  create rate can prolong scheduling
  ([tuning guide](https://volcano.sh/docs/v1.16.0/userguide/user_guide_how_to_tune_volcano_performance/)).
- Full-object status updates by the scheduler exhausted etcd in
  [#5236](https://github.com/volcano-sh/volcano/issues/5236).
- Admission webhook timeouts (root-caused to the admission controller's own
  client-side throttling) cause bulk creation failures
  ([#3318](https://github.com/volcano-sh/volcano/issues/3318)).
- Batches must stay below etcd's default 1.5 MiB request size.

**YuniKorn** replaces kube-scheduler but still submits through the API server.
Its Kubemark benchmark is capped by the Kubernetes master services, not by
YuniKorn. It also rate-limits unschedulable-pod events to protect etcd
([YUNIKORN-3112](https://issues.apache.org/jira/browse/YUNIKORN-3112)).

| | Volcano | YuniKorn |
|---|---|---|
| Bulk strategy | Optimise API writes and scheduling | Custom scheduler; accepts the API ceiling |
| Etcd pain | Write storms, 1.5 MiB ceiling, storage exhaustion | Event floods; benchmark capped by etcd |
| Lesson | API/etcd is the constraint | A faster scheduler does not beat etcd |

## 5. How Armada creates pods today

- `internal/executor/job/submit.go` submits each run through a configurable number
  of workers.
- `internal/executor/context/cluster_context.go` calls
  `CoreV1().Pods(namespace).Create(...)`.
- `internal/common/cluster/kubernetes_client.go` can impersonate the submitting
  user, so the API server enforces that user's RBAC (`impersonateUsers: false` in
  the checked-in config, so today's pod writes run as the executor's service
  account unless impersonation is enabled).
- The checked-in executor config uses `submitConcurrencyLimit: 2` and a shared
  `QPS: 10000` limiter. Production values may differ.
- `internal/scheduler/api.go` adds a node selector; kube-scheduler binds the pod.

The executor already has the pod, the run ID, the user, the groups and the target
node (the Armada scheduler injects the node selector into the leased pod spec, see
`internal/scheduler/api.go` `addNodeIdSelector`), so it can intercept creation.

## 6. What the Kubernetes API does on create

| API server behavior | Why it matters |
|---|---|
| Defaulting | Sets `requests=limits`, grace period, DNS policy, scheduler name, probes, volume modes, image pull policy. Missing values are not harmless. |
| Validation | Rejects invalid names, labels, specs and resources. |
| Namespace, quota, LimitRange | Rejects terminating namespaces, enforces quota, applies defaults. |
| ServiceAccount admission | Sets the account, copies `imagePullSecrets`, projects the token volume. |
| PodSecurity, Priority, tolerations | Enforce security, set `priority`, add default tolerations. |
| Mutating and validating webhooks | Inject sidecars and enforce policy. |
| Metadata and status | Sets UID, creation timestamp, `phase=Pending`, `qosClass`. |
| Storage | Encodes the object, applies encryption, writes it, clears `resourceVersion`. |

## 7. Designs A and B

### 7.1 Shared writer core

`internal/podwriter` owns preparation, allowlist validation, key construction,
protobuf encoding, batching, transactions, conflict resolution, authorization and
metrics. It must not import the executor.

The writer adds one new direct dependency: `go.etcd.io/etcd/client/v3`.

### 7.2 Design A: executor-integrated writer

```text
Armada scheduler
      │ lease
      ▼
  Executor
      ├── podwriter core ──► etcd
      ├── authorization / namespace ──► apiserver
      ├── services / ingresses / deletes ──► apiserver
      └── run state and event reporting

  apiserver watch ──► kubelet
```

#### Components

- **Executor:** leases runs, builds pods, owns the run state, event reporting and
  submitted-pod cache.
- **In-process writer core:** preparation, allowlist, encoding, batching,
  transaction handling and authorization cache.
- **Etcd client:** TLS connection and write path.
- **Kubernetes API client:** authorization checks, namespace reads, service and
  ingress creation, pod deletion and informer watch.

#### Data flow

1. The executor receives leases and builds a batch of pods.
2. The batch goes to the in-process writer; no network hop.
3. The writer checks the namespace and a cached `SubjectAccessReview`.
4. It prepares, validates and encodes each pod, then commits one etcd transaction
   per chunk.
5. The executor records the result, populates the submitted-pod cache, creates
   services and ingresses through the API server, and reports events.
6. The API server watch sees the pods and kubelets start them.

#### Deployment and failure domains

- One writer per executor; there is no separate deployment.
- A feature flag selects the writer for the whole executor, not per namespace.
- The etcd client certificate and key are mounted into the executor pod.
- If the executor restarts, in-flight batches are lost and leases are retried.
- If etcd is unavailable, writes fail; if the API server is unavailable,
  authorization and namespace checks fail closed.
- Multiple executors in one cluster can race safely because writes use etcd
  compares, but the normal topology is one executor per cluster.

#### Security boundary

The executor already has permission to create pods. Adding etcd write access means
a compromised executor can also bypass admission and write arbitrary pods. The
etcd credential is the main new risk, and the audit trail is split between Armada
events and the writer's own log.

#### Pros

- No new service, network hop, API contract or certificate chain beyond etcd.
- Lowest latency and simplest request path.
- Reuses the existing lease, state and failure handling.
- Fastest to prototype and easiest to roll back with a flag.
- No service discovery or extra HA planning.

#### Cons

- Etcd credentials live in the executor, expanding its blast radius.
- No isolation between the unsafe write path and normal execution.
- Enablement is per executor, not per namespace or queue.
- Executor and writer must be upgraded together.
- Harder to audit or rate-limit separately.
- Executor restarts affect dispatch and the writer at the same time.

**Use:** prototype and throughput experiment on a disposable cluster.

### 7.3 Design B: per-cluster bulk writer service

```text
Armada scheduler
      │ lease
      ▼
  Executor ── mTLS BulkCreate ──► pod-bulk-writer ──► etcd
      │                                  │
      │                                  └── authorization / namespace ──► apiserver
      ├── services / ingresses / deletes ──► apiserver
      └── run state and event reporting

  apiserver watch ──► kubelet
```

#### Components

- **Executor:** leases runs, builds batches, calls the writer over mTLS, tracks
  submitted pods, creates services and ingresses, reports events, deletes pods.
- **`pod-bulk-writer`:** a stateless Deployment with the shared writer core, the
  etcd client, and cached authorization and namespace checks.
- **Etcd:** the write target.
- **Kubernetes API server:** used by the writer for authorization and namespace
  reads, and by the executor for all other pod lifecycle operations.
- **Service, mTLS and NetworkPolicy:** the trust boundary between executor and
  writer.

#### Data flow

1. The executor receives leases and builds a batch of pods.
2. It sends a `BulkCreate` request over mTLS with the user, groups and pods.
3. The writer checks authorization and the namespace, prepares and validates each
   pod, encodes it, and commits one etcd transaction per chunk.
4. It returns a per-pod result and the prepared pod with its generated UID.
5. The executor updates the submitted-pod cache, creates services and ingresses
   through the API server, and reports events.
6. The API server watch sees the pods and kubelets start them.

#### Deployment and failure domains

- Stateless Deployment with two or more replicas. No leader election is needed:
  etcd compares make duplicate writes idempotent, and multiple replicas can race
  safely.
- The writer can be upgraded independently of the executor, with a versioned gRPC
  contract.
- If the writer is unavailable, the executor retries and then returns the lease or
  fails the batch; normal pod execution is otherwise unaffected.
- If the writer crashes mid-batch, completed transactions remain and the current
  transaction is atomic; retries are idempotent.
- If etcd or the API server is unavailable, the writer fails closed.
- The NetworkPolicy allows ingress only from the executor and egress only to etcd
  and the API server.

#### Security boundary

The etcd credential is held by one small service, not the executor. The executor's
service account needs no etcd access. The writer's service account needs only
`create subjectaccessreviews` and `get namespaces`. Its audit log and metrics are
separate from the executor's.

The design still bypasses admission, quota, PodSecurity and Kubernetes audit for
the created pods. The allowlist remains the security boundary.

#### Pros

- Isolation: the unsafe write path is a separate, auditable service.
- Least privilege: the executor never holds etcd credentials.
- Independent scaling, upgrade and rollback.
- Per-cluster enable and disable without redeploying the executor.
- Easier chaos testing, rate limiting and canary rollout.
- The extra gRPC hop is small compared with the etcd transaction.

#### Cons

- A new service, API contract, certificate rotation and NetworkPolicy to operate.
- Two failure domains; the executor and writer can disagree on a failed batch, so
  idempotency and reconciliation are mandatory.
- The writer is on the critical path and needs HA.
- More operational surface and a second thing to upgrade with Kubernetes.
- Still bypasses admission, quota and audit for the created pods.

**Use:** production shape if the experiment succeeds.

### 7.4 A versus B

| Criterion | A | B |
|---|---|---|
| Deployment unit | Executor | Separate Deployment |
| Network path | In-process | One mTLS gRPC hop |
| Etcd credential | Executor | Dedicated service |
| Blast radius | Whole executor | Contained service |
| Failure isolation | None | Writer can fail without stopping the executor |
| Per-namespace scoping | Not really; executor-wide flag | Allowlist in the service |
| Per-cluster toggle | Executor config | Deploy or scale the service |
| Upgrade coupling | Executor + writer together | Independent, versioned API |
| Audit and rate limiting | Mixed with executor | Separate |
| Latency | Lower | One small hop |
| Operational cost | Lower | Higher |
| Best for | Prototype | Production |

Design A answers "can this work, and how much faster is it?" Design B answers "can
we operate it safely?" Do not build B before A proves the gain.

## 8. Implementation plan

### 8.1 Package layout

```text
internal/podwriter/
  writer.go        batch orchestration and results
  prepare.go       defaults, metadata, status, node assignment
  policy.go        namespace and pod-shape allowlist
  encode.go        key and protobuf encoding
  etcd.go          client, transactions, retries
  authz.go         SubjectAccessReview cache
pkg/podwriterapi/  Design B protobuf contract
internal/executor/podwrite/
  client.go        Design B client
  writer.go        Design A adapter
cmd/pod-bulk-writer/
  main.go
```

### 8.2 Batch algorithm

1. Check namespaces and `create pods` authorization, cached.
2. Prepare and validate each pod; compute key and encoded value.
3. Chunk by `maxTxnOps / 2` and by bytes.
4. Commit one etcd transaction per chunk with
   `Compare(Version(key) == 0)` and `Put`.
5. On conflict, keep matching existing pods as `AlreadyExists`, report the rest,
   and retry the remainder (identifying existing pods after a failed compare
   requires a follow-up range read; a failed transaction commits nothing).
6. Map etcd errors to transient or fatal for the executor.

A compare and a put both count toward `--max-txn-ops`. With the default of 128, a
batch is about 64 pods, not 128. The 1.5 MiB request limit can reduce it further.

### 8.3 Object preparation

Apply the defaults needed by the allowlisted shape:

- `terminationGracePeriodSeconds=30`, `dnsPolicy=ClusterFirst`, scheduler name,
  security context, service links;
- `requests=limits` when only limits are set;
- container image pull policy, termination message path and policy;
- probe defaults and volume default modes;
- the default tolerations added by the `DefaultTolerationSeconds` admission
  plugin (`node.kubernetes.io/not-ready` and `unreachable`, NoExecute, 300s);
  without them, writer-created pods are evicted immediately instead of after the
  grace period;
- `status.phase=Pending` and `status.qosClass`;
- UID and creation timestamp; empty `resourceVersion`;
- `spec.nodeName` as the node **name** resolved from the scheduler's node
  selector value (a node ID — for non-`hostname` `nodeIdLabel` values, resolve
  via the same node inventory the executor already uses), then remove the selector.

The allowlist accepts regular containers only, no privileged/hostPath/hostNetwork,
the default service account, required CPU/memory requests and limits, and a small
set of volume types. Anything else is rejected.

Parity is checked in CI, not production: for each test pod, call the API server
with `dryRun=All`, run `prepare`, and compare with
`apiequality.Semantic.DeepEqual` after stripping server-set fields, notably
`managedFields`, or the test fails spuriously on every minor.

### 8.4 Encoding and transactions

```go
scheme := kubernetesScheme.Scheme
serializer := protobuf.NewSerializer(scheme, scheme)
pod.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
var buf bytes.Buffer
serializer.Encode(pod, &buf)
```

The value is `k8s\0` plus a `runtime.Unknown` protobuf envelope. The key is
`/registry/pods/<namespace>/<name>`. Only protobuf storage is supported; the writer
refuses to start on JSON storage or encryption at rest (an identity transformer
counts as off).

```go
txn := cli.Txn(ctx).
    If(compares...).Then(puts...)
resp, err := txn.Commit()
```

Use a per-batch timeout, bounded retries for transient errors, and no retry on
authorization failures. Cap in-flight transactions.

### 8.5 Wiring

Design A:

- add `PodWrite` config and build the writer in `application.go`;
- add a batch path in `submit.go`; track created pods in the submitted-pod cache;
- create services and ingresses with the returned UID;
- keep `SubmitPod` and `DeletePods` on the API path.

Design B:

- `cmd/pod-bulk-writer` serves `BulkCreate` over mTLS;
- the service holds the etcd credential and uses the shared core;
- the executor client sends batches and maps results into existing failure types;
- multiple replicas are safe because etcd compares make writes idempotent.

## 9. Cluster upgrades and component tracking

### 9.1 Component identity and inventory

Every deployed component carries the same identification labels:

- `app.kubernetes.io/name`: `armada-executor` or `pod-bulk-writer`;
- `app.kubernetes.io/version`: the build version;
- `armadaproject.io/component`: `executor` or `pod-writer`;
- `armadaproject.io/pod-write-mode`: `api` or `etcd`;
- `armadaproject.io/writer-protocol`: the gRPC protocol version for Design B;
- `armadaproject.io/k8s-minor`: the Kubernetes minor the component was built for.

Note: the existing Armada charts use the legacy `app:` label convention, not
`app.kubernetes.io/*`. Adopting these labels means relabeling the executor chart
(`deployment/executor`) at least, not just the new components.

Each process exports an info metric on startup:

```text
armada_component_info{
  component, version, git_sha, k8s_version,
  pod_write_mode, transport, writer_protocol
} = 1
```

The writer adds the etcd version, prefix, storage media type, encryption mode,
allowlist version and last canary result.

The executor reports its write mode and, for Design B, the writer version it is
connected to. Reuse the existing executor heartbeat or lease request for this; do
not add a second inventory API. Reusing the lease stream requires a `LeaseRequest`
proto field addition. The control plane and dashboards can then answer
"which clusters use the etcd writer, at what version, and is it healthy?" without
requiring `kubectl` access to every worker cluster.

If the existing cluster inventory does not expose component versions, extend it
with these fields rather than building a separate registry.

For operators without the control-plane view, the labels support direct commands:

```bash
kubectl get deploy -A -l armadaproject.io/component=pod-writer -o wide
kubectl get pods -A -l app.kubernetes.io/name=pod-bulk-writer \
  -o jsonpath='{range .items[*]}{.metadata.namespace}{"\t"}{.metadata.name}{"\t"}{.metadata.labels.app\.kubernetes\.io/version}{"\n"}{end}'
```

The label key contains dots and a slash; verify the jsonpath escaping against a
real cluster, or fall back to `-o custom-columns` or `--show-labels`.

### 9.2 Compatibility contract

Each component declares:

- the Kubernetes minor range it supports;
- the storage schema version it writes;
- the writer protocol version it speaks (Design B);
- the allowlist version.

The writer's startup probe checks the API server version, etcd version, storage
media type, encryption configuration and etcd prefix. It refuses to start on an
unsupported combination. This converts a silent upgrade break into a visible
failure.

The gRPC contract is versioned. During a rolling upgrade, the writer must support
the previous protocol version so the executor does not have to be upgraded in the
same window.

The writer's defaulting and validation are Kubernetes-version-specific. A new
Kubernetes minor is a new test target, not an assumption. The compatibility matrix
and the parity tests are the source of truth.

### 9.3 Upgrade order

The safe order is to remove the extra compatibility surface before upgrading the
control plane, then reintroduce it after the cluster is healthy.

For Design B:

1. Switch the executor to `mode: api` for the affected cluster, or reduce the
   etcd path to a canary namespace only.
2. Upgrade the worker cluster's control plane.
3. Upgrade `pod-bulk-writer` to the version matching the new Kubernetes minor.
4. Run the canary and the parity test.
5. Re-enable `mode: etcd` for the canary namespace, then the full cluster.

For Design A, the same sequence applies, but step 3 is an executor upgrade and
there is no separate writer deployment. The canary granularity is a whole cluster:
the flag is executor-wide (§7.2), so a canary namespace is only available in
Design B.

Switching to `mode: api` does not stop existing pods. It only changes how new
leases are written. The API path remains the fallback for the entire upgrade
window.

### 9.4 Rollout phases

| Phase | State | Gate |
|---|---|---|
| 0 | `mode: api`; no writer | Baseline healthy |
| 1 | Writer deployed but disabled | Inventory and health visible |
| 2 | `mode: etcd` for a canary namespace and queue (Design B; Design A canaries a whole cluster, see §9.3) | Canary create, read, status, delete all pass |
| 3 | One queue or a small percentage of jobs | Error rate and latency match the API path |
| 4 | Full cluster | No orphaned or duplicate pods; etcd within budget |
| 5 | Next cluster | All gates repeat per cluster |

The canary must verify the full lifecycle, not just the etcd write:

- the API server can read the pod;
- the pod appears in the executor informer;
- kubelet updates status;
- deletion works;
- services and ingresses get the correct owner UID.

The parity test compares the writer's prepared object with the API server's
`dryRun=All` response for the allowlisted shape. It runs before every rollout and
on every Kubernetes minor upgrade.

### 9.5 Rollback

- Set `mode: api` and restart the executor. New leases use the API path.
- Scale `pod-bulk-writer` to zero; no data migration is needed.
- Existing pods are unaffected.
- If bad pods were written, delete them through the API and reconcile.
- Keep the writer Deployment and logs for forensics; do not delete them as part of
  the rollback.

### 9.6 Upgrade checklist

- [ ] The target Kubernetes minor is in the writer's compatibility matrix.
- [ ] `dryRun=All` parity tests pass for that minor.
- [ ] Storage is protobuf, encryption is off or uses an identity transformer, and
      the etcd prefix is known.
- [ ] Etcd certificates and credentials are valid.
- [ ] Executor and writer protocol versions are compatible.
- [ ] `mode: api` was set before the control-plane upgrade.
- [ ] The canary passed create, read, status and delete.
- [ ] Component versions and modes are visible in the inventory and dashboard.
- [ ] Rollback to `mode: api` has been tested.

## 10. Risks and limitations

| Risk | Impact | Mitigation |
|---|---|---|
| Admission and webhooks bypassed | Privileged pods, policy violations | Dedicated cluster, strict allowlist |
| Quota and LimitRange bypassed | Over-admission | Do not enable on quota-managed namespaces |
| RBAC and audit bypassed | Unauthorized writes, no audit trail | `SubjectAccessReview`, writer audit log, funded security review |
| Etcd credential compromise | Cluster-wide pod compromise | Design B, restricted key range, network policy |
| Storage format mismatch | API server cannot decode objects | Phase 1 byte comparison; refuse unknown storage config |
| Upgrade breakage | Writer stops matching new defaults | Version matrix and CI parity tests |
| No managedFields on written pods | Server-side apply on these pods behaves differently | Document the limitation; check apply behavior in the parity test |
| Orphaned or duplicate pods | Stuck workloads | Idempotent transactions and reconciliation |
| No end-to-end gain | Wasted effort | Measure first; 2-3x gate |

Three technical points often get this wrong:

- **Watch cache:** the API server watches etcd and observes correctly encoded
  writes automatically. No cache warmer is needed. The risk is decode failure.
- **`resourceVersion`:** leave it empty. The API server derives it from the etcd
  revision; do not pre-compute it.
- **Status storms:** Volcano #5236 is about its own full-object status updates.
  The writer does not control kubelet status writes; measure that traffic
  separately.

## 11. Recommendation

1. **Do not build this as a general feature.** The control-plane bypass is too
   broad and the benefit is unproven.
2. **Measure first.** Do a phase decomposition and a
   write-path microbenchmark. If create and bind are under 30% of time to
   `Running`, stop.
3. **Prototype Design A only if the write path is dominant.** Use a narrow
   allowlist and a disposable cluster.
4. **Ship Design B only if the prototype clears the gate.** Target at least a 2-3x
   end-to-end improvement at 10k pods, with parity for the allowlisted shape.
5. **Treat it as an escape hatch.** Restrict it to dedicated, self-hosted clusters
   with a documented reduction in guarantees.

## 12. Open questions

- What is the target pod count, and what do the images look like?
- Is the cluster self-hosted with etcd access, or managed?
- Do target clusters rely on admission, quota or PodSecurity for these workloads?
- Who owns the upgrade matrix and the parity tests?

## References

- Kubernetes API conventions: <https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md>
- Kubernetes issue #9633, bulk creation: <https://github.com/kubernetes/kubernetes/issues/9633>
- KEP-5598, opportunistic batching: <https://github.com/kubernetes/enhancements/issues/5598>
- Kubernetes admission controllers: <https://kubernetes.io/docs/reference/access-authn-authz/admission-controllers/>
- etcd transactions: <https://etcd.io/docs/v3.5/learning/api/>
- Volcano tuning guide: <https://volcano.sh/docs/v1.16.0/userguide/user_guide_how_to_tune_volcano_performance/>
- Volcano #5236, etcd exhaustion: <https://github.com/volcano-sh/volcano/issues/5236>
- Volcano #3318, webhook timeouts (root cause: client-side throttling): <https://github.com/volcano-sh/volcano/issues/3318>
- YuniKorn Kubemark benchmark: <https://yunikorn.apache.org/docs/performance/evaluate_perf_function_with_kubemark/>
- YUNIKORN-3112: <https://issues.apache.org/jira/browse/YUNIKORN-3112>
