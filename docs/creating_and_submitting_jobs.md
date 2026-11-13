# Creating and submitting jobs
- [Creating and submitting jobs](#creating-and-submitting-jobs)
  - [Armada workflow](#armada-workflow)
  - [Submitting preemptive jobs](#submitting-preemptive-jobs)
    - [Example: Creating and submitting an Armada job](#example-creating-and-submitting-an-armada-job)
  - [Job parameters](#job-parameters)

This document is meant to be a guide for new users of how to create and submit Jobs to Armada. Before you can create and submit jobs, make sure you have a basic understanding of Docker and Kubernetes. This is because Armada jobs are run as containers in Kubernetes. To learn more, see:

* [Docker overview](https://docs.docker.com/get-started/overview/)
* [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

For more information about the design of Armada (for example, how jobs are prioritised), see [System overview](./system_overview.md).

## Armada workflow

The Armada workflow is:

1. Create a job specification, which is a Kubernetes podspec with additional metadata fields specific to Armada.
2. Submit the job specification to one of Armada's job queues.

To create a job that sleeps for 60 seconds, start by creating a yaml file with the following contents:

```yaml
queue: test
jobSetId: set1
jobs:
  - priority: 0
    podSpecs:
      - terminationGracePeriodSeconds: 0
        restartPolicy: Never
        containers:
          - name: sleep
            imagePullPolicy: IfNotPresent
            image: busybox:latest
            args:
              - sleep
              - 60s
            resources:
              limits:
                memory: 64Mi
                cpu: 150m
              requests:
                memory: 64Mi
                cpu: 150m
```

In the above yaml snippet, `podSpec` is a Kubernetes podspec, which consists of one or more containers that contain the user code to be run. In addition, the job specification (jobspec) contains metadata fields specific to Armada:

* `queue`: which of the available job queues the job should be submitted to. 
* `priority`: the job priority (lower values indicate higher priority).
* `jobSetId`: jobs with the same `jobSetId` can be followed and cancelled in a single operation. The `jobSetId` has no impact on scheduling.

Resource requests and limits must be equal. Armada does not yet support limit > request.

You can submit the job to Armada using the `armadactl` command-line utility (or alternatively via Armada's gRPC or REST API). In particular, run `armadactl submit <jobspec.yaml>`, where `<jobspec.yaml>` is the path of the file containing the jobspec. Armada automatically handles creating and running the necessary containers.

## Submitting preemptive jobs

Armada supports submitting preemptive jobs, i.e. jobs which can preempt other lower priority jobs when there aren't enough
resources to run the preemptive job.

For scheduling preemptive jobs, Armada relies on the `kube-scheduler` component of Kubernetes for the actual preemption and scheduling the underlying pods.

To learn more about preemption in Kubernetes, [see the official docs](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/).

To use priority and preemption in Armada:

1. Add one or more [PriorityClasses](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#priorityclass).
2. Create Armada jobs with [priorityClassName](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#pod-priority) in the job's `podSpec` set to one of the existing PriorityClasses.

### Example: Creating and submitting an Armada job

1. Create a Kubernetes PriorityClass:

```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: armada-example-priority-class
value: 10
preemptionPolicy: PreemptLowerPriority
globalDefault: false
description: "Example priority class for preemptive Armada jobs."
```
```bash
$ kubectl apply -f ./docs/quickstart/priority-class-example.yaml
```

2. Apply an Armada job which references the PriorityClass in the field `priorityClassName`:

```yaml
queue: queue-a
jobSetId: job-set-1
jobs:
  - priority: 1
    podSpec:
      priorityClassName: armada-example-priority-class
      terminationGracePeriodSeconds: 0
      restartPolicy: Never
      containers:
        - name: sleeper
          image: alpine:latest
          command:
            - sh
          args:
            - -c
            - sleep $(( (RANDOM % 60) + 10 ))
          resources:
            limits:
              memory: 100Mi
              cpu: 100m
            requests:
              memory: 100Mi
              cpu: 100m
```
```bash
$ armadactl submit ./docs/quickstart/job-queue-a-preemptive.yaml
```

## Job parameters

Here, we give a complete example of an Armada jobspec with all available parameters.

```yaml
queue: example                            
jobSetId: test                            
jobs:
  - priority: 1000                        
    namespace: example                    
    clientId: 12345                       
    labels:                               
      example-label: "value"
    annotations:                          
      example-annotation: "value"
    ingress:                              
      - type: NodePort
        ports:
          - 5050
    podSpecs:                             
      - containers:
        name: app
        imagePullPolicy: IfNotPresent
        image: vad1mo/hello-world-rest:latest
        securityContext:
          runAsUser: 1000
        resources:
          limits:
            memory: 1Gi
            cpu: 1
          requests:
            memory: 1Gi
            cpu: 1
        ports:
          - containerPort: 5050
            protocol: TCP
            name: http
```

Using this format, you can submit a job set composed of several jobs. The meaning of each field is as follows.

* `queue`: the queue this job will be submitted to
* `jobSetId`: the name of the job set this job belongs to
* `priority`: the relative priority of the job
* `namespace`: the namespace that the pod's part of this job will be created in (the `default` namespace, if not specified)
* `clientId`: an optional ID that can be set to ensure that jobs are not duplicated, for example, in case of certain network failures. Armada automatically discards any jobs submitted with a `clientId` equal to that of an existing job.
* `labels`: the list of labels that are added to all pods created as part of this job
* `annotations`: the list of annotations that are added to all pods created as part of this job
* `ingress`: the list of ports that are exposed with the specified ingress type. The ingress only exposes ports for pods that also expose the corresponding port via the `containerPort` setting.
* `podSpecs`: the list of podspecs that make up the job; for an overview of the available parameters, [see the Kubernetes documentation](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.35/).

## Environment variables in the containers

The executor sets these environment variables in every container and init container of a job. A value that you set in the pod spec for the same name stays.

| Variable | Value |
| --- | --- |
| `ARMADA_JOB_ID` | The job ID. It is the same for all runs of the job. |
| `ARMADA_JOB_RUN_ID` | The ID of the current run. Each run of the job has a different ID. |
| `ARMADA_QUEUE` | The queue of the job. |
| `ARMADA_JOB_SET_ID` | The job set of the job. |

Gang jobs also get `ARMADA_GANG_ID` and `ARMADA_GANG_CARDINALITY`. When the gang has a node uniformity label, they also get `ARMADA_GANG_NODE_UNIFORMITY_LABEL_NAME` and `ARMADA_GANG_NODE_UNIFORMITY_LABEL_VALUE`.

Use `ARMADA_JOB_ID` as a stable key for a job, for example for checkpoints. Use `ARMADA_JOB_RUN_ID` to keep the output of each run apart.

## Submitter trace context

### Configuration

The server can copy W3C trace context into each new job's annotations. This feature is
opt-in and disabled by default, independently of `observability.enabled`:

```yaml
submission:
  traceContext:
    enabled: false  # Set true to resolve and propagate submitter context.
    aliasAnnotationPrefixes: []  # For example: [otel.example.com/]
    maxTraceStateBytes: 512
    maxBaggageBytes: 8192
```

### Resolution order

For each job, the server chooses the first valid carrier in this order:

1. `otel.armadaproject.io/{traceparent,tracestate,baggage}` on the job item.
2. The same three suffixes under each configured alias prefix, in order.
3. Incoming gRPC metadata, provided it contains a valid W3C `traceparent`. With server
   tracing enabled, the server span in that incoming trace becomes the resolved parent.

A malformed `traceparent` or oversized `tracestate`/`baggage` rejects that carrier and
continues to the next source. Zero byte limits use the defaults shown above. The three
fields always come from one source: missing state or baggage in a chosen fallback clears
any stale canonical value. Alias annotations are preserved. If no source is valid, no
context is added. A locally created server span alone does not establish submitter context.
Existing canonical annotations are preserved when valid, and all annotations retain their
existing behavior when the feature is disabled. Duplicate submissions keep the original job.

### Baggage and annotation limits

Baggage is propagated by default within the byte limit. It travels with job annotations
and may subsequently reach pod environments; do not put secrets in it. For batches from
different traces, set canonical annotations on each item. Jobs lacking item context use
the request context. Resolution works even when server span export is disabled.
Gateways and service meshes may add baggage; operators should audit incoming baggage
before enabling propagation. Size limits do not filter sensitive contents.

Resolution runs after submission validation. Its byte limits apply to the carrier, not
the total size of existing annotations. Copying alias or gRPC fields consumes additional
annotation space. With the feature enabled, canonical trace fields are literal: unlike
ordinary annotations, `{JobId}` in baggage is not expanded during conversion.

### Resolution metrics

`armada_trace_context_source_total{source="annotation|alias|grpc|none|invalid"}` records
resolution while the feature is enabled. Each new job increments its selected source (or
`none`); `invalid` additionally counts each rejected carrier encountered before selection.
Consequently, the sum across source labels is not a job count. Duplicate jobs are excluded.

### Submission spans

When both `submission.traceContext.enabled` and `observability.enabled` are true, each
accepted new job with sampled context emits an `armada.job.submit` root after successful
publication. The trace ID is the job ULID's 16 bytes; the span ID is its last eight bytes.
The span covers request handling and carries the queue, job set and job ID. Its single
link targets the resolved submitter, with `armada.trace_context.source` attribution and
full submitter tracestate. The root retains only the `ot` tracestate entry. Sampling
follows the resolved submitter rather than applying the server's ratio again.
Duplicates, failed submissions and jobs without sampled context emit no job roots.
The server installs the job ID and sampling adapters only when submission tracing is
explicitly enabled. Other components and a server with the feature disabled keep the
SDK's ordinary ID generator and configured sampler.

### Sampling and export caveats

ULID-derived trace IDs are deterministic lookup keys, not uniformly random identifiers.
Their first six bytes encode the millisecond timestamp, and IDs generated within the
same millisecond use monotonic random increments. Job trace IDs and root span IDs in a
batch are therefore correlated. The server honors the resolved submitter's sampling
instead of applying trace-ID-ratio sampling to these IDs.

Downstream samplers that assume independent, uniformly random trace IDs can make
correlated decisions for these jobs. Avoid additional trace-ID-ratio or probabilistic
sampling based on that assumption; honor the propagated decision or use a policy that
explicitly handles ULID-derived IDs. Hashing job IDs would change the specified
job-ID-to-trace-ID mapping and requires a coordinated design change.
Export uses the existing bounded, nonblocking batch queue; collector failures do not
fail submission, and spans can be dropped on backpressure. A ULID with an all-zero
span-ID suffix cannot form a valid W3C span and is skipped with a warning.
Automatic client injection and executor environment/transport changes are separate additions.
