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

Enable context propagation with `submission.traceContext.enabled`. It defaults to off
and works independently of span export:

```yaml
submission:
  traceContext:
    enabled: false  # Set true to propagate submitter context.
    propagateGrpcBaggage: false  # Separate opt-in for request baggage.
    aliasAnnotationPrefixes: []  # For example: [otel.example.com/]
    maxTraceStateBytes: 512
    maxBaggageBytes: 8192
```

### Choosing the context

For each new job, the server uses the first valid context from:

1. `otel.armadaproject.io/{traceparent,tracestate,baggage}` annotations on the item.
2. The same keys under each configured alias prefix, in order.
3. Incoming gRPC metadata containing a valid W3C `traceparent`.

The selected fields are stored together under the `otel.armadaproject.io/` prefix;
alias annotations are retained. Invalid traceparents or fields exceeding the byte limits
cause the server to try the next source. Zero limits use the defaults shown above.
No valid source means no job trace. Duplicate submissions keep their original context.
For batches containing jobs from different traces, set context annotations on each item.

### Baggage

Incoming gRPC baggage is omitted unless `propagateGrpcBaggage: true` is set. Traceparent
and tracestate still propagate when it is off, regardless of the ignored baggage's size.
Baggage explicitly supplied in canonical or alias annotations is unaffected by this flag.
Forwarded baggage must fit within `maxBaggageBytes`.

Baggage may eventually reach job containers. Audit baggage added by gateways or service
meshes before enabling request baggage; do not include secrets. Size limits do not filter
sensitive contents or limit the total size of a job's annotations. When this feature is
enabled, canonical trace fields are literal: `{JobId}` in baggage is not expanded.

### Submission spans

With both context propagation and `observability.enabled` on, each accepted new job
with sampled context gets an `armada.job.submit` root span after successful publication.
Its trace ID is the job ULID's 16 bytes; its span ID is the last eight bytes. It covers
request handling and includes the queue, job set and job ID.

The root links to the selected submitter span, with `armada.trace_context.source`
attribution. For gRPC context with server tracing enabled, it links to the server span
in that incoming trace. The link keeps the full tracestate; the job root keeps only `ot`.
Sampling follows the selected context. Duplicates, failed submissions and jobs without
sampled context emit no job roots.

ULID-derived IDs are correlated within a batch. Avoid downstream ratio or probabilistic
sampling that assumes independent, uniformly random trace IDs; honor the propagated
decision or use a policy that supports ULID-derived IDs. Export failures do not fail
submission, but a full export queue can drop spans. A job ID with an all-zero span-ID
suffix cannot form a valid W3C span and is skipped with a warning.

### Metrics

`armada_trace_context_source_total{source="annotation|alias|grpc|none|invalid"}` records
context resolution for new jobs while the feature is enabled. Each job increments its
selected source or `none`; `invalid` additionally counts rejected sources, so summing
all labels does not give a job count. Duplicates are excluded.

Executor propagation and automatic client injection are separate follow-up changes.
