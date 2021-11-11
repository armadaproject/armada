# User guide

Armada is a system for scheduling and running user jobs (e.g., a batch compute job for training a machine learning model) over one or more Kubernetes clusters. In particular, Armada stores jobs submitted to it in queues and decides in what order, and on which cluster, jobs should run. Armada jobs are represented by Kubernetes pod specifications (podspecs) and Armada handles creating, running, and removing containers as necessary for each job. This document is meant to be an overview of Armada for new users. A basic understanding of Docker and Kubernetes is required; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

The Armada workflow is:

1. Create a job specification, which is a Kubernetes podspec with a few additional metadata fields specific to Armada.
2. Submit the job specification to one of Armada's job queues.

The remainder of this document is divided into two sections, corresponding to the main abstractions that make up an Armada cluster:

* jobs and job sets, and
* queues and scheduling.

## Jobs and job sets

A job is the most basic unit of work in Armada, and is represented by a Kubernetes podspec with additional metadata fields specific to Armada. For example, a job that sleeps for 60 seconds could be represented by the following yaml file.

```yaml
queue: test
priority: 0
jobSetId: set1
podSpec:
  terminationGracePeriodSeconds: 0
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

- `queue`: which of the available job queues the job should be submitted to. 
- `priority`: the job priority (lower values indicate higher priority).
- `jobSetId`: jobs with the same `jobSetId` can be followed and cancelled in a single operation. The `jobSetId` has no impact on scheduling.

Queues and scheduling is explained in more detail below.

Resource requests and limits must be equal. Armada does not yet support limit > request.

All containers part of the same podspec will be run on the same node (a physical or virtual machine), i.e., the amount of CPU and memory the above job could consume is limited by what the nodes that make up the clusters are equipped with. To get around this limitation, jobs need to specify several podspecs, each of which may be scheduled on different nodes within a Kubernetes cluster. For example:

```yaml
queue: test
priority: 0
jobSetId: multi-node-set1
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
  - terminationGracePeriodSeconds: 0
    restartPolicy: Never
    containers:
      ... 
```

All pods part of the same job will be run simultaneously and within a single Kubernetes cluster. If any of the pods that make up the job fails to start within a certain timeout (specified by the `stuckPodExpiry` parameter), the entire job is cancelled and all pods belonging to it removed. Armada may attempt to re-schedule jobs experiencing transient issues. Events relating to a multi-node job contain a `podNumber` identifier, corresponding to the index of pod in the `podSpecs` list, that the event refers to.

Jobs are submitted using either the `armadactl` command-line utility, with `armadactl submit <jobspec.yaml>`, or using the web API directly.

### Job options

Here, we give a complete example of an Armada jobspec with all available parameters.

```yaml
queue: example                            # 1.
jobSetId: test                            # 2.
jobs:
  - priority: 1000                        # 3.
    namespace: example                    # 4.
    clientId: 12345                       # 5.
    labels:                               # 6.
      example-label: "value"
    annotations:                          # 7.
      example-annotation: "value"
    ingress:                              # 8.
      - type: NodePort
        ports:
          - 5050
    podSpecs:                             # 9.
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

Using this format, it is possible to submit a job set composed of several jobs. The meaning of each field is:

1. The queue this job will be submitted to.
2. Name of the job set this job belongs to.
3. Relative priority of the job.
4. The namespace that the pods part of this job will be created in (the `default` namespace if not specified).
5. An optional ID that can be set to ensure that jobs are not duplicated, e.g., in case of certain network failures. Armada automatically discards any jobs submitted with a `clientId` equal to that of an existing job.
6. List of labels that are added to all pods created as part of this job..
7. List annotations that are added to all pods created as part of this job.
8. List of ports that are exposed with the specified ingress type. The ingress only exposes ports for pods that also expose the corresponding port via the `containerPort` setting.
9. List of podspecs that make up the job; see the [Kubernetes documentation](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.19/) for an overview of the available parameters.

## Queues and scheduling

An Armada cluster is configured with one or more queues, to which jobs may be submitted. For example, each user, team, or project may be given separate queues. Both jobs and queues have an integer-valued priority associated with it, and jobs are scheduled according to the following principles:

- The fraction of resources assigned to each queue is proportional to its priority. For example, if there are two queues with priority 1 and 10, respectively, the first queue will, on average, be assigned close to an order of magnitude more resources than the second.
- Within each queue, the next job to be scheduled is the job with lowest priority that would fit within one of the underlying Kubernetes clusters.

Note that low-priority jobs (i.e., having a high priority value) may be scheduled before higher-priority jobs (i.e., with a lower priority value) if there are insufficient resources to run the high-priority job within any of the Kubernetes clusters and the lower-priority job requires fewer resources.

### Permissions

Armada allows for setting user and group permissions for each queue via the `owners` and `groupOwners` options, respectively. Job set events can be seen by all users with `watch_all_events` permissions.

If the `kubernetes.impersonateUsers` flag is set, Armada will create pods in Kubernetes under the username of the job owner, which will enforce Kubernetes permissions and limit access to namespaces.

### Resource limits

Each queue can have resource limits attached to it, which limit the resources consumed by the jobs from the queue to some percentage of all available resources at each point of time. For example, this prevents users from submitting long-running jobs during a period with low utilization that consume all resources in the cluster.
