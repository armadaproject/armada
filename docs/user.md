# User guide

Armada is a cluster manager, and is responsible for scheduling and running user jobs (e.g., a batch compute job for training a machine learning model) over one or more Kubernetes clusters. This document is meant to be an overview of Armada for new users. A basic understanding of Docker and Kubernetes is required; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

The Armada workflow is:

1. Create a job specification, which is a Kubernetes pod specification (podspec) with a few additional metadata fields specific to Armada.
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

For more details on the options available for Armada jobs, see [here](job.md).

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
