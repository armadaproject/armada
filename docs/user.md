# User guide

This document is meant to be a guide for new users of how to create and submit Jobs to Armada. Because Armada jobs are run as containers in Kubernetes, a basic understanding of Docker and Kubernetes is required; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

The guide is deliberately short. For more information, see:

- [System overview](./design.md)

The Armada workflow is:

1. Create a job specification, which is a Kubernetes podspec with additional metadata fields specific to Armada.
2. Submit the job specification to one of Armada's job queues.

To create a job that sleeps for 60 seconds, start by creating a yaml file with the following contents:

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

Resource requests and limits must be equal. Armada does not yet support limit > request.

Now, the job can be submitted to Armada using the `armadactl` command-line utility. In particular, run

`armadactl submit <jobspec.yaml>`,

where `<jobspec.yaml>` is the path of the file containing the jobspec. Armada automatically handles creating and running the necessary containers. For more examples, including of how to submit jobs spanning multiple nodes, see the [system overview](./design.md).