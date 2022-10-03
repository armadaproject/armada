# Preemption

In Kubernetes, scheduling refers to making sure that Pods are matched to Nodes so that the kubelet can run them. 
Preemption is the process of terminating Pods with lower Priority so that Pods with higher Priority can schedule on Nodes. 
Eviction is the process of terminating one or more Pods on Nodes.

## Prerequisites

Create one or more priority classes in the executor clusters.
All executor clusters should have the same priority classes with the same respective priorities.

## Configuration

To enable preemptive jobs, first enable preemption in Armada server:
```yaml
scheduling:
  preemption:
    enabled: true
```

Afterwards, configure priority classes which Armada server should support and their respective priorities:
```yaml
scheduling:
  preemption:
    priorityClasses:
      armada-preemptive-high-priority: 1000
      armada-preemptive-medium-priority: 100
      armada-preemptive-low-priority: 10
```

If you want to configure a default priority class which for all pods:
```yaml
scheduling:
  preemption:
    defaultPriorityClass: armada-preemptive-low-priority
```

In order to submit a preemptive job to Armada server, reference an existing priority class in the underlying job pod spec
by setting the field `priorityClassName`:
```yaml
podSpec:
  priorityClassName: armada-preemptive-high-priority
```

## Example

Example of a preemptive Armada job
```yaml
queue: queue-a
jobSetId: job-set-1
jobs:
  - priority: 1
    podSpec:
      priorityClassName: armada-preemptive-low-priority
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
              memory: 250Mi
              cpu: 1
            requests:
              memory: 250Mi
              cpu: 1
```

## References
* [Preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#preemption)
* [PriorityClass](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#priorityclass)
* [Pod priority](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/#pod-priority)
