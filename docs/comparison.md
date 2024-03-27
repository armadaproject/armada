# Comparison

To support the throughout we need, the Armada scheduler stores jobs in a specialised high-throughput storage layer. We achieve much higher throughput than etcd by sacrificing timeliness, i.e., there may be some lag before changes applied in Armada are visible to all of Armada's components. This storage layer is built on Pulsar and Postrges.

## On etcd

etcd is a strongly consistent distributed key-value store. Formally, etcd implements total order broadcast via the Raft protocol, and key-value storage is implemented on top of total order broadcast. As a result, etcd provides excellent consistency, durability, and availability guarantees, but low throughput.

If we were to use etcd as the primary database for storing jobs, all submitted jobs would need to be written into etcd. Because etcd has low throughput, we would need to limit the rate at which jobs can be submitted.

Exactly what rate we would be able to support depends on how set it up; etcd has a fanout design, such that the load depends on the number of Kubernetes components interested in each etcd key. But the throughout would be orders of magnitude less than what we can do with Armada now.

Further, etcd does not have good ways of protecting itself and is quite fragile. As a result, submitting jobs directly to etcd would risk overloading etcd. If etcd becomes overloaded, the system locks up and requires human intervention. As a side note, Armada currently monitors etcd health to avoid overloading it.

These limitations of etcd are typically not an issue when using Kubernetes to run long-lived services (which is what Kubernetes was originally designed for) since pods may last for days or weeks. However, when running batch jobs, pods may only last for a few minutes, significantly increasing pod churn, at which point etcd throughput becomes a problem.

## Alternatives proposed in the community

Armada is not the only (or first) attempt to run batch workloads on Kubernetes. In particular, the Volcano, YuniKorn, Kueue, and Fluence projects are of note. These all have in common that they introduce a notion of job queues and fair sharing of resources between them (with the exception of Fluence). Here, we give a brief overview.

### Volcano

A replacement of kube-scheduler with added features. Volcano introduces several custom Kubernetes objects – so-called custom resource definitions (CRDs) – including a Volcano PodGroup, used to represent distributed jobs that should be gang scheduled, and a Volcano Job, which is a extension of the standard Kubernetes job definition that adds, e.g., a field to specify which queue the job belongs to. Volcano is a pod that runs in a Kubernetes cluster that knows how to schedule both regular pods and these special Volcano objects. These objects are submitted directly to the standard Kubernetes API and are stored in etcd.

The Volcano scheduler does not support all of the features of kube-scheduler and can thus not entirely replace it. Running the Volcano scheduler and kube-scheduler concurrently is possible but not ideal since the two schedulers would fight each other for resources.

Because Volcano defines its own CRDs and scheduler, using Volcano may result in not being able to make use of improvements made to standard K8s. For the subset of K8s that Volcano supports, there's some lag between K8s features becoming available and Volcano supporting them; since Volcano uses its own scheduler and CRDs, it has to be updated to support new features.

### YuniKorn

Like Volcano, YuniKorn (YK) is a replacement of kube-scheduler with added features. Unlike Volcano, YK does not primarily rely on CRDs. Instead, it uses labels and annotations with special meaning to indicate, e.g., which queue a pod belongs to. Such labels/annotations are also used to connect the pods that make up a distributed job; all pods with the same "applicationId" annotation are considered to be part of the same job. This approach avoids the need to define non-standard objects. Like with Volcano, resources (e.g., pods) are submitted directly to Kubernetes and YK relies on etcd for storage (although YK is less tightly coupled to etcd than Volcano and Kueue).

YK is built by Cloudera. It's essentially an attempt to move YARN users onto K8s and hence YK attempts to replicate YARN features within K8s. YK is implemented as a series of plugins for kube-scheduler. However, the plugins are so extensive as to essentially be a replacement of kube-scheduler. The goal of YK is to provide all of the features of kube-scheduler but to also include YARN-style queuing and fair share, and advanced scheduling features like gang-scheduling and Spark support. However, it doesn't yet support all features of kube-scheduler. For example, they don't support preemption.

YK has a complicated hierarchical queuing structure inherited from YARN. They need to support this in addition to supporting all regular k8s features. Hence, they're trying to solve a much more difficult problem than we do with Armada.

### Kueue

Unlike Volcano and YuniKorn, Kueue is not a replacement of kube-scheduler. Instead, it's a replacement of the standard Kubernetes job controller. A Kubernetes job is the combination of a pod spec and associated instructions, e.g., for how that pod should be run, and how many times it should be retried. The job controller is responsible for creating pods from the job that are then scheduled by the Kubernetes scheduler (which could be kube-scheduler or, e.g., YuniKorn). The Kueue project goes to great lengths to do things in a Kubernetes-native way, e.g., by suggesting changes to the Kubernetes job definition via the Kubernetes Extension Proposal process. Like Volcano and YuniKorn, resources are submitted directly to Kubernetes and are stored in etcd.

Regardless of it Kueue is used in any way on the calc farm, the Kubernetes improvements proposed by the Kueue project are interesting and we should monitor and make use of these where possible.

### Fluence

Is a replacement of kube-scheduler for large-scale HPC/MPI-style batch jobs written by Lawrence Livermore National Labs (LLNL) to enable moving some of their workloads onto Kubernetes. Fluence is designed to bring optimisations frequently used in the HPC community, e.g., workload-aware bin packing and task placement, to Kubernetes.

Fluence is interesting since it attempts to bridge the gap between HPC and Kubernetes. However, Fluence is a relatively new project in the prototype stage and, as a result, there is not much information available. We should monitor this project to consider whether it can be used to replace kube-scheduler on the calc farm.

Fluence does not yet come with multi-user queuing and fair sharing of resources. However, this would not be an issue for us since we handle queuing and fair share within the Armada layer.

## Conclusion

As it stands, it would not be possible to entirely replace Armada with any of Volcano, YuniKorn, or Kueue. These schedulers all introduce interesting features. However, none provides a solution to problems 1. and 2. stated above. These systems are all designed to run within a single Kubernetes scheduler and rely on etcd for storage. There have been attempts to operate Volcano and YuniKorn in a multi-cluster manner, e.g., using a federated approach, where clusters move jobs between themselves, but there is no active work on this. YuniKorn could potentially be used with another storage layer than etcd (to overcome its throughput limitations), but designing, building, and retrofitting a high-throughput storage layer on YuniKorn would likely be challenging, since YuniKorn is built with the assumption of access to a strongly consistent database.

It may however be possible to replace kube-scheduler with either Volcano, YK, or Fluence. Kueue is less promising, since it (so far) only introduces queuing and resource sharing, which in our case must handled in a manner that accounts for jobs and resources across clusters (which Kueue does not do). In particular, YuniKorn and Volcano are promising since they add support for, e.g., gang scheduling. However, neither is necessary for basic distributed jobs, since Armada can already be made to ensure a set of pods can be scheduled at the same time. Further, neither is necessary for preemption, since kube-scheduler already supports it.

- Volcano: deviates too much from the Kubernetes community.
- YuniKorn: does not support preemption. But we may be able to extract pieces of functionality useful to us.
- Kueue: only adds queuing and fair share, and implements these in a manner not compatible with multi-cluster operation.
- Fluence: only at the prototype stage.
