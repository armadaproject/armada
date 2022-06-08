# Scheduler

This document outlines the proposed Armada scheduler; the existing scheduler will be deprecated in favour of the one described here. This is work in progress; everything herein is subject to change.

The purpose this new scheduler is to improve fairness and throughput, to more effectively schedule large and distributed jobs, and to provide a way of running workloads with dynamically changing resource requirements (e.g., Spark clusters) via Armada.

## Armada architecture

Armada consists of the Armada server and one or more Kubernetes worker clusters. Each worker cluster consists of a large number of nodes responsible for executing tasks and is managed by a process referred to as the Armada executor. Jobs are submitted to the Armada server, where they are queued until being handed off to an executor, which assigns them to nodes.

Armada uses a two-stage scheduling approach, consisting of a top-level scheduler (which is part of the server), and a set of per-cluster schedulers- one per executor. The top-level scheduler is responsible for assigning jobs to executors and the per-cluster schedulers are responsible for assigning those jobs to nodes. This two-stage approach allows the scheduler to manage very large numbers of nodes. We refer to the top-level scheduler simply as "the scheduler". The job of the scheduler is to decide in what order to assigns jobs to worker clusters.

### Jobs

Each Armada job consists of a bag of Kubernetes object (e.g., pods, services, and ingresses) with a shared life cycle; all objects are created at the start of the job and are destroyed when the job has finished. Each job is a member of a queue (e.g., corresponding to a particular user) and a job set (a per-queue logical grouping of jobs that can be managed as a unit), and has a priority associated with it, i.e., each job has associated with it a tuple `(queue, jobSet, priority)`. Each job is assigned to a cluster as a unit, i.e., all resources that make up a job are created within the same cluster.

Further, for each submitted job, Armada allows users to optionally specify job lifetime, i.e., an amount of time after which the job may be preempted. Such jobs are run opportunistically since they can be preempted if holding up a higher-priority job. Note that a lifetime of 0 corresponds to a what is usually referred to as a preemtible job. Armada may preempt jobs older than their specified lifetime when a queue has exceeded its fair share (discussed later).

Armada jobs are created by submitting a job specification to the Armada server, either via client libraries or via the `armadactl` command-line-utility. In this way, users can submit jobs to Armada directly. However, users may also submit workflows to higher-level schedulers, such as [Apache AirFlow](https://airflow.apache.org/), which in turn submit jobs to Armada.

### Distributed jobs

Distributed jobs are represented as [PodGroups](https://github.com/kubernetes-sigs/scheduler-plugins/blob/master/kep/42-podgroup-coscheduling/README.md) in Armada. PodGroups are designed to support modern distributed machine learning jobs, which may consist of one or more coordinator nodes, elastic pools of worker nodes, and long-lived auxiliary services, e.g., for recording profiling information. Each PodGroup is a bag of cooperating pods and the pods that make up a `PodGroup` are scheduled jointly using a gang-scheduling algorithm.

Similar to [kube-batch](https://github.com/kubernetes-sigs/kube-batch) and [Volcano](https://volcano.sh/en/docs/podgroup/), the gang-scheduling algorithm is implemented as a [custom Kubernetes scheduler](https://kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers/). Further, gang-scheduling is based on reserving entire nodes for the pods that make up a distributed job. 

The pods that make up each PodGroup are grouped into sub-groups consisting of identical pods (i.e., are composed of the same images run with the same arguments). Each such sub-group specifies a minimum and maximum number of pods, and the sub-group is schedulable when the minimum number of pods can be created. If additional resources are available, pods are created up to the maximum (i.e., sub-groups are elastic). The PodGroup is schedulable when all sub-groups are schedulable.

#### Preemption

Distributed jobs may be preempted both in part and entirely. In particular, Armada will first preempt any pods above the minimum required for the job (as specified by the PodGroup). These pods may later be re-created if additional resources become available (i.e., these jobs are elastic). If not sufficient, Armada may preempt additional pods. If the number of running pods drops below the minimum for some some-group, all remaining pods are immediately preempted.

#### Topology-aware scheduling

For some classes of distributed workloads (e.g., distributed learning), performance is heavily influenced by inter-pod latency. Hence, Armada allows users to request pods to be scheduled in close proximity within the network. Specifically, cluster networks are typically configured as fat trees (sometimes referred to as [Clos networks](https://en.wikipedia.org/wiki/Clos_network)), and Armada tries to minimise the maximum number of hops between any two pods within each sub-group. Users may also constrain the maximum number of such hops. This feature is implemented in a manner similar to Kubernetes [pod topology spread constraints](https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/).

### Elasticity and dynamic resource creation

Armada supports workflows that require a dynamically changing amount of resources (e.g., [Apache Spark](https://spark.apache.org/) clusters and AirFlow jobs) using the following two approaches:

- The workflow is submitted to a higher-level scheduler (e.g., Apache AirFlow) that dynamically submits jobs to Armada, monitors their status, and makes decisions on which jobs to submit next.
- An "operator" job is submitted to Armada (e.g., corresponding to a Spark cluster manager) that can request additional resources from Armada (e.g., to run additional pods) by submitting further Armada jobs and can release resources by cancelling jobs. The operator may do so either via the Armada job submission API (in which case the operator has to be Armada-aware) or via a special set of Kubernets API endpoints that translate resource creation and deletion commands into Armada job submissions and cancellations, respectively (in which case the operator does not need to be Armada-aware).

In this way, Armada is in control of all resource creation and can manage cluster resources effectively.

## Scheduler architecture

At a high level, scheduling consists of:

1. Global prioritisation
2. Cluster assignment
3. Node assignment

The first two stages take place in the server and the third in the executor. We explain the first two stages here and the third stage in a later section. At any one time, each job is in one of the following three stages:

1. Queued
2. Pending
3. Running

Each job that is submitted is part of one of the `N` queues managed by the scheduler and is in the queued stage. Jobs within each queue are ordered by the priorities set by the user but are not globally ordered; the scheduler is responsible for establishing such a global ordering. Jobs are moved from the queued stage into the pending stage one at a time and jobs in the pending stage are assigned to worker clusters on a first-in-first-out basis. Hence, all jobs in the pending stage are ordered and the global prioritisation stage consists of deciding in what order jobs should be moved from the queued to pending stage. Within each queue, jobs are moved from the queued to pending stage strictly in order of priority, i.e., for each queue, the next job to be moved into the pending stage is always the highest priority job in that queue.

The cluster assignment stage consists of assigning jobs to worker clusters. Denote by

`j_1, j_2, ...`

the 1st and 2nd job to enter the pending stage and so on. Normally, jobs are assigned to clusters in order, i.e., `j_i` is assigned before `j_2` and so on. However, if some job cannot be scheduled (e.g., because insufficient resources are available), a lower-priority job may be assigned ahead of a higher-priority job if doing so does not delay assigning the higher-priority job. For example, `j_2` may be assigned to a cluster ahead of `j_1`. This is an optimisation to improve utilisation and is typically referred to as backfilling (see, e.g., [this page](http://docs.adaptivecomputing.com/maui/8.2backfill.php) for some background). For example, backfilling may be possible if a higher-priority job is waiting for GPU to become available and a lower-priority job requests only CPU and RAM. 

Each executor assigns jobs to nodes according to a bin-packing procedure, where jobs are assigned to nodes to minimize the number of nodes required. This is to make room for large incoming jobs. Any jobs that the executor is not able to assign to nodes are returned to the scheduler. For distributed jobs, the executor is responsible for gang-scheduling, i.e., creating all required resources simultaneously.

## Scheduler metrics

We measure scheduler quality quantitatively by

- scheduler throughput, i.e., the average number of jobs scheduled per second,
- submission latency, i.e., the time from job submission to it being considered for scheduling, and
- cluster utilisation, i.e., the average fraction of cluster resources (CPU, RAM, and accelerators) that are utilised.

Armada is designed to schedule batch workloads over systems composed of up to 1 million cores when each job takes, on average, 10 minutes or more to complete, and consumes at least 1 core. Hence, to keep the cluster full, Armada must be able to schedule at least

`1 000 000 / (60 * 10) = 1666.66...`

jobs per second. At this throughput, filling an empty cluster would take 10 minutes. 

The 99-th percentile latency from job submission to the job being available for scheduling should be at most 1 minute (i.e., one tenth of the average job runtime).

Overall average cluster utilisation should be at least 90%. We do not target 100% to allow for making room for scheduling large jobs.

In addition, we consider the following qualitative metrics:

- fairness and
- ability to schedule large jobs, e.g., jobs consuming one or more entire nodes.

Note that the metrics given here are in tension. For example, scheduling a large job may require draining a cluster partially to make space, thus reducing utilisation.

## Resource usage and fairness

The notions of resource usage, flow, and fair share are central to the scheduler design and are explained here. 

### Resource usage

The resource usage of a particular job is a weighted sum of all resources requested by that job. For example, if some job uses CPU, GPU, and RAM, then the resource usage of that job is

`CPU + w_GPU * GPU + w_RAM * RAM`,

where `w_GPU` and `w_RAM` is the cost of `GPU` and `RAM` relative to `1 CPU` core. We define such weights for all resources in the cluster. The resource usage of a particular queue is the sum of all resources requested by jobs in the pending and running stages originating from that queue.

### Fair share

Each queue has a weight associated with it that determines the size of its fair share. Denote these per-queue weights by 

`w_1, ..., w_N`,

where `N` is the total number of queues, and by 

`w = sum(w_1, ..., w_N)`

their sum. The fair share for each of the `N` queues is then
 
`w_1 / w, ..., w_N / w`.

### Flow

The scheduler does not rely on resource usage directly when computing the fair share of each queue. Instead, it uses the related notion of *flow*, defined recursively for each queue as

`queueFlow = queueResourceUsage + max(queueFlow - queueResourceUsage, 0) * beta`,

where

`beta = 0.5 ^ (timeSinceLastUpdate / halfTime)`,

and `halfTime` is a parameter of the scheduler. Flow is initially set to zero, is lower-bounded by resource usage, and converges to resource usage over time. Relying on flow instead of resource usage ensures fair scheduling in cases where it is not possible to have jobs from all queues running simultaneously.

## Global prioritisation

The goal of the scheduler is to schedule jobs in a manner that achieves weighted [max-min fairness](https://en.wikipedia.org/wiki/Max-min_fairness), i.e., such that an attempt to increase the flow of any one queue necessarily results in reducing the flow of at least one queue currently below its fair share. In network scheduling, where several incoming data streams are multiplexed onto a single outgoing data stream, max-min fairness is achieved by gradually increasing the flow allocated to incoming streams in equal infinitesimal amounts, while ignoring incoming streams for which the capacity limit has been hit. This is known as progressive filling, and the Armada scheduler uses a similar algorithm when moving jobs from the queued to pending stage. Because the scheduler cannot increase flow in infinitesimal amounts (jobs may be of arbitrary size) it approximates progressive filling in the following way:

1. For each non-empty queue, get the highest-priority job.
2. For each such job, compute the flow of the corresponding queue if this job were to be scheduled.
3. Select the job that would result in the smallest flow.
4. If there are no constraints preventing the job from being scheduled (see below), move the selected job into the pending stage.

This process is repeated for each job moved into the pending stage.

### Scheduling constraints

To prevent all resources being allocated to any single single user, Armada relies on per-queue scheduling constraints. Recall that users may optionally specify job lifetime, i.e., an amount of time (which may be zero) after which the job may be preempted. Armada uses separate scheduling constraints for jobs with finite and infinite lifetime. In particular

First, the flow of each queue corresponding to jobs with infinite lifetime (i.e., non-preemptible jobs) may only exceed its fair share by a certain configurable factor (e.g., 20% above its fair share).

Second, each queue has a budget for running jobs with finite lifetime. This budget is measured in resource-minutes and we refer to it as the excess flow budget of the queue. For example, say that some queue `Q` is using all of its fair share and has an unused excess flow budget of `B`. If the highest-priority job in `Q` has lifetime `X` minutes and is requesting `Y` resources (the weighted sum of all resources requested), this job can be scheduled only if `X*Y <= B` and, if scheduled, the remaining excess flow budget of `Q` would be `B - X*Y`. Similarly, as jobs leave the pending and running stages, the excess flow budget of the queue increases again.

The excess flow budget prevents a single queue from hanging on to all resources in the cluster for more than a limited time. Note that jobs with a lifetime of `0` consume no budget, i.e., a single queue may still consume the entire cluster if there are no other users present and the jobs scheduled are immediately preemptible.

## Executor implementation

TODO: Explain gang scheduling.

- Gang scheduling is an assignment optimisation problem.
- Define lost opportunity as the amount of resources that potentially become unavailable as a result of assigning a pod to a node. For example, if a node has 72 cores and 128 GB of RAM, and a pod is scheduled on it that requests 50 cores and 32 GB of RAM, then the lost opportunity is 22 cores and 96 GB of RAM; the lost opportunity would be smaller if the pod can be scheduled on a smaller node.
- Gang scheduling consists of assigning some number of pods to nodes to minimise lost opportunity.

TODO: We can probably omit this from this document.

Scheduling decisions are recorded in the form of lease messages, which are published to Pulsar and subsequently writte to a database table. Each row of this table is a tuple

`(jobId, executorId, Kubernetes object spec, Kubernetes object name, state)`.

Hence, jobs containing several Kubernetes objects (e.g., a job with a pod and a service object), result in several rows being written to the database.

Each executor is responsible for polling the database and reconciling any differences between the list of objects written to it with the objects that currently exist in Kubernetes, creating and deleting objects as necessary. The executor writes the current state of each object back to the database.
