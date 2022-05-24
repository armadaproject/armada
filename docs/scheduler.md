# Scheduler

This document outlines the proposed Armada scheduler. The existing scheduler will be deprecated in favour of the one described here sometime during 2022. This is work in progress; everything herein is subject to change.

The purpose of developing this new scheduler is to improve fairness and throughput, and to more effectively schedule large and distributed jobs, compared to the existing scheduler.


- The scheduler is a multiplexer.
- The scheduler achieves max-min fairness among the currently active queues.
- Each queue accrues share over time while these is at least one job queued in the queue.
- We use a backfilling approach to improve utilisation, i.e., lower-priority jobs are scheduled ahead of higher-priority jobs only if the lower-priority job does not delay the higher-priority job.
- We use per-queue and per-job resource limits to prevent a single queue from using all resources. Preemptible jobs do not need to adhere to these limits.
- We limit the unfairness in the system. This prevents jobs from a single queue of using more than a certain amount of resources. Time-limited jobs are scheduled as long as the bound on unfairness doesn't exceed some limit.
- We preempt jobs when if doing so improves fairness.
- We unify all available resources. We use an event-driven architecture to decide which job to schedule next.
- All pod creation has to be performed via the Armada scheduler. We plan to support systems that dynamically create workers (e.g., Spark) by either building in first-class support for Armada into the system (in this case Spark) or by hijacking the Kubernetes resource creation API, so that creating a pod results in submitting an Armada job that eventually causes those pods to be created.

## Armada architecture

Armada consists of the Armada server and one or more Kubernetes worker clusters. Each worker cluster consists of a large number of nodes responsible for executing tasks and is managed by a process referred to as the Armada executor. Jobs are submitted to the Armada server, where they are queued until being handed off to an executor, which assigns them to nodes.

Armada uses a two-stage scheduling approach, consisting of a top-level scheduler (part of the server), and a set of per-cluster schedulers- one per executor. The top-level scheduler is responsible for assigning jobs to executors and the per-cluster schedulers are responsible for assigning those jobs to nodes. This two-stage approach allows the scheduler to manage very large numbers of nodes. We refer to the top-level scheduler simply as "the scheduler". 

Each job is a member of exactly one job queue (e.g., corresponding to a particular user) and has a priority associated with it, i.e., each job has associated with it a tuple `(queue, priority)`. The job of the scheduler is to decide in what order to schedule jobs to worker clusters.

## Scheduler architecture

At a high level, scheduling consists of three stages:

1. Global prioritisation
2. Cluster assignment
3. Node assignment

The two first stages take place in the server and the third stage in the executor. We explain the first two stages here and the third stage in a later section. At any time, the scheduler manages `N` job queues. Suppose that `Q <= N` of these queues have at least 1 job waiting to be scheduled (i.e., the remaining `N-Q` queues are empty). We refer to these `Q` queues as active and the remaining `N-Q` queues as inactive. Within each of the active queues, jobs are ordered by priority. The first scheduling stage consists computing a global order of all jobs across all active queues. Denote by

`j_1, j_2, ...`

the 1st and 2nd job in this order and so on. Later, we show how to compute this order fairly and efficiently.

The second stage consists of assigning jobs to worker clusters. Normally, jobs are assigned to clusters in order, i.e., `j_i` is assigned before `j_2` and so on. However, if some job cannot be scheduled (e.g., because insufficient resources are available), a lower-priority job (i.e., a job further back in the global order) may be scheduled ahead of a higher-priority job (i.e., a job ocurring earlier in the global order) if doing so does not delay scheduling the higher-priority job. This is an optimisation to improve utilisation and is referred to as backfilling (see, e.g., [this page](http://docs.adaptivecomputing.com/maui/8.2backfill.php) for some background).

TODO: Explain the settings we allow (preemptible, lifetime).

Next, we explain how fairness is defined in Armada.

## Fairness

- Armada allocates jobs to clusters in a manner that satisfies weighted max-min allocation.
- Queues accrue budget while active. These queues are effectively standing in line waiting for their jobs to be scheduled. Queues with no jobs are not standing in line.
- Queues spend budget while having jobs running.
- The net loss or gain in budget is determined by the difference between these.
- The highest priority job is the job that brings the largest increase in fairness. So let's find a fairness measure.

Here, we describe the definition of fairness used by Armada and how the scheduler fairly divides scarce resources between job queues. This determines how jobs are ordered across queues. First, the resource usage of a particular job is a weighted sum of all resources allocated to that job. For example, if the resources considered for some job are CPU, GPU, and RAM, then the resource usage of that job is

`CPU + w_GPU * GPU + w_RAM * RAM`,

where `w_GPU` and `w_RAM` is the cost of `GPU` and `RAM` relative to `1 CPU` core. We define such weights for all resources in the cluster (e.g., different types of GPUs). The resource usage of a particular queue is the sum of all resources allocated to curently running jobs originating from that queue.

Second, each queue has a weight associated with it that determines the size of its fair share. Denote these per-queue weights by 

`w_1, ..., w_N`,

where `N` is the total number of queues, and by 

`w = sum(w_1, ..., w_N)`

their sum. The fair share for each of the N queues is then
 
`w_1 / w, ..., w_N / w`.

TODO: Explain how budget is accrued

Armada divides resources among currently active queues, i.e., queues with at least one job currently queued. The priority of each queue is computed as follows:

Iterate over all active queues. Compute the sum of their weights. The priority of the i-th queue is increased by w_i / w * t_i. At each time intervall, the priority of the i-th queue is decreased by q_i / resource_sum * T. So queues both accrue and lose priority continually. A queue that is allocated more than it's fair share loses priority at each instant (never going below zero). A queue allocated less than its fair share gains priority.

Armada allocates jobs to nodes to achieve weighted [max-min fairness](https://en.wikipedia.org/wiki/Max-min_fairness). Specifically, Armada allocates jobs to nodes such that an attempt to increase the resources allocated to any queue necessarily results in reducing the resources allocated to at least one queue that is currently below its fair share.

The scheduler achieves fairness via a weighted fair queuing approach.

- We only schedule a job from a lower-priority queue before a higher-priority queue if doing so does not delay scheduling the highest-priority job of that higher-priority queue.
- We only schedule the lower-priority jobs from some queue before higher-priority jobs in that queue if doing so does not delay scheduling the higher-priority job.
- For jobs with equal priority, the job queued first has higher priority.

## Scheduler metrics

TODO: Add a formal fairness measure.

TODO: Consider renaming to "Scheduler performance" and move qualitative metrics out of this section.

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

## Implementation

Here, we outline the scheduler implementation. At a high level, the scheduler

1. selects an executor and
2. assigns jobs to this executor until full.

For simplicity and scalability, the scheduler only considers one executor at a time. The above algorithm runs periodically. In addition, to improve utilisation and throughput, Armada continually schedules preemtible and lime-limited jobs.

### Fairness

The scheduler achieves fairness via a weighted fair queuing approach. Specifically, each queue has a weight associated with it that determines its target resource usage. Denote the per-queue weights by 

`w_1, ..., w_N`,

where `N` is the total number of queues, and by 

`w = sum(w_1, ..., w_N)`

 their sum. The target resource usage of each queue is then 
 
 `w_1 / w, ..., w_N / w`. 
 
 We combine all resources (CPU, RAM, and accelerators) when computing resource usage. The order in which queues are selected is determined by how far below their target resource usage they are.

### Executor selection

The scheduler assigns jobs to only one executor at a time. In particular, each worker cluster has associated with it a lower utilisation threshold and the scheduler activates when a cluster falls below this threshold (e.g., when 10% of its CPU is idle). At that point, the scheduler assigns jobs to the cluster until full. Tuning this threshold provides a trade-off between the size of jobs that can be scheduled and cluster utilisation. For example, if the threshold is 90% for a particular resource, up to 10% of cluster resources are idle at any time and jobs consuming up to 10% of that resource can be scheduled.

To improve utilisation, the scheduler schedules time-limited and preemptible jobs opportunistically.

### Main scheduling algorithm

Once activated for a particular cluster, the scheduler assigns jobs to the cluster according to the following procedure. Initially, all queues are considered for scheduling.

1. Select the queue furthest below its target resource usage. Denote this queue by Q.
2. Select the highest-priority job from Q. Denote this job by J. If assigning J to the cluster would exceed any of the following, remove Q from the scheduler and go to step 4.
    * Cluster capacity.
    * Per-job resource quotas.
    * Per-queue resource quotas.
3. Assign J to the cluster. If Q contains no more jobs, remove it from the scheduler.
4. Repeat until the scheduler contains no more queues.

### Opportunistic scheduling algorithm

To increase utilisation and throughput, Armada may opportunistically schedule jobs outside of the main algorithm if doing so can be done without delaying jobs scheduled by the main algorithm. Only jobs that are preemptible or that specify a max lifespan (i.e., a time beyond which Armada may preempt the job) may be scheduled opportunistically. Note that a preemptible job is equivalent to a job with a lifespan of 0. The key idea is that a lower-priority job may be run before a higher-priority job if doing so does not delay the higher-priority job.

Preemtible and time-limited jobs are scheduled continuously and do not need to adhere to per-job or per-queue resource quotas. For each cluster, Armada estimates the time at which it will fall below its resource utilisation. Armada continually schedules preemptible and time-limited jobs that do not increase this estimate. If necessary, the main algorithm will preempt such jobs to make room for higher-priority jobs.

### Per-cluster scheduling

Jobs assigned to an executor are assigned to nodes according to a bin-packing procedure, where jobs are assigned to nodes to minimize the number of nodes required. This is to make room for large incoming jobs. Any jobs that the executor is not able to schedule are returned to the scheduler.

## Executor implementation

Scheduling decisions are recorded in the form of lease messages, which are published to Pulsar and subsequently writte to a database table. Each row of this table is a tuple 

`(jobId, executorId, Kubernetes object spec, Kubernetes object name, state)`. 

Hence, jobs containing several Kubernetes objects (e.g., a job with a pod and a service object), result in several rows being written to the database.

Each executor is responsible for polling the database and reconciling any differences between the list of objects written to it with the objects that currently exist in Kubernetes, creating and deleting objects as necessary. The executor writes the current state of each object back to the database.

## Scheduling limitations

Jobs can optionally specify the following parameters:

- `atMostOnce`: If false (the default), Armada will attempt to run the job only once. If true, Armada may retry running the job if it doesn't succeed.
- `concurrencySafe`: If true, Armada may assign the job to multiple executors simultaneously. Once one of the executors starts the job, the other replicas are cancelled. This can reduce scheduling delay but may result in several executors running the job concurrently.

## Distributed jobs

Distributed jobs are represented as [PodGroups](https://github.com/kubernetes-sigs/scheduler-plugins/blob/master/kep/42-podgroup-coscheduling/README.md). All pods that make up a `PodGroup` are scheduled jointly using a gang-scheduling algorithm. Similar to to [kube-batch](https://github.com/kubernetes-sigs/kube-batch) and [Volcano](https://volcano.sh/en/docs/podgroup/), the gang-scheduling algorithm is implemented as a [custom Kubernetes scheduler](https://kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers/).