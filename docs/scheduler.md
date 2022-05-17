# Scheduler

This document outlines the proposed Armada scheduler. The existing scheduler will be deprecated in favour of the one described here sometime during 2022. This is work in progress; everything herein is subject to change.

## Architecture

Armada consists of the Armada server and one or more Kubernetes worker clusters. Each worker cluster consists of a large number of nodes responsible for executing tasks and is managed by a process referred to as the Armada executor. Jobs are submitted to the Armada server, where they are queued until being handed off to an executor, which assigns them to nodes.

Armada uses a two-stage scheduling approach, consisting of a top-level scheduler (part of the server), and a set of per-cluster schedulers- one per executor. The top-level scheduler is responsible for assigning jobs to executors and the per-cluster schedulers are responsible for assigning those jobs to nodes. We refer to the top-level scheduler simply as "the scheduler".

The scheduler manages several queues of jobs and executors, and assigns jobs to executors one at a time. Further, for simplicity and scalability, the scheduler only considers one executor at a time. Hence, once an executor has been selected, the job of the scheduler is to join N streams of jobs, where N is the number of queues, into a single stream of jobs, i.e., the scheduler is essentially a multiplexer. Each executor is responsible for deciding how jobs are allocated within the cluster it manages. This two-stage approach allows the scheduler to manage very large numbers of nodes (hundreds of thousands).

Once an executor, denoted by E, has been selected, the scheduler repeatedly performs the following steps until some stopping criterium is met:

1. Select a queue, denoted by Q.
2. Select a job, denoted by J, from Q.
3. Assign J to E.

This process is explained in detail below.

## Metrics

We measure scheduler quality quantitatively by

- scheduler throughput, i.e., the average number of jobs scheduled per second,
- submission latency, i.e., the time from job submission to it being considered for scheduling, and
- cluster utilisation, i.e., the average fraction of cluster resources (CPU, RAM, and accelerators) that are utilised.

Armada is designed to schedule batch workloads over systems composed of up to 1 million cores when each job takes, on average, 10 minutes or more to complete, and consumes at least 1 core. Hence, to keep the cluster full, Armada must be able to schedule at least

1 000 000 / (60 * 10) = 1666.66...

jobs per second. At this throughput, filling an empty cluster would take 10 minutes. 

The 99-th percentile time from job submission to the job being available for scheduling should be at most 1 minute (i.e., one tenth of the average job runtime).

Overall average cluster utilisation should be at least 90%. We do not target 100% to allow for making room for scheduling large jobs.

In addition, we consider the following qualitative metrics:

- fairness and
- ability to schedule large jobs, e.g., jobs consuming one or more entire nodes.

Note that the metrics given here are in tension. For example, scheduling a large job may require draining a cluster partially to make space, thus reducing utilisation.

## Implementation

Here, we outline the scheduler implementation. At a high level, the scheduler

1. selects an executor and
2. assigns jobs to this executor until full.

The above algorithm runs periodically. In addition, Armada continually schedules preemtible and lime-limited jobs to improve utilisation and throughput.

### Fairness

The scheduler achieves fairness via a weighted fair queuing approach. Specifically, each queue has a weight associated with it that determines its target resource usage. Denote the per-queue weights by w_1, ..., w_N, where N is the total number of queues, and by w = sum(w_1, ..., w_N) their sum. The target resource usage of each queue is then w_1 / w, ..., w_N / w. The order in which queues are selected is determined by how far below their target resource usage they are.

### Executor selection

The scheduler assigns jobs to only one executor at a time. In particular, each worker cluster has associated with it a lower utilisation threshold. The scheduler activates when a cluster falls below this threshold (e.g., when 10% of its CPU is idle) and assigns jobs to the cluster until it is full. Tuning this threshold provides a trade-off between the size of jobs that can be scheduled and cluster utilisation. For example, if the threshold is 90% for a particular resource, up to 10% of cluster resources are idle at any time and jobs consuming up to 10% of that resource can be scheduled.

### Main scheduling algorithm

Once activated, the scheduler assigns jobs to the cluster according to the following procedure. Initially, all queues are considered for scheduling:

1. Select the queue furthest below its target resource usage. Denote this queue by Q.
2. Select the highest-priority job from Q. Denote this job by J. If assigning J to the cluster would exceed any of the following, remove Q from the scheduler and go to step 4.
    * Cluster capacity.
    * Per-job resource quotas.
    * Per-queue resource quotas.
3. Assign J to the cluster. If the Q contains no more jobs, remove it from the scheduler.
4. Repeat until the scheduler contains no more queues.

### Opportunistic scheduling algorithm

To increase utilisation and throughput, Armada may opportunistically schedule jobs outside of the main algorithm if doing so can be done without delaying jobs scheduled by the main algorithm. Only jobs that specify at least one of the following parameters can be scheduled opportunistically:

- `preemptible`: Indicates if Armada may preempt the job or not. The default is false.
- `lifetime`: Maximum lifespan of the job in seconds. Jobs that exceed their lifetime may be preempted. A value of 0 (the default) indicates infinite lifetime.

Preemtible and time-limited jobs are scheduled continuously and do not need to adhere to per-job or per-queue resource quotas. For each cluster, Armada estimates the time at which it will fall below its resource utilisation. Armada may schedule time-limited jobs that do not increase this time. Armada may always schedule preemtible jobs. However, these may be preempted to make room for jobs scheduled by the main algorithm.

### Per-cluster scheduling

Jobs assigned to an executor are assigned to nodes according to a bin-packing procedure, where jobs are assigned to nodes to minimize the number of nodes necessary. Any jobs that do not fit (e.g., because the scheduler made a mistake) are returned to the scheduler.

## Executor implementation

Scheduling decisions are recorded in the form of lease messages, which are published to Pulsar and subsequently writte to a database table. Each row of this table is a tuple 

`(jobId, executorId, Kubernetes object spec, Kubernetes object name, state)`. 

Hence, jobs containing several Kubernetes objects (e.g., a job with a pod and a service object), result in several rows being written to the database.

Each executor is responsible for polling the database and reconciling any differences between the list of objects written to it with the objects that currently exist in Kubernetes, creating and deleting objects as necessary (using the name to map database rows to objects in Kubernetes). The executor writes the current state of each object back to the database.

## Scheduling limitations

Jobs can optionally specify the following parameters:

- `atMostOnce`: If false (the default), Armada will attempt to run the job only once. If true, Armada may retry running the job if it doesn't succeed.
- `concurrencySafe`: If true, Armada may assign the job to multiple executors simultaneously. Once one of the executors starts the job, the other replicas are cancelled. This can reduce scheduling delay but may result in several executors running the job concurrently.