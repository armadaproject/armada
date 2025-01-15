# Scheduler

The scheduler is the component of Armada responsible for determining when and where each job should run. Here, we describe how it works, including its support for

* fair resource allocation,
* bin-packing,
* gang-scheduling, and
* preemption (urgency-based and to fair share).

## Jobs

An Armada job represents a computational task to be carried out and consists of a bag of Kubernetes objects (e.g., pods, services, and ingresses) with a shared life cycle, i.e., all objects are created at the start of the job (within the same cluster) and are destroyed at its end. Each job is a member of a queue (e.g., corresponding to a particular user) and a job set (a per-queue logical grouping of jobs that can be managed as a unit), and has a priority associated with it, i.e., each job has associated with it a tuple (queue, jobSet, priority). Each job also has a priority class (PC) associated with it; see the section on preemption.

Each job is submitted to a specific queue and is stored in that queue while waiting to be scheduled. Job sets typically represent workflows consisting of multiple jobs. For example, if fitting a machine learning model against several different data sets, there may be a separate job for each data set, and these jobs could all be members of the same job set. Job sets can be monitored and, e.g., cancelled as a unit, which simplifies managing large numbers of jobs.

Armada jobs are created by submitting a job specification to the Armada server via either the gRPC API, client libraries, or the armadactl command-line-utility.

## Resource usage and fairness

Each job has a cost associated with it that is the basis of fair resource allocation. In particular, Armada tries to balance the aggregate cost of jobs between queues. Specifically, the cost of each job is a weighted sum of all resources requested by that job. For example, a job requesting CPU, GPU, and RAM, has cost

CPU + w_GPU * GPU + w_RAM * RAM,

where w_GPU and w_RAM is the cost of GPU and RAM relative to 1 CPU. Currently,  w_GPU=1 and w_RAM=0.

Further, the cost associated with each queue is the sum of the cost of all jobs in the pending or running state associated with the queue; this per-queue cost is what the Armada scheduler tries to balance. (This notion of fairness is sometimes referred to as asset fairness.) Specifically, each queue has a weight associated with it that determines the size of its fair share. If we denote these per-queue weights by 

w_1, ..., w_N,

where N is the number of active queues (see below) and by

w = sum(w_1, ..., w_N) 

their sum, then the fair share of each of the N queues is

w_1 / w, ..., w_N / w.

This computation only includes active queues, i.e., queues for which there are jobs in the queued, pending, or running state. Hence, the fair share of a queue may vary over time as other queues transition between active and inactive. Armada considers the cost associated with each queue (more specifically, the fraction of its fair share each queue is currently assigned) when selecting which job to schedule next; see the following section.

## Job scheduling order

Armada schedules one job at a time, and choosing the order in which jobs are attempted to be scheduled is the mechanism by which Armada ensures resources are divided fairly between queues. In particular, jobs within each queue are ordered by per-job priorities set by the user, but there is no inherent ordering between jobs associated with different queues; the scheduler is responsible for establishing such a global ordering. To divide resources fairly, Armada establishes such a global ordering as follows:

1. For each queue, find the next schedulable job in that queue, where jobs are considered in order of per-job priority first and submission time second.
2. For each queue, compute what fraction of its fair share the queue would have if the next schedulable job were to be scheduled.
3. Select for scheduling the next schedulable job from the queue for which this computation resulted in the smallest fraction of fair share.

Including the next schedulable job in the computation in step 2. is important since the next job may be a gang job requesting thousands of nodes.

This approach is sometimes referred to as progressive filling and is known to achieve max-min fairness, i.e., for an allocation computed in this way, an attempt to increase the allocation of one queue necessarily results in decreasing the allocation of some other queue with equal or smaller fraction of its fair share, under certain conditions, e.g., when the increments are sufficiently small.

Note that when finding the next schedulable job, there is a limit to the number of jobs considered, i.e., there may be schedulable job in a queue blocked behind a long sequence of unschedulable jobs.

## Bin-packing
When assigning jobs to nodes, Armada adheres to the following principles:

* When choosing a node to assign a job to, Armada first considers the set of nodes on which the queue the job is associated with is the only user. If not schedulable on any of those nodes, Armada considers the set of unused nodes, and, only if not schedulable there either, considers nodes with multiple users. This principle is important to reduce interference between jobs associated with different queues during the scheduling cycle and serves to reduce the number of preemptions necessary when preempting to fair share (see the section on preemption). 
* When choosing among the nodes in such a set, Armada attempts to schedule the job onto the node with the smallest amount of available resources on which the job fits. This principle is important to increase the amount of contiguous resources available, thus facilitating scheduling large jobs later – Armada is optimised for large jobs in this way since the difficulty of scheduling a job increases with its size; very small jobs are typically easy to schedule regardless.

Together, these principles result in jobs being bin-packed on a per-queue basis – the second step above can be seen as greedy bin-packing solver.

This approach comes with an important trade-off compared global bin-packing in that it reduces cross-queue job contention at the expense of potentially increasing inter-queue job contention. I.e., each user has a greater level of control of how the resources on a node are utilised – since a user submitting a large number of jobs is likely to be the only user on most of the nodes assigned to those jobs. However, this approach also results in jobs that are likely to have similar resource usage profiles being clustered together – since jobs originating from the same queue are more likely to, e.g., consume large amounts of network bandwidth at the same time, than jobs originating from different queues. We opt for giving users the greater level of control since it can allow for overall more performant applications (hence, this is also the approach typically taken in the high-performance computing community). 

## Gang scheduling
Armada supports gang scheduling of jobs, i.e., all-or-nothing scheduling of a set of jobs, such that all jobs in the gang are scheduled onto the same cluster at the same time or not at all. Specifically, Armada implicitly groups jobs using a special annotation set on the pod spec embedded in the job. A set of jobs (not necessarily a "job set") for which the value of this annotation is the same across all jobs in the set is referred to as a gang. All jobs in a gang are gang-scheduled onto the same cluster at the same time. The cluster is chosen dynamically by the scheduler and does not need to be pre-specified.

Details related to the gang-scheduling algorithm:

* Jobs are grouped using the armadaproject.io/gangId annotation. The value of the armadaproject.io/gangId annotation must not be re-used. For example, a unique UUID generated for each gang is a good choice.
* All jobs in a gang must also specify the total number of jobs in the gang using another annotation armadaproject.io/gangCardinality. It's the responsibility of the submitter to ensure this value is set correctly on each job that makes up a gang.
* All jobs in a gang must be submitted within the same request to Armada. This is to ensure that Armada can validate at submit-time that all jobs in the gang are present.
* During scheduling, Armada iterates over jobs. Whenever the Armada scheduler find a job that sets the armadaproject.io/gangId annotation, it stores that job in a separate place. Armada only considers these jobs for scheduling once it has found all of the jobs that make up the gang. Note that the scheduler object already supports several pods.

## Preemption

Armada supports two forms of preemption:

1. Urgency-based preemption, i.e., making room for a job by preempting less urgent jobs. This form of preemption works in the same way as Kubernetes priority classes (PCs).
2. Preemption to fair share, i.e., preempting jobs belonging to users with more than their fair share of resources, such that those resources can be re-allocated to improve fairness.

These forms of preemption are driven by separate processes and operate independently of each other. Incidentally, preemption also makes it easier to schedule large jobs, since currently running jobs can be preempted to make room for them.

Both forms of preemption are based on Armada job PCs, each of which is represented by a tuple (name, priority, isPreemptible). Armada comes with two PCs by default:

* (armada-default, 30000, false) 
* (armada-preemptible, 20000, true) 

Job priority classes are set by setting the priorityClassName field of the embedded podspec. Jobs with no PC are automatically assigned the armada-default PC (i.e., preemption is opt-in). We describe both forms of preemption in more detail below.

### Urgency-based preemption

When scheduling a job, Armada may preempt other jobs to make room for it. Specifically, Armada may preempt running preemptible jobs with a lower-priority PC. Hence, the PC priority of a job expresses how urgent a job is. In this way, PCs support the following use cases:

* A user wants to run, e.g., a speculative job and doesn't want to occupy the farm if there are more urgent jobs to run. To that end, the user chooses a low-priority preemptible PC when submitting it. If there are more urgent jobs to run (i.e., with a higher-priority PC), those jobs may preempt the submitted job.
* A user has an urgent job they want to be run immediately. To that end, they choose a high-priority PC, such that it may preempt currently running preemptible jobs to make room for itself.

Hence, this is a cooperative form of preemption that requires users to coordinate among themselves to set appropriate PCs.

Urgency-based preemption is implemented in Armada via tracking the allocatable resources at different PC priorities. For example, if a 32-core node has running on it

* an armada-default job requesting 10 cores and
* an armada-preemptible job requesting 20 cores,

then Armada understands that up to 22 cores can be allocated to armada-default jobs (allocating more than 2 cores is possible by preempting the armada-preemptible job) and up to 2 cores can be allocated to armada-preemptible jobs. If during scheduling a job needs to be preempted, kube-scheduler (a Kubernetes component) makes the decision on which job should be preempted.

### Preemption to fair share

Whereas urgency-based preemption is triggered by a job being submitted that can not be scheduled without preempting a lower-urgency job, preemption to fair share is triggered whenever a queue A has been allocated more than their fair share of resources – and at least some of those jobs are preemptible – and another queue B has jobs of equal PC priority queued. Recall that the fair share of each queue is computed from the set of currently active queues, i.e., queue A may be exceeding its fair share because a previously inactive queue B has become active as a result of jobs being submitted to it, thus reducing the size of the fair share of queue A.

For example, if jobs of PC armada-preemptible are submitted to queue A and are allocated all available resources, and later jobs of PC armada-preemptible are submitted to queue B, Armada would preempt some of the jobs of user A to make room for the jobs submitted by user B (the fraction of resources reclaimed from queue A depends on the relative weights of the two user's queues).

At a high level, preemption to fair share works as follows:

1. Some preemptible jobs are evicted from the nodes to which they are assigned and are placed at the front of their respective queues. Specifically, for each node, all preemptible jobs on that node are evicted with a configurable probability. If a job part of gang is evicted, all other jobs in that gang that are still running are also evicted, regardless of which node they are assigned to. This happens only within the scheduler, i.e., no running jobs are preempted at this stage. The cost of the evicted jobs are subtracted from the cost of their respective queues.
2. Armada performs a scheduling cycle, thus computing a mapping from jobs to nodes as if the evicted jobs had never started running and were still queued. Recall that Armada selects which queue to schedule from next based on what fraction of its fair share the queue is currently allocated. Hence, any queue above its fair share is unlikely to have all of its evicted jobs re-scheduled as part of the cycle (unless other jobs for some reason can not be scheduled).
3. Any jobs that were evicted and not re-scheduled as part of step 2. are preempted. Any jobs scheduled in step 2. that were not previously evicted are new jobs that should be allowed to start running. Because evicted jobs are placed at the front of each queue, Armada will always, for each queue, try to re-schedule evicted jobs before trying to schedule now jobs.

In this way Armada makes a unified of which jobs should be preempted and scheduled. It is then the responsibility of the rest of the system to reconcile against this new desired state.

For this process to be effective, i.e., not cause large numbers of avoidable preemptions, the mapping from jobs to nodes must be stable across invocations of the scheduler. We use two strategies to increase stability:

* Evicted jobs may only be scheduled onto the node they were evicted from, i.e., no moving jobs between nodes.
* Jobs are bin-packed on a per-queue basis. This reduces interference between queues during the scheduling cycle.

For example, consider a scenario with two queues A and B of equal weight and two nodes – node 1 and node 2 –  with 32 cores each. Say that initially 40 1-core preemptible jobs are submitted to queue A, 32 of which are scheduled onto node 1 and 8 on node 2. Later, 50 1-core preemptible jobs are submitted to queue B. During the next scheduler invocation, Armada evicts all jobs from nodes 1 and 2 and places these at the front of queue A. At this point, there are 40 jobs in queue A and 50 jobs in queue B. Then, Armada starts scheduling from both queue A and B. Because Armada selects which queue to schedule from next based on which queue is currently assigned the smallest fraction of its fair share, Armada will alternate between scheduling from queue A and B, such that at the end of the scheduling cycle, 32 jobs are scheduled from each queue; the scheduling cycle will have terminated because both node 1 and 2 are full at this point. For queue A, all these jobs are re-scheduled evicted jobs, whereas for queue B the 32 scheduled jobs are new jobs. For queue A, 8 evicted jobs were not re-scheduled and are thus preempted. For queue B, 18 jobs could not be scheduled and are still queued.

The above example also illustrates why per-queue bin-packing (as opposed to bin-packing in a non-queue-aware manner) is important; because Armada alternates between scheduling from queue A and B, non-queue-aware bin-packing would fill up node 1 with 16 jobs from each queue before scheduling onto node 2, thus unnecessarily preempting 16 jobs from queue A. Per-queue bin-packing, on the other hand, avoids scheduling jobs from queue B onto node 1 when possible, which results in the 32 jobs of queue B all being scheduled onto node 2.

To control the rate of preemptions, the expected fraction of currently running jobs considered for preemption to fair share is configurable. Specifically, for each node, the preemptible jobs on that node are evicted with a configurable probability.

## Graceful termination

Armada will sometimes kill pods, e.g., because the pod is being preempted or because the corresponding job has been cancelled. Pods can optionally specify a graceful termination period, i.e., an amount of time that the pod is given to exit gracefully before being terminated. Graceful termination works as follows:

1. The pod is sent a SIGTERM. Kubernetes stores the time at which the signal was sent.
2. After the amount of time specified in the graceful termination period field of the pod has elapsed, the job is sent a SIGKILL if still running.

Armada implements graceful termination periods as follows:

* Jobs submitted to Armada with no graceful termination period set, are assigned a 1s period.
* Armada validates that all submitted jobs have a termination period between 1s and a configurable maximum. It is important that the minimum is positive, since a 0s grace period is interpreted by Kubernetes as a "force delete", which may result in inconsistencies due to pods being deleted from etcd before they have stopped running.

Jobs that set a termination period of 0s will be updated in-place at submission to have a 1s termination period. This is to ensure that current workflows, some of which unnecessarily set a 0s termination period, are not disrupted.

Jobs that explicitly set a termination period higher than the limit will be rejected at submission. Jobs that set a termination period greater than 0s but less than 1s will also be rejected at submission.

## Job deadlines

All Armada jobs can be assigned default job deadlines, i.e., jobs have a default maximum runtime after which the job will be killed. These defaults are:

* CPU jobs: 3 days
* GPU jobs: 14 days

Default deadlines are only added to jobs that do not already specify one. To manually specify a deadline, set the ActiveDeadlineSeconds field of the pod spec embedded in the job; see https://github.com/kubernetes/api/blob/master/core/v1/types.go#L3182
