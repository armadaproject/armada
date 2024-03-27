<div align="center">
<img src="./logo.svg" width="200"/>

<p><a href="https://circleci.com/gh/armadaproject/armada"><img src="https://circleci.com/gh/helm/helm.svg?style=shield" alt="CircleCI"></a>
<a href="https://goreportcard.com/report/github.com/armadaproject/armada"><img src="https://goreportcard.com/badge/github.com/armadaproject/armada" alt="Go Report Card"></a></p>
</div>

# Armada

Armada is a system built on top of [Kubernetes](https://kubernetes.io/docs/concepts/overview/) for running batch workloads. With Armada as middleware for batch, Kubernetes can be a common substrate for batch and service workloads.  Armada is used in production and can run millions of jobs per day across tens of thousands of nodes. 

Armada addresses the following limitations of Kubernetes:

1. Scaling a single Kubernetes cluster beyond a certain size is [challenging](https://openai.com/blog/scaling-kubernetes-to-7500-nodes/). Hence, Armada is designed to effectively schedule jobs across many Kubernetes clusters. Many thousands of nodes can be managed by Armada in this way.
2. Achieving very high throughput using the in-cluster storage backend, etcd, is [challenging](https://etcd.io/docs/v3.5/op-guide/performance/). Hence, Armada performs queueing and scheduling out-of-cluster using a specialized storage layer. This allows Armada to maintain queues composed of millions of jobs.
3. The default [kube-scheduler](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-scheduler/) is not suitable for batch. Instead, Armada includes a novel multi-Kubernetes cluster scheduler with support for important batch scheduling features, such as:
   * Fair queuing and scheduling across multiple users. Based on dominant resource fairness.
   * Resource and job scheduling rate limits.
   * Gang-scheduling, i.e., atomically scheduling sets of related jobs.
   * Job preemption, both to run urgent jobs in a timely fashion and to balance resource allocation between users.

Armada also provides features to help manage large compute clusters effectively, including:

* Detailed analytics exposed via [Prometheus](https://prometheus.io/) showing how the system behaves and how resources are allocated.
* Automatically removing nodes exhibiting high failure rates from consideration for scheduling.
* A mechanism to earmark nodes for a particular set of jobs, but allowing them to be used by other jobs when not used for their primary purpose.

Armada is designed with the enterprise in mind; all components are secure and highly available.

Armada is a [CNCF](https://www.cncf.io/) Sandbox project and is used in production at [G-Research](https://www.gresearch.co.uk/).

For an overview of Armada, see the following videos:

- [Armada - high-throughput batch scheduling](https://www.youtube.com/watch?v=FT8pXYciD9A)
- [Building Armada - Running Batch Jobs at Massive Scale on Kubernetes](https://www.youtube.com/watch?v=B3WPxw3OUl4)

The Armada project adheres to the CNCF [Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## System overview

At a high level, Armada is a multi-Kubernetes cluster batch job scheduler. Each Armada job represents a computational task to be carried out over a finite amount of time and consists of

- a Kubernetes pod,
- zero or more auxiliary Kubernetes objects (e.g., services and ingresses), and
- metadata specific to Armada (e.g., the name of the queue to which job belongs).

Jobs are submitted to Armada and Armada is responsible for determining when and where each job should be run.

An Armada deployments consists of:
- One or more Kubernetes clusters containing nodes to be used for running jobs. Such clusters are referred to as worker clusters.
- The Armada control plane components responsible for job submission and scheduling. Jobs are submitted to the control plane and are automatically transferred to worker clusters once scheduled.
- The Armada executors responsible for communication between the Armada control plane and the [Kubernetes control plane](https://kubernetes.io/docs/concepts/overview/kubernetes-api/). There is one executor per worker cluster.

The Armada control plane components may run anywhere from which they can be reached by the executors, e.g., in the same cluster as an executor, on a different Kubernetes cluster, or on a node not managed by Kubernetes. Each executor normally runs within the worker cluster it is responsible for.

We show an overview of an Armada deployment with three worker clusters below. Clients here refers to systems submitting jobs to Armada.

```
                                                    Worker clusters                              
                                                                                                 
                    ┌──────────────────────┐        ┌──────────┐ ┌──────────┐ ┌──────────┐       
┌──────────┐        │                      │        │┼────────┼│ │┼────────┼│ │┼────────┼│       
│ Clients  │ ─────► │ Armada control plane │ ─────► ││Executor││ ││Executor││ ││Executor││       
│          │        │                      │        │┼────────┼│ │┼────────┼│ │┼────────┼│       
└──────────┘        │                      │        │┼───▼────┼│ │┼───▼────┼│ │┼───▼────┼│       
                    │                      │        ││kube-api││ ││kube-api││ ││kube-api││       
                    │                      │        │┼────────┼│ │┼────────┼│ │┼────────┼│       
                    └──────────────────────┘        └──────────┘ └──────────┘ └──────────┘       
```

The arrows in the above diagram show the path jobs take though the system. Specifically, each job passes through the following stages:

1. The job is submitted to Armada, where it is stored in one of several queues, e.g., corresponding to different users. At this point, the job is stored in the Armada control plane and does not exist in any Kubernetes cluster. The job is in the *queued* state.
2. The Armada scheduler eventually determines that the job should be run and marks the job with the Kubernetes cluster and node it should be run on. The job is in the *leased* state.
3. The Kubernetes resources that make up the job are created in the cluster it was assigned to via kube-api calls. The pod contained in the job is bound to the node it was assigned to. The job is in the "pending" state.
4. Container images are pulled, volumes and secrets mounted, and init containers run, after which the main containers of the pod start running. The job is in the *running* state.
5. The job terminates once all containers in the pod have stopped running. Depending on the exit codes of the containers, the job transitions to either the *succeeded* or *failed* state.

All Kubernetes resources associated with the job are deleted once the job terminates. For more information on jobs, see the scheduler documentation.

## The Armada control plane

The Armada control plane is the set of components responsible for job submission and scheduling. It consists of the following subsystems:

- Job submission and control. Exposes an API through which clients can submit, re-prioritise, and cancel jobs.
- Job state querying. Allows clients to query the current state of a job.
- Job state querying (streaming). Allows clients to subscribe to a stream of updates to a set of jobs.
- Scheduler. Responsible for assigning jobs to clusters and nodes.
- Lookout. Web UI showing the current state of jobs.

The services that make up these subsystems communicate according to event-sourcing principles, i.e., message routing is handled by a log-based message broker shared by all services. In particular, Armada relies on Apache Pulsar for message routing. Each subsystem then operates in cycles consisting of:

1. Receive messages from Pulsar.
2. Do either or both of:
   - Update the internal state of the subsystem.
   - Publish messages to Pulsar. 

Those published messages may then be received by other subsystems, causing them to publish other messages, and so on. Note that the log is the source of truth; each subsystem can recover its internal state by replaying messages from the log. We include a diagram showing this process below with two subsystems.

```
┌────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                    │
│   ┌───────────────────────────────────────┐        ┌───────────────────────────┐   │
│   │ Pulsar                                │        │ Subsystem 1               │   │
└►  │ ┌───────────────────────────────────┐ │ ─────► │                           │ ──┘
    │ │ Multiple topics/topic partitions. │ │        └───────────────────────────┘    
    │ │                                   │ │                                         
    │ │                                   │ │        ┌───────────────────────────┐    
┌►  │ └───────────────────────────────────┘ │ ─────► │ Subsystem 2               │ ──┐
│   │                                       │        │                           │   │
│   └───────────────────────────────────────┘        └───────────────────────────┘   │
│                                                                                    │
└────────────────────────────────────────────────────────────────────────────────────┘
```

## Scheduler: features and principles

Here, we give an overview of the algorithm used by Armada to determine which jobs to schedule and preempt. This algorithm runs within the scheduling subsystem of the Armada control plane. Note that Armada does not rely on kube-scheduler (or other in-cluster schedulers) for scheduling and preemption.

Scheduling requires balancing throughput, timeliness, and fairness. The Armada scheduler operates according to the following principles:

- Throughput: Maximise resource utilisation by scheduling jobs onto available nodes whenever possible.
- Timeliness: Schedule more urgent jobs before less urgent jobs. Always preempt less urgent jobs to make room for more urgent jobs, but never the opposite.
- Fairness: Schedule jobs from queues allocated a smaller fraction of their fair share of resources before those allocated a larger fraction. Always preempt jobs from queues with a larger fraction of their fair share if doing so helps schedule jobs from queues with a smaller fraction.

The Armada scheduler also satisfies (with some exceptions as noted throughout the documentation) the following properties: 

- Sharing incentive: Each user should be better off sharing the cluster than exclusively using their own partition of the cluster.
- Strategy-proofness: Users should not benefit from lying about their resource requirements.
- Envy-freeness: A user should not prefer the allocation of another. This property embodies the notion of fairness.
- Pareto efficiency: It should not be possible to increase the allocation of a user without decreasing the allocation of another.
- Preemption stability: Whenever a job is preempted and subsequently re-submitted before any other jobs have completed, the re-submitted job should not trigger further preemptions.

Next, we cover some specific features of the scheduler.

### Jobs and queues

Each Armada job represents a computational task to be carried out and consists of:

- A Kubernetes podspec representing the workload to be run.
- Auxiliary Kubernetes objects, e.g., services and ingresses.

All objects that make up the job are created at job startup and are deleted when the job terminates.

Jobs are annotated with the following Armada-specific metadata:

- Queue: Each job belongs to a queue, which is the basis for fair share in Armada.
- Job set: A per-queue logical grouping of jobs meant to make it easier to manage large number of jobs. Jobs within the same job set can be managed as a unit.
- Priority: Controls the order in which jobs appear in the queue.
- Armada priority class, which itself contains a priority that controls preemption.

Jobs are totally ordered within each queue by:

1. Priority class priority.
2. Job priority.
3. Time submitted.

Armada attempts to schedule one job at a time. When scheduling from a particular queue, Armada chooses the next job to schedule according to this order.

### Resource usage and fairness

Armada divides resources fairly between queues. In particular, Armada will schedule and preempt jobs to balance the vector

```
c_1/w_1, c_2/w_2, ..., c_n/w_n 
```

where `c_i` and `w_i`, is the cost and weight associated with the `i`-th active queue, respectively, and `n` is the number of active queues. Only active queues, i.e., queues with jobs either queued or running, are considered when computing fairness. Hence, the fair share of a queue may change over time as other queues become active or inactive.

The cost of each queue is computed from the resource requests of running jobs originating from the queue. In particular, Armada relies on dominant resource fairness to compute cost, such that

```
c_i = max(cpu_i/cpu_total, memory_i/memory_total, ...)
```

where `cpu_i` is the total CPU allocated to jobs from the `i`-th queue and so on, and the totals is the total amount of resources available for scheduling. This fairness model has several desirable proprties; see 

- [Dominant Resource Fairness: Fair Allocation of Multiple Resource Types](https://amplab.cs.berkeley.edu/wp-content/uploads/2011/06/Dominant-Resource-Fairness-Fair-Allocation-of-Multiple-Resource-Types.pdf)

The weight of each queue is the reciprocal of its priority factor, which is configured on a per-queue basis.

### Priority classes and preemption

Armada supports two forms of preemption:

1. Urgency-based preemption, i.e., making room for a job by preempting less urgent jobs. This form of preemption works in the same way as the normal Kubernetes preemption.
2. Preemption to fair share, i.e., preempting jobs belonging to users with more than their fair share of resources, such that those resources can be re-allocated to improve fairness.

Both forms of preemption are based on Armada priority classes (PCs). These are similar to but distinct from Kubernetes PCs. All Armada jobs have an Armada PC associated with them. Each Armada PC is represented by the following fields:

- name: A unique name associated with each Armada PC.
- priority: An integer encoding the urgency of jobs with this PC. Jobs with a PC with higher priority can always preempt jobs with a PC with lower priority.
- isFairSharePreemptible: A boolean indicating whether jobs with this PC can be preempted via preemption to fair share. Note that all jobs can be preempted via urgency-based preemption, unless there is no other job with a higher PC priority.

Job priority classes are set by setting the `priorityClassName` field of the podspec embedded in the job. Jobs with no PC are automatically assigned one. We describe both forms of preemption in more detail below.

### Gang scheduling

Armada supports gang scheduling of jobs, i.e., all-or-nothing scheduling of a set of jobs. Gang scheduling is controlled by the following annotations set on individual jobs submitted to Armada:

* `armadaproject.io/gangId`: Jobs with the same value for this annotation are considered part of the same gang. The value of this annotation should not be re-used. For example, a unique UUID generated for each gang is a good choice. For jobs that do not set this annotation, a randomly generated value is filled in, i.e., single jobs are considered gangs of cardinality one.
* `armadaproject.io/gangCardinality`: Total number of jobs in the gang. The Armada scheduler relies on this value to know when it has collected all jobs that make up the gang. It is the responsibility of the submitter to ensure this value is set correctly for gangs.
* `armadaproject.io/gangMinimumCardinality`: Minimum number of jobs in the gang that must be scheduled. If the number of jobs scheduled is less than the cardinality of the gang, the remaining unscheduled jobs are failed by the scheduler. The value of this annotation defaults to that of `armadaproject.io/gangCardinality`.
* `armadaproject.io/gangNodeUniformityLabel`: Constrains the jobs that make up a gang to be scheduled across a uniform set of nodes. Specifically, if set, all gang jobs are scheduled onto nodes for which the value of the provided label is equal. This can be used to ensure, e.g., that all gang jobs are scheduled onto the same cluster or rack.

### Node selection and bin-packing

Armada schedules one job at a time. This process consists of: 

1. Selecting a job to schedule.
2. Assigning the selected job to a node.

Here, we explain the second step. Armada adheres to the following principles in the order listed:

1. Avoid preempting running jobs if possible.
2. If necessary, preempt according to the following principles:
   1. Preempt jobs of as low PC priority as possible.
   2. For jobs of equal PC priority, preempt jobs from queues allocated the largest fraction of fair share possible.
3. Assign to a node with the smallest amount of resources possible.

These principles result in Armada doing the best it can to avoid preemptions, or at least preempt fairly, and then greedily bin-packing jobs.

### Graceful termination

Armada will sometimes kill pods, e.g., because the pod is being preempted or because the corresponding job has been cancelled. Pods can optionally specify a graceful termination period, i.e., an amount of time that the pod is given to exit gracefully before being terminated. Graceful termination works as follows:

1. The pod is sent a SIGTERM. Kubernetes stores the time at which the signal was sent.
2. After the amount of time specified in the graceful termination period field of the pod has elapsed, the job is sent a SIGKILL if still running.

Armada implements graceful termination periods as follows:

* Jobs submitted to Armada with no graceful termination period set, are assigned a 1s period.
* Armada validates that all submitted jobs have a termination period between 1s and a configurable maximum. It is important that the minimum is positive, since a 0s grace period is interpreted by Kubernetes as a "force delete", which may result in inconsistencies due to pods being deleted from etcd before they have stopped running.

Jobs that set a termination period of 0s will be updated in-place at submission to have a 1s termination period. This is to ensure that current workflows, some of which unnecessarily set a 0s termination period, are not disrupted.

Jobs that explicitly set a termination period higher than the limit will be rejected at submission. Jobs that set a termination period greater than 0s but less than 1s will also be rejected at submission.

### Job deadlines

All Armada jobs can be assigned default job deadlines, i.e., jobs have a default maximum runtime after which the job will be killed. Default deadlines are only added to jobs that do not already specify one. To manually specify a deadline, set the `ActiveDeadlineSeconds` field of the podspec embedded in the job.

## Scheduler: implementation

Each scheduling cycle can be seen as a pure function that takes the current state as its input and returns a new desired state. We could express this in code as the following function:

```go
// Schedule determines which queued jobs to schedule and which running jobs to preempt.
func Schedule(
	// Map from queues to the jobs in that queue.
	queues map[Queue][]Job
	// Nodes over which to schedule jobs.
	nodes []Node
    // Map from jobs to nodes. Queued jobs are not assigned to any node.
	nodeByJob map[Job]Node
) map[Job]Node {
	// Scheduling logic 
	... 

	// Return an updated mapping from jobs to nodes. 
	// - nodeByJob[job] == nil && updatedNodeByJob != nil: job was scheduled. 
	// - nodeByJob[job] != nil && updatedNodeByJob == nil: job was preempted. 
	// - nodeByJob[job] == updatedNodeByJob: no change for job. 
	return updatedNodeByJob
}
```

Each scheduling cycle thus produces a mapping from jobs to nodes that is the desired state of the system. It is the responsibility of the rest of the system to reconcile any differences between the actual and desired state. Note that in actuality the scheduler does maintain state between iterations for efficiency and for certain features (e.g., rate-limiting).

Each scheduling cycle in turn consists of the following steps:

1. Eviction
2. Queue ordering
3. Job scheduling:
   1. Job selection
   2. Node selection

Which we express in pseudocode as (a more detailed version of the above snippet):

```go
func Schedule(queues map[Queue][]Job, nodes []Node, nodeByJob map[Job]Node) map[Job]Node {
	queues, nodeByJob := evict(queues, nodeByJob)
	queues = sortQueues(queues) 
	for {
		gang := selectNextQueuedGangToSchedule(queues)
		if gang == nil {
			// No more jobs to schedule.
			break
		}
		nodeByJobForGang := selectNodesForGang(gang, nodes)
		copy(nodeByJob, nodeByJobForGang)
	}
	return nodeByJob
}

func evict(queues map[Queue][]Job, nodeByJob map[Job]Node) (map[Queue][]Job, map[Job]Node) {
	updatedNodeByJob := make(map[Job]Node)
	for job, node := range nodeByJob {
		if isFairSharePreemptible(job) {
			queue := queueFromJob(job)
			queues[queue] = append(queues[queue], job)
		} else {
			updatedNodeByJob[job] = node
		}
	}
	return updatedNodeByJob
}

func sortQueues(queues map[Queue][]Job) map[Queue][]Job {
	updatedQueues := clone(queues)
	for queue, jobs := range queues {
		updatedQueues[queue] = sort(jobs, sortFunc)
	}
	return updatedQueues
}

func selectNextQueuedGangToSchedule(queues map[Queue][]Job) Gang {
	var Gang selectedGang
	var Queue selectedQueue
	for queue, jobs := range queues {
		// Consider jobs in the order they appear in the queue.
		gang := firstGangFromJobs(jobs)
		// Select the queue with smallest fraction of its fair share.
		if fractionOfFairShare(queue, gang) < fractionOfFairShare(selectedQueue, selectedGang) {
			selectedJob = job
			selectedQueue = queue
		}
	}
	// Remove the selected gang from its queue.
	popFirstGang(queues[selectedQueue])
	return gang
}

func selectNodesForGang(gang Gang, nodes []Node) map[Job]Node {
	nodes = clone(nodes)
	nodeByJob := make(map[Job]Node)
	for _, job := range jobsFromGang(gang) {
		node = selectNodeForJob(job, nodes)
		nodeByJob[job] = node
	}
	return nodeByJob
}

func selectNodeForJob(job Job, nodes []Node) Node {
	var Node selectedNode
	for _, node := range nodes {
		if jobFitsOnNode(job, node) {
			if fitScore(job, node) > fitScore(job, selectedNode) {
				selectedNode = node
			} 
		}
	}
	return selectedNode
}
```

Next, we explain eviction and job ordering in more detail.

### Eviction

Eviction is part of the preemption strategy used by Armada. It consists of, at the start of each cycle, moving all currently running preemptible jobs from the nodes to which they are assigned back to their respective queues. As a result, those jobs appear to Armada as if they had never been scheduled and are still queued. We refer to such jobs moved back to the queue as *evicted*.

Whether a job is evicted or not and whether it is assigned to a node in the job scheduling stage or not determines which jobs are scheduled, preempted, or neither. Specifically:

- Not evicted and assigned a node: Queued jobs that should be scheduled.
- Not evicted and not assigned a node: Queued jobs that remain queued.
- Evicted and assigned a node: Running jobs that should remain running.
- Evicted and not assigned a node: Running jobs that should be preempted.

Eviction and (re-)scheduling thus provides a unified mechanism for scheduling and preemption. This approach comes with several benefits:

- No need to maintain separate scheduling and preemption algorithms. Improvements to scheduling also improves preemption.
- We are guaranteed there are no preemptions that do not help scheduling new jobs without needing to check specifically that is the case.
- Preemption and scheduling is consistent in the sense that a job that was preempted will if re-submitted not be scheduled.
- Many-to-many preemption, i.e., one preemption may facilitate scheduling several other jobs.

There are two caveats for which some care needs to be taken:

1. Evicted jobs may only be re-scheduled onto the node from which they were evicted.
2. We should avoid preventing re-scheduling evicted jobs when scheduling new jobs.

To address these issues, Armada maintains a record of which node each job was evicted from that is used when assigning jobs to nodes.


### Job scheduling order

Armada schedules one job at a time, and choosing the order in which jobs are attempted to be scheduled is the mechanism by which Armada ensures resources are divided fairly between queues. In particular, jobs within each queue are totally ordered, but there is no inherent ordering between jobs associated with different queues; the scheduler is responsible for establishing such a global ordering. To divide resources fairly, Armada establishes such a global ordering as follows:

1. Get the topmost gang (which may be a single job) in each queue.
2. For each queue, compute what fraction of its fair share the queue would have if the topmost gang were to be scheduled.
3. Select for scheduling the next gang from the queue for which this computation resulted in the smallest fraction of fair share.

Including the topmost gang in the computation in step 2. is important since it may request thousands of nodes.

This approach is sometimes referred to as progressive filling and is known to achieve max-min fairness, i.e., for an allocation computed in this way, an attempt to increase the allocation of one queue necessarily results in decreasing the allocation of some other queue with equal or smaller fraction of its fair share, under certain conditions, e.g., when the increments are sufficiently small.

## Documentation

For an overview of the architecture and design of Armada, and instructions for submitting jobs, see:


- [Components overview](./docs/design/relationships_diagram.md)
- [Scheduler](./docs/design/scheduler.md)
- [Architecture](./docs/design/architecture.md)
- [User guide](./docs/user.md)
- [Quickstart](./docs/quickstart/index.md)

For a full developer guide, see:

- [Development guide](./docs/developer.md)

For API reference, see:
- [API Documentation](./docs/developer/api.md)

We expect readers of the documentation to have a basic understanding of Docker and Kubernetes; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

## Contributions
Thank you for considering contributing to Armada!
We want everyone to feel that they can contribute to the Armada Project.
Your contributions are valuable, whether it's fixing a bug, implementing a new feature, improving documentation, or suggesting enhancements.
We appreciate your time and effort in helping make this project better for everyone.
For more information about contributing to Armada see [CONTRIBUTING.md](https://github.com/armadaproject/armada/blob/master/CONTRIBUTING.md) and before proceeding to contributions see [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## Discussion

If you are interested in discussing Armada you can find us on  [![slack](https://img.shields.io/badge/slack-armada-brightgreen.svg?logo=slack)](https://cloud-native.slack.com/?redir=%2Farchives%2FC03T9CBCEMC)
