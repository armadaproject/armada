# High level design of Armada
Armada is job queueing system for multiple Kubernetes clusters.

## Goals for the project
- Easily handle large queues of jobs (million +)
- Enforce fair share over time
- Failure of some worker clusters should not cause an outage
- Reasonable latency between job submission and job start when there is resource available (in order of 10s)
- Maximize utilization of the cluster
- Smart queueing instead of scheduler - implement only minimum logic needed on global level, let cluster scheduler do its own work.
- All components should be highly available

## Data model
### Job
A Job is an executable unit, currently containing a Kubernetes pod specification.

### Job Set
All jobs are grouped into Job Sets with user-specified identifier. A Job Set represents a project or other higher level unit of work. Users can observe events in Job Sets through the API.

### Queue
All jobs need to be placed into queues. Resources allocation is controlled using queues.

**Queue Current Priority**: Current priority is calculated from resource usage of jobs in the queue. This number approaches the amount of resource used by the queue with configurable speed by `priorityHalfTime` configuration. If the queue priority is `A` and queue is using `B` amount of resource, after time defined by `priorityHalfTime` the new priority will be `A + (B - A) / 2`.

**Queue Priority Factor**: Each queue has priority factor which determines how important the queue is (lower number makes queue more important).

**Queue Effective Priority** = **Queue Priority Factor** * **Queue Current Priority**

To achieve fairness between queues, when Armada schedules jobs resources are divided based on Queue Effective Priority.

More information about queue priority and scheduling can be found [here](./priority.md)

## Design
![Diagram](./batch-api.svg)

### Armada Server
The Armada Server is a central component which manages queues of jobs.
It stores all active jobs in the job database (current implementation use Redis).

### Cluster Executor
The Cluster Executor is a component running on each Kubernetes worker cluster. It keeps all pod and node information in memory and manages jobs within the cluster.
It proactively reports the current state of the cluster and asks for jobs to run.
Queue Usage is recorded in the database and is used to update priorities of individual queues.
The executor can also refuse to execute an assigned job and return it.

#### Job Leasing
Executors periodically ask the server for jobs to run, reporting available resources. Armada distributes these available resources among queues according to Queue Effective Priority. 
Jobs are taken from the top of each queue until the available resource is filled. These jobs are then returned to the executor to be executed on the cluster and marked as leased with a timestamp to show when the lease began.

The executor must regularly renew the lease of all jobs it leases, otherwise leases expire and jobs will be considered failed and executed on different cluster.

#### Job Events
Job events are used to show when a job reaches a new state, such as submitted, running, completed. They hold generic information about events (such as created-time) along with state specific information (such as exit-code for completed jobs).

Armada records events of all jobs against the job set the job belongs to. Events for a given job set are available through the API.

Armada records all necessary events to fully reconstruct state of the job at any time. This allows us to erase all job data from the jobs database after the job finishes and keep only the events.

The current implementation utilises Redis streams to store job events.
