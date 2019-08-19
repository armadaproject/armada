Disclaimer: This document is in progress, anything can change or can be out of date with current implementation

# High level design of Armada
Armada is job queueing system for multiple Kubernetes clusters.

## Goals for the project
- Easily handle large queues of jobs (million +)
- Enforce fair share over time
- Failure of few clusters should not cause an outage
- Reasonable latency between job submission and job start when there is resource available (in order of 10s)
- Maximize utilization of the cluster
- Smart queue instead of scheduler - implement only minimum logic needed on global level, let cluster scheduler do its own work.
- All components should be highly available

## Data model
### Job
Job is executable unit, currently contains Kubernetes Pod specification.

### Job Set
All jobs are grouped into Job Sets with user specified identifier. Job set represent project or other higher level unit of work. Users can observer events in Job Sets through api.

### Queue
All jobs needs to be placed into queues. Resources allocation is controlled using queues.

Queues has its own priority (lower number makes queue more important). Queue current priority is calculated from combination of resources used by jobs from the queue over time and queue priority. Current priority is used to decide which jobs to run first.

Usual setup maps users or teams one to one to queues to control resource usage.

## Proposed design
![Diagram](./batch-api.svg)

### Cluster Executor
Cluster executor is component running on each Kubernetes cluster. It keeps all pods and nodes information in memory and manages jobs within the cluster.
It proactively reports current state of the cluster and asks for jobs to run.
Executor can also refuse to execute assigned job and return it.

### Armada server
Armada server is central component which manages queues of jobs.
It stores all active jobs in Job database (current implementation use Redis).

#### Accounting
Executor periodically reports resource usage details to Armada server. 
Usage is recorded in database and used to update priorities of individual queues.

#### Job Leasing
Executor periodically ask server for jobs to run reporting available resources. Armada distributes these available resources among queues according to queue current priority. 
Jobs from the top of each queue which fit into the allocated resources are provided to be executed in the cluster. 
Jobs are marked as Leased with a time stamp. Executor needs to renew 
job leases otherwise leases expire and jobs will be considered failed and executed on different cluster.

#### Job Events
All jobs are grouped in Job Sets with user defined identifier. Armada records events of all jobs in job sets and makes them available through api. 

Armada records all necessary events to fully reconstruct state of the job at any time. This allows us to erase all job data from Jobs database after the job finishes and keep only the events.

Current implementation utilise Redis streams to store job events.
