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

## Proposed design

### Job Queues and submission API
Any jobs submitted to api will be added to a specific queue.
Based on queue priorities (priority is determined by past resource usage by jobs from the particular queue) jobs are offered to cluster executors to be run.

### Cluster Executor
Cluster executor is component running on each Kubernetes cluster. It keeps all pods and nodes information in memory and manages jobs within the cluster.
It proactively reports current state of the cluster and asks for jobs to run.
Executor can also refuse to execute assigned job and return it.

### Accounting 
Accounting monitors individual queues resource usages. This is used to calculate queue priority when deciding which Jobs to run first.

### Job monitoring (Events)
All job events are recorded and contain all necessary information to reconstruct state of the job in any time. This will allow to erase all other data about jobs after it finishes except this stream of events.
Jobs can be grouped in JobSets. Job Events from jobs in particular JobSet will be exposed to user through api.

![Diagram](./batch-api.svg)
