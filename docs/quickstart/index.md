# Armada Quickstart

The main purpose of this guide is to install a minimal local Armada deployment for testing and evaluation purposes.

## Pre-requisites

- Git
- Docker (Docker Desktop recommended for local development on Windows/OSX)
- Helm v3.5+
- Kind v0.11.1+
- Kubectl

## Installation
This guide will install Armada on 3 local Kubernetes clusters; one server and two executor clusters.

Clone [this](https://github.com/armadaproject/armada) repository:

```bash
git clone https://github.com/armadaproject/armada.git
cd armada
```

All commands are intended to be run from the root of the repository.

Armada is a resource intensive application due to the need to run multiple Kubernetes clusters - for a local installation you will need at least 4 CPU cores and 16GB of RAM available.

### One-click Setup

To install Armada and all its dependencies you can use this script: 

```
docs/local/setup.sh
```

Once completed, wait for all pods to be running via `kubectl get pod`

Likewise you can remove the Armada components from your system: 

```
docs/local/destroy.sh
```

## Usage
Create queues, submit some jobs and monitor progress:

### Queue Creation
```bash
./armadactl create queue queue-a --priorityFactor 1
./armadactl create queue queue-b --priorityFactor 2
```
For queues created in this way, user and group owners of the queue have permissions to:
- submit jobs
- cancel jobs
- reprioritize jobs
- watch queue

For more control, queues can be created via `armadactl create`, which allows for setting specific permission; see the following example.

```bash
./armadactl create -f ./docs/quickstart/queue-a.yaml
./armadactl create -f ./docs/quickstart/queue-b.yaml
```


### Job Submission
```
./armadactl submit ./docs/quickstart/job-queue-a.yaml
./armadactl submit ./docs/quickstart/job-queue-b.yaml
```

Watch individual queues:

```bash
./armadactl watch queue-a job-set-1
```
```bash
./armadactl watch queue-b job-set-1
```

Log in to the Grafana dashboard at [http://localhost:30001](http://localhost:30001) using the default credentials of `admin` / `prom-operator`.
Navigate to the Armada Overview dashboard to get a view of jobs progressing through the system.

Try submitting lots of jobs and see queues get built and processed:

```bash
for i in {1..50}
do
  ./armadactl submit ./docs/quickstart/job-queue-a.yaml
  ./armadactl submit ./docs/quickstart/job-queue-b.yaml
done
```

## Example output:

CLI:

```bash
$ ./armadactl watch queue-a job-set-1
Watching job set job-set-1
Nov  4 11:43:36 | Queued:   0, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobSubmittedEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:36 | Queued:   1, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobQueuedEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:36 | Queued:   1, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobSubmittedEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:36 | Queued:   2, Leased:   0, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobQueuedEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:38 | Queued:   1, Leased:   1, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobLeasedEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:38 | Queued:   0, Leased:   2, Pending:   0, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobLeasedEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:38 | Queued:   0, Leased:   1, Pending:   1, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobPendingEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:43:38 | Queued:   0, Leased:   0, Pending:   2, Running:   0, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobPendingEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:41 | Queued:   0, Leased:   0, Pending:   1, Running:   1, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobRunningEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:43:41 | Queued:   0, Leased:   0, Pending:   0, Running:   2, Succeeded:   0, Failed:   0, Cancelled:   0 | event: *api.JobRunningEvent, job id: 01drv3mey2mzmayf50631tzp9m
Nov  4 11:44:17 | Queued:   0, Leased:   0, Pending:   0, Running:   1, Succeeded:   1, Failed:   0, Cancelled:   0 | event: *api.JobSucceededEvent, job id: 01drv3mf7b6fd1rraeq1f554fn
Nov  4 11:44:26 | Queued:   0, Leased:   0, Pending:   0, Running:   0, Succeeded:   2, Failed:   0, Cancelled:   0 | event: *api.JobSucceededEvent, job id: 01drv3mey2mzmayf50631tzp9m
```


### Grafana Configuration

Run the following commands to setup Grafana in your environment:

```bash
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-0","type":"prometheus","url":"http://'$EXECUTOR_0_IP':30001","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-1","type":"prometheus","url":"http://'$EXECUTOR_1_IP':30001","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/dashboards/import --data-binary @./docs/quickstart/grafana-armada-dashboard.json -H "Content-Type: application/json"
```

Grafana:

![Armada Grafana dashboard](./img/grafana-screenshot.png "Armada Grafana dashboard")

Note that the jobs in this demo simply run the `sleep` command so do not consume many resources.

### Lookout Configuration

Armada Lookout UI can be configured by doing the following:

```bash
kubectl port-forward svc/armada-lookout 8080:8080 
```
Then access it by opening [http://localhost:8080](http://localhost:8080) in your browser.

![Lookout UI](./img/lookout.png "Lookout UI")
