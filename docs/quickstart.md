# Armada Quickstart

The purpose of this guide is to install a minimal local Armada deployment for testing and evaluation purposes.

## Pre-requisites

- Git
- Docker
- Helm v3
- Kind
- Kubectl
### OS specifics

#### Linux

Ensure the current user has permission to run the `docker` command without `sudo`.

#### macOS

You can install the pre-requisites with [Homebrew](https://brew.sh):

```bash
brew cask install docker
brew install helm kind kubernetes-cli
```

Ensure at least 5GB of RAM are allocated to the Docker VM (see Preferences -> Resources -> Advanced).

### Helm

Make sure helm is configured to use the official Helm stable charts:

```bash
helm repo add stable https://kubernetes-charts.storage.googleapis.com/
```

## Installation
This guide will install Armada on 3 local Kubernetes clusters; one server and two executor clusters. 

Set the `ARMADA_VERSION` environment variable and clone this repository with the same version tag as you are installing. For example to install version `v0.1.1`:
```bash
export ARMADA_VERSION=v0.1.1
git clone https://github.com/G-Research/armada.git --branch $ARMADA_VERSION
cd armada
```

All commands are intended to be run from the root of the repository.

### Server deployment

```bash
kind create cluster --name quickstart-armada-server --config ./docs/quickstart/kind-config-server.yaml

# Install Redis
helm install redis stable/redis-ha -f docs/quickstart/redis-values.yaml

# Install Prometheus
helm install prometheus-operator stable/prometheus-operator -f docs/quickstart/server-prometheus-values.yaml

# Install Armada server
helm template ./deployment/armada --set image.tag=$ARMADA_VERSION -f ./docs/quickstart/server-values.yaml | kubectl apply -f -
```
### Executor deployments

First executor:
```bash
kind create cluster --name quickstart-armada-executor-0 --config ./docs/quickstart/kind-config-executor-0.yaml
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')

# Install Prometheus
helm install prometheus-operator stable/prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml --set prometheus.service.nodePort=30002

# Install executor
helm template ./deployment/executor --set image.tag=$ARMADA_VERSION --set applicationConfig.apiConnection.armadaUrl="$DOCKERHOSTIP:30000" --set applicationConfig.apiConnection.forceNoTls=true --set prometheus.enabled=true --set applicationConfig.kubernetes.minimumPodAge=0s | kubectl apply -f -
helm template ./deployment/executor-cluster-monitoring -f docs/quickstart/executor-cluster-monitoring-values.yaml --set interval=5s | kubectl apply -f -
```
Second executor:
```bash
kind create cluster --name quickstart-armada-executor-1 --config ./docs/quickstart/kind-config-executor-1.yaml
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')

# Install Prometheus
helm install prometheus-operator stable/prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml --set prometheus.service.nodePort=30003

# Install executor
helm template ./deployment/executor --set image.tag=$ARMADA_VERSION --set applicationConfig.apiConnection.armadaUrl="$DOCKERHOSTIP:30000" --set applicationConfig.apiConnection.forceNoTls=true --set prometheus.enabled=true --set applicationConfig.kubernetes.minimumPodAge=0s | kubectl apply -f -
helm template ./deployment/executor-cluster-monitoring -f docs/quickstart/executor-cluster-monitoring-values.yaml --set interval=5s | kubectl apply -f -
```
### Grafana configuration

```bash
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-0","type":"prometheus","url":"http://'$DOCKERHOSTIP':30002","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-1","type":"prometheus","url":"http://'$DOCKERHOSTIP':30003","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/dashboards/import --data-binary @./docs/quickstart/grafana-armada-dashboard.json -H "Content-Type: application/json"
```
### CLI installation

The following steps download the `armadactl` CLI to the current directory:
```bash
armadaCtlTar=armadactl-$ARMADA_VERSION-linux-amd64.tar
curl -LO https://github.com/G-Research/armada/releases/download/$ARMADA_VERSION/$armadaCtlTar.gz
gunzip $armadaCtlTar.gz && tar xf $armadaCtlTar && rm $armadaCtlTar
chmod u+x armadactl
```

## Usage
Create queues, submit some jobs and monitor progress:
```bash
./armadactl --armadaUrl=localhost:30000 create-queue queue-a --priorityFactor 1
./armadactl --armadaUrl=localhost:30000 create-queue queue-b --priorityFactor 2
./armadactl --armadaUrl=localhost:30000 submit ./docs/quickstart/job-queue-a.yaml
./armadactl --armadaUrl=localhost:30000 submit ./docs/quickstart/job-queue-b.yaml
```

Watch individual queues:
```bash
./armadactl --armadaUrl=localhost:30000 watch queue-a job-set-1
```
```bash
./armadactl --armadaUrl=localhost:30000 watch queue-b job-set-1
```

Log in to the Grafana dashboard at http://localhost:30001/ using the default credentials of `admin` / `prom-operator`.
Navigate to the Armada Overview dashboard to get a view of jobs progressing through the system.

Try submitting lots of jobs and see queues build and get processed:

```bash
for i in {1..50}
do
  ./armadactl --armadaUrl=localhost:30000 submit ./docs/quickstart/job-queue-a.yaml
  ./armadactl --armadaUrl=localhost:30000 submit ./docs/quickstart/job-queue-b.yaml
done
```

## Example output:

CLI:

```bash
$ ./armadactl --armadaUrl=localhost:30000 watch queue-a job-set-1
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

Grafana:

![Armada Grafana dashboard](./quickstart/grafana-screenshot.png "Armada Grafana dashboard")

Note that the jobs in this demo simply run the `sleep` command so do not consume much resource.
