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

#### Windows

You can install the pre-requisites with [Chocolatey](https://chocolatey.org):

```cmd
choco install git docker-desktop kubernetes-helm kind kubernetes-cli
```

Ensure at least 5GB of RAM are allocated to the Docker VM (see Settings -> Resources -> Advanced).

In order to follow the instructions below as-is, you should also install [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/about).
After you installed the distro of your choice, open a terminal and alias the following commands:

```bash
alias git=git.exe
alias docker=docker.exe
alias helm=helm.exe
alias kind=kind.exe
alias kubectl=kubectl.exe
```

All the commands below should be executed in a Windows directory (i.e. `/mnt/c/Users/<username>`) so change to it before going further.

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

# Get server IP for executors
SERVER_IP=$(kubectl get nodes quickstart-armada-server-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
```
### Executor deployments

First executor:
```bash
kind create cluster --name quickstart-armada-executor-0 --config ./docs/quickstart/kind-config-executor.yaml

# Install Prometheus
helm install prometheus-operator stable/prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml

# Install executor
helm template ./deployment/executor --set image.tag=$ARMADA_VERSION --set applicationConfig.apiConnection.armadaUrl="$SERVER_IP:30000" --set applicationConfig.apiConnection.forceNoTls=true --set prometheus.enabled=true --set applicationConfig.kubernetes.minimumPodAge=0s | kubectl apply -f -
helm template ./deployment/executor-cluster-monitoring -f docs/quickstart/executor-cluster-monitoring-values.yaml --set interval=5s | kubectl apply -f -

# Get executor IP for Grafana
EXECUTOR_0_IP=$(kubectl get nodes quickstart-armada-executor-0-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
```
Second executor:
```bash
kind create cluster --name quickstart-armada-executor-1 --config ./docs/quickstart/kind-config-executor.yaml

# Install Prometheus
helm install prometheus-operator stable/prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml

# Install executor
helm template ./deployment/executor --set image.tag=$ARMADA_VERSION --set applicationConfig.apiConnection.armadaUrl="$SERVER_IP:30000" --set applicationConfig.apiConnection.forceNoTls=true --set prometheus.enabled=true --set applicationConfig.kubernetes.minimumPodAge=0s | kubectl apply -f -
helm template ./deployment/executor-cluster-monitoring -f docs/quickstart/executor-cluster-monitoring-values.yaml --set interval=5s | kubectl apply -f -

# Get executor IP for Grafana
EXECUTOR_1_IP=$(kubectl get nodes quickstart-armada-executor-1-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
```
### Grafana configuration

```bash
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-0","type":"prometheus","url":"http://'$EXECUTOR_0_IP':30001","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-1","type":"prometheus","url":"http://'$EXECUTOR_1_IP':30001","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/dashboards/import --data-binary @./docs/quickstart/grafana-armada-dashboard.json -H "Content-Type: application/json"
```
### CLI installation

The following steps download the `armadactl` CLI to the current directory:
```bash
curl -L https://github.com/G-Research/armada/releases/download/$ARMADA_VERSION/armadactl-$ARMADA_VERSION-$(uname)-amd64.tar.gz | tar xzf -
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
