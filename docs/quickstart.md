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

All the commands below should be executed in Git Bash.

### Helm

Make sure helm is configured to use the official Helm stable charts:

```bash
helm repo add stable https://kubernetes-charts.storage.googleapis.com/
```

And add the G-Research helm charts repository, too:

```bash
helm repo add gresearch https://g-research.github.io/charts/
```

## Installation
This guide will install Armada on 3 local Kubernetes clusters; one server and two executor clusters. 

You should then clone this repository and step into it:

```bash
git clone https://github.com/G-Research/armada.git
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
helm install armada-server gresearch/armada -f ./docs/quickstart/server-values.yaml

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
helm install armada-executor gresearch/armada-executor --set applicationConfig.apiConnection.armadaUrl="$SERVER_IP:30000" -f docs/quickstart/executor-values.yaml
helm install armada-executor-cluster-monitoring gresearch/executor-cluster-monitoring -f docs/quickstart/executor-cluster-monitoring-values.yaml

# Get executor IP for Grafana
EXECUTOR_0_IP=$(kubectl get nodes quickstart-armada-executor-0-worker -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
```

Second executor:

```bash
kind create cluster --name quickstart-armada-executor-1 --config ./docs/quickstart/kind-config-executor.yaml

# Install Prometheus
helm install prometheus-operator stable/prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml

# Install executor
helm install armada-executor gresearch/armada-executor --set applicationConfig.apiConnection.armadaUrl="$SERVER_IP:30000" -f docs/quickstart/executor-values.yaml
helm install armada-executor-cluster-monitoring gresearch/executor-cluster-monitoring -f docs/quickstart/executor-cluster-monitoring-values.yaml

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
#!/bin/bash

echo "Downloading armadactl for your platform"

# Determine Platform
SYSTEM=$(uname | sed 's/MINGW.*/windows/' | tr A-Z a-z)
if [ $SYSTEM == "windows" ]; then
  ARCHIVE_TYPE=zip
  UNARCHIVE="zcat > armadactl.exe"
else
  ARCHIVE_TYPE=tar.gz
  UNARCHIVE="tar xzf -"
fi

# Find the latest Armada version
LATEST_GH_URL=$(curl -fsSLI -o /dev/null -w %{url_effective} https://github.com/G-Research/armada/releases/latest)
ARMADA_VERSION=${LATEST_GH_URL##*/}
ARMADACTL_URL="https://github.com/G-Research/armada/releases/download/$ARMADA_VERSION/armadactl-$ARMADA_VERSION-$SYSTEM-amd64.$ARCHIVE_TYPE"

# Download and untar/unzip armadactl
if curl -sL $ARMADACTL_URL | sh -c "$UNARCHIVE" ; then
	echo "armadactl downloaded successfully"
else
	echo "Something is amiss!"
	echo "Please visit:"
	echo "  - https://github.com/G-Research/armada/releases/latest"
	echo "to find the latest armadactl binary for your platform"
fi
```

Alternatively, you can find the latst armadactl binaries at:

  * [https://github.com/G-Research/armada/releases/latest](https://github.com/G-Research/armada/releases/latest)

Simply download the latest release for your platform and unzip or untar.


## Usage
Create queues, submit some jobs and monitor progress:

```bash
./armadactl create-queue queue-a --priorityFactor 1
./armadactl create-queue queue-b --priorityFactor 2
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

Try submitting lots of jobs and see queues build and get processed:

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

Grafana:

![Armada Grafana dashboard](./quickstart/grafana-screenshot.png "Armada Grafana dashboard")

Note that the jobs in this demo simply run the `sleep` command so do not consume much resource.
