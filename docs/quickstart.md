# Armada Quickstart

The purpose of this guide is to install a minimal local Armada deployment for testing and evaluation purposes.

## Pre-requisites
- Linux OS
- Git
- Docker
- Helm
- Kind
- Kubectl

N.B.  Ensure the current user has permission to run the `docker` command without `sudo`.

## Installation
This guide will install Armada on 3 local Kubernetes clusters; one server and two executor clusters. 

Set `ARMADA_VERSION` environment variable and clone this repository repository with the same version tag as you are installing. For example to install version `v0.0.3`:
```bash
export ARMADA_VERSION=v0.0.3
git clone https://github.com/G-Research/armada.git --branch $ARMADA_VERSION
```

All commands are intended to be run from the root of the repository.

### Cluster creation

First create the 3 required Kind clusters:
```bash
kindImage=kindest/node:v1.13.10
kind create cluster --image $kindImage --name quickstart-armada-server --config ./docs/quickstart/kind-config-server.yaml
kind create cluster --image $kindImage --name quickstart-armada-executor-0 --config ./docs/quickstart/kind-config-executor-0.yaml
kind create cluster --image $kindImage --name quickstart-armada-executor-1 --config ./docs/quickstart/kind-config-executor-1.yaml
```

Ensure all clusters are up and running before continuing.

### Server deployment

N.B. The official Prometheus chart is complex and may take up to 1 minute to install in the following instructions.

```bash
export KUBECONFIG=$(kind get kubeconfig-path --name="quickstart-armada-server")

# Configure Helm
helm init && kubectl apply -f docs/quickstart/server-helm-clusterrolebinding.yaml
kubectl wait --for=condition=available --timeout=600s deployment/tiller-deploy -n kube-system

# Install Redis
helm install stable/redis-ha --name=redis -f docs/quickstart/redis-values.yaml

# Install Prometheus
helm install stable/prometheus-operator --name=prometheus-operator -f docs/quickstart/server-prometheus-values.yaml

# Install Armada server
helm template ./deployment/armada --set image.tag=$ARMADA_VERSION -f ./docs/quickstart/server-values.yaml | kubectl apply -f -
```
### Executor deployments

First executor:
```bash
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
export KUBECONFIG=$(kind get kubeconfig-path --name="quickstart-armada-executor-0")	

# Configure Helm
helm init && kubectl apply -f docs/quickstart/server-helm-clusterrolebinding.yaml
kubectl wait --for=condition=available --timeout=600s deployment/tiller-deploy -n kube-system

# Install Prometheus
helm install stable/prometheus-operator --name=prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml --set prometheus.service.nodePort=30002
kubectl apply -f docs/quickstart/prometheus-kubemetrics-rules.yaml

# Install executor
helm template ./deployment/executor --set image.tag=$ARMADA_VERSION --set applicationConfig.armada.url="$DOCKERHOSTIP:30000" | kubectl apply -f -
```
Second executor:
```bash
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
export KUBECONFIG=$(kind get kubeconfig-path --name="quickstart-armada-executor-1")	

# Configure Helm
helm init && kubectl apply -f docs/quickstart/server-helm-clusterrolebinding.yaml
kubectl wait --for=condition=available --timeout=600s deployment/tiller-deploy -n kube-system

# Install Prometheus
helm install stable/prometheus-operator --name=prometheus-operator -f docs/quickstart/executor-prometheus-values.yaml --set prometheus.service.nodePort=30003
kubectl apply -f docs/quickstart/prometheus-kubemetrics-rules.yaml

# Install executor
helm template ./deployment/executor --set image.tag=$ARMADA_VERSION --set applicationConfig.armada.url="$DOCKERHOSTIP:30000" | kubectl apply -f -
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
armadaRelease=v0.0.1
armadaCtlTar=armadactl-$armadaRelease-linux-amd64.tar
curl -LO https://github.com/G-Research/armada/releases/download/$armadaRelease/$armadaCtlTar.gz
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
./armadactl --armadaUrl=localhost:30000 watch job-set-1
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
$ ./armadactl --armadaUrl=localhost:30000 watch job-set-1
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
