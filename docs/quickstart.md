# Armada Quickstart

The purpose of this guide is to install a minimal local Armada deployment for testing and evaluation purposes.

## Pre-requisites
- Git
- Docker
- Helm
- Kind
- Kubectl

N.B.  Ensure the current user has permission to run the `docker` command without `sudo`.

## Installation
This guide will install Armada on 3 local Kubernetes clusters; one server and two executor clusters. 
This repository must be cloned locally. All commands are intended to be run from the root of the repository.

### Cluster creation

First create the 3 required Kind clusters:
```
kindImage=kindest/node:v1.13.10
kind create cluster --image $kindImage --name quickstart-armada-server --config ./docs/quickstart/kind-config-server.yaml
kind create cluster --image $kindImage --name quickstart-armada-executor-0 --config ./docs/quickstart/kind-config-executor-0.yaml
kind create cluster --image $kindImage --name quickstart-armada-executor-1 --config ./docs/quickstart/kind-config-executor-1.yaml
```

Ensure all clusters are up and running before continuing.

### Server deployment
```
export KUBECONFIG=$(kind get kubeconfig-path --name="quickstart-armada-server")

# Configure Helm
helm init && kubectl apply -f docs/quickstart/server-helm-clusterrolebinding.yaml
kubectl wait --for=condition=available --timeout=600s deployment/tiller-deploy -n kube-system

# Install Redis
helm install stable/redis-ha --name=redis --set persistentVolume.enabled=false --set hardAntiAffinity=false

# Install Prometheus
helm install stable/prometheus-operator --name=prometheus-operator --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false --set grafana.service.type=NodePort --set grafana.service.nodePort=30001 --set prometheus.service.type=NodePort

# Install Armada server
helm template ./deployment/armada -f ./docs/quickstart/server-values.yaml --set prometheus.enabled=true | kubectl apply -f -
```
### Executor deployments

First executor:
```
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
export KUBECONFIG=$(kind get kubeconfig-path --name="quickstart-armada-executor-0")	

# Configure Helm
helm init && kubectl apply -f docs/quickstart/server-helm-clusterrolebinding.yaml
kubectl wait --for=condition=available --timeout=600s deployment/tiller-deploy -n kube-system

# Install Prometheus
helm install stable/prometheus-operator --name=prometheus-operator --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false --set prometheus.service.type=NodePort --set prometheus.service.nodePort=30002
kubectl apply -f docs/quickstart/prometheus-kubemetrics-rules.yaml

# Install executor
helm template ./deployment/executor --set applicationConfig.armada.url="$DOCKERHOSTIP:30000" | kubectl apply -f -
```
Second executor:
```
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
export KUBECONFIG=$(kind get kubeconfig-path --name="quickstart-armada-executor-1")	

# Configure Helm
helm init && kubectl apply -f docs/quickstart/server-helm-clusterrolebinding.yaml
kubectl wait --for=condition=available --timeout=600s deployment/tiller-deploy -n kube-system

# Install Prometheus
helm install stable/prometheus-operator --name=prometheus-operator --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false --set prometheus.service.type=NodePort --set prometheus.service.nodePort=30003
kubectl apply -f docs/quickstart/prometheus-kubemetrics-rules.yaml

# Install executor
helm template ./deployment/executor --set applicationConfig.armada.url="$DOCKERHOSTIP:30000" | kubectl apply -f -
```
### Grafana configuration

```
export DOCKERHOSTIP=$(ip -4 addr show docker0 | grep -Po 'inet \K[\d.]+')
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-0","type":"prometheus","url":"http://'$DOCKERHOSTIP':30002","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/datasources -H "Content-Type: application/json" -d '{"name":"cluster-1","type":"prometheus","url":"http://'$DOCKERHOSTIP':30003","access":"proxy","basicAuth":false}'
curl -X POST -i http://admin:prom-operator@localhost:30001/api/dashboards/import --data-binary @./docs/quickstart/grafana-armada-dashboard.json -H "Content-Type: application/json"
```
### CLI installation

The following steps download the `armadactl` CLI to the current directory:
```
armadaRelease=v0.0.1
armadaCtlTar=armadactl-$armadaRelease-linux-amd64.tar
curl -LO https://github.com/G-Research/armada/releases/download/$armadaRelease/$armadaCtlTar.gz
gunzip $armadaCtlTar.gz && tar xf $armadaCtlTar && rm $armadaCtlTar
chmod u+x armadactl
```

## Usage
Create queues, submit some jobs and monitor progress:
```
./armadactl --armadaUrl=localhost:30000 create-queue queue-a --priorityFactor 1
./armadactl --armadaUrl=localhost:30000 create-queue queue-b --priorityFactor 2
./armadactl --armadaUrl=localhost:30000 submit ./docs/quickstart/jobs.yaml
./armadactl --armadaUrl=localhost:30000 watch job-set-1
```
Log in to the Grafana dashboard at http://localhost:30001/ using the default credentials of `admin` / `prom-operator`.
Navigate to the Armada Overview dashboard to get a view of jobs progressing through the system.

Try submitting lots of jobs and see queues build and get processed:

```
for i in {1..100}
do
  ./armadactl --armadaUrl=localhost:30000 submit ./docs/quickstart/jobs.yaml
done
```
