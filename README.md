# Armada

![Armada](./logo.svg)

[![CircleCI](https://circleci.com/gh/helm/helm.svg?style=shield)](https://circleci.com/gh/G-Research/armada)
[![Go Report Card](https://goreportcard.com/badge/github.com/G-Research/armada)](https://goreportcard.com/report/github.com/G-Research/armada)

Armada is an experimental application to achieve high throughput of run-to-completion jobs on multiple Kubernetes clusters.

It stores queues for users/projects with pod specifications and creates these pods once there is available resource in one of the connected Kubernetes clusters.

## Documentation

- [Design Documentation](./docs/design.md)
- [Development Guide](./docs/developer.md)
- [User Guide](./docs/user.md)
- [Installation in Production](./docs/production-install.md)

## Key features
- Armada maintains fair resource share over time (inspired by HTCondor priority)
- It can handle large amounts of queued jobs (million+)
- It allows adding and removing clusters from the system without disruption
- By utilizing multiple Kubernetes clusters the system can scale beyond the limits of a single Kubernetes cluster

## Key concepts

**Queue:** Represent user or project, used to maintain fair share over time, has priority factor

**Job:** Unit of work to be run (described as Kubernetes PodSpec)

**Job Set:** Group of related jobs, api allows observing progress of job set together


## Try it out locally

Prerequisites: 
* Git
* Go 1.12+ 
* Docker installed. Ensure the current user has permission to run the `docker` command without sudo.
* 3GB Disk space

1. Clone repository & build
```bash
git clone https://github.com/G-Research/armada.git
cd armada
make build
```

2. Get Kind (Installation help [here](https://kind.sigs.k8s.io/docs/user/quick-start/))

Kind is Kubernetes in Docker. It allows us to easily run a local Kubernetes cluster using Docker.

```bash
GO111MODULE="on" go get sigs.k8s.io/kind@v0.5.1
```
 
3. Create 2 Kind clusters

```bash
kind create cluster --name demoA --config ./example/kind-config.yaml
kind create cluster --name demoB --config ./example/kind-config.yaml 
```

4. Start Redis
```bash
docker run -d -p 6379:6379 redis
```

5. Start server in one terminal
```bash
./bin/server
```

6. Start executor for demoA cluster in a new terminal
```bash
KUBECONFIG=$(kind get kubeconfig-path --name="demoA") ARMADA_APPLICATION_CLUSTERID=demoA ARMADA_METRICSPORT=9001 ./bin/executor
```

7. Start executor for demoB cluster in a new terminal
```bash
KUBECONFIG=$(kind get kubeconfig-path --name="demoB") ARMADA_APPLICATION_CLUSTERID=demoB ARMADA_METRICSPORT=9002 ./bin/executor
```

8. Create queue, submit jobs and watch progress
```bash
./bin/armadactl create-queue test --priorityFactor 1
./bin/armadactl submit ./example/jobs.yaml
./bin/armadactl watch job-set-1
```
