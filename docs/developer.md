# Developer setup

## Getting started 

There are many ways you can setup you local environment, this is just a basic quick example of how to setup everything you'll need to get started running and developing Armada.

### Pre-requisites
To follow this section it is assumed you have:
* Golang >= 1.12 installed (https://golang.org/doc/install)
* `kubectl` installed (https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* Docker installed, configured for the current user
* This repository cloned. The guide will assume you are in the root directory of this repository

### Running Armada locally

It is possible to develop Armada locally with [kind](https://github.com/kubernetes-sigs/kind) Kubernetes clusters.

1. Get kind (Installation help [here](https://kind.sigs.k8s.io/docs/user/quick-start/))
```bash
GO111MODULE="on" go get sigs.k8s.io/kind@v0.5.1
``` 
2. Create kind clusters (you can create any number of clusters)

As this step is using Docker, it will require root to run

```bash
kind create cluster --name demoA --config ./example/kind-config.yaml
kind create cluster --name demoB --config ./example/kind-config.yaml 
```
3. Start Redis
```bash
docker run -d -p 6379:6379 redis
```
4. Start server in one terminal
```bash
go run ./cmd/armada/main.go
```
5. Start executor for demoA in a new terminal
```bash
KUBECONFIG=$(kind get kubeconfig-path --name="demoA") ARMADA_APPLICATION_CLUSTERID=demoA ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go
```
6. Start executor for demoB in a new terminal
```bash
KUBECONFIG=$(kind get kubeconfig-path --name="demoB") ARMADA_APPLICATION_CLUSTERID=demoB ARMADA_METRIC_PORT=9002 go run ./cmd/executor/main.go
```
7. Create queue & Submit job
```bash
go run ./cmd/armadactl/main.go create-queue test --priorityFactor 1
go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
go run ./cmd/armadactl/main.go watch test job-set-1
```

For more details on submitting jobs to Armada, see [here](usage.md#submitting-jobs).

Once you submit jobs, you should be able to see pods appearing in your cluster(s), running what you submitted.


**Note:** Depending on your Docker setup you might need to load images for jobs you plan to run manually:
```bash
kind load docker-image busybox:latest
```

### Running tests
For unit tests run
```bash
make tests
```

For end to end tests run:
```bash
make tests-e2e
# optionally stop kubernetes cluster which was started by test
make e2e-stop-cluster
```

## Code Generation

This project uses code generation.

The armada api is defined using proto files which are used to generate Go source code (and the c# client) for our gRPC communication.

To generate source code from proto files:

```
make proto
```

### Command-line tools

Our command-line tools used the cobra framework (https://github.com/spf13/cobra).

You can use the cobra cli to add new commands, the below will describe how to add new commands for `armadactl` but it can be applied to any of our command line tools.

##### Steps

Get cobra cli tool:

```
go get -u github.com/spf13/cobra/cobra
```

Change to the directory of the command-line tool you are working on:

```
cd ./cmd/armadactl
```

Use cobra to add new command:

```
cobra add commandName
```

You should see a new file appear under `./cmd/armadactl/cmd` with the name you specified in the command.
