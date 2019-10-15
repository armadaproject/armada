# Developer setup

[Getting Started](#getting-started)

[Code Generation](#code-generation)
* [gRPC](#grpc)
* [Command line tools](#command-line-tools)

# Getting Started 

There are many ways you can setup you local environment, this is just a basic quick example of how to setup everything you'll need to get started running and developing Armada.

### Pre-requisites
To follow this section I am assuming you have:
* Golang >= 1.12 installed (https://golang.org/doc/install)
* Kubectl installed (https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* Docker installed (ideally in the sudo group)
* This repository cloned. The guide will assume you are in the root directory of this repository

#### Steps

1. Set up a Kubernetes cluster, this can be a local instance such as Kind (https://github.com/kubernetes-sigs/kind)
    * For Kind simply run `GO111MODULE="on" go get sigs.k8s.io/kind@v0.5.1 && kind create cluster`
2. Put the kubernetes config file so your kubectl can find it. %HOME/.kube/config or set env variable KUBECONFIG=/config/file/location
    * If using Kind, you can find the config file location by running command: `kind get kubeconfig-path`
3. Start redis with default values `docker run -d --expose=6379 --network=host redis`
    * You may need to run this as sudo
4. In separate terminals run:
    * go run /cmd/armada/main.go
    * go run /cmd/executor/main.go
    
You now have Armada setup and can submit jobs to it, see [here](usage.md#submitting-jobs). 

Likely you'll want to run the last steps via an IDE to make developing easier, so you can benefit from debug features etc.
    

#### Multi cluster Kind

It is possible to develop Armada locally with [kind](https://github.com/kubernetes-sigs/kind) Kubernetes clusters.

```bash
# Download Kind
go get sigs.k8s.io/kind
 
# create 2 clusters
kind create cluster --name demoA --config ./example/kind-config.yaml
kind create cluster --name demoB --config ./example/kind-config.yaml 

# run armada
go run ./cmd/armada/main.go

# run executors for each cluster
KUBECONFIG=$(kind get kubeconfig-path --name="demoA") ARMADA_APPLICATION_CLUSTERID=demoA go run ./cmd/executor/main.go
KUBECONFIG=$(kind get kubeconfig-path --name="demoB") ARMADA_APPLICATION_CLUSTERID=demoB go run ./cmd/executor/main.go
```

Depending on your docker setup you might need to load images for jobs you plan to run manually 
```bash
kind load docker-image busybox:latest
```

#### Using Armada locally

The most basic example would be:

```
# Create queue
go run ./cmd/armadactl/main.go create-queue test 1

# Submit example job
go run ./cmd/armadactl/main.go submit ./example/jobs.yaml

# Watch events of example job
go run ./cmd/armadactl/main.go  watch job-set-1
```

For more details on submitting jobs jobs to Armada, see [here](usage.md#submitting-jobs).

Once you submit jobs, you should be able to see pods appearing in your cluster(s), running what you submitted.


#### Troubleshooting

* If the executor component is failing to contact kubernetes
    * Make sure your config file is placed in the correct place
    * You can test it by checking you can use Kubectl to access your cluster. The executor should be looking in the same place as Kubectl


## Code Generation

This project uses code generation in a couple of places. The below sections will discuss how to install and use the code generation tools.

### gRPC

We use protoc to generate the Go files used for our gRPC communication. 

The github for this utility ca be found: https://github.com/protocolbuffers/protobuf

##### Set up steps

* Install protoc (https://github.com/protocolbuffers/protobuf/releases/download/v3.8.0/protoc-3.8.0-linux-x86_64.zip) and make sure it is on $PATH.
* Make sure Go is installed and $GOPATH/bin is in your $PATH


Install required proto libraries:

```
go get github.com/gogo/protobuf/proto
go get github.com/gogo/protobuf/protoc-gen-gogofaster
go get github.com/gogo/protobuf/gogoproto
go get google.golang.org/grpc

GO111MODULE=off go get k8s.io/api
GO111MODULE=off go get k8s.io/apimachinery
GO111MODULE=off go get github.com/gogo/protobuf/gogoproto

```

Now everything is installed, you can generate types based on the .proto files found in ./internal/armada/api.

There is a script that will regenerate all proto files, assuming the above setup is installed:

```
./scripts/proto.sh
```

### Command line tools

Our commandline tools used the cobra framework (https://github.com/spf13/cobra).

You can use the cobra cli to add new commands, the below will describe how to add new commands for armadactl but it can be applied to any of our command line tools.

#####Steps

Get cobra cli tool:

```
go get -u github.com/spf13/cobra/cobra
```

Go to directory of commandline tool you are working on:

```
cd ./cmd/armadactl
```

Use cobra to add new command:

```
cobra add commandName
```

You should see a new file appear under ./cmd/armadactl/cmd with the name you specified in the command.