# Installation

There are multiple combinations on how you can install Armada, this guide will cover the two most common scenarios:
* Local development
* Production setup

## Pre-requisites

* At least one running Kubernetes cluster
* One running redis instance
    * This can be local using:
        * docker run --expose=6379 --network=host redis
    * Hosted somewhere such as AWS
    * Installed on your cluster using:
        * Redis HA: https://github.com/helm/charts/tree/master/stable/redis-ha
        * Redis: https://github.com/helm/charts/tree/master/stable/redis
        
        

## Local development setup

There are many ways you can setup you local environment, this is just a basic quick example of how to setup everything you'll need to get started running and developing Armada.

### Pre-requisites
To follow this section I am assuming you have:
* Golang >= 1.12 installed (https://golang.org/doc/install)
* Kubectl installed (https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* Docker installed (ideally in the sudo group)
* This repository cloned. The guide will assume you are in the root directory of this repository

### Steps

1. Set up a Kubernetes cluster, this can be a local instance such as Kind (https://github.com/kubernetes-sigs/kind)
    * For Kind simply run `GO111MODULE="on" go get sigs.k8s.io/kind@v0.5.1 && kind create cluster`
2. Put the kubernetes config file so your kubectl can find it. %HOME/.kube/config or set env variable KUBECONFIG=/config/file/location
    * If using Kind, you can find the config file location by running command: `kind get kubeconfig-path`
3. Start redis with default values `docker run -d --expose=6379 --network=host redis`
    * You may need to run this as sudo
4. In separate terminals run:
    * go run /cmd/armada/main.go
    * go run /cmd/executor/main.go
    
You now have Armada setup and can submit jobs to it (See Submitting Jobs section below). 

* The server component should be up and talking to Redis and ready to accept gRPC calls
* The executor component should be up and talking to the server component and Kubernetes. 
    * If it is failing to contact kubernetes, make sure your config file is placed in the correct place. 
    * You can test it by checking you can use Kubectl to access your cluster. The executor should be looking in the same place as Kubectl

Likely you'll want to run the last step via an IDE to make developing easier, so you can benefit from debug features etc.


## Production setup

### Installing Armada Server

For production it is recommended but not essential that the server component runs inside a Kubernetes cluster.

The below sections will cover how to install the component into a Kubernetes server. 

#### Recommended pre-requisites

* Cert manager installed (https://hub.helm.sh/charts/jetstack/cert-manager)
* gRPC compatible ingress controller installed (for gRPC ingress)
    * Such as https://github.com/helm/charts/tree/master/stable/nginx-ingress
* Redis installed (https://github.com/helm/charts/tree/master/stable/redis / https://github.com/helm/charts/tree/master/stable/redis-ha)
    * This doesn't actually need to be running in the cluster, just accessible from it

*You could also run using a NodePort service, meaning you could drop cert manager + ingress, however this is not the ideal set up*

#### Installing server component


To install the server component, you can simply use helm.

You'll likely need to customise the values file with your custom config, likely a minimum values file will look like:

```yaml

ingressClass: "nginx"
clusterIssuer: "letsencrypt-prod"
hostname: "server.component.url.com"
replicas: 3

image:
  tag: "sometag"

applicationConfig:
  redis:
    masterName: "mymaster"
    sentinelAddresses: 
      - "example-redis-ha.default.svc.cluster.local:26379"
  eventsRedis:   
    masterName: "mymaster"
    sentinelAddresses: 
      - "example-redis-ha.default.svc.cluster.local:26379"

credentials:
  users:
    "user1": "password1"
```

Fill in the appropriate values in the above template and save as server-values.yaml.

Then run:

```bash
helm install ./deployment/armada -f ./server-values.yaml
```

For all configuration options you can specify in your values file, see [server helm docs](./docs/helm/server.md).

### Installing Armada Executor

For production it is highly recommended the executor component runs inside the cluster it is "managing".

To install the executor into a cluster, we will use helm. 

Before installing you'll almost certainly need to make a values file with custom config for you setup, likely a minimum values file will need to look like:

```yaml
image:
  tag: "sometag"

applicationConfig:
  application:
    clusterId : "clustername"
  armada:
    url : "server.component.url.com:443"
    
credentials:
  username: "user1"
  password: "password1"
 ```

Fill in the appropriate values in the above template and save as executor-values.yaml.

Then run:

```bash
helm install ./deployment/armada-executor -f ./executor-values.yaml
```

For all configuration options you can specify in your values file, see [executor helm docs](./docs/helm/executor.md).


# Setting up armadactl

Armadactl expects you to provide to provide several arguments that deal with connection/authentication information.


| Parameter    | Description                                                              | Default            |
|--------------|--------------------------------------------------------------------------|--------------------|
| `armadaUrl`  | URL of Armada server component                                           | `localhost:50051`  |
| `username`   | Only required when connection to armada instance with authentication on  | `""`               |
| `password`   | Only required when connection to armada instance with authentication on  | `""`               |

*Defaults to working for local development setup*

You can provide these with each invocation of armadactl, however this can be tiresome.

Instead you can try one of the options below:

#### Config file

You can also store them in a yaml file and provide this to armadactl on each invocation:

armada command --config=/config/location/config.yaml

The format of this file is a simple yaml file:

```yaml
armadaUrl: "server.component.url.com:443"
username: "user1"
password: "password1"
```

#### Persistent config

Building on the to the config file above. If you store the config file as $HOME/.armadactl

This file will automatically be picked up and used by armadactl

#### Environment variables

 --- TBC ---

# Submitting Jobs
Describe jobs in yaml file:
```yaml
jobs:
  - queue: test
    priority: 0
    jobSetId: job-set-1
    podSpec:
      terminationGracePeriodSeconds: 0
      restartPolicy: Never
      containers:
          ... any Kubernetes pod spec ...

```

Use armadactl command line utility to submit jobs to armada server
```bash
# create a queue:
armadactl create-queue test 1

# submit jobs in yaml file:
armadactl submit ./example/jobs.yaml

# watch jobs events:
armadactl watch job-set-1

```
