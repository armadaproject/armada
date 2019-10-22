# Installation

### Pre-requisites

* At least one running Kubernetes cluster

### Installing Armada Server

For production it is assumed that the server component runs inside a Kubernetes cluster.

The below sections will cover how to install the component into Kubernetes. 

#### Recommended pre-requisites

* Cert manager installed (https://hub.helm.sh/charts/jetstack/cert-manager)
* gRPC compatible ingress controller installed (for gRPC ingress)
    * Such as https://github.com/helm/charts/tree/master/stable/nginx-ingress
* Redis installed (https://github.com/helm/charts/tree/master/stable/redis-ha)

#### Installing server component

To install the server component, we will use helm.

You'll need to provide custom config via the values file, below is a minimal template that you can fill in:

```yaml
ingressClass: "nginx"
clusterIssuer: "letsencrypt-prod"
hostname: "server.component.url.com"
replicas: 3

applicationConfig:
  redis:
    masterName: "mymaster"
    addrs:
      - "redis-ha-announce-0.default.svc.cluster.local:26379"
      - "redis-ha-announce-1.default.svc.cluster.local:26379"
      - "redis-ha-announce-2.default.svc.cluster.local:26379"
    poolSize: 1000
  eventsRedis:
    masterName: "mymaster"
    addrs:
      - "redis-ha-announce-0.default.svc.cluster.local:26379"
      - "redis-ha-announce-1.default.svc.cluster.local:26379"
      - "redis-ha-announce-2.default.svc.cluster.local:26379"
    poolSize: 1000

basicAuth:
  users:
    "user1": "password1"
```

For all configuration options you can specify in your values file, see [server helm docs](./helm/server.md).

Fill in the appropriate values in the above template and save it as server-values.yaml

Then run:

```bash
helm install ./deployment/armada -f ./server-values.yaml
```

### Installing Armada Executor

For production the executor component should run inside the cluster it is "managing".

**Please note by default the executor runs on the control plane. This is recommended but can be configured differently, see the helm chart page [here](./helm/executor.md) for details.**

To install the executor into a cluster, we will use helm. 

You'll need to provide custom config via the values file, below is a minimal template that you can fill in:

```yaml
applicationConfig:
  application:
    clusterId : "clustername"
  armada:
    url : "server.component.url.com:443"
    
credentials:
  username: "user1"
  password: "password1"
```

For all configuration options you can specify in your values file, see [executor helm docs](./helm/executor.md).

Fill in the appropriate values in the above template and save it as executor-values.yaml.

Then run:

```bash
helm install ./deployment/armada-executor -f ./executor-values.yaml
```
# Interacting with Armada

Once you have Armada components running, you can interact with them via the command line tool called armadactl.

## Setting up armadactl

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

## Submitting Jobs
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

**Note: Job resource request and limit should be equal. Armada does not support limit > request currently.**

# How to use Armada

We have now covered how to setup Armada and how to interact with it. 

However this section will briefly talk about how to use Armada as an application.

The main concepts to understand in Armada are:
* Job
* Job Set
* Queue

### Job

A Job in Armada is equivalent to a Pod in Kubernetes, with a few extra metadata fields.
 
It is the most basic unit of work in Armada and if you are familiar with Kubernetes it will be very familiar to you.
  
A simple example of a job in yaml form looks like:

```yaml
queue: test
priority: 0
jobSetId: set1
podSpec:
  terminationGracePeriodSeconds: 0
  restartPolicy: Never
  containers:
    - name: sleep
      imagePullPolicy: IfNotPresent
      image: busybox:latest
      args:
        - sleep
        - 60s
      resources:
        limits:
          memory: 64Mi
          cpu: 150m
        requests:
          memory: 64Mi
          cpu: 150m
```
A Job belongs to a Queue and a Job Set, seen by the corresponding fields. Finally it has a priority, which is a relative priority to other Jobs in the same queue, lower the number, the higher the priority.

So if you submit 2 Jobs, A and B, and you want B to be ordered before A in the queue, B should be given a priority lower than A using the priority field.

All jobs of priority 0 will be taken from the queue before any with priority 1 and time of submission is not taken into account.

**Note: Job resource request and limit should be equal. Armada does not support limit > request currently.**

### Job Set

A Job Set is a logical grouping of Jobs.

The reasons you may want to do this is because you can;
* Watch all events of a Job Set together
* Cancel all jobs in a Job Set together

The Job Set represents are typically meant to be used to represent a single task, where many Jobs are involved. 

You can then follow this Job Set as a single entity rather than having to remember every Job that are associated in some way.

A Job Set has no impact on the running of jobs a this moment and is purely an abstraction over a group of Jobs.

### Queue

A queue is the likely most important aspect of Armada.

Conceptually is it quite simple, it is just a Queue of jobs waiting to be run on a Kubernetes cluster.

In addition to being a queue, it is also used:
* Maintaining fair share over time
* Security boundary (Not yet implemented) between jobs

##### Fair share

Queues are provided with a fair share of the resource available on all clusters over time. 

That is to say if there are 5 queues and they were all to submit infinite jobs (Well in excess of cluster capacity), then each queue should receive the  same amount of compute resource over the course of time. No single queue should get a larger share of the available resource.

The reason we say over time, as at any single moment we cannot guarantee the queues will be exactly equal, however each job submitted has a cost. So if one queue gets a larger share early, they'll receive a lesser share later to make up for it.

##### Priority

A Queue has a Priority.

The lower the number, the greater the share that queue will receive.

So if you have 2 Queues:
* A - Priority 10
* B - Priority 1

We would expect B to receive substantially more resource on the clusters than A over time. 

All priorities are relative, so you can make your own scheme of priority numbers. So for example you could have "normal" be 100, "high" be 50 and "max" be 10.

Priority allows great flexibility, as it means you can predictably give certain queues a bigger share of resource than others.

##### Security Boundary (Not yet implemented)

A user(or group) who has permission to submit to Queue A and only A cannot therefore submit and/or delete from any other Queue.

This allows users work to be separated by Queue, with no concern someone else will delete or view their Jobs.


#### Considerations when setting up Queues

So now you know what Queues are and what they can do. We'll briefly cover what to consider when setting them up.

You should consider how your organisation should be split up so peoples work is provided with an appropriate amount of resource allocation.

There are many ways this could be done, here are some ideas:

* By user. All users will be equal (or scaled) relative to each other
* By team. All teams will be equal (or scaled) relative to each other
* By project. All projects will be equal (or scaled) relative to each other
* A mixture of all the above. So a given user may have their own queue, be part of a team queue and part of multiple project queues. They would then submit the appropriate jobs to the appropriate queues and expect them to all be given a fair amount of resource.
    * This allows a lot of flexibility in how you slice up your organisation, and combined with differing priorities it is possible to achieve any mix of resource allocation you want.

For each of the above, if you went for "By user". Simply make each user of the system their own Queue and tell them to submit to it. 

They will then receive the same amount of resource as any other user in the company.
