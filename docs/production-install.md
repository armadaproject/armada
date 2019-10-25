# Production Installation

### Pre-requisites

* At least one running Kubernetes cluster

### Installing Armada Server

For production it is assumed that the server component runs inside a Kubernetes cluster.

The below sections will cover how to install the component into Kubernetes. 

#### Recommended pre-requisites

* Cert manager installed (https://hub.helm.sh/charts/jetstack/cert-manager)
* gRPC compatible ingress controller installed for gRPC ingress (such as https://github.com/helm/charts/tree/master/stable/nginx-ingress)
* Redis installed (https://github.com/helm/charts/tree/master/stable/redis-ha)

#### Installing server component

To install the server component, we will use Helm.

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

For all configuration options you can specify in your values file, see [server Helm docs](./helm/server.md).

Fill in the appropriate values in the above template and save it as `server-values.yaml`

Then run:

```bash
helm install ./deployment/armada -f ./server-values.yaml
```

### Installing Armada Executor

For production the executor component should run inside the cluster it is "managing".

**Please note by default the executor runs on the control plane. This is recommended but can be configured differently, see the Helm chart page [here](./helm/executor.md) for details.**

To install the executor into a cluster, we will use Helm. 

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

For all configuration options you can specify in your values file, see [executor Helm docs](./helm/executor.md).

Fill in the appropriate values in the above template and save it as `executor-values.yaml`.

Then run:

```bash
helm install ./deployment/armada-executor -f ./executor-values.yaml
```
# Interacting with Armada

Once you have the Armada components running, you can interact with them via the command-line tool called `armadactl`.

## Setting up armadactl

`armadactl` expects you to provide several arguments that deal with connection/authentication information.

| Parameter    | Description                                                                 | Default            |
|--------------|-----------------------------------------------------------------------------|--------------------|
| `armadaUrl`  | URL of the Armada server component                                          | `localhost:50051`  |
| `username`   | Only required when connecting to an Armada instance with authentication on  | `""`               |
| `password`   | Only required when connecting to an Armada instance with authentication on  | `""`               |

*Defaults to working for local development setup*

You can provide these with each invocation of `armadactl`, however this can be tiresome.

Instead you can try one of the options below.

For server with Open Id authentication config file is required.

#### Config file

You can also store them in a yaml file and provide this to armadactl on each invocation:

armada command --config=/config/location/config.yaml

The format of this file is a simple yaml file:

```yaml
armadaUrl: "server.component.url.com:443"
username: "user1"
password: "password1"
```

For Open Id protected server armadactl will perform PKCE flow opening web browser.
Config file should look like this:
```yaml
armadaUrl: "server.component.url.com:443"
openIdConnect:
  providerUrl: "https://myproviderurl.com"
  clientId: "***"
  localPort: 26354
  useAccessToken: true
  scopes: []
```

#### Persistent config

Building on the config file above. If you store the config file as `$HOME/.armadactl`

This file will automatically be picked up and used by `armadactl`.

#### Environment variables

 --- TBC ---

## Submitting Test Jobs

For more information about usage please see the [User Guide](./user.md)

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

Use the `armadactl` command line utility to submit jobs to the Armada server
```bash
# create a queue:
armadactl create-queue test --priorityFactor 1

# submit jobs in yaml file:
armadactl submit ./example/jobs.yaml

# watch jobs events:
armadactl watch job-set-1

```

**Note: Job resource request and limit should be equal. Armada does not support limit > request currently.**

## Metrics

All Armada components provide a `/metrics` endpoint providing relevant metrics to the running of the system.

We actively support Prometheus through our Helm charts (see below for details), however any metrics solution that can scrape an endpoint will work.

### Component metrics

#### Server

The server component provides metrics on the `:9000/metrics` endpoint.

You can enable Prometheus components when installing with Helm by setting `prometheus.enabled=true`.

#### Executor

The executor component provides metrics on the `:9001/metrics` endpoint.

You can enable Prometheus components when installing with Helm by setting `prometheus.enabled=true`.

