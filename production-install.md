# Production installation
- [Production installation](#production-installation)
  - [Installing the Armada Server](#installing-the-armada-server)
    - [Recommended prerequisites](#recommended-prerequisites)
  - [Installing the `server` component](#installing-the-server-component)
    - [Using NATS Streaming](#using-nats-streaming)
  - [Installing the Armada Executor](#installing-the-armada-executor)
    - [Moving the Executor off the control plane](#moving-the-executor-off-the-control-plane)
  - [Interacting with Armada](#interacting-with-armada)
    - [Setting up armadactl](#setting-up-armadactl)
    - [Config file](#config-file)
    - [Environment variables](#environment-variables)
  - [Submitting test jobs](#submitting-test-jobs)
  - [Getting system metrics](#getting-system-metrics)
    - [Server](#server)
    - [Executor](#executor)

## Installing the Armada Server

For production, it is assumed that the server component runs inside a Kubernetes cluster.

The following sections cover how to install the component into Kubernetes. 

### Recommended prerequisites

You should have at least one running Kubernetes cluster, and you should also install the following tools:

* [Cert manager](https://cert-manager.io/docs/installation/helm/#installing-with-helm)
* [gRPC compatible ingress controller installed for gRPC ingress](https://github.com/kubernetes/ingress-nginx)
* [Redis]([https://github.com/helm/charts/tree/master/stable/redis-ha)
* (optional) [NATS streaming server Helm chart](https://github.com/nats-io/k8s/tree/main/helm/charts/stan)
  
[Learn more about NATS services](https://docs.nats.io/running-a-nats-service/nats-kubernetes).

Set `ARMADA_VERSION` environment variable and [clone the `armadaproject` repository](https://github.com/armadaproject/armada.git) with the same version tag as you are installing. For example, to install version `v1.2.3`:

```bash
export ARMADA_VERSION=v1.2.3
git clone https://github.com/armadaproject/armada.git --branch $ARMADA_VERSION
```

## Installing the `server` component

To install the server component, use Helm.

You'll need to provide custom config via the values file. Use the following example as a template you can fill in.

```yaml
ingressClass: "nginx"
clusterIssuer: "letsencrypt-prod"
hostnames: 
  - "server.component.url.com"
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

For all configuration options you can specify in your values file, [see the server Helm docs](https://armadaproject.io/helm#server-helm-chat).

Fill in the appropriate values in the above template and save it as `server-values.yaml`.

Then run:

```bash
helm install ./deployment/armada --set image.tag=$ARMADA_VERSION -f ./server-values.yaml
```

### Using NATS Streaming

You can optionally set up Armada to route all job events through persistent NATS Streaming subject before saving them to Redis. This is useful if additional application needs to consume events from Armada as NATS subject contains job events from all job sets.

Required additional server configuration is:

```yaml
eventsNats:
  servers:
    - "armada-nats-0.default.svc.cluster.local:4222"
    - "armada-nats-1.default.svc.cluster.local:4222"
    - "armada-nats-2.default.svc.cluster.local:4222"
  clusterID: "nats-cluster-ID"
  subject: "ArmadaEvents"
  queueGroup: "ArmadaEventsRedisProcessor"
```

## Installing the Armada Executor

For production, the executor component should run inside the cluster it is 'managing'.

To install the executor into a cluster, use Helm. 

You'll need to provide custom config via the values file. Use the following example as a template you can fill in.

```yaml
applicationConfig:
  application:
    clusterId : "clustername"
  apiConnection:
    armadaUrl : "server.component.url.com:443"    
    basicAuth:
      username: "user1"
      password: "password1"
```

<br/>

### Moving the Executor off the control plane

By default, the executor runs on the control plane. 
 
When that isn't an option, maybe because you are using a managed Kubernetes service where you cannot access the master nodes.

Add the following to your values file:

 ```yaml
nodeSelector: null
tolerations: []
```
<br/>

For other node configurations and all other executor options you can specify in your values file, [see the executor Helm docs](https://armadaproject.io/helm#Executor-helm-chart).

Fill in the appropriate values in the above template and save it as `executor-values.yaml`.

Then run:

```bash
helm install ./deployment/armada-executor --set image.tag=$ARMADA_VERSION -f ./executor-values.yaml
```
## Interacting with Armada

Once you have the Armada components running, you can interact with them via the command-line tool called `armadactl`.

### Setting up armadactl

`armadactl` connects to `localhost:50051` by default with no authentication.

For authentication, create a config file as follows.

### Config file

By default, config is loaded from `$HOME/.armadactl.yaml`.

You can also set the location of the config file using command line argument:

```bash
armada command --config=/config/location/config.yaml
```

The format of this file is a simple yaml file:

```yaml
armadaUrl: "server.component.url.com:443"
basicAuth:
  username: "user1"
  password: "password1"
```

For an Open Id protected server, armadactl will perform PKCE flow opening web browser.

The config file should look like this:

```yaml
armadaUrl: "server.component.url.com:443"
openIdConnect:
  providerUrl: "https://myproviderurl.com"
  clientId: "***"
  localPort: 26354
  useAccessToken: true
  scopes: []
```

To invoke an external program to generate an access token, the config file should be as follows:

```yaml
armadaUrl: "server.component.url.com:443"
execAuth:
  # Command to run.  Needs to be on the path and should write only a token to stdout. Required.
  cmd: some-command
  # Environment variables to set when executing the command. Optional.
  args:
    - "arg1"
  # Arguments to pass when executing the command. Optional.
  env:
    - name: "FOO"
      value: "bar"
  # Whether the command requires user input. Optional   
  interactive: true
```

For Kerberos authentication, the config file should contain the following:

```
KerberosAuth:
  enabled: true
```

### Environment variables

 --- TBC ---

## Submitting test jobs

For more information about usage, [see the user guide](./user_guide.md)

Specify the jobs to be submitted in a yaml file:

```yaml
queue: test
jobSetId: job-set-1
jobs:
  - priority: 0    
    podSpec:
      terminationGracePeriodSeconds: 0
      restartPolicy: Never
      containers:
          ... any Kubernetes pod spec ...

```

Use the `armadactl` command line utility to submit jobs to the Armada server:

```bash
# create a queue:
armadactl create queue test --priorityFactor 1

# submit jobs in yaml file:
armadactl submit ./example/jobs.yaml

# watch jobs events:
armadactl watch test job-set-1

```

**Note:** Job resource request and limit should be equal. Armada does not support limit > request currently.

## Getting system metrics

All Armada components provide a `/metrics` endpoint providing relevant metrics to the running of the system.

We actively support Prometheus through our Helm charts. However, any metrics solution that can scrape an endpoint will work.

### Server

The server component provides metrics on the `:9000/metrics` endpoint.

You can enable Prometheus components when installing with Helm by setting `prometheus.enabled=true`.

### Executor

The executor component provides metrics on the `:9001/metrics` endpoint.

You can enable Prometheus components when installing with Helm by setting `prometheus.enabled=true`.

