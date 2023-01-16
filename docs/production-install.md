# Production Installation

### Prerequisites

* At least one running Kubernetes cluster

### Installing Armada Server

For production it is assumed that the server component runs inside a Kubernetes cluster.

The below sections will cover how to install the component into Kubernetes. 

#### Recommended prerequisites


* Cert manager installed [https://cert-manager.io/docs/installation/helm/#installing-with-helm](https://cert-manager.io/docs/installation/helm/#installing-with-helm)
* gRPC compatible ingress controller installed for gRPC ingress such as [https://github.com/kubernetes/ingress-nginx](https://github.com/kubernetes/ingress-nginx)
* Redis installed [https://github.com/helm/charts/tree/master/stable/redis-ha](https://github.com/helm/charts/tree/master/stable/redis-ha)
* Optionally install NATS streaming server helm chart:[https://github.com/nats-io/k8s/tree/main/helm/charts/stan](https://github.com/nats-io/k8s/tree/main/helm/charts/stan), additional docs: [https://docs.nats.io/running-a-nats-service/nats-kubernetes](https://docs.nats.io/running-a-nats-service/nats-kubernetes)


Set `ARMADA_VERSION` environment variable and clone [this repository](https://github.com/armadaproject/armada.git) with the same version tag as you are installing. For example to install version `v1.2.3`:
```bash
export ARMADA_VERSION=v1.2.3
git clone https://github.com/armadaproject/armada.git --branch $ARMADA_VERSION
```

#### Installing server component

To install the server component, we will use Helm.

You'll need to provide custom config via the values file, below is a minimal template that you can fill in:

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

For all configuration options you can specify in your values file, see [server Helm docs](https://armadaproject.io/helm#server-helm-chat).

Fill in the appropriate values in the above template and save it as `server-values.yaml`

Then run:

```bash
helm install ./deployment/armada --set image.tag=$ARMADA_VERSION -f ./server-values.yaml
```

#### Using NATS Streaming
You can optionally setup Armada to route all job events through persistent NATS Streaming subject before saving them to redis. This is useful if additional application needs to consume events from Armada as NATS subject contains job events from all job sets.

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

### Installing Armada Executor

For production the executor component should run inside the cluster it is "managing".

To install the executor into a cluster, we will use Helm. 

You'll need to provide custom config via the values file, below is a minimal template that you can fill in:

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

##### Moving Executor off the control plane

By default, the executor runs on the control plane. 
 
When that isn't an option, maybe because you are using a managed kubernetes service where you cannot access the master nodes.

Add the following to your values file:
 ```yaml
nodeSelector: null
tolerations: []
```
<br/>


For other node configurations and all other executor options you can specify in your values file, see [executor Helm docs](https://armadaproject.io/helm#Executor-helm-chart).

Fill in the appropriate values in the above template and save it as `executor-values.yaml`.

Then run:

```bash
helm install ./deployment/armada-executor --set image.tag=$ARMADA_VERSION -f ./executor-values.yaml
```
# Interacting with Armada

Once you have the Armada components running, you can interact with them via the command-line tool called `armadactl`.

## Setting up armadactl

`armadactl` connects to `localhost:50051` by default with no authentication.

For authentication please create a config file described below.

#### Config file

By default config is loaded from `$HOME/.armadactl.yaml`.

You can also set location of the config file  using command line argument:

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

To Invoke an external program to generate an access token, config file should be as follows:
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

For Kerberos authentication, config file should contain this:
```
KerberosAuth:
  enabled: true
```

#### Environment variables

 --- TBC ---

## Submitting Test Jobs

For more information about usage please see the [User Guide](./user.md)

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

Use the `armadactl` command line utility to submit jobs to the Armada server
```bash
# create a queue:
armadactl create queue test --priorityFactor 1

# submit jobs in yaml file:
armadactl submit ./example/jobs.yaml

# watch jobs events:
armadactl watch test job-set-1

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

