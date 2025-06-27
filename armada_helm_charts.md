# Armada Helm charts
- [Armada Helm charts](#armada-helm-charts)
  - [Executor Helm chart options](#executor-helm-chart-options)
    - [Values](#values)
      - [Executor node configuration](#executor-node-configuration)
    - [Application config](#application-config)
      - [Credentials](#credentials)
        - [Open ID](#open-id)
        - [Basic authorisation](#basic-authorisation)
      - [Kubernetes config](#kubernetes-config)
        - [`impersonateUsers`](#impersonateusers)
        - [`minimumPodAge`](#minimumpodage)
        - [`failedPodExpiry`](#failedpodexpiry)
        - [`stuckPodExpiry`](#stuckpodexpiry)
        - [`trackedNodeLabels`](#trackednodelabels)
        - [`toleratedTaints`](#toleratedtaints)
        - [`minimumJobSize`](#minimumjobsize)
      - [Metrics](#metrics)
        - [`port`](#port)
        - [`exposeQueueUsageMetrics`](#exposequeueusagemetrics)
  - [Server Helm chart](#server-helm-chart)
    - [Values](#values-1)
    - [Application config](#application-config-1)
      - [Authentication](#authentication)
        - [Anonymous users](#anonymous-users)
        - [OpenId authentication](#openid-authentication)
        - [Basic authentication](#basic-authentication)
        - [Kerberos authentication](#kerberos-authentication)
      - [Permissions](#permissions)
      - [Job resource defaults](#job-resource-defaults)
      - [Scheduling](#scheduling)
        - [`queueLeaseBatchSize`](#queueleasebatchsize)
        - [`minimumResourceToSchedule`](#minimumresourcetoschedule)
        - [`maximalClusterFractionToSchedule`](#maximalclusterfractiontoschedule)
      - [Queue resource limits](#queue-resource-limits)
        - [`maximalResourceFractionPerQueue`](#maximalresourcefractionperqueue)
        - [`maximalResourceFractionToSchedulePerQueue`](#maximalresourcefractiontoscheduleperqueue)
      - [Job lease configuration](#job-lease-configuration)
        - [`lease`](#lease)
        - [`expireAfter`](#expireafter)
        - [`expiryLoopInterval`](#expiryloopinterval)

Armada provides Helm charts to assist with deployment of Armada executors and services.
These require Helm `3.10.0` or later.

## Executor Helm chart options

This document outlines the customisation options for the Executor Helm chart.

### Values

| Parameter                         | Description                                                                                                                                                                      | Default                                                                                 |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `image.repository`                | Executor image name                                                                                                                                                              | `gresearchdev/armada-executor`                                                          |
| `image.tag`                       | Executor image tag                                                                                                                                                               | `v0.0.1`                                                                                |
| `resources`                       | Executor resource request/limit                                                                                                                                                  | Request: <br/> `200m`, <br/> `512Mi` <br/>  Limit:  <br/>  `300m`,  <br/>  `1Gi`        |
| `additionalLabels`                | Additional labels that'll be added to all executor components                                                                                                                    | `{}`                                                                                    |
| `terminationGracePeriodSeconds`   | Executor termination grace period in seconds                                                                                                                                     | `5`                                                                                     |                                                   
| `nodeSelector`                    | Node labels for the executor pod assignment                                                                                                                                          | `node-role.kubernetes.io/master: ""`                                                    |
| `tolerations`                     | Node taint label tolerations for the executor assignment                                                                                                                             | `effect: NoSchedule`<br>`key: node-role.kubernetes.io/master`<br>`operator: Exists`     |
| `additionalClusterRoleBindings`   | Additional cluster role bindings that'll be added to the service account the executor pod runs with                                                                              | `""`                                                                                    |
| `additionalVolumeMounts`          | Additional volume mounts that'll be mounted to the executor container                                                                                                            | `""`                                                                                    |
| `additionalVolumes`               | Additional volumes that'll be mounted to the executor pod                                                                                                                        | `""`                                                                                    |
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the executor component. | `false`                                                                                 |
| `prometheus.labels`               | Additional labels that'll be added to executor prometheus components                                                                                                             | `{}`                                                                                    |
| `prometheus.scrapeInterval`       | Scrape interval of the serviceMonitor and prometheusRule                                                                                                                         | `10s`                                                                                   |
| `customServiceAccount`            | Use existing service account for the pod instead of creating a new one                                                                                                               | `null`                                                                                  |
| `serviceAccount`                  | Additional properties of the service account (like imagePullSecrets)                                                                                                                 | `{}`                                                                                    |
| `applicationConfig`               | Config file override values, merged with /config/executor/config.yaml to make up the config file used when running the application                                               | `nil`                                                                                   |

#### Executor node configuration

By default, the executor runs on the control plane. This is because it's a vital part of the cluster that it runs on and manages.

However there are times when this is not possible to do, such as using a managed kubernetes cluster where you cannot access the control plane.

To turn off running on the control plane, and instead just run on normal work nodes, add the following to your configuration:
 
 ```yaml
 nodeSelector: null
 tolerations: []
 ```

Alternatively, you could have a dedicated node that the executor runs on. Then use the nodeSelector + tolerations to enforce it running that node.

This has the benefit of being separated from the cluster it is managing, but not being on the control plane. 

**Note:** If the executor runs on the worker nodes, it could potentially get slowed down by the jobs it is scheduling on the cluster.

### Application config

The `applicationConfig` section of the values file is purely used to override the default config for the executor component.

It can override any value found in the `/config/executor/config.yaml` file.

Usually, this involves overriding the server URL, for example:

```yaml
applicationConfig:
  application:
    clusterId: "cluster-1"
  apiConnection:
    armadaUrl: "server.url.com:443"  
```

**Note:** The values you enter in this section are placed into a Kubernetes configmap. For sensitive values such as database passwords, we recommend setting them as environmental variables from a non-Helm managed secret:

```yaml
env:
- name: ARMADA_POSTGRES_CONNECTION_PASSWORD
  valueFrom:
    secretKeyRef:
      key: password
      name: lookout-postgres-password
```

#### Credentials

##### Open ID

For connection to Armada server protected by open Id auth, you can configure executor to perform a Client Credentials flow.
To do this, you should configure new client application with an Open Id provider for executor and permission executor using custom scope.

For Amazon Cognito, the configuration could look like this:

```yaml
applicationConfig:
  apiConnection:
    openIdClientCredentialsAuth:
      providerUrl: "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_*** your user pool id ***"
      clientId: "***"
      clientSecret: "***"
      scopes: ["armada/executor"]
```

To authenticate with username and password instead, use Resource Owner (Password) flow:

```yaml
applicationConfig:
  apiConnection:
    openIdPasswordAuth:
      providerUrl: "https://myopenidprovider.com"
      clientId: "***"
      scopes: []
      username: "executor-service"
      password: "***"
```

##### Basic authorisation

The credentials section of the values file is used to provide the username/password used for communication with the server component.

The username/password used in this section must exist in the server components list of known users (populated by the servers .Values.credentials section of its Helm chart).
If the username/password are not present in the server components secret, communication with the server component is rejected.

As an example, this section is formatted as follows:

```yaml
applicationConfig:
  apiConnection:
    basicAuth:
      username: "user1"
      password: "password1"
```

#### Kubernetes config

The default Kubernetes-related configuration is as follows:

```yaml
applicationConfig:
  kubernetes:
    impersonateUsers: false
    minimumPodAge: 3m
    failedPodExpiry: 10m
    stuckPodExpiry: 3m
```

##### `impersonateUsers`

If `impersonateUsers` is turned on, Armada creates pods in Kubernetes, impersonating the submitter of the job. 

This enables Kubernetes to enforce permissions and limit access to namespaces. This is useful to prevent users from submitting jobs to namespaces that their submitting user does not have access to. 

##### `minimumPodAge`

This is the minimum amount of time a pod exists for. If a pod completes in less time than this, it isn't cleaned up until it has reached this age. This is helpful, especially with short jobs, as it gives you time to view the pod and see the logs prior to cleanup.

##### `failedPodExpiry`

This is the amount of time after a pod fails before it is cleaned up. This enables you to view the logs of failed pods more easily, as they won't be cleaned up immediately. 

##### `stuckPodExpiry`

This is how long the executor lets a pod sit in `Pending` state before it considers the job stuck.

Once a job is considered stuck, a `JobUnableToScheduleEvent` is be reported for the job, leading to one of two outcomes:

* if the problem is deemed unretryable (for example, the image is getting `InvalidImageName`), the job gets a `JobFailedEvent` and is considered 'done'
* if the problem is deemed retryable, the job's lease is returned to `armada-server` (`JobLeaseReturnedEvent`) and the job is rescheduled 

```yaml
applicationConfig:
  kubernetes:
    trackedNodeLabels:
    - label1
    - label2
    toleratedTaints:
    - taintName1
    - taintName2
    minimumJobSize:
      memory: 0.25
      cpu: 0.25
```

##### `trackedNodeLabels`

This is a list of node labels that `armada-executor` pays attention to, while ignoring any other node labels. 

`armada-executor` reports these labels back to `armada-server` to enable jobs setting `labelSelector`s to be matched to these nodes for scheduling purposes. 

##### `toleratedTaints`

This is a list of node taints that `armada-executor` considers usable by jobs. 

By default, tainted nodes are ignored by `armada-executor`, and armada does not try to schedule jobs on them.

This is an opt-in for tainted nodes that you want Armada to use and schedule jobs on.

It reports these taints back to `armada-server`, so it uses them when schedule jobs onto clusters. In other words, only jobs that tolerate these taints are scheduled onto these nodes.

##### `minimumJobSize`

This is the minimum size a job must satisfy before it can be leased by this cluster.

This can be useful for giving different clusters different roles.

For example, let's say you had a cluster with only GPU nodes. To make sure only GPU jobs are sent to this cluster, you could set the `minimumJobSize` like:

```yaml
minimumJobSize:
  nvidia.com/gpu: 1 
```

#### Metrics

The default metrics configuration is as follows:

```yaml
applicationConfig:
  metric:
    port: 9001
    exposeQueueUsageMetrics: false
```

##### `port`
 
 This controls the port that `armada-executor` exposes its metrics on.

##### `exposeQueueUsageMetrics`

This feature attempts to use `metrics-server` on the cluster. 

If `metrics-server` is neither installed nor accessible, errors display in the logs and usage is incorrectly reported.

This determines if `armada-executor`:

  - reports `JobUtilisationEvent` (containing the job's maximum CPU/memory usage)
  - populates `armada_executor_job_pod_cpu_usage` and `armada_executor_job_pod_memory_usage_bytes` metrics with non-zero values

## Server Helm chart

This document outlines the customisation options for the server Helm chart.

### Values

| Parameter                         | Description                                                                                                                                                                    | Default                                                                       |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `image.repository`                | Server image name                                                                                                                                                              | `gresearchdev/armada-server`                                                  |
| `image.tag`                       | Server image tag                                                                                                                                                               | `v0.0.1`                                                                      |
| `resources`                       | Server resource request/limit                                                                                                                                                  | Request: <br/> `200m`, <br/> `512Mi`.<br/>  Limit: <br/> `300m`, <br/>  `1Gi` |
| `env`                             | Environment variables add to the server container                                                                                                                              | `""`                                                                          |
| `hostnames`                       | A list of hostnames to to use for ingress                                                                                                                                      | not set                                                                       |
| `additionalLabels`                | Additional labels that'll be added to all server components                                                                                                                    | `{}`                                                                          |                                                   
| `additionalVolumeMounts`          | Additional volume mounts that'll be mounted to the server container                                                                                                            | `""`                                                                          |
| `additionalVolumes`               | Additional volumes that'll be mounted to the server pod                                                                                                                        | `""`                                                                          |
| `terminationGracePeriodSeconds`   | Server termination grace period in seconds                                                                                                                                     | `5`                                                                           |                                                   
| `replicas`                        | Number of replicas (pods) of the server component                                                                                                                              | `1`                                                                           |                                                   
| `ingress.annotations`             | Additional annotations that'll be added to the server ingresses                                                                                                                    | `{}`                                                                          |
| `ingress.labels`                  | Additional labels that'll be added to the server ingresses                                                                                                                         | `{}`                                                                          |
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the server component. | `false`                                                                       |
| `prometheus.labels`               | Additional labels that'll be added to server prometheus components                                                                                                             | `{}`                                                                          |
| `prometheus.scrapeInterval`       | Scrape interval of the serviceMonitor and prometheusRule                                                                                                                       | `10s`                                                                         |
| `customServiceAccount`            | Use existing service account for the pod instead of creating a new one                                                                                                             | `null`                                                                        |
| `serviceAccount`                  | Additional properties of service account (like imagePullSecrets)                                                                                                               | `{}`                                                                          |
| `applicationConfig`               | Config file override values, merged with /config/armada/config.yaml to make up the config file used when running the application                                               |`grpcPort: 50051`                                                              |

### Application config

The applicationConfig section of the values file is purely used to override the default config for the server component.

It can override any value found in the `/config/armada/config.yaml` file.

Usually, this involves overriding the `redis` URL, for example:

As an example, this section is formatted as:

```yaml
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
```

#### Authentication

You can configure the server with multiple authentication methods.

##### Anonymous users

To enable anonymous users on the server, include the following in your config:

```yaml
anonymousAuth: true
```

##### OpenId authentication

You can configure Armada to use tokens signed by the OpenId provider for authentication.

**Note:** Currently only JSON Web Tokens (JWTs) are supported.

To determine group membership of a particular user, Armada reads the JWT claim specified in configuration as `groupsClaim`.

For example, configuration using Amazon Cognito as provider clooks like this:

```yaml
openIdAuth:
   providerUrl: "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_*** your user pool id ***"
   groupsClaim: "cognito:groups"
```

##### Basic authentication

**Note:** Basic authentication is not recommended for production deployment.

The `basicAuth` section of the `values` file is used to provide a list of valid username/password pairs.

This list determines all the valid users for the server components gRPC API.  All clients (executor or armadactl) must provide a valid set of credentials from this list when communicating with the server.

As an example, this section is formatted as:

```yaml
basicAuth:
  users:
    "user1": 
      password: "password1"
      groups: ["administrator", "teamB"]
    "user2": 
      password: "password1"
      groups: ["teamA"]
```

##### Kerberos authentication

The server also supports Kerberos authentication, but this feature might be removed in a future version. If you can, use different authentication methods.

```yaml
kerberos:
  keytabLocation: /etc/auth/serviceUser.kt  # location of keytab
  principalName: serviceUser                # name of service user
  userNameSuffix: -suffix                   # optional suffix appended to username which is read from kerberos ticket
```

#### Permissions

Armada allows you to specify these permissions for the user:

| Permission         | Details                                                                           |
|--------------------|-----------------------------------------------------------------------------------|
| `submit_any_jobs`  | Enables the user to submit jobs to any queue.                                            |
| `create_queue`     | Enables the user to submit jobs to create queue.                                         |
| `cancel_any_jobs`  | Enables the user to cancel jobs from any queue.                                          |
| `watch_all_events` | Enables the user to watch all events.                                                   |
| `execute_jobs`     | Protects APIs used by executor (only executor service should have this permission). |

Permissions can be assigned to user by group membership, like this:

```yaml
permissionGroupMapping:
  submit_any_jobs: ["administrators"]
  create_queue: ["administrators"]
  cancel_any_jobs: ["administrators"]
  watch_all_events: ["administrators"]
  execute_jobs: ["armada-executor"]
```

In case of Open Id access tokens, permissions can be assigned based on token scope.
This is useful when using clients credentials flow for the executor component.

```yaml
permissionScopeMapping:
  execute_jobs: ["armada/executor"]
```
 
By default, all users (including anonymous ones) are members of the `everyone` group.

#### Job resource defaults

By default, `armada-server` validates submitted jobs that set values for resource request and limit. 

It also makes sure that `resource request = resource limit`. 

However, if you remove the requirement for your users to set a resource request/limit or provide sensible defaults, you can specify defaults with:

```yaml
scheduling:
  defaultJobLimits:
    memory: 8Gi
    cpu: 1
```

The default is only used when a submitted job doesn't specify a request/limit for the resource type specified.

Using the values from the previous example, if a job was submitted with:

```yaml
  resources:
    limits:
      memory: 64Mi
    requests:
      memory: 64Mi
```

The job is then transformed as follows:

```yaml
  resources:
    limits:
      memory: 64Mi
      cpu: 1
    requests:
      memory: 64Mi
      cpu: 1
```

#### Scheduling

The default scheduling configuration is as follows:

```yaml
scheduling:
  queueLeaseBatchSize: 200
  minimumResourceToSchedule:
    memory: 100000000 # 100Mb
    cpu: 0.25
  maximalClusterFractionToSchedule:
    memory: 0.25
    cpu: 0.25
```

##### `queueLeaseBatchSize`

This is the number of jobs at the top of a queue the scheduler looks at when trying to find one that'll fit in the available resource.

This is an optimisation so the scheduler doesn't need to check through the entire queue (which could be very large).

The lower this value is, the faster scheduling is, but at the expense that jobs lower down in the queue may not get considered, even if they would have fitted into the available resource.

##### `minimumResourceToSchedule`

This is the minimum amount of resource a cluster has to have left available before jobs are scheduled on it. 

This is just an optimisation to speed up scheduling, rather than looking through thousands of jobs to find one small enough. 

If you have many tiny jobs or very small clusters, you may want to decrease this below your average expected smallest job.

##### `maximalClusterFractionToSchedule`

This is the maximum percentage of resource to schedule for a cluster per round.

If a cluster had 1000 CPU, the settings in the previous example would mean that only 250 CPU are scheduled for each scheduling round.
 
#### Queue resource limits 

By default, there are no queue resource limits.

You may want to impose limit to stop a single queue from grabbing all the resource on all clusters (during a quiet period), and holding it for a long time (if all jobs are long-running).

All limits are proportional to overall amount of resources in the system. 

You can specify resource limits per queue as follows:

```yaml
scheduling:
  maximalResourceFractionToSchedulePerQueue:
    memory: 0.05
    cpu: 0.05
  maximalResourceFractionPerQueue:
    memory: 0.25
    cpu: 0.25
```

In this example, a queue can use at most 25% of all available CPU and memory.

##### `maximalResourceFractionPerQueue`

This is the maximum resource a queue can hold as a percentage of the total resource of this type over all clusters.

##### `maximalResourceFractionToSchedulePerQueue`

This is the percentage of total resource of this resource type a queue can be allocated in a single scheduling round.

Currently scheduling is done in parallel, so it can happen that we exceed the resource limit set in `maximalResourceFractionPerQueue`, due to two clusters requesting new jobs at the exact same time. To mitigate this, `maximalResourceFractionToSchedulePerQueue` specifies how much can be scheduled in a single round, and can be thought of as the margin for error.

Using an example of having 1000 CPU over all your clusters:

* `maximalResourceFractionPerQueue` - limits a queue to 250 CPU
* `maximalResourceFractionToSchedulePerQueue` - limits the amount of resource a queue can be allocated in a single round to 50 CPU

So in the extreme case two clusters request resource at the exact same time, a queue could in theory get to 300 CPU.

We have tested this with many extremely large clusters and even when empty, the resource limit in the worst case is:

`worst case resource limit` = `maximalResourceFractionPerQueue` + `maximalResourceFractionToSchedulePerQueue`

For any resource type not specified in `maximalResourceFractionPerQueue`, a queue can be allocated 100% of that resource type. (Hence the default is 100% of all resource types.) 

#### Job lease configuration

The default job lease configuration is as follows:

```yaml
scheduling:
  lease:
    expireAfter: 15m
    expiryLoopInterval: 5s
```

##### `lease`

Leases expire when an `armada-executor` on a cluster stops contacting `armada-server`. 

`armada-server` waits for a specified length of time (`expireAfter`), then expires the leases that enable jobs to be leased on other clusters that are still contacting `armada-server`. This prevents jobs from getting lost forever because a cluster goes down.

##### `expireAfter`

If you have long-running jobs and want to tolerate short network outages, increase `expireAfter`. If you want jobs to be quickly rescheduled onto new clusters when `armada-executor` loses contact, decrease `expireAfter`.

##### `expiryLoopInterval`

This controls how often the loop checks for expired leases runs. 
