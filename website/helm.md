# Armada helm charts

Armada provides helm charts to assist with deployment of armada executors and services.
These require Helm `3.10.0` or later.

## Executor helm chart

This document briefly outlines the customisation options of the Executor helm chart.

### Values

| Parameter                         | Description                                                                                                                                                                      | Default                                                                                 |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| `image.repository`                | Executor image name                                                                                                                                                              | `gresearchdev/armada-executor`                                                          |
| `image.tag`                       | Executor image tag                                                                                                                                                               | `v0.0.1`                                                                                |
| `resources`                       | Executor resource request/limit                                                                                                                                                  | Request: <br/> `200m`, <br/> `512Mi` <br/>  Limit:  <br/>  `300m`,  <br/>  `1Gi`        |
| `additionalLabels`                | Additional labels that'll be added to all executor components                                                                                                                    | `{}`                                                                                    |
| `terminationGracePeriodSeconds`   | Executor termination grace period in seconds                                                                                                                                     | `5`                                                                                     |                                                   
| `nodeSelector`                    | Node labels for executor pod assignment                                                                                                                                          | `node-role.kubernetes.io/master: ""`                                                    |
| `tolerations`                     | Node taint label tolerations for executor assignment                                                                                                                             | `effect: NoSchedule`<br>`key: node-role.kubernetes.io/master`<br>`operator: Exists`     |
| `additionalClusterRoleBindings`   | Additional cluster role bindings that'll be added to the service account the executor pod runs with                                                                              | `""`                                                                                    |
| `additionalVolumeMounts`          | Additional volume mounts that'll be mounted to the executor container                                                                                                            | `""`                                                                                    |
| `additionalVolumes`               | Additional volumes that'll be mounted to the executor pod                                                                                                                        | `""`                                                                                    |
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the executor component | `false`                                                                                 |
| `prometheus.labels`               | Additional labels that'll be added to executor prometheus components                                                                                                             | `{}`                                                                                    |
| `prometheus.scrapeInterval`       | Scrape interval of the serviceMonitor and prometheusRule                                                                                                                         | `10s`                                                                                   |
| `customServiceAccount`            | Use existing service account for pod instead of creating a new one                                                                                                               | `null`                                                                                  |
| `serviceAccount`                  | Additional properties of service account (like imagePullSecrets)                                                                                                                 | `{}`                                                                                    |
| `applicationConfig`               | Config file override values, merged with /config/executor/config.yaml to make up the config file used when running the application                                               | `nil`                                                                                   |

#### Executor node configuration

By default the executor runs on the control plane. This is because it as a vital part of the cluster it is running on and managing.

*If the executor runs on the worker nodes, it could potentially get slowed down by the jobs it is scheduling on the cluster*

However there are times when this is not possible to do, such as using a managed kubernetes cluster where you cannot access the control plane.

To turn off running on the control plane, and instead just run on normal work nodes, add the following to your configuration:
 
 ```yaml
 nodeSelector: null
 tolerations: []
 ```

Alternatively you could have a dedicated node that the executor runs on. Then use the nodeSelector + tolerations to enforce it running that node.

This has the benefit of being separated from the cluster it is managing, but not being on the control plane. 

### Application Config

The applicationConfig section of the values file is purely used to override the default config for the executor component

It can override any value found in /config/executor/config.yaml

Commonly this will involve overriding the server url for example

The example format for this section looks like:

```yaml
applicationConfig:
  application:
    clusterId: "cluster-1"
  apiConnection:
    armadaUrl: "server.url.com:443"  
```

**Note:** The values you enter in this section will be placed into a K8s configmap. For senistive values (e.g. database passwords) we recommend setting them as environmental variables from a non-helm managed secret:
```yaml
env:
- name: ARMADA_POSTGRES_CONNECTION_PASSWORD
  valueFrom:
    secretKeyRef:
      key: password
      name: lookout-postgres-password
```

#### Credentials

##### Open Id

For connection to Armada server protected by open Id auth you can configure executor to perform Client Credentials flow.
To do this it is recommended to configure new client application with Open Id provider for executor and permission executor using custom scope.

For Amazon Cognito the configuration could look like this:
```yaml
applicationConfig:
  apiConnection:
    openIdClientCredentialsAuth:
      providerUrl: "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_*** your user pool id ***"
      clientId: "***"
      clientSecret: "***"
      scopes: ["armada/executor"]
```

To authenticate with username and password instead you can use Resource Owner (Password) flow:
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

##### Basic Auth

The credentials section of the values file is used to provide the username/password used for communication with the server component

The username/password used in this section must exist in the server components list of known users (populated by the servers .Values.credentials section of its helm chart).
If the username/password are not present in the server components secret, communication with the server component will be rejected

As an example, this section is formatted as:

```yaml
applicationConfig:
  apiConnection:
    basicAuth:
      username: "user1"
      password: "password1"
```

#### Kubernetes config

The default kubernetes related configuration is below:

```yaml
applicationConfig:
  kubernetes:
    impersonateUsers: false
    minimumPodAge: 3m
    failedPodExpiry: 10m
    stuckPodExpiry: 3m
```

**impersonateUsers**

If `impersonateUsers` is turned on, Armada will create pods in kubernetes impersonating the submitter of the job. 

This will allow Kubernetes to enforce permissions and limit access to namespaces. This is useful to prevent users from submitting jobs to namespaces their submitting user does not have access to. 

**minimumPodAge**

This is the minimum amount of time a pod exists for. If a pod completes in less than this time it will not be cleaned up until it has reached this age. This is helpful, especially with short jobs, as it gives you time to view pod, see the logs etc before it is cleaned up.

**failedPodExpiry**

This is the amount of after a pod fails before it is cleaned up. This allows you to view the logs of failed pods more easily, as they won't be cleaned up immediately. 

**stuckPodExpiry**

This is how long the executor will let a pod will sit in `Pending` state before it considers the Job stuck.

Once a Job is considered stuck a JobUnableToScheduleEvent will be reported for the job. 

It will then either have:
 - If the problem is deemed unretryable (for example the image is getting `InvalidImageName`) the job will get a JobFailedEvent and be considered Done
 - If the problem is deemed retryable, the job will have its lease returned to armada-server (JobLeaseReturnedEvent) and the job will be rescheduled 

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
**trackedNodeLabels**

This is a list of node labels that armada-executor will pay attention to, any other node labels will be ignored. 

Armada-executor will report these labels back to armada-server to allow jobs setting labelSelectors to be matched to these nodes for scheduling purposes. 

**toleratedTaints**

This is a list of node taints that armada-executor will consider usable by jobs. 

By default tainted nodes will be ignored by armada-executor and armada will not try to schedule jobs on them.

This is an opt-in for tainted nodes that you want armada to use and schedule jobs on.

It will report these taints back to armada-server so it use them when schedule jobs onto clusters. (i.e only jobs which tolerate these taints will be scheduled onto these nodes)

**minimumJobSize**

This is the minimum size a job must satisfy before it can be leased by this cluster.

This can be useful for giving different clusters different roles.

For example if you had a cluster with only GPU nodes, to make sure only GPU jobs are sent to this cluster, you could set the minimumJobSize like:

```yaml
minimumJobSize:
  nvidia.com/gpu: 1 
```

#### Metrics

The default metrics configuration is below:

```yaml
applicationConfig:
  metric:
    port: 9001
    exposeQueueUsageMetrics: false
```

**port**
 
 This controls the port that armada-executor exposes its metrics on

**exposeQueueUsageMetrics**

This feature will try to use metrics-server on the cluster. 

If metrics-server is not installed/accessible you'll see errors in the logs and usage will be incorrectly reported.

This determines if armada-executor:
  - Reports JobUtilisationEvent (containing the job's max cpu/memory usage)
  - Populates `armada_executor_job_pod_cpu_usage` and `armada_executor_job_pod_memory_usage_bytes` metrics with non-zero values

## Server helm chart

This document briefly outlines the customisation options of the server helm chart.

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
| `ingress.annotations`             | Additional annotations that'll be added to server ingresses                                                                                                                    | `{}`                                                                          |
| `ingress.labels`                  | Additional labels that'll be added to server ingresses                                                                                                                         | `{}`                                                                          |
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the server component | `false`                                                                       |
| `prometheus.labels`               | Additional labels that'll be added to server prometheus components                                                                                                             | `{}`                                                                          |
| `prometheus.scrapeInterval`       | Scrape interval of the serviceMonitor and prometheusRule                                                                                                                       | `10s`                                                                         |
| `customServiceAccount`            | Use existing service account for pod instead of creating a new one                                                                                                             | `null`                                                                        |
| `serviceAccount`                  | Additional properties of service account (like imagePullSecrets)                                                                                                               | `{}`                                                                          |
| `applicationConfig`               | Config file override values, merged with /config/armada/config.yaml to make up the config file used when running the application                                               |`grpcPort: 50051`                                                              |


### Application Config

The applicationConfig section of the values file is purely used to override the default config for the server component

It can override any value found in /config/armada/config.yaml

Commonly this will involve overriding the redis url for example

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

Server can be configured with multiple authentication methods.

##### Anonymous users
To enable anonymous users on server include this in your config:
```yaml
anonymousAuth: true
```

##### OpenId Authentication

Armada can be configured to use tokens signed by OpenId provider for authentication.
**Currently only JWT tokens are supported.**

To determine group membership of particular user, armada will read jwt claim specified in configuration as `groupsClaim`.

For example configuration using Amazon Cognito as provider can look like this:
```yaml
openIdAuth:
   providerUrl: "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_*** your user pool id ***"
   groupsClaim: "cognito:groups"
```

##### Basic Authentication

**Note: Basic authentication is not recommended for production deployment.**

The basicAuth section of the values file is used to provide a list of valid username/password pairs.

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

##### Kerberos Authentication
Server also support Kerberos authentication, but this feature might be removed in future version. If you can use different authentication method, please do.

```yaml
kerberos:
  keytabLocation: /etc/auth/serviceUser.kt  # location of keytab
  principalName: serviceUser                # name of service user
  userNameSuffix: -suffix                   # optional suffix appended to username which is read from kerberos ticket
```

#### Permissions
Armada allows you to specify these permissions for user:

| Permission         | Details                                                                           |
|--------------------|-----------------------------------------------------------------------------------|
| `submit_any_jobs`  | Allows users submit jobs to any queue.                                            |
| `create_queue`     | Allows users submit jobs to create queue.                                         |
| `cancel_any_jobs`  | Allows users cancel jobs from any queue.                                          |
| `watch_all_events` | Allows for watching all events.                                                   |
| `execute_jobs`     | Protects apis used by executor, only executor service should have this permission |

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
This is useful when using clients credentials flow for executor component.

```yaml
permissionScopeMapping:
  execute_jobs: ["armada/executor"]
```
 
By default every user (including anonymous one) is member of group `everyone`.

#### Job resource defaults

By default Armada-server will validate submitted jobs set some value for resource request and limit. 

It also makes sure `resource request = resource limit`. 

However if you remove the requirement for your users to set a resource request/limit or simply provide sensible defaults you can specify defaults with:

```yaml
scheduling:
  defaultJobLimits:
    memory: 8Gi
    cpu: 1
```

The default will only be used when a submitted job doesn't specify a request/limit for the resource type specified.

Using the above values, if a job was submitted with:

```yaml
  resources:
    limits:
      memory: 64Mi
    requests:
      memory: 64Mi
```

It will be transformed to this:

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

The default scheduling configuration can be seen below:

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

`queueLeaseBatchSize` This is the number of jobs at the top of a queue the scheduler will look at when trying to find one that'll fit in the available resource.

This is an optimisation so the scheduler doesn't need to check through the entire queue (which could be very large).

The lower this value is, the faster scheduling will be, but at the expense that jobs lower down in the queue which would have fit into the available resource may not get considered.


`minimumResourceToSchedule` is the minimum amount of resource a cluster has to have left available before jobs will be scheduled on it. 

This is just an optimisation to speed up scheduling, rather than looking through thousands of jobs to find one small enough. 

If you have many tiny jobs or very small clusters, you may want to decrease this below your average expected smallest job.

`maximalClusterFractionToSchedule` This is the maximum percentage of resource to schedule for a cluster per round.

If a cluster had 1000 cpu, the above settings would mean only 250 cpu would be scheduled each scheduling round.
 
#### Queue resource limits 

By default there is no Queue resource limits.

You may want these to stop a single queue being able to grab all the resource on all clusters during a quiet period and holding it for a long time (if all jobs are very long running).

You can specify resource limits per queue like:

```yaml
scheduling:
  maximalResourceFractionToSchedulePerQueue:
    memory: 0.05
    cpu: 0.05
  maximalResourceFractionPerQueue:
    memory: 0.25
    cpu: 0.25
```

All limits are proportional to overall amount of resources in the system. 

In this example, a queue can use at most 25% of all available cpu **and** memory.

`maximalResourceFractionPerQueue` Is the maximum resource a queue can hold as a percentage of the total resource of this type over all clusters.

`maximalResourceFractionToSchedulePerQueue` Is the percentage of total resource of this resource type a queue can be allocated in a single scheduling round.

Currently scheduling is done in parallel, so it can happen that we exceed the resource limit set in `maximalResourceFractionPerQueue` due to two clusters requesting new jobs at the exact same time.

To mitigate this, `maximalResourceFractionToSchedulePerQueue` specifies how much can be scheduled in a single round and can be thought of as the margin for error.

Using an example of having 1000 cpu over all your clusters:
`maximalResourceFractionPerQueue` Limits a queue to 250 cpu
`maximalResourceFractionToSchedulePerQueue` Limits the amount of resource a queue can be allocated in a single round to 50 cpu.

So in the extreme case two clusters request resource at the exact same time a queue could in theory get to 300 cpu.

We have tested this with many extremely large clusters and even when empty, it is pretty safe to assume the resource limit in the worst case is:

`worst case resource limit` = `maximalResourceFractionPerQueue` + `maximalResourceFractionToSchedulePerQueue`

For any resource type not specified in `maximalResourceFractionPerQueue` a queue can be allocated 100% of that resource type. (Hence the default is 100% of all resource types) 

#### Job lease configuration

The default job lease configuration can be seen below.

```yaml
scheduling:
  lease:
    expireAfter: 15m
    expiryLoopInterval: 5s
```

Leases expire when a armada-executor on a cluster stops contacting armada-server. 

Armada-server will wait a duration (`expireAfter`), then expire the leases which allows jobs to be leased on other clusters that are still contacting armada-server. This prevents jobs just getting lost forever because a cluster goes down.

If you have very long running jobs and you want to tolerate short network outages, increase `expireAfter`. If you want jobs to be quickly rescheduled onto new clusters when armada-executor loses contact, decrease `expireAfter`.

`expiryLoopInterval` simply controls how often the loop checking for expired leases runs. 
