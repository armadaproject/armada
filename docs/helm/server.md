# Server helm chart

This document briefly outlines the customisation options of the server helm chart.

## Values

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


## Application Config

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

### Authentication

Server can be configured with multiple authentication methods.

#### Anonymous users
To enable anonymous users on server include this in your config:
```yaml
anonymousAuth: true
```

#### OpenId Authentication

Armada can be configured to use tokens signed by OpenId provider for authentication.
**Currently only JWT tokens are supported.**

To determine group membership of particular user, armada will read jwt claim specified in configuration as `groupsClaim`.

For example configuration using Amazon Cognito as provider can look like this:
```yaml
openIdAuth:
   providerUrl: "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_*** your user pool id ***"
   groupsClaim: "cognito:groups"
```

#### Basic Authentication

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

#### Kerberos Authentication
Server also support Kerberos authentication, but this feature might be removed in future version. If you can use different authentication method, please do.

```yaml
kerberos:
  keytabLocation: /etc/auth/serviceUser.kt  # location of keytab
  principalName: serviceUser                # name of service user
  userNameSuffix: -suffix                   # optional suffix appended to username which is read from kerberos ticket
```

### Permissions
Armada allows you to specify these permissions for user:

| Permission         | Details
|--------------------|-------------------------------------------
| submit_jobs        | Allows users submit jobs to their queue.
| submit_any_jobs    | Allows users submit jobs to any queue.
| create_queue       | Allows users submit jobs to create queue.
| cancel_jobs        | Allows users cancel jobs from their queue.
| cancel_any_jobs    | Allows users cancel jobs from any queue.
| watch_all_events   | Allows for watching all events.
| execute_jobs       | Protects apis used by executor, only executor service should have this permission

Permissions can be assigned to user by group membership, like this:

```yaml
permissionGroupMapping:
  submit_jobs: ["teamA", "administrators"]
  submit_any_jobs: ["administrators"]
  create_queue: ["administrators"]
  cancel_jobs: ["teamA", "administrators"]
  cancel_any_jobs: ["administrators"]
  watch_all_events: ["teamA", "administrators"]
  execute_jobs: ["armada-executor"]
```

In case of Open Id access tokens, permissions can be assigned based on token scope.
This is useful when using clients credentials flow for executor component.

```yaml
permissionScopeMapping:
  execute_jobs: ["armada/executor"]
```
 
By default every user (including anonymous one) is member of group `everyone`.

### Job resource defaults

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

### Scheduling

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
 
### Queue resource limits 

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

### Job lease configuration

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
