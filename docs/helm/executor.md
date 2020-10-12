# Executor helm chart

This document briefly outlines the customisation options of the Executor helm chart.

## Values

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

### Executor node configuration

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

## Application Config

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

### Credentials

#### Open Id

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

#### Basic Auth

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

### Kubernetes config

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
 - If the probelm is deemed retryable, the job will Have its lease returned to armada-server (JobLeaseReturnedEvent) and the job will be rescheduled 

```yaml
applicationConfig:
  kubernetes:
    trackedNodeLabels:
    - somevalue
    toleratedTaints:
    - somevalue
    minimumJobSize:
      memory: 0.25
      cpu: 0.25
```
**trackedNodeLabels**

This is a list of node labels that armada-executor will pay attention to, any other node labels will be ignored. 

Armada-executor will report these labels back to armada-server to allow jobs setting labelSelectors to be matched to these nodes for scheduling purposes. 

**toleratedTaints**

This is a list of node taints that armada-executor will pay attention to, any other node taints will be ignored. 

It will report these taints back to armada-server so it use them when schedule jobs onto clusters. (i.e only jobs which tolerate these taints will be scheduled onto these nodes)

**minimumJobSize**

This is the minimum size a job must satisfy before it can be leased by this cluster.

This can be useful for giving different clusters different roles.

For example if you had a cluster with only GPU nodes, to make sure only GPU jobs are sent to this cluster, you could set the minimumJobSize like:

```yaml
minimumJobSize:
  nvidia.com/gpu: 1 
```

### Metrics

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
