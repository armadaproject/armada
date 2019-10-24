# Server helm chart

This document briefly outlines the customisation options of the server helm chart.

## Values

| Parameter                         | Description                                                                                                                                                                    | Default                                                                       |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `image.repository`                | Server image name                                                                                                                                                              | `tba`                                                                         |
| `image.tag`                       | Server image tag                                                                                                                                                               | `{TAG_NAME}`                                                                  |
| `resources`                       | Server resource request/limit                                                                                                                                                  | Request: <br/> `200m`, <br/> `512Mi`.<br/>  Limit: <br/> `300m`, <br/>  `1Gi` |
| `terminationGracePeriodSeconds`   | Server termination grace period in seconds                                                                                                                                     | `0`                                                                           |                                                   
| `replicas`                        | Number of replicas (pods) of the server component                                                                                                                              | `1`                                                                           |                                                   
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the server component | `false`                                                                       |
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
