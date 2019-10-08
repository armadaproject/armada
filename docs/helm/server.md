# Server values

This document briefly outlines the options found in the server values file and how to use them

| Parameter                         | Description                                                                                                                                                                    | Default                                                                       |
|-----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `image.repository`                | Server image name                                                                                                                                                              | `tba`                                                                         |
| `image.tag`                       | Server image tag                                                                                                                                                               | `{TAG_NAME}`                                                                  |
| `resources`                       | Server resource request/limit                                                                                                                                                  | Request: <br/> `200m`, <br/> `512Mi`.<br/>  Limit: <br/> `300m`, <br/>  `1Gi` |
| `terminationGracePeriodSeconds`   | Server termination grace period in seconds                                                                                                                                     | `0`                                                                           |                                                   
| `replicas`                        | Number of replicas (pods) of the server component                                                                                                                              | `1`                                                                           |                                                   
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the server component | `false`                                                                       |
| `applicationConfig`               | Config file override values, merged with /config/armada/config.yaml to make up the config file used when running the application                                               |`grpcPort: 50051`                                                              |
| `credentials`                     | List of valid users/passwords used to login to the application                                                                                                                 | `nil`                                                                         |

## applicationConfig example

The applicationConfig section of the values file is purely used to override the default config for the server component

It can override any value found in /config/armada/config.yaml

Commonly this will involve overriding the redis url for example

As an example, this section is formatted as:

```yaml
applicationConfig:
  redis:
    masterName: "mymaster"
    sentinelAddresses: 
      - "redis-ha.default.svc.cluster.local:26379"
  eventsRedis:   
    masterName: "mymaster"
    sentinelAddresses: 
      - "redis-ha.default.svc.cluster.local:26379"
```

## credentials example

The credentials section of the values file is used to provide a list of valid username/password pairs.

This list determines all the valid users for the server components gRPC API.  All clients (executor or armadactl) must provide a valid set of credentials from this list when communicating with the server.

As an example, this section is formatted as:

```yaml
credentials:
  users:
    "user1": "password1"
```
