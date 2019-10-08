# Server values

This document briefly outlines the options found in the executor values file and how to use them

| Parameter                         | Description                                                                                                                                                                      | Default                                                 |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| `image.repository`                | Executor image name                                                                                                                                                              | `tba`                                                   |
| `image.tag`                       | Executor image tag                                                                                                                                                               | `{TAG_NAME}`                                            |
| `resources`                       | Executor resource request/limit                                                                                                                                                  | Request: `200m`, `512Mi`. Limit: `300m`, `1Gi`          |
| `terminationGracePeriodSeconds`   | Executor termination grace period in seconds                                                                                                                                     | `0`                                                     |                                                   
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the executor component | `false`                                                 |
| `applicationConfig`               | Config file override values, merged with /config/executor/config.yaml to make up the config file used when running the application                                               | `nil'                                                   |
| `credentials`                     | The credentials used by the executor when communicating with the server component                                                                                                | `nil`                                                   |


## applicationConfig example

The applicationConfig section of the values file is purely used to override the default config for the executor component

It can override any value found in /config/executor/config.yaml

Commonly this will involve overriding the server url for example

The example format for this section looks like:

```yaml
applicationConfig:
  application:
    clusterId : "cluster-1"
  armada:
    url : "server.url.com:443"  
```

## credentials example

The credentials section of the values file is used to provide the username/password used for communication with the server component

The username/password used in this section must exist in the server components list of known users (populated by the servers .Values.credentials section of its helm chart).
If the username/password are not present in the server components secret, communication with the server component will be rejected

As an example, this section is formatted as:

```yaml
credentials:
  username: "user1"
  password: "password1"
```
