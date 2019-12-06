# Executor helm chart

This document briefly outlines the customisation options of the Executor helm chart.

## Values

| Parameter                         | Description                                                                                                                                                                      | Default                                                                          |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `image.repository`                | Executor image name                                                                                                                                                              | `gresearchdev/armada-executor`                                                   |
| `image.tag`                       | Executor image tag                                                                                                                                                               | `v0.0.1`                                                                         |
| `resources`                       | Executor resource request/limit                                                                                                                                                  | Request: <br/> `200m`, <br/> `512Mi` <br/>  Limit:  <br/>  `300m`,  <br/>  `1Gi` |
| `additionalLabels`                | Additional labels that'll be added to all executor components                                                                                                                    | `{}`                                                                             |
| `terminationGracePeriodSeconds`   | Executor termination grace period in seconds                                                                                                                                     | `5`                                                                              |                                                   
| `additionalClusterRoleBindings`   | Additional cluster role bindings that'll be added to the service account the executor pod runs with                                                                              | `""`                                                                             |
| `additionalVolumeMounts`          | Additional volume mounts that'll be mounted to the executor container                                                                                                            | `""`                                                                             |
| `additionalVolumes`               | Additional volumes that'll be mounted to the executor pod                                                                                                                        | `""`                                                                             |
| `prometheus.enabled`              | Flag to determine if Prometheus components are deployed or not. This should only be enabled if Prometheus is deployed and you want to scrape metrics from the executor component | `false`                                                                          |
| `prometheus.labels`               | Additional labels that'll be added to executor prometheus components                                                                                                             | `{}`                                                                             |
| `prometheus.scrapeInterval`       | Scrape interval of the serviceMonitor and prometheusRule                                                                                                                         | `10s`                                                                         |
| `applicationConfig`               | Config file override values, merged with /config/executor/config.yaml to make up the config file used when running the application                                               | `nil`                                                                            |

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
openIdClientCredentialsAuth:
  providerUrl: "https://cognito-idp.eu-west-2.amazonaws.com/eu-west-2_*** your user pool id ***"
  clientId: "***"
  clientSecret: "***"
  scopes: ["armada/executor"]
```

To authenticate with username and password instead you can use Resource Owner (Password) flow:
```yaml
OpenIdPasswordAuth:
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
basicAuth:
  username: "user1"
  password: "password1"
```

### Executor node configuration

By default the executor runs on the control plane. This is because it as a vital part of the cluster it is running on and managing.

*If the executor runs on the worker nodes, it could potentially get slowed down by the jobs it is scheduling on the cluster*

However there are times when this is not possible to do, such as using a managed kubernetes cluster where you cannot access the control plane.

To turn off running on the control plane, and instead just run on normal work nodes, add the following to your configuration:
 
 ```yaml
 nodeSelector: {}
 tolerations: {}
 ```

Alternatively you could have a dedicated node that the executor runs on. Then use the nodeSelector + tolerations to enforce it running that node.

This has the benefit of being separated from the cluster it is managing, but not being on the control plane. 
