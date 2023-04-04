# armada

![Version: LATEST](https://img.shields.io/badge/Version-LATEST-informational?style=flat-square)

Armada Server is the centralized Control Plane for Armada, the multi-cluster batch scheduler.

## Prerequisites

Armada Server requires the following components to be accessible and configured:
* [Pulsar](https://pulsar.apache.org/) - open-source, distributed messaging and streaming platform built for the cloud.
* [Redis](https://redis.io/) - open source, in-memory data store
* [Postgres](https://www.postgresql.org/) - powerful, open source object-relational database

## Install

Add `gresearch` Helm repository and fetch latest charts info:

```sh
helm repo add gresearch https://g-research.github.io/charts
helm repo update
```

Install `armada-server` using Helm:

```sh
helm install armada-server gresearch/armada \
    --set ingress.enabled=false
```

## Uninstall

Uninstall armada-server using Helm:

```sh
helm uninstall armada-server
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` | Additional labels for all Armada Server K8s resources |
| additionalVolumeMounts | list | `[]` | Additional volume mounts for Armada Server Deployment resource |
| additionalVolumes | list | `[]` | Additional volumes for Armada Server Deployment resource |
| applicationConfig.eventsApiRedis.addrs | list | `[]` | Redis hosts |
| applicationConfig.eventsApiRedis.password | string | `""` | Redis password |
| applicationConfig.grpcPort | int | `50051` | Armada Server gRPC port |
| applicationConfig.httpPort | int | `8080` | Armada Server REST port |
| applicationConfig.postgres.connection.dbname | string | `"postgres"` | Postgres database name |
| applicationConfig.postgres.connection.host | string | `"postgres"` | Postgres host |
| applicationConfig.postgres.connection.password | string | `"psw"` | Postgres password |
| applicationConfig.postgres.connection.port | int | `5432` | Postgres port |
| applicationConfig.postgres.connection.sslmode | string | `"disable"` | Toggle whether to use SSL mode |
| applicationConfig.postgres.connection.user | string | `"postgres"` | Postgres user |
| applicationConfig.pulsar.URL | string | `"pulsar://pulsar:6650"` | Pulsar Broker URL |
| applicationConfig.pulsar.authenticationEnabled | bool | `false` | Toggle whether to mount Pulsar Token secret |
| applicationConfig.pulsar.authenticationSecret | string | `"armada-pulsar-token-armada-admin"` | Name of the secret which contains the Pulsar Token |
| applicationConfig.pulsar.cacert | string | `"armada-pulsar-ca-tls"` | Name of the secret which contains the Pulsar CA certificate |
| applicationConfig.pulsar.tlsEnabled | bool | `false` | Toggle whether to mount Pulsar CA certificate secret |
| applicationConfig.redis.addrs | list | `[]` | Redis hosts |
| applicationConfig.redis.password | string | `""` |  |
| clusterIssuer | string | `""` | cert-manager's ClusterIssuer from which to request TLS certificate |
| containerSecurityContext | object | `{"allowPrivilegeEscalation":false}` | Security Context for armada Container |
| customServiceAccount | string | `""` | If specified, custom ServiceAccount name will be attached to Armada Server Deployment resource and the default ServiceAccount will not be created |
| env | object | `{}` | Additional environment variables for Armada Server Deployment resource |
| fullnameOverride | string | `""` |  |
| hostnames | list | `[]` | Hostnames for which to create gRPC and REST Ingress rules |
| image.repository | string | `"gresearchdev/armada-server"` |  |
| image.tag | string | `"v0.3.39"` |  |
| ingress.annotations | object | `{}` | Additional annotations for Ingress resource |
| ingress.enabled | bool | `true` | Toggle whether to create gRPC and HTTP Ingress for Armada Server |
| ingress.labels | object | `{}` | Additional labels for Ingress resource |
| ingress.nameOverride | string | `""` | Ingress resource name override |
| ingressClass | string | `"nginx"` | Ingress Class |
| nameOverride | string | `""` |  |
| podDisruptionBudget | object | `{}` |  |
| podSecurityContext | object | `{}` | Pod Security Context |
| prometheus.enabled | bool | `false` | Toggle whether to install ServiceMonitor and PrometheusRule for Armada Server monitoring |
| prometheus.labels | object | `{}` | Additional labels for ServiceMonitor and PrometheusRule |
| prometheus.scrapeInterval | string | `"15s"` | Prometheus scrape interval |
| replicas | int | `1` | Armada Server replica count |
| resources | object | `{"limits":{"cpu":"300m","memory":"1Gi"},"requests":{"cpu":"200m","memory":"512Mi"}}` | resource configuration |
| serviceAccount | object | `{}` | Additional ServiceAccount properties (e.g. automountServiceAccountToken, imagePullSecrets, etc.) |
| strategy.rollingUpdate.maxUnavailable | int | `1` |  |
| strategy.type | string | `"RollingUpdate"` |  |
| terminationGracePeriodSeconds | int | `30` | Number of seconds to wait for Armada Server to gracefully shutdown |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.11.0](https://github.com/norwoodj/helm-docs/releases/v1.11.0)
