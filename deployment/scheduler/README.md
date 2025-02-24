# armada-scheduler

![Version: 0.0.0-latest](https://img.shields.io/badge/Version-0.0.0--latest-informational?style=flat-square) ![AppVersion: 0.0.0-latest](https://img.shields.io/badge/AppVersion-0.0.0--latest-informational?style=flat-square)

A helm chart for Armada Scheduler component

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` |  |
| ingester.additionalVolumeMounts | list | `[]` |  |
| ingester.additionalVolumes | list | `[]` |  |
| ingester.applicationConfig.metricsPort | int | `9003` |  |
| ingester.applicationConfig.pulsar | object | `{}` |  |
| ingester.customServiceAccount | string | `nil` |  |
| ingester.image.repository | string | `"gresearchdev/armada-scheduler-ingester"` |  |
| ingester.image.tag | string | `"0.0.0-latest"` |  |
| ingester.prometheus.enabled | bool | `false` |  |
| ingester.prometheus.labels | object | `{}` |  |
| ingester.prometheus.scrapeInterval | string | `"15s"` |  |
| ingester.prometheus.scrapeTimeout | string | `"10s"` |  |
| ingester.replicas | int | `1` |  |
| ingester.resources.limits.cpu | string | `"300m"` |  |
| ingester.resources.limits.memory | string | `"1Gi"` |  |
| ingester.resources.requests.cpu | string | `"200m"` |  |
| ingester.resources.requests.memory | string | `"512Mi"` |  |
| ingester.serviceAccount | string | `nil` |  |
| ingester.strategy.rollingUpdate.maxUnavailable | int | `1` |  |
| ingester.strategy.type | string | `"RollingUpdate"` |  |
| ingester.terminationGracePeriodSeconds | int | `30` |  |
| pruner.applicationConfig | object | `{}` |  |
| pruner.args.batchsize | int | `10000` |  |
| pruner.args.expireAfter | string | `"2h"` |  |
| pruner.args.timeout | string | `"5m"` |  |
| pruner.enabled | bool | `true` |  |
| pruner.resources.limits.cpu | string | `"300m"` |  |
| pruner.resources.limits.memory | string | `"1Gi"` |  |
| pruner.resources.requests.cpu | string | `"200m"` |  |
| pruner.resources.requests.memory | string | `"512Mi"` |  |
| pruner.schedule | string | `"@hourly"` |  |
| scheduler.additionalVolumeMounts | list | `[]` |  |
| scheduler.additionalVolumes | list | `[]` |  |
| scheduler.applicationConfig.grpc.port | int | `50051` |  |
| scheduler.applicationConfig.grpc.tls.certPath | string | `"/certs/tls.crt"` |  |
| scheduler.applicationConfig.grpc.tls.enabled | bool | `false` |  |
| scheduler.applicationConfig.grpc.tls.keyPath | string | `"/certs/tls.key"` |  |
| scheduler.applicationConfig.http.port | int | `8080` |  |
| scheduler.applicationConfig.metrics.port | int | `9001` |  |
| scheduler.applicationConfig.pulsar | object | `{}` |  |
| scheduler.customServiceAccount | string | `nil` |  |
| scheduler.image.repository | string | `"gresearchdev/armada-scheduler"` |  |
| scheduler.image.tag | string | `"0.0.0-latest"` |  |
| scheduler.ingress.annotations | object | `{}` |  |
| scheduler.ingress.labels | object | `{}` |  |
| scheduler.ingress.nameOverride | string | `""` |  |
| scheduler.prometheus.enabled | bool | `false` |  |
| scheduler.prometheus.labels | object | `{}` |  |
| scheduler.prometheus.scrapeInterval | string | `"15s"` |  |
| scheduler.prometheus.scrapeTimeout | string | `"10s"` |  |
| scheduler.replicas | int | `1` |  |
| scheduler.resources.limits.cpu | string | `"300m"` |  |
| scheduler.resources.limits.memory | string | `"1Gi"` |  |
| scheduler.resources.requests.cpu | string | `"200m"` |  |
| scheduler.resources.requests.memory | string | `"512Mi"` |  |
| scheduler.serviceAccount | string | `nil` |  |
| scheduler.terminationGracePeriodSeconds | int | `30` |  |
| scheduler.updateStrategy.rollingUpdate.maxUnavailable | int | `1` |  |
| scheduler.updateStrategy.type | string | `"RollingUpdate"` |  |
| tolerations | list | `[]` | Tolerations |

