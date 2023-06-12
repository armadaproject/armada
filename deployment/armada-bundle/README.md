# armada-bundle

![Version: 0.0.1](https://img.shields.io/badge/Version-0.0.1-informational?style=flat-square) ![AppVersion: 0.0.0-latest](https://img.shields.io/badge/AppVersion-0.0.0--latest-informational?style=flat-square)

A helm chart which bundles Armada components

## Requirements

| Repository | Name | Version |
|------------|------|---------|
| https://charts.bitnami.com/bitnami | postgresql | 11.1.27 |
| https://dandydeveloper.github.io/charts | redis-ha | 4.15.0 |
| https://armadaproject.github.io/charts/ | armada | v0.3.36 |
| https://armadaproject.github.io/charts/ | armada-executor | v0.3.36 |
| https://armadaproject.github.io/charts/ | armada-lookout | v0.3.36 |
| https://armadaproject.github.io/charts/ | armada-lookout-ingester | v0.3.36 |
| https://armadaproject.github.io/charts/ | armada-lookout-migration | v0.3.20 |
| https://armadaproject.github.io/charts | executor-cluster-monitoring | v0.1.9 |
| https://nats-io.github.io/k8s/helm/charts | stan | 0.13.0 |
| https://pulsar.apache.org/charts | pulsar | 2.9.3 |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| armada-executor.applicationConfig.apiConnection.armadaUrl | string | `"armada.default.svc.cluster.local:50051"` | URL of Armada Server gRPC endpoint |
| armada-executor.applicationConfig.apiConnection.forceNoTls | bool | `true` | Only to be used for development purposes and in cases where Armada server does not have a certificate |
| armada-executor.applicationConfig.kubernetes.minimumPodAge | string | `"0s"` |  |
| armada-executor.image.repository | string | `"gresearchdev/armada-executor"` |  |
| armada-executor.image.tag | string | `"v0.3.36"` |  |
| armada-executor.nodeSelector | string | `"nil"` |  |
| armada-executor.prometheus.enabled | bool | `true` | Toggle whether to create ServiceMonitor for Armada Executor |
| armada-lookout-ingester.applicationConfig.postgres.connMaxLifetime | string | `"30m"` | Postgres connection max lifetime |
| armada-lookout-ingester.applicationConfig.postgres.connection.dbname | string | `"postgres"` | Postgres database |
| armada-lookout-ingester.applicationConfig.postgres.connection.host | string | `"postgresql.armada.svc.cluster.local"` | Postgres host |
| armada-lookout-ingester.applicationConfig.postgres.connection.password | string | `"psw"` | Postgres user password |
| armada-lookout-ingester.applicationConfig.postgres.connection.port | int | `5432` | Postgres port |
| armada-lookout-ingester.applicationConfig.postgres.connection.sslmode | string | `"disable"` | Postgres SSL mode |
| armada-lookout-ingester.applicationConfig.postgres.connection.user | string | `"postgres"` | Postgres username |
| armada-lookout-ingester.applicationConfig.postgres.maxIdleConns | int | `25` | Postgres max idle connections |
| armada-lookout-ingester.applicationConfig.postgres.maxOpenConns | int | `100` | Postgres max open connections |
| armada-lookout-ingester.applicationConfig.pulsar.URL | string | `"pulsar://pulsar-broker.armada.svc.cluster.local:6650"` | Pulsar connection string |
| armada-lookout-ingester.applicationConfig.pulsar.enabled | bool | `true` | Toggle whether to connect to Pulsar |
| armada-lookout-ingester.applicationConfig.pulsar.jobsetEventsTopic | string | `"persistent://armada/armada/events"` |  |
| armada-lookout-ingester.image.repository | string | `"gresearchdev/armada-lookout-ingester-dev"` |  |
| armada-lookout-ingester.image.tag | string | `"88ea8f0b8124c7dbbb44f7c7315c0fca13655f18"` |  |
| armada-lookout-migration.applicationConfig.postgres.connMaxLifetime | string | `"30m"` | Postgres connection max lifetime |
| armada-lookout-migration.applicationConfig.postgres.connection.dbname | string | `"postgres"` | Postgres database |
| armada-lookout-migration.applicationConfig.postgres.connection.host | string | `"postgresql.armada.svc.cluster.local"` | Postgres host |
| armada-lookout-migration.applicationConfig.postgres.connection.password | string | `"psw"` | Postgres user password |
| armada-lookout-migration.applicationConfig.postgres.connection.port | int | `5432` | Postgres port |
| armada-lookout-migration.applicationConfig.postgres.connection.user | string | `"postgres"` | Postgres username |
| armada-lookout-migration.applicationConfig.postgres.maxIdleConns | int | `25` | Postgres max idle connections |
| armada-lookout-migration.applicationConfig.postgres.maxOpenConns | int | `100` | Postgres max open connections |
| armada-lookout-migration.clusterIssuer | string | `"letsencrypt-dev"` |  |
| armada-lookout-migration.image.tag | string | `"v0.3.36"` |  |
| armada-lookout-migration.ingressClass | string | `"nginx"` |  |
| armada-lookout-migration.prometheus.enabled | bool | `true` |  |
| armada-lookout.applicationConfig.disableEventProcessing | bool | `true` | Armada does not require a streaming backend anymore so this options turns off processing via streaming backend (Jetstream, SNAT) |
| armada-lookout.applicationConfig.eventQueue | string | `"ArmadaLookoutEventProcessor"` |  |
| armada-lookout.applicationConfig.postgres.connection.dbname | string | `"postgres"` | Postgres database |
| armada-lookout.applicationConfig.postgres.connection.host | string | `"postgresql.armada.svc.cluster.local"` | Postgres host |
| armada-lookout.applicationConfig.postgres.connection.password | string | `"psw"` | Postgres user password |
| armada-lookout.applicationConfig.postgres.connection.port | int | `5432` | Postgres port |
| armada-lookout.applicationConfig.postgres.connection.user | string | `"postgres"` | Postgres username |
| armada-lookout.clusterIssuer | string | `"dev-ca"` | ClusterIssuer from whom a Let's Encrypt certificate will be requested |
| armada-lookout.hostnames | list | `[]` | Ingress hostnames |
| armada-lookout.image.repository | string | `"gresearchdev/armada-lookout"` |  |
| armada-lookout.image.tag | string | `"v0.3.36"` |  |
| armada-lookout.ingress.annotations | object | `{}` | Ingress annotations |
| armada-lookout.ingress.labels | object | `{}` | Ingress labels |
| armada-lookout.ingressClass | string | `"nginx"` | Ingress class |
| armada-lookout.prometheus.enabled | bool | `true` | Toggle whether to create a ServiceMonitor for Lookout |
| armada.applicationConfig.auth | object | `{}` | Armada auth config |
| armada.applicationConfig.eventsNats.QueueGroup | string | `"ArmadaEventRedisProcessor"` |  |
| armada.applicationConfig.eventsNats.clusterId | string | `"armada-cluster"` | STAN cluster ID |
| armada.applicationConfig.eventsNats.servers | list | `[]` | events STAN URIs |
| armada.applicationConfig.eventsNats.subject | string | `"ArmadaTest"` |  |
| armada.applicationConfig.eventsNats.timeout | string | `"10s"` |  |
| armada.applicationConfig.eventsRedis.addrs | list | `[]` | events Redis cluster instance URLs |
| armada.applicationConfig.eventsRedis.masterName | string | `"mymaster"` |  |
| armada.applicationConfig.eventsRedis.poolSize | int | `1000` |  |
| armada.applicationConfig.grpcPort | int | `50051` |  |
| armada.applicationConfig.httpPort | int | `8080` |  |
| armada.applicationConfig.pulsar.URL | string | `"pulsar://pulsar-broker.armada.svc.cluster.local:6650"` | Pulsar connection string |
| armada.applicationConfig.pulsar.enabled | bool | `true` | Toggle whether to connect to Pulsar |
| armada.applicationConfig.redis.addrs | list | `[]` | master Redis cluster instance URLs |
| armada.applicationConfig.redis.masterName | string | `"mymaster"` |  |
| armada.applicationConfig.scheduling.defaultJobTolerations | list | `[]` | default node tolerations for Armada jobs |
| armada.applicationConfig.scheduling.probabilityOfUsingNewScheduler | int | `1` |  |
| armada.clusterIssuer | string | `"dev-ca"` | ClusterIssuer from whom a Let's Encrypt certificate will be requested |
| armada.hostnames | list | `[]` |  |
| armada.image.repository | string | `"gresearchdev/armada-server"` |  |
| armada.image.tag | string | `"v0.3.36"` |  |
| armada.ingressClass | string | `"nginx"` | Ingress class |
| armada.nodePort | int | `30000` |  |
| armada.prometheus.enabled | bool | `true` | Toggle whether to create a ServiceMonitor for Armada Server |
| dependencies.armada-executor | bool | `true` | Toggle whether to install Armada Executor |
| dependencies.armada-lookout | bool | `true` | Toggle whether to install Armada Lookout UI |
| dependencies.armada-lookout-ingester | bool | `true` | Toggle whether to install Armada Lookout Ingester |
| dependencies.armada-lookout-migration | bool | `true` | Toggle whether to install Armada Lookout migrations |
| dependencies.armada-server | bool | `true` | Toggle whether to install Armada Server |
| dependencies.executor-cluster-monitoring | bool | `true` | Toggle whether to install executor cluster monitoring rules (NOTE: requires Prometheus CRDs) |
| dependencies.postgresql | bool | `true` | Toggle whether to install PostgreSQL |
| dependencies.pulsar | bool | `true` | Toggle whether to install Pulsar |
| dependencies.redis-ha | bool | `true` | Toggle whether to install Redis HA cluster |
| dependencies.stan | bool | `true` | Toggle whether to install Streaming NATS |
| executor-cluster-monitoring.additionalLabels.app | string | `"prometheus-operator"` |  |
| executor-cluster-monitoring.additionalLabels.release | string | `"prometheus-operator"` |  |
| executor-cluster-monitoring.interval | string | `"5s"` |  |
| postgresql.auth.postgresPassword | string | `"psw"` |  |
| postgresql.fullnameOverride | string | `"postgresql"` |  |
| pulsar.armadaInit.adminPort | int | `8080` | Pulsar admin (REST) port |
| pulsar.armadaInit.brokerHost | string | `"pulsar-broker.armada.svc.cluster.local"` | Pulsar Broker host |
| pulsar.armadaInit.enabled | bool | `false` | Toggle whether to enable the job which creates necessary Pulsar resources needed by Armada |
| pulsar.armadaInit.image.repository | string | `"apachepulsar/pulsar"` | Pulsar image which contains pulsar-admin |
| pulsar.armadaInit.image.tag | string | `"3.0.0"` | Pulsar image tag |
| pulsar.armadaInit.port | int | `6650` | Pulsar application port |
| pulsar.armadaInit.protocol | string | `"http"` | Protocol used for connecting to Pulsar Broker host (either `http` or `https`) |
| pulsar.fullnameOverride | string | `"pulsar"` | Fullname override for Pulsar release |
| pulsar.grafana.service.type | string | `"ClusterIP"` | Pulsar Grafana kubernetes service type |
| pulsar.initialize | bool | `true` |  |
| pulsar.proxy.service.type | string | `"ClusterIP"` | Pulsar Proxy kubernetes service type |
| redis-ha.fullnameOverride | string | `"redis-ha"` |  |
| redis-ha.hardAntiAffinity | bool | `false` |  |
| redis-ha.persistentVolume.enabled | bool | `false` |  |
| stan.nameOverride | string | `"stan"` |  |
| stan.stan.clusterID | string | `"armada-cluster"` | Streaming NATS Cluster ID (set during install and saved in the persistence backend) |

----------------------------------------------
Autogenerated from chart metadata using [helm-docs v1.11.0](https://github.com/norwoodj/helm-docs/releases/v1.11.0)
