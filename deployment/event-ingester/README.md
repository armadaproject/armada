# armada-event-ingester

![Version: 0.0.0-latest](https://img.shields.io/badge/Version-0.0.0--latest-informational?style=flat-square) ![AppVersion: 0.0.0-latest](https://img.shields.io/badge/AppVersion-0.0.0--latest-informational?style=flat-square)

A helm chart for Armada Event Ingester component

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` |  |
| additionalVolumeMounts | list | `[]` |  |
| additionalVolumes | list | `[]` |  |
| applicationConfig.batchMessages | int | `10000` |  |
| applicationConfig.batchSize | int | `1048576` |  |
| applicationConfig.metrics.redis.enabled | bool | `true` | Enable separate Redis client for metrics collection |
| applicationConfig.metrics.redis.connectionInfo.addrs | list | `["localhost:6379"]` | Redis addresses (standalone or sentinels) |
| applicationConfig.metrics.redis.connectionInfo.db | int | `1` | Redis database |
| applicationConfig.metrics.redis.connectionInfo.masterName | string | `""` | Sentinel master name (triggers sentinel mode) |
| applicationConfig.metrics.redis.connectionInfo.readOnly | bool | `false` | Use replica-only routing for metrics (required for Sentinel) |

### Redis Metrics Sentinel Configuration

When `applicationConfig.metrics.redis.enabled` is true, a separate Redis client is used for metrics scanning to avoid impact on the main event stream.

To enable Sentinel mode for metrics:
1. Set `applicationConfig.metrics.redis.connectionInfo.masterName` to your sentinel master name.
2. Provide sentinel endpoints in `applicationConfig.metrics.redis.connectionInfo.addrs`.
3. Set `applicationConfig.metrics.redis.connectionInfo.readOnly` to `true` to ensure metrics are collected from replicas.

**Note on Fail-Fast Behavior:**
If `masterName` is provided, the configuration MUST include valid `addrs` and `readOnly: true`. If the configuration is invalid or the client cannot be initialized in Sentinel mode, the application will fail to start with an explicit error. Fallback to the main event database connection is NOT supported when a metrics connection is explicitly configured.

## Values
