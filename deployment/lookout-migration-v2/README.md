# armada-lookout-migration-v2

![Version: 0.0.0-latest](https://img.shields.io/badge/Version-0.0.0--latest-informational?style=flat-square) ![AppVersion: 0.0.0-latest](https://img.shields.io/badge/AppVersion-0.0.0--latest-informational?style=flat-square)

A Helm chart for the Armada Lookout v2 database migration

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| additionalLabels | object | `{}` |  |
| additionalVolumeMounts | list | `[]` |  |
| additionalVolumes | list | `[]` |  |
| applicationConfig | object | `{}` |  |
| customServiceAccount | string | `nil` |  |
| image.repository | string | `"gresearchdev/armada-lookout-v2"` |  |
| image.tag | string | `"0.0.0-latest"` |  |
| resources.limits.cpu | string | `"200m"` |  |
| resources.limits.memory | string | `"256Mi"` |  |
| resources.requests.cpu | string | `"100m"` |  |
| resources.requests.memory | string | `"128Mi"` |  |
| serviceAccount | string | `nil` |  |
| tolerations | list | `[]` | Tolerations |

