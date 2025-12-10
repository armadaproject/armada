<div align="center">
 <img src="./logo.svg" alt="Armada logo" width="200"/>
 <p>
  <a href="https://circleci.com/gh/armadaproject/armada"><img src="https://circleci.com/gh/helm/helm.svg?style=shield" alt="CircleCI"></a>
  <a href="https://goreportcard.com/report/github.com/armadaproject/armada"><img src="https://goreportcard.com/badge/github.com/armadaproject/armada" alt="Go Report Card"></a>
  <a href="https://artifacthub.io/packages/helm/gresearch/armada" title="Go to Artifact Hub"><img src="https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/armada" alt="Artifact Hub"></a>
  <a href="https://insights.linuxfoundation.org/project/armada" title="Click to view project insights and health checks"><img src="https://insights.linuxfoundation.org/api/badge/health-score?project=armada" alt="LFX Health Score"></a>
 </p>
</div>

# Armada

Armada is a system built on top of [Kubernetes](https://kubernetes.io/docs/concepts/overview/) for running batch workloads. With Armada as middleware for batch, Kubernetes can be a common substrate for batch and service workloads. Armada is used in production and can run millions of jobs per day across tens of thousands of nodes.

Armada addresses the following limitations of Kubernetes:

1. Scaling a single Kubernetes cluster beyond a certain size is [challenging](https://openai.com/blog/scaling-kubernetes-to-7500-nodes/). Hence, Armada is designed to effectively schedule jobs across many Kubernetes clusters. Many thousands of nodes can be managed by Armada in this way.
2. Achieving very high throughput using the in-cluster storage backend, etcd, is [challenging](https://etcd.io/docs/v3.5/op-guide/performance/). Hence, Armada performs queueing and scheduling out-of-cluster using a specialized storage layer. This allows Armada to maintain queues composed of millions of jobs.
3. The default [kube-scheduler](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-scheduler/) is not suitable for batch. Instead, Armada includes a novel multi-Kubernetes cluster scheduler with support for important batch scheduling features, such as:
    - Fair queuing and scheduling across multiple users. Based on dominant resource fairness.
    - Resource and job scheduling rate limits.
    - Gang-scheduling, i.e., atomically scheduling sets of related jobs.
    - Job preemption, both to run urgent jobs in a timely fashion and to balance resource allocation between users.

Armada also provides features to help manage large compute clusters effectively, including:

- Detailed analytics exposed via [Prometheus](https://prometheus.io/) showing how the system behaves and how resources are allocated.
- Automatically removing nodes exhibiting high failure rates from consideration for scheduling.
- A mechanism to earmark nodes for a particular set of jobs, but allowing them to be used by other jobs when not used for their primary purpose.

Armada is designed with the enterprise in mind; all components are secure and highly available.

Armada is a [CNCF](https://www.cncf.io/) Sandbox project and is used in production at [G-Research](https://www.gresearch.co.uk/).

For an overview of Armada, see the following videos:

- [Armada - high-throughput batch scheduling](https://www.youtube.com/watch?v=FT8pXYciD9A)
- [Building Armada - Running Batch Jobs at Massive Scale on Kubernetes](https://www.youtube.com/watch?v=B3WPxw3OUl4)

The Armada project adheres to the CNCF [Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## Installation

### Armada Operator

For installation instructions, easiest way is to use the Armada Operator.
For more information, see the [Armada Operator repository](https://github.com/armadaproject/armada-operator).

Alternatively, you can install Armada manually by using the Helm charts defined in the `deployment` directory.

### armadactl

Armada also provides a command-line interface, `armadactl`, which can be used to interact with the Armada system.

To install `armadactl`, run the following script:

```bash
scripts/get-armadactl.sh
```

Or download it from the [GitHub Release](https://github.com/armadaproject/armada/releases/latest) page for your platform.

## Local Development

### Local Development with Goreman

[Goreman](https://github.com/mattn/goreman) is a Go-based clone of [Foreman](https://github.com/ddollar/foreman) that manages Procfile-based applications,
allowing you to run multiple processes with a single command.

Goreman will build the components from source and run them locally, making it easy to test changes quickly.

1. Install `goreman`:

    ```shell
    go install github.com/mattn/goreman@latest
    ```

2. Start dependencies:

    ```shell
    docker-compose -f _local/docker-compose-deps.yaml up -d
    ```

    - **Note**: Images can be overridden using environment variables:
      `REDIS_IMAGE`, `POSTGRES_IMAGE`, `PULSAR_IMAGE`, `KEYCLOAK_IMAGE`

3. Initialize databases and Kubernetes resources:

    ```shell
    scripts/localdev-init.sh
    ```

4. Start Armada components:

    ```shell
    goreman -f _local/procfiles/no-auth.Procfile start
    ```

### Local Development with Authentication

To run Armada with OIDC authentication enabled using Keycloak:

1. Start dependencies with the auth profile:

    ```shell
    docker-compose -f _local/docker-compose-deps.yaml --profile auth up -d
    ```

    This starts Redis, PostgreSQL, Pulsar, and Keycloak with a pre-configured realm.

2. Initialize databases and Kubernetes resources:

    ```shell
    scripts/localdev-init.sh
    ```

3. Start Armada components with auth configuration:

    ```shell
    goreman -f _local/procfiles/auth.Procfile start
    ```

4. Use armadactl with OIDC authentication:

    ```shell
    armadactl --config _local/.armadactl.yaml --context auth-oidc get queues
    ```

#### Authentication Configuration

The auth profile configures:

- **Keycloak**: OIDC provider running on <http://localhost:8180> with pre-configured realm, users, and clients
- **Users**: `admin/admin` (admin group), `user/password` (users group) for both OIDC and basic auth
- **Service accounts**: Executor and Scheduler use OIDC Client Credentials flow for service-to-service authentication
- **APIs**: Server, Lookout, and Binoculars APIs are secured with OIDC and basic auth
- **Web UIs**: Lookout UI uses OIDC for user authentication
- **armadactl**: Supports multiple authentication flows - OIDC PKCE flow (`auth-oidc`), OIDC Device flow (`auth-oidc-device`), OIDC Password flow (`auth-oidc-password`), and basic auth (`auth-basic`)

All components support both OIDC and basic auth for convenience.

### Local Development with Fake Executor

For testing Armada without a real Kubernetes cluster, you can use the fake executor that simulates a Kubernetes environment:

```shell
goreman -f _local/procfiles/fake-executor.Procfile start
```

The fake executor simulates:

- 2 virtual nodes with 8 CPUs and 32Gi memory each
- Pod lifecycle management without actual container execution
- Resource allocation and job state transitions

This is useful for:

- Testing Armada's scheduling logic
- Development when Kubernetes is not available
- Integration testing of job flows

### Available Procfiles

All Procfiles are located in `_local/procfiles/`:

| Procfile                 | Description                                       |
| ------------------------ | ------------------------------------------------- |
| `no-auth.Procfile`       | Standard setup without authentication             |
| `auth.Procfile`          | Standard setup with OIDC authentication           |
| `fake-executor.Procfile` | Uses fake executor for testing without Kubernetes |

Restart individual processes with `goreman restart <component>` (e.g., `goreman restart server`).

### Service Ports

Run `goreman run status` to check the status of the processes (running processes are prefixed with `*`):

```shell
$ goreman run status
*server
*scheduler
*scheduleringester
*eventingester
*executor
*lookout
*lookoutingester
*binoculars
*lookoutui
```

Goreman exposes services on the following ports:

| Service                    | Port  | Description         |
| -------------------------- | ----- | ------------------- |
| Server gRPC                | 50051 | Armada gRPC API     |
| Server HTTP                | 8081  | REST API & Health   |
| Server Metrics             | 9000  | Prometheus metrics  |
| Scheduler gRPC             | 50052 | Scheduler API       |
| Scheduler Metrics          | 9001  | Prometheus metrics  |
| Scheduler Ingester Metrics | 9006  | Prometheus metrics  |
| Lookout UI                 | 3000  | Frontend dev server |
| Lookout Metrics            | 9003  | Prometheus metrics  |
| Lookout Ingester Metrics   | 9005  | Prometheus metrics  |
| Executor Metrics           | 9002  | Prometheus metrics  |
| Event Ingester Metrics     | 9004  | Prometheus metrics  |
| Executor HTTP              | 8082  | Executor HTTP       |
| Binoculars HTTP            | 8084  | Binoculars HTTP     |
| Binoculars gRPC            | 50053 | Binoculars gRPC     |
| Binoculars Metrics         | 9007  | Prometheus metrics  |
| Redis                      | 6379  | Cache & events      |
| PostgreSQL                 | 5432  | Database            |
| Pulsar                     | 6650  | Message broker      |

## Documentation

For documentation, see the following:

- [Overview](https://armadaproject.io/)
- [Getting Started](https://armadaproject.io/getting-started)
- [Architecture](https://armadaproject.io/understanding-armada/architecture)
- [User guide](https://armadaproject.io/user-guide)
- [Development guide](https://armadaproject.io/developer-guide)
- [Release notes/Version history](https://github.com/armadaproject/armada/releases)
- [API Documentation](https://armadaproject.io/user-guide/api)

We expect readers of the documentation to have a basic understanding of Docker and Kubernetes; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

## Contributions

Thank you for considering contributing to Armada!
We want everyone to feel that they can contribute to the Armada Project.
Your contributions are valuable, whether it's fixing a bug, implementing a new feature, improving documentation, or suggesting enhancements.
We appreciate your time and effort in helping make this project better for everyone.
For more information about contributing to Armada see [CONTRIBUTING.md](https://github.com/armadaproject/armada/blob/master/CONTRIBUTING.md) and before proceeding to contributions see [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## Discussion

If you are interested in discussing Armada you can find us on [![slack](https://img.shields.io/badge/slack-armada-brightgreen.svg?logo=slack)](https://cloud-native.slack.com/?redir=%2Farchives%2FC03T9CBCEMC)
