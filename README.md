<div align="center">
 <img src="./logo.svg" alt="Armada logo" width="200"/>
 <p>
  <a href="https://circleci.com/gh/armadaproject/armada"><img src="https://circleci.com/gh/helm/helm.svg?style=shield" alt="CircleCI"></a>
  <a href="https://goreportcard.com/report/github.com/armadaproject/armada"><img src="https://goreportcard.com/badge/github.com/armadaproject/armada" alt="Go Report Card"></a>
  <a href="https://artifacthub.io/packages/helm/gresearch/armada" title="Go to Artifact Hub"><img src="https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/armada" alt="Artifact Hub"></a>        
  <a href="https://insights.lfx.linuxfoundation.org/foundation/cncf/overview?project=armada" title="Click to view project insights and health checks"><img src="https://img.shields.io/badge/LFX%20Insights-0094FF" alt="LFX Insights Dashboard"></a>
 </p>
</div>

# Armada

Armada is a system built on top of [Kubernetes](https://kubernetes.io/docs/concepts/overview/) for running batch workloads. With Armada as middleware for batch, Kubernetes can be a common substrate for batch and service workloads.  Armada is used in production and can run millions of jobs per day across tens of thousands of nodes. 

Armada addresses the following limitations of Kubernetes:

1. Scaling a single Kubernetes cluster beyond a certain size is [challenging](https://openai.com/blog/scaling-kubernetes-to-7500-nodes/). Hence, Armada is designed to effectively schedule jobs across many Kubernetes clusters. Many thousands of nodes can be managed by Armada in this way.
2. Achieving very high throughput using the in-cluster storage backend, etcd, is [challenging](https://etcd.io/docs/v3.5/op-guide/performance/). Hence, Armada performs queueing and scheduling out-of-cluster using a specialized storage layer. This allows Armada to maintain queues composed of millions of jobs.
3. The default [kube-scheduler](https://kubernetes.io/docs/reference/command-line-tools-reference/kube-scheduler/) is not suitable for batch. Instead, Armada includes a novel multi-Kubernetes cluster scheduler with support for important batch scheduling features, such as:
   * Fair queuing and scheduling across multiple users. Based on dominant resource fairness.
   * Resource and job scheduling rate limits.
   * Gang-scheduling, i.e., atomically scheduling sets of related jobs.
   * Job preemption, both to run urgent jobs in a timely fashion and to balance resource allocation between users.

Armada also provides features to help manage large compute clusters effectively, including:

* Detailed analytics exposed via [Prometheus](https://prometheus.io/) showing how the system behaves and how resources are allocated.
* Automatically removing nodes exhibiting high failure rates from consideration for scheduling.
* A mechanism to earmark nodes for a particular set of jobs, but allowing them to be used by other jobs when not used for their primary purpose.

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

### Goreman (Procfile-based)

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
     `REDIS_IMAGE`, `POSTGRES_IMAGE`, `PULSAR_IMAGE`
3. Initialize databases and Kubernetes resources:
    ```shell
    scripts/localdev-init.sh
    ```
4. Start Armada components:
    ```shell
    goreman start
    ```

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

Restart individual processes with `goreman restart <component>` (e.g., `goreman restart server`).

### Skaffold (Kubernetes)

[Skaffold](https://skaffold.dev) handles the workflow for building, pushing, and deploying applications to Kubernetes.
It provides automatic hot-reload, debugging support, and port-forwarding for local development.

Skaffold requires a running Kubernetes cluster (ideally `kind` or `minikube`).

Skaffold will build the components from source, package them as Docker images, inject them into the Kubernetes cluster, and deploy them using the Armada Helm charts.

1. Install [Skaffold](https://skaffold.dev/docs/install/)
2. Run `skaffold dev` for faster dev mode or `skaffold debug` to inject [delve](https://github.com/go-delve/delve) for step-by-step debugging.
3. Press `CTRL-C` to cleanup installed resources.

**Note**: When using `skaffold`, the Lookout UI is served by the Lookout backend on port 8089.
When using `goreman`, the Lookout UI runs on port 3000 with hot-reload.

### Service Ports

Both Goreman and Skaffold expose services on the same ports for consistency:

| Service                    | Port  | Description         |
|----------------------------|-------|---------------------|
| Server gRPC                | 50051 | Armada gRPC API     |
| Server HTTP                | 8081  | REST API & Health   |
| Server Metrics             | 9000  | Prometheus metrics  |
| Scheduler gRPC             | 50052 | Scheduler API       |
| Scheduler Metrics          | 9001  | Prometheus metrics  |
| Scheduler Ingester Metrics | 9006  | Prometheus metrics  |
| Lookout API/UI (skaffold)  | 8089  | Backend & Web UI    |
| Lookout UI (goreman)       | 3000  | Frontend dev server |
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

- [System overview](./docs/system_overview.md)
- [Scheduler](./docs/scheduler.md)
- [User guide](./docs/user.md)
- [Development guide](./docs/developer.md)
- [Release notes/Version history](https://github.com/armadaproject/armada/releases)
- [API Documentation](./docs/developer/api.md)

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

If you are interested in discussing Armada you can find us on  [![slack](https://img.shields.io/badge/slack-armada-brightgreen.svg?logo=slack)](https://cloud-native.slack.com/?redir=%2Farchives%2FC03T9CBCEMC)

