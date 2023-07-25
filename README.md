<div align="center">
<img src="./logo.svg" width="200"/>

<p><a href="https://circleci.com/gh/armadaproject/armada"><img src="https://circleci.com/gh/helm/helm.svg?style=shield" alt="CircleCI"></a>
<a href="https://goreportcard.com/report/github.com/armadaproject/armada"><img src="https://goreportcard.com/badge/github.com/armadaproject/armada" alt="Go Report Card"></a></p>
</div>

# Armada

Armada is a multi-[Kubernetes](https://kubernetes.io/docs/concepts/overview/) cluster batch job scheduler.

Armada is designed to address the following issues:

1. A single Kubernetes cluster can not be scaled indefinitely, and managing very large Kubernetes clusters is [challenging](https://openai.com/blog/scaling-kubernetes-to-7500-nodes/). Hence, Armada is a multi-cluster scheduler built on top of several Kubernetes clusters.
2. Acheiving very high throughput using the in-cluster storage backend, etcd, is [challenging](https://etcd.io/docs/v3.5/op-guide/performance/). Hence, queueing and scheduling is performed partly out-of-cluster using a specialized storage layer.

Armada is designed primarily for machine learning, AI, and data analytics workloads, and to:

- Manage compute clusters composed of tens of thousands of nodes in total.
- Schedule a thousand or more pods per second, on average.
- Enqueue tens of thousands of jobs over a few seconds.
- Divide resources fairly between users.
- Provide visibility for users and admins.
- Ensure near-constant uptime.

Armada is a [CNCF](https://www.cncf.io/) Sandbox project used in production at [G-Research](https://www.gresearch.co.uk/).

For an overview of Armada, see these videos:

- [Armada - high-throughput batch scheduling](https://www.youtube.com/watch?v=FT8pXYciD9A)
- [Building Armada - Running Batch Jobs at Massive Scale on Kubernetes](https://www.youtube.com/watch?v=B3WPxw3OUl4)

Armada adheres to the CNCF [Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## Documentation

For an overview of the architecture and design of Armada, and instructions for submitting jobs, see:

- [Components overview](./docs/design/relationships_diagram.md)
- [Scheduler](./docs/design/scheduler.md)
- [Architecture](./docs/design/architecture.md)
- [User guide](./docs/user.md)
- [Quickstart](./docs/quickstart/index.md)

For a full developer guide, see:

- [Development guide](./docs/developer.md)

For API reference, see:
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


