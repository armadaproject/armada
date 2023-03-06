<div align="center">
<img src="./logo.svg" width="200"/>

<p>
    <a href="https://circleci.com/gh/armadaproject/armada"><img src="https://circleci.com/gh/helm/helm.svg?style=shield" alt="CircleCI"></a>
    <a href="https://goreportcard.com/report/github.com/armadaproject/armada"><img src="https://goreportcard.com/badge/github.com/armadaproject/armada" alt="Go Report Card"></a>
    <a href="https://armadaproject.io"><img src="https://img.shields.io/badge/GitBook-Docu-lightblue" alt="Docs"></a>
    <a href="https://app.codecov.io/github/armadaproject/armada"><img alt="Codecov" src="https://img.shields.io/codecov/c/github/armadaproject/armada"></a>
    <img alt="GitHub go.mod Go version" src="https://img.shields.io/github/go-mod/go-version/armadaproject/armada">
    <img alt="GitHub Workflow Status - Test" src="https://img.shields.io/github/actions/workflow/status/armadaproject/armada/test.yml?label=tests%20workflow"></p>
    <img alt="GitHub Workflow Status - Lint" src="https://img.shields.io/github/actions/workflow/status/armadaproject/armada/lint.yml?label=lint%20workflow">
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

- [System overview](./docs/design.md)
- [User guide](./docs/user.md)
- [Quickstart](./docs/quickstart/index.md)

To set up Armada for local development, see:

- [Development guide](./docs/development_guide.md)

For API reference, see:
- [API Documentation](./docs/api.md)

We expect readers of the documentation to have a basic understanding of Docker and Kubernetes; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)

## Discussion

If you are interested in discussing Armada you can find us on  [![slack](https://img.shields.io/badge/slack-armada-brightgreen.svg?logo=slack)](https://cloud-native.slack.com/?redir=%2Farchives%2FC03T9CBCEMC)


