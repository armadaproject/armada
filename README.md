<img src="./logo.svg" width="200"/>

[![CircleCI](https://circleci.com/gh/helm/helm.svg?style=shield)](https://circleci.com/gh/G-Research/armada)
[![Go Report Card](https://goreportcard.com/badge/github.com/G-Research/armada)](https://goreportcard.com/report/github.com/G-Research/armada)

# Armada

Armada is a multi-[Kubernetes](https://kubernetes.io/docs/concepts/overview/) cluster batch job scheduler.

Users submit jobs, which are expressed as a Kubernetes pod spec plus Armada-specific metadata, to a central Armada server. Armada stores jobs in user or project-specific queues that are backed by a specialized high-throughput storage layer. Armada manages several Kubernetes worker clusters that queued jobs are dispatched to.

Armada is designed to operate at scale and to address the following issues:

1. A single Kubernetes cluster can not be scaled indefinitely, and managing very large Kubernetes clusters is [challenging](https://openai.com/blog/scaling-kubernetes-to-7500-nodes/). Hence, Armada is a multi-cluster scheduler built on top of several single-cluster schedulers, e.g., the vanilla scheduler or [Volcano](https://github.com/volcano-sh/volcano).
2. Acheiving very high throughput using the in-cluster storage backend, etcd, is [challenging](https://etcd.io/docs/v3.5/op-guide/performance/). Hence, queueing and scheduling is performed partly out-of-cluster using a specialized storage layer (i.e., Armada, does not primarily rely on etcd).

Further, Armada is designed primarily for machine learning, AI, and data analytics workloads, and to:

- Manage compute clusters composed of tens of thousands of nodes in total.
- Schedule a thousand or more pods per second, on average.
- Enqueue tens of thousands of jobs over a few seconds.
- Divide resources fairly between users.
- Provide visibility for users and admins.
- Ensure near-constant uptime.

Armada is a [CNCF](https://www.cncf.io/) Sandbox project in production at [G Research](https://www.gresearch.co.uk/) and is actively developed.

For an overview of Armada, see [this video](https://www.youtube.com/watch?v=FT8pXYciD9A).

## Documentation

For an overview of the architecture and design of Armada, and instructions for submitting jobs, see:

- [System overview](./docs/design.md)
- [User guide](./docs/user.md)

For instructions of how to setup and develop Armada, see:
- [Quickstart](./docs/quickstart/index.md)
- [Development guide](./docs/developer.md)
- [Installation in production](./docs/production-install.md)

For API reference, see:
- [API Documentation](./docs/api.md)

We expect readers of the documentation to have a basic understanding of Docker and Kubernetes; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)
