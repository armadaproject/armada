<img src="./logo.svg" width="200"/>

[![CircleCI](https://circleci.com/gh/helm/helm.svg?style=shield)](https://circleci.com/gh/G-Research/armada)
[![Go Report Card](https://goreportcard.com/badge/github.com/G-Research/armada)](https://goreportcard.com/report/github.com/G-Research/armada)

Armada is a system for scheduling and running batch jobs (e.g., a compute job for training a machine learning model) over Kubernetes clusters. Armada is designed for high availability and to handle scheduling hundreds of jobs per second (with potentially millions of jobs queued) over thousands of nodes.

To achieve this, Armada, unlike previous Kubernetes batch schedulers (e.g., [kube-batch](https://github.com/kubernetes-sigs/kube-batch)), can schedule jobs over multiple Kubernetes clusters simultaneously and hence scale beyond the limitations of a single Kubernetes cluster (which we find to be about 1000 nodes). In addition, Kubernetes clusters can be connected and disconnected from Armada on the fly without disruption. Jobs are submitted to job queues, of which there may be many (for example, each user could have a separate queue), and Armada divides compute resources fairly between queues.

Armada is loosely based on the [HTCondor](https://research.cs.wisc.edu/htcondor/) batch scheduler and can be used as a replacement of HTCondor, provided all nodes are enrolled in Kubernetes clusters.

**Armada is not**

- A service scheduler (i.e., Armada jobs should have a finite lifetime)
- Designed for low-latency scheduling (expect on the order of 10 seconds from job submission)
- Designed to schedule jobs over underlying systems other than Kubernetes clusters

## Documentation

For an overview of the architecture and design of Armada, and instructions for submitting jobs, see:

- [System overview](./docs/design.md)
- [User guide](./docs/user.md)

For instructions of how to setup and develop Armada, see:
- [Quickstart](./docs/quickstart/index.md)
- [Development guide](./docs/developer.md)
- [Installation in production](./docs/production-install.md)

For API reference, see:
- [Api Documentation](./docs/api.md)

We expect readers of the documentation to have a basic understanding of Docker and Kubernetes; see, e.g., the following links:

- [Docker overiew](https://docs.docker.com/get-started/overview/)
- [Kubernetes overview](https://kubernetes.io/docs/concepts/overview/)
