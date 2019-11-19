# Armada

![Armada](./logo.svg)

[![CircleCI](https://circleci.com/gh/helm/helm.svg?style=shield)](https://circleci.com/gh/G-Research/armada)
[![Go Report Card](https://goreportcard.com/badge/github.com/G-Research/armada)](https://goreportcard.com/report/github.com/G-Research/armada)

Armada is an experimental application to achieve high throughput of run-to-completion jobs on multiple Kubernetes clusters.

It stores queues for users/projects with pod specifications and creates these pods once there is available resource in one of the connected Kubernetes clusters.

## Documentation

- [Design Documentation](./docs/design.md)
- [Development Guide](./docs/developer.md)
- [User Guide](./docs/user.md)
- [Installation in Production](./docs/production-install.md)
- [Quickstart](./docs/quickstart.md)

## Key features
- Armada maintains fair resource share over time (inspired by HTCondor priority)
- It can handle large amounts of queued jobs (million+)
- It allows adding and removing clusters from the system without disruption
- By utilizing multiple Kubernetes clusters the system can scale beyond the limits of a single Kubernetes cluster

## Key concepts

**Queue:** Represent user or project, used to maintain fair share over time, has priority factor

**Job:** Unit of work to be run (described as Kubernetes PodSpec)

**Job Set:** Group of related jobs, api allows observing progress of job set together


## Try it out locally

Follow the [quickstart](./docs/quickstart.md) guide to get Armada up and running locally.
