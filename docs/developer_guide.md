# Developer guide

- [Developer guide](#developer-guide)
    - [Quickstart](#quickstart)
    - [Dealing with Arm and Windows problems](#dealing-with-arm-and-windows-problems)
    - [Armada design docs](#armada-design-docs)
    - [Other developer docs](#other-developer-docs)
    - [Pre-requisites](#pre-requisites)
    - [Using `mage`](#using-mage)
    - [Setting up `LocalDev`](#setting-up-localdev)
    - [Debugging error: port 6443 is already in use after running `mage localdev full`](#debugging-error-port-6443-is-already-in-use-after-running-mage-localdev-full)
        - [Identifying the conflict](#identifying-the-conflict)
        - [Testing if `LocalDev` is working](#testing-if-localdev-is-working)
        - [Running the UI](#running-the-ui)
        - [Choosing components to run](#choosing-components-to-run)
        - [Running Pulsar-backed scheduler with `LocalDev`](#running-pulsar-backed-scheduler-with-localdev)
    - [Debugging](#debugging)
    - [GoLand run configurations](#goland-run-configurations)
    - [VS Code debug configurations](#vs-code-debug-configurations)
        - [Other debugging methods](#other-debugging-methods)

This document is intended for developers who want to contribute to the project. It contains information about the project structure, how to build the project and how to run the tests.

## Quickstart

Want to quickly get Armada running and test it? Install the [prerequisites](#pre-requisites) and then run:

```bash
mage dev:full && mage testsuite
```

To get the UI running, run:

```bash
mage ui
```

## Dealing with Arm and Windows problems

There is limited information on problems that appear on Arm/Windows Machines when running this setup.

If you encounter any problems, you can create a ticket and link it to the relevant issue, for example:

- [Arm issue](https://github.com/armadaproject/armada/issues/2493)
- [Windows issue](https://github.com/armadaproject/armada/issues/2492)

## Armada design docs

For more information about Armada's design, see the following pages:

- [Armada Components Diagram](./design/relationships_diagram.md)
- [Armada Architecture](./design/architecture.md)
- [Armada Design](./design/index.md)
- [How Priority Functions](./design/priority.md)
- [Armada Scheduler Design](./design/scheduling_and_preempting_jobs.md)

## Other developer docs

- [Armada API](./developer/api.md)
- [Running Armada in an EC2 instance](./developer/aws-ec2.md)
- [Armada UI](./developer/ui.md)
- [Usage metrics](./developer/usage_metrics.md)
- [Using OIDC with Armada](./developer/oidc.md)
- [Building the website](./developer/website.md)
- [Using `LocalDev` manually](./developer/manual-localdev.md)
- [Inspecting and debugging etcd in `LocalDev` setup](./developer/etc-localdev.md)

## Pre-requisites

Before you can start using Armada, you first need to install the following items:

- [`Go`](https://go.dev/doc/install) (version 1.26 or later)
- `gcc` (for Windows, [see `tdm-gcc`](https://jmeubank.github.io/tdm-gcc/))
- [`mage`](https://magefile.org/)
- [`docker`](https://docs.docker.com/get-docker/)
- [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases)
- [`helm`](https://helm.sh/docs/intro/install/) (version 3.10.0 or later)

## Using `mage`

`mage` is a build tool that we use to build Armada. It is similar to Make, but written in Go. It is used to build Armada, run tests and run other useful commands. To see a list of available commands, run `mage -l`.

## Setting up `LocalDev`

`LocalDev`provides a reliable and extendable way to install Armada as a developer. It runs the following steps:

- bootstrap the required tools from [tools.yaml](https://github.com/armadaproject/armada/blob/master/tools.yaml)
- create a local Kubernetes cluster using [kind](https://kind.sigs.k8s.io/)
- start the dependencies of Armada, including Pulsar, Redis, and Postgres.

**Note:** If you edit a proto file, you also need to run `mage proto` to regenerate the Go code.

It has the following options to customise further steps:

- `mage dev:full` - runs the full Armada stack in containers (deps + all components) against a Kind cluster
- `mage dev:up [profile]` - runs deps in containers and Armada components as host processes via goreman; profile is `no-auth` (default), `auth`, or `fake-executor`
- `mage dev:deps` - runs only the dependency containers (redis, postgres, pulsar)

We use `mage dev:full` to test the CI pipeline. You should therefore use it to test changes to the core components of Armada.

## Debugging error: port 6443 is already in use after running `mage localdev full`

### Identifying the conflict

Before making any changes, identify which port is causing the conflict. Port 6443 is a common source of conflicts. You can check for existing bindings to this port using commands like `netstat` or `lsof`.

1. The Kind cluster config is where you define port mappings. To resolve port conflicts, open your [`_local/kind/cluster.yaml`](https://github.com/armadaproject/armada/blob/master/_local/kind/cluster.yaml) file.
2. Locate the relevant section where the `hostPort` is set. It may look something like this:

    ```
    - containerPort: 6443 # control plane
      hostPort: 6443  # exposes control plane on localhost:6443
      protocol: TCP
    ```

    - Modify the hostPort value to a port that is not in use on your system. For example:

    ```
    - containerPort: 6443 # control plane
      hostPort: 6444  # exposes control plane on localhost:6444
      protocol: TCP
    ```

    You are not limited to using port 6444. You can choose any available port that doesn't conflict with other services on your system. Select a port that suits your system configuration.

### Testing if `LocalDev` is working

Running `mage testsuite` runs the full test suite against the `LocalDev` cluster. You should therefore use this to test changes to the core components of Armada.

You can also run the same commands yourself:

```bash
go run cmd/armadactl/main.go create queue e2e-test-queue

# To enable Ingress tests to pass
export ARMADA_EXECUTOR_INGRESS_URL="http://localhost"
export ARMADA_EXECUTOR_INGRESS_PORT=5001

go run cmd/testsuite/main.go test --tests "testsuite/testcases/basic/*" --junit junit.xml
```

### Running the UI

In `LocalDev`, the UI is built separately with `mage ui`. To access it, open http://localhost:8089 in your browser.

For more information, [see the UI Developer Guide](./developer/developing-locally.md).

### Choosing components to run

You can set the `ARMADA_COMPONENTS` environment variable to choose which components to run. It is a comma-separated list of components to run. For example, to run only the server and executor, run:

```bash
export ARMADA_COMPONENTS="server,executor"
```

### Running Pulsar-backed scheduler with `LocalDev`

Ensure your local environment is completely torn down with:

```bash
mage LocalDevStop
```

And then run:

```bash
mage LocalDev minimal
```

Ensure your `LocalDev` environment is completely torn down when switching between Pulsar-backed and legacy setups.

If the eventsingester or the scheduleringester don't come up then just manually spin them up with `docker-compose up`.

## Debugging

The goreman-based flow (`mage dev:up`) builds each component with debug flags (`-gcflags="all=-N -l"`)
and runs them as host processes, so you can attach a debugger to any component directly. Bring up the
dependencies and components with `mage dev:up`, then attach your debugger (Delve, VS Code, or GoLand) to
the running process you want to inspect. Each component reads `_local/<component>/config.yaml`.

## GoLand run configurations

We provide a number of run configurations within the `.run` directory of this project. These will be accessible when opening the project in GoLand, enabling you to run Armada in both standard and debug mode.

The following high-level configurations are provided, each composed of sub-configurations:

- `Armada Infrastructure Services` - runs infrastructure services required to run Armada, irrespective of scheduler type
- `Armada (Legacy Scheduler)` - runs Armada with the Legacy Scheduler
- `Armada (Pulsar Scheduler)` - runs Armada with the Pulsar Scheduler (recommended)
- `Lookout UI` - script that configures a local UI development setup

A minimal local Armada setup using these configurations would be `Armada Infrastructure Services` and one of (`Armada (Legacy Scheduler)` or `Armada (Pulsar Scheduler)`). Running the `Lookout UI` script on top of this configuration enables you to develop the Lookout UI live from GoLand, and see the changes visible in your browser.

**Note:** These configurations (executor specifically) require a kubernetes config in `$PROJECT_DIR$/.kube/internal/config`.

GoLand does not allow us to specify an ordering for services within docker compose configurations. As a result, some database migration services may require rerunning.

## VS Code Run and Debug configurations

We similarly provide run and debug configurations for VS Code users to run each Armada service and use the debugger (provided with VS Code).

The following compound configurations are provided, each launching all relevant service debuggers at once:

| Configuration                            | Description                                                                 |
| ---------------------------------------- | --------------------------------------------------------------------------- |
| `Armada (no-auth)`                       | Runs all core services without authentication                               |
| `Armada (auth)`                          | Runs all core services with authentication enabled                          |
| `Armada (fake-executor)`                 | Runs core services with a fake executor (no real Kubernetes cluster needed) |
| `Armada (no-auth with prometheus)`       | Same as `no-auth`, but also starts Prometheus                               |
| `Armada (auth with prometheus)`          | Same as `auth`, but also starts Prometheus                                  |
| `Armada (fake-executor with prometheus)` | Same as `fake-executor`, but also starts Prometheus                         |

Each compound configuration attaches to already-running processes via Delve remote debugging. The individual service configurations and their debug ports are:

| Service             | Debug port |
| ------------------- | ---------- |
| `server`            | `2345`     |
| `scheduler`         | `2346`     |
| `scheduleringester` | `2347`     |
| `eventingester`     | `2348`     |
| `executor`          | `2349`     |
| `lookout`           | `2350`     |
| `lookoutingester`   | `2351`     |
| `binoculars`        | `2352`     |
| `fakeexecutor`      | `2353`     |

Each compound configuration has a `preLaunchTask` that sets up and starts the relevant services via Goreman before attaching the debuggers. For example, `Armada (no-auth)` uses the task `Set up and start (no-auth)`.

### Other debugging methods

Run `mage dev:deps` to spin up only the dependencies (redis, postgres, pulsar), then run individual
Armada components yourself (for example under a debugger). Each component reads its config from
`_local/<component>/config.yaml`. See the [README](../README.md#local-development) for the goreman-based
workflow and available profiles.
