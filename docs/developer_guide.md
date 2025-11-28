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
    - [VS Code debugging](#vs-code-debugging)
    - [Delve Debugging](#delve-debugging)
      - [External debug port mappings](#external-debug-port-mappings)
  - [GoLand run configurations](#goland-run-configurations)
  - [VS Code debug configurations](#vs-code-debug-configurations)
    - [Other debugging methods](#other-debugging-methods)
  - [Finer-grain control](#finer-grain-control)

This document is intended for developers who want to contribute to the project. It contains information about the project structure, how to build the project and how to run the tests.

## Quickstart

Want to quickly get Armada running and test it? Install the [prerequisites](#pre-requisites) and then run:

```bash
mage localdev minimal testsuite
```

To get the UI running, run:

```bash
mage ui
```

## Dealing with Arm and Windows problems

There is limited information on problems that appear on Arm/Windows Machines when running this setup.

If you encounter any problems, you can create a ticket and link it to the relevant issue, for example:

* [Arm issue](https://github.com/armadaproject/armada/issues/2493)
* [Windows issue](https://github.com/armadaproject/armada/issues/2492)

## Armada design docs

For more information about Armada's design, see the following pages:

* [Armada Components Diagram](./design/relationships_diagram.md)
* [Armada Architecture](./design/architecture.md)
* [Armada Design](./design/index.md)
* [How Priority Functions](./design/priority.md)
* [Armada Scheduler Design](./design/scheduling_and_preempting_jobs.md)

## Other developer docs

* [Armada API](./developer/api.md)
* [Running Armada in an EC2 instance](./developer/aws-ec2.md)
* [Armada UI](./developer/ui.md)
* [Usage metrics](./developer/usage_metrics.md)
* [Using OIDC with Armada](./developer/oidc.md)
* [Building the website](./developer/website.md)
* [Using `LocalDev` manually](./developer/manual-localdev.md)
* [Inspecting and debugging etcd in `LocalDev` setup](./developer/etc-localdev.md)

## Pre-requisites

Before you can start using Armada, you first need to install the following items:

- [`Go`](https://go.dev/doc/install) (version 1.25 or later)
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

* bootstrap the required tools from [tools.yaml](https://github.com/armadaproject/armada/blob/master/tools.yaml)
* create a local Kubernetes cluster using [kind](https://kind.sigs.k8s.io/)
* start the dependencies of Armada, including Pulsar, Redis, and Postgres.

**Note:** If you edit a proto file, you also need to run `mage proto` to regenerate the Go code.

It has the following options to customise further steps:

* `mage localdev full` - runs all components of Armada, including the Lookout UI
* `mage localdev minimal` - runs only the core components of Armada (such as the API server and an executor)
* `mage localdev no-build` - skips the build step; set `ARMADA_IMAGE` and `ARMADA_TAG` to choose the Docker image to use

We use `mage localdev minimal` to test the CI pipeline. You should therefore use it to test changes to the core components of Armada.

## Debugging error: port 6443 is already in use after running `mage localdev full`

### Identifying the conflict

Before making any changes, identify which port is causing the conflict. Port 6443 is a common source of conflicts. You can check for existing bindings to this port using commands like `netstat` or `lsof`.

1. The `kind.yaml` file is where you define the configuration for your Kind clusters. To resolve port conflicts, open your [`kind.yaml`](https://github.com/armadaproject/armada/blob/master/e2e/setup/kind.yaml) file.
2. Locate the relevant section where the `hostPort` is set. It may look something like this:
   
   ```
   - containerPort: 6443 # control plane
     hostPort: 6443  # exposes control plane on localhost:6443
     protocol: TCP
   ```

   * Modify the hostPort value to a port that is not in use on your system. For example:
   
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

In `LocalDev`, the UI is built seperately with `mage ui`. To access it, open http://localhost:8089 in your browser.

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

The mage target `mage debug` supports multiple methods for debugging, and runs the appropriate parts of `LocalDev` as required.

It supports the following commands:

* `mage debug vscode` - runs the server and executor in debug mode, and provides a launch.json file for VS Code
* `mage debug delve` - runs the server and executor in debug mode, and starts the Delve debugger

**Note** We are actively accepting contributions for more debugging guides.

### VS Code debugging

After running `mage debug vscode`, you can attach to the running processes using VS Code.

To use VS Code debugging, [see the VSCode Debugging Guide](https://code.visualstudio.com/docs/editor/debugging).

### Delve Debugging

The Delve target creates a new `docker-compose` file: `./docker-compose.dev.yaml` with the correct volumes, commands and images for debugging.

To manually create the compose file and run it yourself, run the following commands:

```bash
mage createDelveCompose

# You can then start components manually
docker compose -f docker-compose.dev.yaml up -d server executor
```

After running `mage debug delve`, you can attach to the running processes using Delve.

```bash
$ docker compose exec -it server bash
root@3b5e4089edbb:/app# dlv connect :4000
Type 'help' for list of commands.
(dlv) b (*SubmitServer).CreateQueue
Breakpoint 3 set at 0x1fb3800 for github.com/armadaproject/armada/internal/armada/server.(*SubmitServer).CreateQueue() ./internal/armada/server/submit.go:137
(dlv) c
> github.com/armadaproject/armada/internal/armada/server.(*SubmitServer).CreateQueue() ./internal/armada/server/submit.go:140 (PC: 0x1fb38a0)
   135: }
   136:
=> 137: func (server *SubmitServer) CreateQueue(ctx context.Context, request *api.Queue) (*types.Empty, error) {
   138:         err := checkPermission(server.permissions, ctx, permissions.CreateQueue)
   139:         var ep *ErrUnauthorized
   140:         if errors.As(err, &ep) {
   141:                 return nil, status.Errorf(codes.PermissionDenied, "[CreateQueue] error creating queue %s: %s", request.Name, ep)
   142:         } else if err != nil {
   143:                 return nil, status.Errorf(codes.Unavailable, "[CreateQueue] error checking permissions: %s", err)
   144:         }
   145:
(dlv)
```

You can find all outputs of delve in the `./delve` directory.

#### External debug port mappings

| Armada service  | Debug host     |
|-----------------|----------------|
| `server`          | `localhost:4000` |
| `executor`        | `localhost:4001` |
| `binoculars`      | `localhost:4002` |
| `eventingester`   | `localhost:4003` |
| `lookoutui`       | `localhost:4004` |
| `lookout`         | `localhost:4005` |
| `lookoutingester` | `localhost:4007` |

## GoLand run configurations

We provide a number of run configurations within the `.run` directory of this project. These will be accessible when opening the project in GoLand, enabling you to run Armada in both standard and debug mode.

The following high-level configurations are provided, each composed of sub-configurations:

* `Armada Infrastructure Services` - runs infrastructure services required to run Armada, irrespective of scheduler type
* `Armada (Legacy Scheduler)` - runs Armada with the Legacy Scheduler
* `Armada (Pulsar Scheduler)` - runs Armada with the Pulsar Scheduler (recommended)
* `Lookout UI` - script that configures a local UI development setup

A minimal local Armada setup using these configurations would be `Armada Infrastructure Services` and one of (`Armada (Legacy Scheduler)` or `Armada (Pulsar Scheduler)`). Running the `Lookout UI` script on top of this configuration enabsle you to develop the Lookout UI live from GoLand, and see the changes visible in your browser.

**Note:** These configurations (executor specifically) require a kubernetes config in `$PROJECT_DIR$/.kube/internal/config`.

GoLand does not allow us to specify an ordering for services within docker compose configurations. As a result, some database migration services may require rerunning.

## VS Code debug configurations

We similarly provide run and debug configurations for VS Code users to run each Armada service and use the debugger (provided with VS Code).

The `Armada` configuration performs all required setup (setting up the Kind cluster, spinning up infrastructure services, performing database migrations) and then runs all services.

### Other debugging methods

Run `mage debug local` to only spin up the dependencies of Armada, and then run the individual components yourself.

For required enviromental variables, [see the Enviromental Variables guide](https://github.com/armadaproject/armada/tree/master/developer/env/README.md).

## Finer-grain control

To run the individual `mage` targets yourself, [see the Manually running LocalDev guide](./developer/manual-localdev.md).
