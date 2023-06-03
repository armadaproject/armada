# Developer Guide

## Introduction

This document is intended for developers who want to contribute to the project. It contains information about the project structure, how to build the project, and how to run the tests.

## TLDR

Want to quickly get Armada running and test it? Install the [Pre-requisites](#pre-requisites) and then run:

```bash
mage localdev minimal testsuite
```

## A note for Devs on Arm / Windows

There is limited information on issues that appear on Arm / Windows Machines when running this setup.

Feel free to create a ticket if you encounter any issues, and link them to the relavent issue:

* https://github.com/armadaproject/armada/issues/2493 (Arm)
* https://github.com/armadaproject/armada/issues/2492 (Windows)


## Design Docs

Please see these documents for more information about Armadas Design:

* [Armada Components Diagram](./design/relationships_diagram.md)
* [Armada Architecture](./design/architecture.md)
* [Armada Design](./design/index.md)
* [How Priority Functions](./design/priority.md)
* [Armada Scheduler Design](./design/scheduler.md)

## Other Useful Developer Docs

* [Armada API](./developer/api.md)
* [Running Armada in an EC2 Instance](./developer/aws-ec2.md)
* [Armada UI](./developer/ui.md)
* [Usage Metrics](./developer/usage_metrics.md)
* [Using OIDC with Armada](./developer/oidc.md)
* [Building the Website](./developer/website.md)
* [Using Localdev Manually](./developer/manual-localdev.md)

## Pre-requisites

- [Go](https://go.dev/doc/install) (version 1.20 or later)
- gcc (for Windows, see, e.g., [tdm-gcc](https://jmeubank.github.io/tdm-gcc/))
- [mage](https://magefile.org/)
- [docker](https://docs.docker.com/get-docker/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl)
- [protoc](https://github.com/protocolbuffers/protobuf/releases)


## Using Mage

Mage is a build tool that we use to build Armada. It is similar to Make, but written in Go. It is used to build Armada, run tests, and run other useful commands. To see a list of available commands, run `mage -l`.

## LocalDev Setup

LocalDev provides a reliable and extendable way to install Armada as a developer. It runs the following steps:

* Bootstrap the required tools from [tools.yaml](https://github.com/armadaproject/armada/blob/master/tools.yaml)
* Create a local Kubernetes cluster using [kind](https://kind.sigs.k8s.io/)
* Start the dependencies of Armada, including Pulsar, Redis, and Postgres.

**Note: If you edit a proto file, you will also need to run `mage proto` to regenerate the Go code.**

It has the following options to customize further steps:

* `mage localdev full` - Installs all components of Armada, including the UI.
* `mage localdev minimal` - Installs only the core components of Armada, the server, executor and eventingester.
* `mage localdev no-build` - skips the build step. Assumes that a separate image has been set from `ARMADA_IMAGE` and `ARMADA_TAG` environment variables or it has already been built.

`mage localdev minimal` is what is used to test the CI pipeline, and is the recommended way to test changes to the core components of Armada.

### Testing if LocalDev is working

Running `mage testsuite` will run the full test suite against the localdev cluster. This is the recommended way to test changes to the core components of Armada.

You can also run the same commands yourself:

```bash
go run cmd/armadactl/main.go create queue e2e-test-queue

# To allow Ingress tests to pass
export ARMADA_EXECUTOR_INGRESS_URL="http://localhost"
export ARMADA_EXECUTOR_INGRESS_PORT=5001

go run cmd/testsuite/main.go test --tests "testsuite/testcases/basic/*" --junit junit.xml
```

### Running the UI

In LocalDev, the UI is pre-built and served from the lookout component. To access it, open http://localhost:8089 in your browser.

If you wish to run the UI locally, see the [UI Developer Guide](./developer/ui.md).


### Choosing components to run

You can set the `ARMADA_COMPONENTS` environment variable to choose which components to run. It is a comma separated list of components to run. For example, to run only the server and executor, you can run:

```bash
export ARMADA_COMPONENTS="server,executor"
```

## Debugging

The mage target `mage debug` supports multiple methods for debugging, and runs the appropriate parts of localdev as required.

**NOTE: We are actively accepting contributions for more debugging guides!**

It supports the following commands:

* `mage debug vscode` - Runs the server and executor in debug mode, and provides a launch.json file for VSCode.
* `mage debug delve` - Runs the server and executor in debug mode, and starts the Delve debugger.

### VSCode Debugging

After running `mage debug vscode`, you can attach to the running processes using VSCode.
The launch.json file can be found [Here](../developer/debug/launch.json)

For using VSCode debugging, see the [VSCode Debugging Guide](https://code.visualstudio.com/docs/editor/debugging).

### Delve Debugging

The delve target creates a new docker-compose file: `./docker-compose.dev.yaml` with the correct volumes, commands and images for debugging.

If you would like to manually create the compose file and run it yourself, you can run the following commands:

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

All outputs of delve can be found in the `./delve` directory.

External Debug Port Mappings:

|Armada service     |Debug host    |
|-------------------|--------------|
|server             |localhost:4000|
|executor           |localhost:4001|
|binoculars         |localhost:4002|
|eventingester      |localhost:4003|
|lookout            |localhost:4004|
|lookoutv2          |localhost:4005|
|lookoutingester    |localhost:4006|
|lookoutingesterv2  |localhost:4007|
|jobservice         |localhost:4008|


### Other Debugging Methods

Run `mage debug local` to only spin up the dependencies of Armada, and then run the individual components yourself.

For required enviromental variables, please see [The Enviromental Variables Guide](https://github.com/armadaproject/armada/tree/master/developer/env/README.md).

## Finer-Grain Control

If you would like to run the individual mage targets yourself, you can do so.
See the [Manually Running LocalDev](./developer/manual-localdev.md) guide for more information.