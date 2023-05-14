# Developer Guide

## Introduction

This document is intended for developers who want to contribute to the project. It contains information about the project structure, how to build the project, and how to run the tests.

## Project Structure

Please see these documents for more information about the project structure:

* [Armada Components Diagram](./design/relationships_diagram.md)
* [Armada Architecture](./design/architecture.md)
* [Armada Design](./design/design.md)
* [How Priority Functions](./design/priority.md)
* [Armada Scheduler Design](./design/scheduler.md)

## Other Useful Developer Docs

* [Armada API](./developer/api.md)

## Pre-requisites

Before starting, please ensure you have installed [Go](https://go.dev/doc/install) (version 1.20 or later), gcc (for Windows, see, e.g., [tdm-gcc](https://jmeubank.github.io/tdm-gcc/)), [mage](https://magefile.org/), [docker](https://docs.docker.com/get-docker/), [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl), and, if you need to compile `.proto` files, [protoc](https://github.com/protocolbuffers/protobuf/releases).

## Using Mage

Mage is a build tool that we use to build Armada. It is similar to Make, but written in Go. It is used to build Armada, run tests, and run other useful commands. To see a list of available commands, run `mage -l`.

## LocalDev Setup

LocalDev provides a reliable and extendable way to install Armada as a developer. It runs the follwing steps:

* Bootstrap the required tools from [tools.yaml](../tools.yaml)
* Create a local Kubernetes cluster using [kind](https://kind.sigs.k8s.io/)
* Start the dependencies of Armada, including Pulsar, Redis, and Postgres.

It has the following options to customise further steps:

* `mage localdev full` - Installs all components of Armada, including the UI.
* `mage localdev minimal` - Installs only the core components of Armada, the server, executor and eventingester.
* `mage localdev no-build` - skips the build step. Assumes that a seperate image has been set from `ARMADA_IMAGE` and `ARMADA_TAG` environment variables or it has already been built.

`mage localdev minimal` is what is used to test the CI pipeline, and is the recommended way to test changes to the core components of Armada.

### Testing if LocalDev is working

Running `mage testsuite` will run the full test suite against the localdev cluster. This is the recommended way to test changes to the core components of Armada.

### Debugging

The mage target `mage debug` supports multiple methods for debugging, and runs the appropreiate parts of localdev as required.

**NOTE: We are actively accepting contributions for more debugging guides!**

It supports the following commands:

* `mage debug vscode` - Runs the server and executor in debug mode, and provides a launch.json file for VsCode.
* `mage debug delve` - Runs the server and executor in debug mode, and starts the Delve debugger.

### VsCode Debugging

For using VsCode debugging, see the [VsCode Debugging Guide](./debugging/vscode.md).

### Delve Debugging

For using Delve debugging, see the [Delve Debugging Guide](./debugging/delve.md).

External Debug Port Mappings:

|Armada service     |Debug host    |
|-------------------|--------------|
|Server             |localhost:4000|
|Lookout            |localhost:4001|
|Executor           |localhost:4002|
|Binoculars         |localhost:4003|
|Jobservice         |localhost:4004|
|Lookout-ingester   |localhost:4005|
|Lookout-ingesterv2 |localhost:4006|
|Event-ingester     |localhost:4007|
|Lookoutv2          |localhost:4008|