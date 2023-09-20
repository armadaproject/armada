# Manual Local Development

Here, we give an overview of a development setup for Armada that gives users full control over the Armada components and dependencies.

Before starting, please ensure you have installed [Go](https://go.dev/doc/install) (version 1.20 or later), gcc (for Windows, see, e.g., [tdm-gcc](https://jmeubank.github.io/tdm-gcc/)), [mage](https://magefile.org/), [docker](https://docs.docker.com/get-docker/), [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl), and, if you need to compile `.proto` files, [protoc](https://github.com/protocolbuffers/protobuf/releases).

For a full lust of mage commands, run `mage -l`.

## Setup

### Note for Arm/M1 Mac Users

You will need to set the `PULSAR_IMAGE` enviromental variable to an arm64 image.

We provide an optimised image for this purpose:

```bash
export PULSAR_IMAGE=richgross/pulsar:2.11.0
```

```bash
# Download Go dependencies.
go mod tidy

# Install necessary tooling.
mage BootstrapTools

# Compile .pb.go files from .proto files
# (only necessary after changing a .proto file).
mage proto
mage dotnet

# Build the Docker images containing all Armada components.
# Only the main "bundle" is needed for quickly testing Armada.
mage buildDockers "bundle,lookout-bundle,jobservice"

# Setup up a kind (i.e., Kubernetes-in-Docker) cluster; see
# https://kind.sigs.k8s.io/ for details.
mage Kind

# Start necessary dependencies.
# Verify that dependencies started successfully
# (check that Pulsar has fully started as it is quite slow (~ 1min )).
mage StartDependencies && mage checkForPulsarRunning

# Start the Armada server and executor.
# Alternatively, run the Armada server and executor directly on the host,
# e.g., through your IDE; see below for details.
docker compose up -d server executor

# Wait for Armada to come online
mage checkForArmadaRunning
```

Run the Armada test suite against the local environment to verify that it is working correctly.
```bash
# Create an Armada queue to submit jobs to.
go run cmd/armadactl/main.go create queue e2e-test-queue

# To allow Ingress tests to pass
export ARMADA_EXECUTOR_INGRESS_URL="http://localhost"
export ARMADA_EXECUTOR_INGRESS_PORT=5001

# Run the Armada test suite against the local environment.
go run cmd/testsuite/main.go test --tests "testsuite/testcases/basic/*" --junit junit.xml
```

Tear down the local environment using the following:
```bash
# Stop Armada components and dependencies.
docker compose down

# Tear down the kind cluster.
mage KindTeardown
```
