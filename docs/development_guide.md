# Development guide

Here, we give an overview of a development setup for Armada that is closely aligned with how Armada is built and tested in CI.

Before starting, please ensure you have installed [Go](https://go.dev/doc/install) (version 1.18 or later), gcc (for Windows, see, e.g., [tdm-gcc](https://jmeubank.github.io/tdm-gcc/)), [mage](https://magefile.org/), [docker](https://docs.docker.com/get-docker/), [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl), and, if you need to compile `.proto` files, [protoc](https://github.com/protocolbuffers/protobuf/releases).

Then, use the following commands to setup a local Armada system.
```bash
# Download Go dependencies.
go mod tidy

# Install necessary tooling.
mage BootstrapTools

# Compile .pb.go files from .proto files
# (only necessary after changing a .proto file).
mage proto

# Build a Docker image containing all Armada components.
mage buildDockers "bundle"

# Setup up a kind (i.e., Kubernetes-in-Docker) cluster; see
# https://kind.sigs.k8s.io/ for details.
mage Kind

# Start necessary dependencies.
# On Arm-based Macs, you may need to change the pulsar image
# in docker-compose.yaml to be kezhenxu94/pulsar.
docker-compose up -d redis postgres pulsar eventingester

# Verify that dependencies started successfully.
# You may need to run docker-compose up again if some component failed to start, e.g., due to timeout.
docker ps

# Start the Armada server and executor.
# Alternatively, run the Armada server and executor directly on the host, e.g., through your IDE; see below for details.
docker-compose up -d server executor
```

Run the Armada test suite against the local environment to verify that it is working correctly.
```bash
# Create an Armada queue to submit jobs to.
go run cmd/armadactl/main.go create queue e2e-test-queue

# Run the Armada test suite against the local environment.
# (The ingress test requires additional setup and will fail using this setup.)
go run cmd/testsuite/main.go test --tests "testsuite/testcases/basic/*" --junit junit.xml
```

Tear down the local environment using the following:
```bash
# Stop Armada components and dependencies.
docker-compose down

# Tear down the kind cluster.
mage KindTeardown
```

## Running the Armada server and executor in Visual Studio Code

To run the Armada server and executor from Visual Studio Code for debugging purposes, add, e.g., the following config to `.vscode/launch.json` and start both from the "Run and Debug" menu (see the Visual Studio Code [documentation](https://code.visualstudio.com/docs/editor/debugging) for more information).

```json
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "server",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "env": {
                "CGO_ENABLED": "0",
                "ARMADA_REDIS_ADDRS": "localhost:6379",
                "ARMADA_EVENTSAPIREDIS_ADDRS": "localhost:6379",
                "ARMADA_EVENTAPI_POSTGRES_CONNECTION_HOST": "localhost",
                "ARMADA_POSTGRES_CONNECTION_HOST": "localhost",
                "ARMADA_PULSAR_URL": "pulsar://localhost:6650"
            },
            "cwd": "${workspaceFolder}/",
            "program": "${workspaceFolder}/cmd/armada/main.go",
            "args": [
                "--config", "${workspaceFolder}/localdev/config/armada/config.yaml"                
            ]
        },
        {
            "name": "executor",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "env": {
                "CGO_ENABLED": "0",
                "ARMADA_HTTPPORT": "8081",
                "ARMADA_APICONNECTION_ARMADAURL": "localhost:50051",
                "KUBECONFIG": "${workspaceFolder}/.kube/external/config"
            },
            "cwd": "${workspaceFolder}/",
            "program": "${workspaceFolder}/cmd/executor/main.go",
            "args": [
                "--config", "${workspaceFolder}/localdev/config/executor/config.yaml"
            ]
        }
    ],
    "compounds": [
        {
          "name": "server/executor",
          "configurations": ["server", "executor"],
          "stopAll": true
        }
    ]    
}
```
