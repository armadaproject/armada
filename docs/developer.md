# Developer setup

## Getting started 

There are many ways you can setup you local environment, this is just a basic quick example of how to setup everything you'll need to get started running and developing Armada.

### Pre-requisites
To follow this section it is assumed you have:
* Golang >= 1.16 installed [https://golang.org/doc/install](https://golang.org/doc/install)
* `kubectl` installed [https://kubernetes.io/docs/tasks/tools/install-kubectl/](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* Docker installed, configured for the current user [https://docs.docker.com/engine/install/](https://docs.docker.com/engine/install/)
* This repository cloned. The guide will assume you are in the root directory of this repository [https://github.com/G-Research/armada.git](https://github.com/G-Research/armada.git)

### Running Armada locally

There are two options for developing Armada locally. 

##### Kind
You can use a [kind](https://github.com/kubernetes-sigs/kind) Kubernetes clusters.
 - The advantage of this is it is more like a real kubernetes cluster
 - However it is a bit more effort to set it up and it can only emulate a cluster with as much resource as your computer has
 
This is recommended if load doesn't matter or you are working on features that rely on integrating with kubernetes functionality
  
##### Fake-executor
You can use fake-executor

 - It is easy to setup, as it is just a Go program that emulates a kubernetes cluster
 - It allows you emulate clusters much larger than your current machine
 - As the jobs aren't really running, it won't properly emulate a real kubernetes cluster
 
This is recommended when working on features that are purely Armada specific or if you want to get a high load of jobs running through Armada components 

#### Setup Kind development

1. Get kind (Installation help [here](https://kind.sigs.k8s.io/docs/user/quick-start/))
    ```bash
    GO111MODULE="on" go get sigs.k8s.io/kind@v0.11.1
    ``` 
2. Create kind clusters (you can create any number of clusters)

    As this step is using Docker, it may require root to run
    
    ```bash
    kind create cluster --name demo-a --config ./example/kind-config.yaml
    kind create cluster --name demo-b --config ./example/kind-config.yaml
    ```
3. Start Redis
    ```bash
    docker run -d -p 6379:6379 redis
    ```
    
    The following steps are shown in a terminal, but for development is it recommended they are run in your IDE

4. Start server in one terminal
    ```bash
    go run ./cmd/armada/main.go --config ./e2e/setup/insecure-armada-auth-config.yaml
    ```
5. Start executor for demo-a in a new terminal
    ```bash
    ARMADA_APPLICATION_CLUSTERID=kind-demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/executor/main.go
    ```
6. Start executor for demo-b in a new terminal
    ```bash
    ARMADA_APPLICATION_CLUSTERID=kind-demo-b ARMADA_METRIC_PORT=9002 go run ./cmd/executor/main.go
    ```

#### Setup Fake-executor development

1. Start Redis
    ```bash
    docker run -d -p 6379:6379 redis
    ```

    The following steps are shown in a terminal, but for development is it recommended they are run in your IDE

2. Start server in one terminal
    ```bash
    go run ./cmd/armada/main.go --config ./e2e/setup/insecure-armada-auth-config.yaml
    ```
3. Start executor for demo-a in a new terminal
    ```bash
    ARMADA_APPLICATION_CLUSTERID=demo-a ARMADA_METRIC_PORT=9001 go run ./cmd/fakeexecutor/main.go
    ```
4. Start executor for demo-b in a new terminal
    ```bash
    ARMADA_APPLICATION_CLUSTERID=demo-b ARMADA_METRIC_PORT=9002 go run ./cmd/fakeexecutor/main.go
    ```

#### Optional components

##### NATS Streaming
Armada can be set up to use NATS Streaming as message queue for events.
To run NATS Streaming for development you can use docker:
```bash
docker run -d -p 4223:4223 -p 8223:8223 nats-streaming -p 4223 -m 8223
```

For Armada configuration check end to end test setup:
```bash
go run ./cmd/armada/main.go --config ./e2e/setup/insecure-armada-auth-config.yaml --config ./e2e/setup/nats/armada-config.yaml
```

##### Lookout - Armada UI
Lookout requires Armada to be configured with NATS Streaming.
To run Lookout, firstly build frontend:
```bash
cd ./internal/lookout/ui
npm install
npm run openapi
npm run build
```
Start a Postgres database:
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=psw postgres
```
Migrate database:
```bash
go run ./cmd/lookout/main.go --migrateDatabase
```
Then run go application:
```bash
go run ./cmd/lookout/main.go 
```
For UI development you can also use the React development server.
Note that the Lookout API will still have to be running for this to work.
```bash
npm run start
```

#### Quick dev setup

Optionally, you can get a `kind` cluster and redis, NATS and PostgreSQL containers up by running
```bash
./docs/dev/setup.sh
```
The script will print out commands to execute each of the three Armada components, `armada`, `armada-lookout`, and `armada-executor`.

If you want to run Armada components using NATS Jetstream (as opposed to NATS streaming), you can run and copy the commands from the following command:
```bash
./docs/dev/setup.sh jetstream
```

When you're done, you can run
```bash
./docs/dev/teardown.sh
```
to tear down your development environment.

#### Testing your setup

1. Create queue & Submit job
```bash
go run ./cmd/armadactl/main.go create queue test --priorityFactor 1
go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
go run ./cmd/armadactl/main.go watch test job-set-1
```

For more details on submitting jobs to Armada, see [here](https://github.com/G-Research/armada/blob/master/docs/user.md).

Once you submit jobs, you should be able to see pods appearing in your cluster(s), running what you submitted.


**Note:** Depending on your Docker setup you might need to load images for jobs you plan to run manually:
```bash
kind load docker-image busybox:latest
```

### Running tests
For unit tests run
```bash
make tests
```

For end to end tests run:
```bash
make tests-e2e
# optionally stop kubernetes cluster which was started by test
make e2e-stop-cluster
```

## Code Generation

This project uses code generation.

The Armada api is defined using proto files which are used to generate Go source code (and the c# client) for our gRPC communication.

To generate source code from proto files:

```
make proto
```

### Usage metrics

Some functionality the executor has is to report how much cpu/memory jobs are actually using.

This is turned on by changing the executor config file to include:
``` yaml
metric:
   exposeQueueUsageMetrics: true
```

The metrics are calculated by getting values from metrics-server.

When developing locally with Kind, you will also need to deploy metrics server to allow this to work.

The simplest way to do this it to apply this to your kind cluster:

```
kubectl apply -f https://gist.githubusercontent.com/hjacobs/69b6844ba8442fcbc2007da316499eb4/raw/5b8678ac5e11d6be45aa98ca40d17da70dcb974f/kind-metrics-server.yaml
```

### Command-line tools

Our command-line tools used the cobra framework [https://github.com/spf13/cobra](https://github.com/spf13/cobra).

You can use the cobra cli to add new commands, the below will describe how to add new commands for `armadactl` but it can be applied to any of our command line tools.

##### Steps

Get cobra cli tool:

```
go get -u github.com/spf13/cobra/cobra
```

Change to the directory of the command-line tool you are working on:

```
cd ./cmd/armadactl
```

Use cobra to add new command:

```
cobra add commandName
```

You should see a new file appear under `./cmd/armadactl/cmd` with the name you specified in the command.
