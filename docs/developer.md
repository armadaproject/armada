# Developer setup

## Getting started 

There are many ways you can setup you local environment, this is just a basic quick example of how to setup everything you'll need to get started running and developing Armada.

### Pre-requisites
To follow this section it is assumed you have:
* Golang >= 1.18 installed [https://golang.org/doc/install](https://golang.org/doc/install)
* `kubectl` installed [https://kubernetes.io/docs/tasks/tools/install-kubectl/](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* Docker installed, configured for the current user [https://docs.docker.com/engine/install/](https://docs.docker.com/engine/install/)
* This repository cloned. The guide will assume you are in the root directory of this repository [https://github.com/G-Research/armada.git](https://github.com/G-Research/armada.git)

### Running Armada locally

There are two options for developing Armada locally. 

##### Kind + Docker Compose
You can use a [kind](https://github.com/kubernetes-sigs/kind) Kubernetes cluster with Docker Compose to run a development stack.
 - The advantage of this is it is more like a real kubernetes cluster
 - However it can only emulate a small cluster
 
This is recommended if load doesn't matter or you are working on features that rely on integrating with kubernetes functionality
  
##### Fake-executor
You can use fake-executor
 - It is easy to setup, as it is just a Go program that emulates a kubernetes cluster
 - It allows you emulate clusters much larger than your current machine
 - As the jobs aren't really running, it won't properly emulate a real kubernetes cluster
 
This is recommended when working on features that are purely Armada specific or if you want to get a high load of jobs running through Armada components 

#### Setup Kind + Docker Compose development

1. Start the "localdev" docker-compose environment
    ```bash
    localdev/run.sh
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

##### Lookout - Armada UI
Lookout requires Postgres to be up and running. Additionally, the react app must be built.
```bash
cd ./internal/lookout/ui
yarn install
yarn run openapi
yarn run build
```

After this, the lookout UI should be accessible through your browser at `http://localhost:8089`

For UI development you can also use the React development server.
Note that the Lookout API will still have to be running for this to work.
```bash
yarn run start
```

#### Testing your setup

1. Create queue & Submit job
```bash
go run ./cmd/armadactl/main.go create queue test --priorityFactor 1
go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
go run ./cmd/armadactl/main.go watch test job-set-1
```

2. Run the end-to-end integration tests
```bash
make tests-e2e-no-setup
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

#### Debugging

1. Stop the docker-compose service you want to debug and replace with debugging run (using delve here)
```bash
docker-compose -f localdev/docker-compose.yaml stop armada-server
docker-compose -f localdev/docker-compose.yaml run --entrypoint bash  armada-server
[+] Running 4/0
 ⠿ Container postgres  Running                                                                                                                                                0.0s
 ⠿ Container redis     Running                                                                                                                                                0.0s
 ⠿ Container pulsar    Running                                                                                                                                                0.0s
 ⠿ Container stan      Running                                                                                                                                                0.0s
root@808012d9bdf0:/app# dlv debug ./cmd/armada/main.go -- --config ./localdev/config/armada/config.yaml
Type 'help' for list of commands.
(dlv) b validateArmadaConfig
Breakpoint 1 set at 0x20b6010 for github.com/G-Research/armada/internal/armada.validateArmadaConfig() ./internal/armada/server.go:500
(dlv) c
```


## Code Generation

This project uses code generation.

The Armada api is defined using proto files which are used to generate Go source code (and the c# client) for our gRPC communication.

To generate source code from proto files:

```
make proto
```

Additionally, the `moq` tool has been used to generate test mocks in `internal/jobservice/events` and `internal/jobservice/repository` 
(and possibly other places). If you need to update the moqs, do the following:

```
go install github.com/matryer/moq@latest
cd internal/jobservice/events (or wherever)
rm ./*_moq.go
go generate
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
go install github.com/spf13/cobra/cobra
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

### Setting up OIDC for developers.

Setting up OIDC can be an art.  The [Okta Developer Program](https://developer.okta.com/signup/) provides a nice to test OAuth flow.

1) Create a Okta Developer Account
    - I used my github account.
2) Create a new App in the Okta UI.
    - Select OIDC - OpenID Connect.
    - Select Web Application.
3) In grant type, make sure to select Client Credentials.  This has the advantage of requiring little interaction. 
4) Select 'Allow Everyone to Access'
5) Deselect Federation Broker Mode.
6) Click okay and generate a client secret.
7) Navigate in the Okta settings to the API settings for your default authenticator.
8) Select Audience to be your client id.


Setting up OIDC for Armada requires two separate configs (one for Armada server and one for the clients)

You can add this to your armada server config.
```
 auth:
    anonymousAuth: false
    openIdAuth:
      providerUrl: "https://OKTA_DEV_USERNAME.okta.com/oauth2/default"
      groupsClaim: "groups"
      clientId: "CLIENT_ID_FROM_UI"
      scopes: []
```

For client credentials, you can use the following config for the executor and other clients.

```
  openIdClientCredentialsAuth:
      providerUrl: "https://OKTA_DEV_USERNAME.okta.com/oauth2/default"
    clientId: "CLIENT_ID_FROM_UI"
    clientSecret: "CLIENT_SECRET"
    scopes: []
```

If you want to interact with Armada, you will have to use one of our client APIs.  The armadactl is not setup to work with OIDC at this time.
