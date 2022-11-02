# Developer setup

Here, we show how to setup Armada for local development.

**Prerequisites:**
* Golang >= 1.18 [https://golang.org/doc/install](https://golang.org/doc/install)
* `kubectl` [https://kubernetes.io/docs/tasks/tools/install-kubectl/](https://kubernetes.io/docs/tasks/tools/install-kubectl/)
* Docker installed and configured for the current user [https://docs.docker.com/engine/install/](https://docs.docker.com/engine/install/)
* Dependencies and tooling installed via `make download`.

This guide assumes you have cloned this repository and are executing commands from its root directory.

## Running Armada locally

Armada schedules pods across Kubernetes clusters. Hence, for a local setup there needs to be at least one worker Kubernetes cluster available on the local machine, for which we use [kind](https://github.com/kubernetes-sigs/kind). Further, the Armada server, which is responsible for job submission and queuing, and an Armada executor must be running. The executor is responsible for interacting with the worker Kubernetes cluster.

In addition, Armada relies on the following components for storage and communication:

- Pulsar: used for passing messages between components.
- Redis: the main database of Armada; used, e.g., to store queued jobs.
- PostgreSQL: used for auxilliary storage. In the future, PostgreSQL will be the main database, instead of Redis.

All of these components can be started and initialised with `./localdev/run.sh` Once complete, you will have a fully functional local deployment of armada via docker.

Create a queue and submit a job:
```bash
go run ./cmd/armadactl/main.go create queue test --priorityFactor 1
go run ./cmd/armadactl/main.go submit ./example/jobs.yaml
go run ./cmd/armadactl/main.go watch test job-set-1
```

**Note:** In the default setup you should submit jobs to the kubernetes `personal-anonymous` namespace. See this job-spec snippet:
```yaml
queue: test
jobSetId: job-set-1
jobs:
  - priority: 0
    namespace: personal-anonymous
    podSpec:
```

For more details on submitting jobs to Armada, see [the user guide](https://github.com/G-Research/armada/blob/master/docs/user.md). Once you submit jobs, you should see pods appearing in your worker cluster(s).

**Note:** Depending on your Docker setup you might need to load images for jobs you plan to run manually:
```bash
kind load docker-image busybox:latest
```

Armada uses proto files extensively. Code-generation based on these files is run via `make proto`.

## Lookout - Armada web UI

Armada bundles a web UI referred to as Lookout. Lookout requires PostgreSQL. Lookout is based on React and is built with:
```bash
cd ./internal/lookout/ui
yarn install
yarn run openapi
yarn run build
```

Once completed, the Lookout UI should be accessible through your browser at `http://localhost:8089`

For UI development you can also use the React development server. Note that the Lookout API will still have to be running for this to work.
```bash
yarn run start
```

## Usage metrics

Some functionality the executor has is to report how much cpu/memory jobs are using.

This is turned on by changing the executor config file to include:
``` yaml
metric:
   exposeQueueUsageMetrics: true
```

The metrics are calculated by getting values from metrics-server.

When developing locally with Kind, you will also need to deploy metrics-server to allow this to work.

The simplest way to do this it to apply this to your kind cluster:

```
kubectl apply -f https://gist.githubusercontent.com/hjacobs/69b6844ba8442fcbc2007da316499eb4/raw/5b8678ac5e11d6be45aa98ca40d17da70dcb974f/kind-metrics-server.yaml
```

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
