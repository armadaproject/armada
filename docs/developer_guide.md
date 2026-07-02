# Developer guide

- [Developer guide](#developer-guide)
    - [Quickstart](#quickstart)
    - [Armada design docs](#armada-design-docs)
    - [Other developer docs](#other-developer-docs)
    - [Pre-requisites](#pre-requisites)
    - [Using `mage`](#using-mage)
    - [Setting up the local dev stack](#setting-up-the-local-dev-stack)
        - [Authentication (auth profile)](#authentication-auth-profile)
        - [Fake executor](#fake-executor)
        - [Compose profiles](#compose-profiles)
        - [Procfiles](#procfiles)
        - [Service ports](#service-ports)
        - [Testing if the local dev stack is working](#testing-if-the-local-dev-stack-is-working)
        - [Running the UI](#running-the-ui)
    - [Debugging error: port 6443 is already in use after running `mage dev:full`](#debugging-error-port-6443-is-already-in-use-after-running-mage-devfull)
        - [Identifying the conflict](#identifying-the-conflict)
    - [Debugging](#debugging)
    - [GoLand run configurations](#goland-run-configurations)
    - [VS Code Run and Debug configurations](#vs-code-run-and-debug-configurations)
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

## Armada design docs

For more information about Armada's design, see the following pages:

- [Armada Components Diagram](https://armadaproject.io/design/relationships_diagram)
- [Armada Architecture](https://armadaproject.io/design/architecture)
- [Armada Design](https://armadaproject.io/design)
- [How Priority Functions](https://armadaproject.io/design/priority)
- [Armada Scheduler Design](https://armadaproject.io/design/scheduler)

## Other developer docs

- [Armada API](./developer/armada-api.md)
- [Running Armada in an EC2 instance](https://armadaproject.io/developer/aws-ec2)
- [Armada UI](https://armadaproject.io/developer/ui)
- [Usage metrics](./developer/usage_metrics.md)
- [Using OIDC with Armada](./developer/setting-up-oidc.md)
- [Building the website](./developer/website.md)

## Pre-requisites

Before you can start using Armada, you first need to install the following items:

- [`Go`](https://go.dev/doc/install) (version 1.26 or later)
- `gcc` (for Windows, [see `tdm-gcc`](https://jmeubank.github.io/tdm-gcc/))
- [`mage`](https://magefile.org/) (version 1.16 or later) - optional, every target also runs as `go run github.com/magefile/mage@v1.17.2 <target>`, which is how CI invokes mage
- [`docker`](https://docs.docker.com/get-docker/)
- [`kubectl`](https://kubernetes.io/docs/tasks/tools/#kubectl)
- [`protoc`](https://github.com/protocolbuffers/protobuf/releases)
- [`helm`](https://helm.sh/docs/intro/install/) (version 3.10.0 or later)

## Using `mage`

`mage` is a build tool that we use to build Armada. It is similar to Make, but written in Go. It is used to build Armada, run tests and run other useful commands. To see a list of available commands, run `mage -l`.

## Setting up the local dev stack

There are two ways to run Armada locally:

- `mage dev:up <profiles>` - runs the dependencies (Pulsar, Redis, Postgres) in containers and the Armada components as host processes via goreman. The profile argument is required: `mage dev:up no-auth` is the standard setup, `mage dev:up auth` adds OIDC, and `mage dev:up fake-executor` runs without a Kubernetes cluster.
- `mage dev:full` - runs the full Armada stack in containers (deps + all components) against a [kind](https://kind.sigs.k8s.io/) cluster.

Two smaller targets support these: `mage dev:deps` runs only the dependency containers, and `mage dev:migrate` re-applies the database migrations.

`dev:up` installs `goreman` to `./bin/` if missing, brings up redis/postgres/pulsar via `_local/compose/stack.yaml`, runs `_local/scripts/init.sh` to create databases and apply migrations, then runs `goreman` with the chosen procfile in the foreground. Ctrl+C stops everything cleanly, and `mage dev:down` removes the dependency containers. Image versions for the dependencies can be overridden via `REDIS_IMAGE`, `POSTGRES_IMAGE`, `PULSAR_IMAGE`, `KEYCLOAK_IMAGE`.

The `no-auth` and `auth` profiles run a real executor, which needs a Kubernetes cluster. Create one with `mage kind`, which writes its kubeconfig to `.kube/external/config`, and start the stack with `KUBECONFIG=.kube/external/config mage dev:up no-auth`. Without `KUBECONFIG` set, the executor falls back to your default kubeconfig and connects to whatever cluster that selects. Use the `fake-executor` profile if you do not want a cluster at all.

The profile argument is required and is a comma-separated list of tokens: `no-auth`, `auth`, `fake-executor`, and `hot-cold` pick the procfile, and any other token (for example `prometheus`) is passed to docker compose as a `--profile` flag. The optional `-dap` flag starts every component under a headless [Delve](https://github.com/go-delve/delve) DAP server so your editor can attach a debugger, e.g. `mage dev:up auth,prometheus -dap`.

If `mage dev:up no-auth` reports `Unknown target`, your mage binary is too old for optional flags (`mage checkDeps` verifies this). Upgrade it, or use `go run github.com/magefile/mage@v1.17.2 <target>`, which also works without installing mage at all.

For the layout of the `_local` directory itself, see [_local/README.md](https://github.com/armadaproject/armada/blob/master/_local/README.md).

**Note:** If you edit a proto file, you also need to run `mage proto` to regenerate the Go code.

We use `mage dev:full` to test the CI pipeline. You should therefore use it to test changes to the core components of Armada.

### Authentication (auth profile)

`mage dev:up auth` brings up the auth flow: Keycloak is added to the dependency containers, the auth-flavoured procfile starts the components, and you can talk to Armada with OIDC:

```shell
armadactl --config _local/.armadactl.yaml --context auth-oidc get queues
```

The auth profile configures:

- **Keycloak**: OIDC provider running on <http://localhost:8180> with pre-configured realm, users, and clients
- **Users**: `admin/admin` (admin group), `user/password` (users group) for both OIDC and basic auth
- **Service accounts**: Executor and Scheduler use OIDC Client Credentials flow for service-to-service authentication
- **APIs**: Server, Lookout, and Binoculars APIs are secured with OIDC and basic auth
- **Web UIs**: Lookout UI uses OIDC for user authentication
- **armadactl**: Supports multiple authentication flows - OIDC PKCE flow (`auth-oidc`), OIDC Device flow (`auth-oidc-device`), OIDC Password flow (`auth-oidc-password`), and basic auth (`auth-basic`)

All components support both OIDC and basic auth for convenience.

### Fake executor

For testing Armada without a real Kubernetes cluster, `mage dev:up fake-executor` runs an executor that simulates a Kubernetes environment: 2 virtual nodes with 8 CPUs and 32Gi memory each, pod lifecycle management without actual container execution, and resource allocation and job state transitions. This is useful for testing scheduling logic and job flows when Kubernetes is not available.

### Compose profiles

The compose file `_local/compose/stack.yaml` supports the following profiles:

| Profile      | Brings up                                  |
| ------------ | ------------------------------------------ |
| (none)       | Dependencies only: redis, postgres, pulsar |
| `auth`       | Adds keycloak (OIDC provider)              |
| `prometheus` | Adds prometheus (scrapes local components) |

For Apache Airflow, use the separate compose file: `docker compose -f _local/airflow/docker-compose.yaml up -d`.

### Procfiles

All Procfiles are located in `_local/procfiles/`:

| Procfile                 | Description                                       |
| ------------------------ | ------------------------------------------------- |
| `no-auth.Procfile`       | Standard setup without authentication             |
| `auth.Procfile`          | Standard setup with OIDC authentication           |
| `fake-executor.Procfile` | Uses fake executor for testing without Kubernetes |
| `hot-cold.Procfile`      | Runs the parallel hot/cold Lookout stack          |

Each profile also has a `-dap` variant, described under [Debugging](#debugging). Restart individual processes with `goreman restart <component>` (e.g., `goreman restart server`).

### Service ports

Run `goreman run status` to check the status of the processes (running processes are prefixed with `*`). Goreman exposes services on the following ports:

| Service                    | Port  | Description                       |
| -------------------------- | ----- | --------------------------------- |
| Server gRPC                | 50051 | Armada gRPC API                   |
| Server HTTP                | 8081  | REST API & Health                 |
| Server Metrics             | 9009  | Prometheus metrics                |
| Scheduler gRPC             | 50052 | Scheduler API                     |
| Scheduler HTTP             | 8080  | Scheduler HTTP                    |
| Scheduler Metrics          | 9001  | Prometheus metrics                |
| Scheduler Ingester Metrics | 9006  | Prometheus metrics                |
| Lookout API                | 8089  | Lookout REST API                  |
| Lookout UI                 | 3000  | Frontend dev server               |
| Lookout Metrics            | 9003  | Prometheus metrics                |
| Lookout Ingester Metrics   | 9005  | Prometheus metrics                |
| Executor Metrics           | 9002  | Prometheus metrics                |
| Event Ingester Metrics     | 9004  | Prometheus metrics                |
| Executor HTTP              | 8082  | Executor HTTP                     |
| Binoculars HTTP            | 8084  | Binoculars HTTP                   |
| Binoculars gRPC            | 50053 | Binoculars gRPC                   |
| Binoculars Metrics         | 9007  | Prometheus metrics                |
| Redis                      | 6379  | Cache & events                    |
| PostgreSQL                 | 5432  | Database                          |
| Pulsar                     | 6650  | Message broker                    |
| Pulsar Admin               | 8090  | Pulsar REST admin API             |
| Keycloak                   | 8180  | OIDC provider (`auth` profile)    |
| Prometheus                 | 9090  | Metrics UI (`prometheus` profile) |

### Testing if the local dev stack is working

Running `mage testsuite` runs the full test suite against the local dev stack. You should therefore use this to test changes to the core components of Armada.

You can also run the same commands yourself:

```bash
go run cmd/armadactl/main.go create queue e2e-test-queue

# To enable Ingress tests to pass
export ARMADA_EXECUTOR_INGRESS_URL="http://localhost"
export ARMADA_EXECUTOR_INGRESS_PORT=5001

go run cmd/testsuite/main.go test --tests "testsuite/testcases/basic/*" --junit junit.xml
```

### Running the UI

In the goreman flow (`dev:up`), the `lookoutui` process runs the Vite dev server with hot reload on http://localhost:3000. In the containerized flow (`mage dev:full`), the UI is built with `mage ui` and served by lookout on http://localhost:8089.

For more information, [see the UI Developer Guide](./developer/developing-locally.md).

## Debugging error: port 6443 is already in use after running `mage dev:full`

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

## Debugging

The goreman-based flow (`dev:up`) builds each component with debug flags (`-gcflags="all=-N -l"`)
and runs them as host processes, so you can attach a debugger to any component directly. Bring up the
dependencies and components with `mage dev:up no-auth`, then attach your debugger (Delve, VS Code, or GoLand) to
the running process you want to inspect. Each component reads `_local/<component>/config.yaml`.

Alternatively, the `-dap` flag (e.g. `mage dev:up no-auth -dap`) selects the `-dap` procfile variant,
which starts each component under a headless Delve DAP server (ports 2345-2352) that an editor debugger can
connect to. The VS Code tasks in `.vscode/tasks.json` use this flow.

## GoLand run configurations

We provide a number of run configurations within the `.run` directory of this project. These will be accessible when opening the project in GoLand, enabling you to run Armada in both standard and debug mode.

The following high-level configurations are provided, each composed of sub-configurations:

- `Start Dependencies` - creates the Kind cluster and brings up the dependency containers (redis, postgres, pulsar)
- `Armada` - runs the full Armada stack (migrations and components)
- `Lookout UI` - script that configures a local UI development setup
- `Armada HC` - runs the full Armada stack plus a parallel Lookout Hot/Cold stack (for testing the Lookout Hot/Cold partitioned, to be removed once graduated)
- `Lookout HC UI` - similarly, a script that configures a local UI with hot/cold configs

A minimal local Armada setup using these configurations would be `Start Dependencies` and `Armada`. If you already have a Kind cluster running, use `Infrastructure Services` instead of `Start Dependencies` to bring up just the dependency containers. Running the `Lookout UI` script on top of this configuration enables you to develop the Lookout UI live from GoLand, and see the changes visible in your browser.

**Note:** These configurations (executor specifically) require a kubernetes config in `$PROJECT_DIR$/.kube/external/config`, which `Start Dependencies` writes via `mage kind`.

GoLand runs the configurations in a compound in parallel, so `Run Migrations` starts alongside the components. The components retry their database and Pulsar connections until the migrations finish, so a short burst of connection errors at startup is expected.

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
| `Armada (hot-cold)`                      | Same as `no-auth` but also starts a parallel hot/cold Lookout stack         |

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
| `lookouthc`         | `2354`     |
| `lookouthcingester` | `2355`     |

Each compound configuration has a `preLaunchTask` that sets up and starts the relevant services via Goreman before attaching the debuggers. For example, `Armada (no-auth)` uses the task `Set up and start (no-auth)`.

### Other debugging methods

Run `mage dev:deps` to spin up only the dependencies (redis, postgres, pulsar), then run individual
Armada components yourself (for example under a debugger). Each component reads its config from
`_local/<component>/config.yaml`. See the [README](../README.md#local-development) for the goreman-based
workflow and available profiles.
