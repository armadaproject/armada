## Hot/Cold Local Development

### Goreman

[Goreman](https://github.com/mattn/goreman) is a Go-based clone of [Foreman](https://github.com/ddollar/foreman) that manages Procfile-based applications,
allowing you to run multiple processes with a single command.

Goreman will build the components from source and run them locally, making it easy to test changes quickly.

1. Install `goreman`:

    ```shell
    go install github.com/mattn/goreman@latest
    ```

2. Start dependencies:

    ```shell
    docker-compose -f _local/docker-compose-deps.yaml up -d
    ```

    - **Note**: Images can be overridden using environment variables:
      `REDIS_IMAGE`, `POSTGRES_IMAGE`, `PULSAR_IMAGE`, `KEYCLOAK_IMAGE`

3. Initialize databases and Kubernetes resources using hot/cold option:

    ```shell
    scripts/localdev-init.sh --hotCold
    ```
    The `--hotCold` option will instruct the script to create the hot/cold database and run the associated migrations

4. Start Armada components using hot/cold Procfile:

    ```shell
    goreman -f _local/procfiles/hot-cold.Procfile start
    ```

### JetBrains

Armada can be run using JetBrains products such as [IntelliJ](https://www.jetbrains.com/idea/) or [GoLand](https://www.jetbrains.com/go/). The run configurations under the `.run` directory contain instructions to start each service, as well as compounds which will start all services with a single command.

1. Run the `Start Dependencies` config
    
    This will start the Redis, Postgres, and Pulsar dependencies using `mage kind startDependencies`

2. Run the `Armada HC` compound

    This will start all of the Armada services, including a parallel Lookout Hot/Cold stack, as well as running the necessary database migrations

### VSCode

The VSCode debugger can also be used to run Armada, complete with all its normal debugging features!

1. Open the debugger tab on the left side menu

2. Run the `Armada HC` compound
    
    This will start the Armada dependencies, build the Lookout UI, and start all of the Armada services.
