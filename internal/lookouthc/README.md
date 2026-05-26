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

3. Initialize databases and Kubernetes resources:

    ```shell
    scripts/localdev-init.sh --hotCold
    ```

4. Start Armada components using hot-cold Procfile:

    ```shell
    goreman -f _local/procfiles/hot-cold.Procfile start

### JetBrains

Armada can be run using JetBrains products such as [IntelliJ](https://www.jetbrains.com/idea/) or [GoLand](https://www.jetbrains.com/go/). 

1. 
