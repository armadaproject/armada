# Armadactl

Armadactl is a command-line tool used for managing jobs in the Armada workload orchestration system. It provides functionality for creating, updating, and deleting jobs, as well as monitoring job status and resource usage.

## Armadactl configuration
Armadactl config files are structured as follows:
```yaml
currentContext: main # Default context to be used by Armadactl
contexts:
  main:
    armadaUrl: <Your Armada API endpoint>
    execAuth:
      cmd: <Your auth command>
      args:
        - <Arg>
  test:
    armadaUrl: <Your Armada API endpoint>
    execAuth:
      cmd: <Your auth command>
      args:
          - <Arg>
```

By default, armadactl assumes that a configuration file exists at `$HOME/.armadactl.yaml`. You can provide your own
config file by specifying `--config $CONFIG_FILE_PATH` when running armadactl.

We also support a legacy armadactl config structure, although this will soon be deprecated:
```yaml
armadaUrl: <Your Armada API endpoint>
execAuth:
  cmd: <Your auth command>
  args:
      - <Arg>
```

Under both structures, BasicAuth and various oidc auth methods are also supported.

## Usage
Once Armadactl is successfully installed, you can use it to execute Armada subcommands by running the following command:
```bash
armadactl [subcommand] [flags]
```

### Available subcommands:
- **config** : The config subcommand enables users to configure how they interact with multiple Armada instances, which we refer to as _contexts_. The subcommand allows users to view, get and set contexts. Contexts are not supported with the legacy armadactl configuration.
  - **use-context** : Sets the default context for future armadactl commands
  ```bash
  armadactl config use-context <context> 
  ``` 
  - **get-contexts** : Retrieves all contexts from the current armadactl configuration. If no configuration has been specified, armadactl defaults to `$HOME/.armadactl.yaml`
  ```bash
  armadactl config get-contexts 
  ``` 
  - **current-context** : Retrieves the context which is currently set as the default.
  ```bash
  armadactl config current-context 
  ``` 
- **preempt** : The preempt subcommand can be used to preempt running Armada resources.
  - **job** : Preempts an individual job.
  ```bash
  armadactl preempt job <queue> <job-set> <job-id> [flags]
  ```
- **cancel** : The cancel subcommand can be used to cancel running Armada resources.
  - **job** : Cancels an individual job.
  ```bash
  armadactl cancel job <queue> <job-set> <job-id> [flags]
  ```
  - **job-set** : Cancels all jobs within a job-set.
  ```bash
  armadactl cancel job-set <queue> <job-set> [flags]
  ```
- **create** : The create subcommand can be used to create a new Armada resource.
  - **queue** : Allows users to create a queue.
  ```bash
  armadactl create queue <queue-name> [flags]
  ```
- **delete** : The delete subcommand can be used to delete an existing Armada resource.
  - **queue** : Allows users to delete a given queue.
  ```bash
  armadactl delete queue <queue-name> [flags]
  ```
- **update** : The update subcommand can be used to update an existing Armada resource.
  - **queue** : Allows users to update queue priority, owners or group-owners of a given queue.
  ```bash
  armadactl update queue <queue-name> [flags]
  ```
- **reprioritize** : The reprioritize subcommand can be used to change the priority of a running Armada resource.
  - **job** : Allows users to change the priority of an individual job
  ```bash
  armadactl reprioritize job <queue> <job-set> <job-id> <new-priority> [flags]
  ```
  - **job-set** : Allows users to change the priority of all jobs in a job-set
  ```bash
  armadactl reprioritize job-set <queue> <job-set> <new-priority> [flags]
  ```
- **submit** : The submit subcommand can be used to submit a set of manifests to an existing Armada deployment.
```bash
armadactl submit <job-manifest-location> [flags]
```
- **version** : The version subcommand can be used to get the version of Armada that is currently installed.
```bash
armadactl version [flags]
```
- **watch** : The watch subcommand can be used to watch the status of an Armada job.
```bash
armadactl watch <queue> <job-set> [flags]
```
- **get** : Allows users to retrieve more information about Armada resources.
  - **queue** : This subcommand retrieves a report of the current scheduling status of all queues in the Armada cluster.
  ```bash
  armadactl get queue
  ```
  - **queue-report** : This subcommand retrieves a report of the current scheduling status of all queues in the Armada cluster.
  ```bash
  armadactl get queue-report
  ```
  - **job-report** : This subcommand retrieves a report of the current scheduling status of all jobs in the Armada cluster.
  ```bash
  armadactl get job-report
  ```
  - **scheduling-report** : This subcommand retrieves a report of the current scheduling status in the Armada cluster.
  ```bash
  armadactl get scheduling-report
  ```

For a full list of subcommands and options, you can run **armadactl --help**.
