# Armadactl

Armadactl is a command-line tool used for managing jobs in the Armada workload orchestration system. It provides functionality for creating, updating, and deleting jobs, as well as monitoring job status and resource usage.

## Usage
Once Armadactl is successfully installed, you can use it to execute Armada subcommands by running the following command:
```bash
armadactl [subcommand] [flags]
```

### Here are the available subcommands:
- analyze : The analyze subcommand can be used to analyze a set of manifests and provide a report on their compatibility with a given Kubernetes cluster version.
```bash
armadactl analyze [path/to/manifests] [flags]
```
- cancel : The cancel subcommand can be used to cancel a running Armada deployment.
```bash
armadactl cancel [deployment_name] [flags]
```
- create : The create subcommand can be used to create a new Armada deployment.
```bash
armadactl create [path/to/manifests] [flags]
```
- delete : The delete subcommand can be used to delete an existing Armada deployment.
```bash
armadactl delete [deployment_name] [flags]
```
- update : The update subcommand can be used to update an existing Armada deployment.
```bash
armadactl update [deployment_name] [path/to/new_manifests] [flags]
```
- describe : The describe subcommand can be used to get detailed information about an existing Armada deployment.
```bash
armadactl describe [deployment_name] [flags]
```
- kube : The kube subcommand can be used to generate a Kubernetes kubeconfig file for a specific deployment.
```bash
armadactl kube [deployment_name] [flags]
```
- reprioritize : The reprioritize subcommand can be used to change the priority of a running Armada deployment.
```bash
armadactl reprioritize [deployment_name] [new_priority] [flags]
```
- resources : The resources subcommand can be used to get information about the resources used by an Armada deployment.
```bash
armadactl resources [deployment_name] [flags]
```
- submit : The submit subcommand can be used to submit a set of manifests to an existing Armada deployment.
```bash
armadactl submit [deployment_name] [path/to/new_manifests] [flags]
```
- version : The version subcommand can be used to get the version of Armada that is currently installed.
```bash
armadactl version [flags]
```
- watch : The watch subcommand can be used to watch the status of an Armada deployment.
```bash
armadactl watch [deployment_name] [flags]
```
- getQueueSchedulingReport : This subcommand retrieves a report of the current scheduling status of all queues in the Armada cluster.
```bash
armadactl getQueueSchedulingReport
```
- getJobSchedulingReport : This subcommand retrieves a report of the current scheduling status of all jobs in the Armada cluster.
```bash
armadactl getJobSchedulingReport
```

For a full list of subcommands and options, you can run **armadactl --help**.

## Configuration
Armadactl can be customized using a configuration file. By default, Armadactl looks for a file named **config.yaml** in the user's home directory. Here is an example configuration file:

 ```yaml
armada:
    namespace: armada
    tiller_namespace: kube-system
    tiller_host: localhost:44134
    helm:
        timeout: 300
    manifests:
        path: /etc/armada
```
