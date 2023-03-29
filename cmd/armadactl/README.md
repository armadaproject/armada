# Armadactl

Armadactl is a command line tool for managing and deploying Armada charts. It allows users to install, upgrade, and delete charts, as well as view their status and history.

## Installation
To install Armada, follow these steps:

- Install Armada CLI:
```bash
curl -sL https://github.com/armadaplatform/armada-cli/releases/latest/download/armada-linux-amd64 -o armada
sudo mv armada /usr/local/bin/
sudo chmod +x /usr/local/bin/armada
```
- Check that Armada CLI is installed properly:
```bash
armada version
```

Alternatively, Armadactl can be installed manually by downloading the latest release from the Armada GitHub repository and adding the binary to your system path.

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

## Contributing
Contributions to Armadactl are welcome! Please see the [CONTRIBUTING.md](https://github.com/armadaproject/armada/blob/master/CONTRIBUTING.md) file in the [Armada GitHub repository](https://github.com/armadaproject/armada) for more information.

## License
Armadactl is licensed under the Apache License, Version 2.0. See the [LICENSE file](https://github.com/armadaproject/armada/blob/master/LICENSE) in the [Armada GitHub repository](https://github.com/armadaproject/armada) for more information.
