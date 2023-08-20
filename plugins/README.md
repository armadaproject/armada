This documnetation will walk you through how you can self-host `armadactl` on your local machine but first let's take a look at what Krew is.

### Krew

Krew is the plugin manager for [kubectl](https://kubernetes.io/docs/tasks/tools/) command-line tool. Krew works across all major platforms, like macOS, Linux and Windows.

Krew also helps kubectl plugin developers: You can package and distribute your plugins on multiple platforms easily and makes them discoverable through a centralized plugin repository with Krew.

## Self-hosting armadactl

- Make sure you have [kubectl](https://kubernetes.io/docs/tasks/tools/) installed on your machine.

- Head on over to [Krew](https://krew.sigs.k8s.io/docs/user-guide/setup/install/) and install it based on your OS. If you're on MacOS/Linux you can follow the steps below:

1. Make sure that [git](https://git-scm.com/downloads) is installed.
2. Run this command to download and install krew:
```
(
  set -x; cd "$(mktemp -d)" &&
  OS="$(uname | tr '[:upper:]' '[:lower:]')" &&
  ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')" &&
  KREW="krew-${OS}_${ARCH}" &&
  curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz" &&
  tar zxvf "${KREW}.tar.gz" &&
  ./"${KREW}" install krew
)
```
  3. Add the $HOME/.krew/bin directory to your PATH environment variable. To do this, update your .bashrc or .zshrc file and append the following line:
```
export PATH="${KREW_ROOT:-$HOME/.krew}/bin:$PATH"
```
  and restart your shell.

   4. Run kubectl krew to check if the installation is a success or not.

- It should look something like this:
![Krew Install](https://github.com/ShivangShandilya/armada/assets/101946115/a4640b5c-656f-466b-bf87-11b402d9e838)

- Change the directory to [plugins](https://github.com/armadaproject/armada/tree/master/plugins).

- Run this command in order to install `armadactl` as a Krew plugin and to use it alongside `kubectl`.
```
kubectl krew install --manifest=armadactl.yaml
```
- It should show something like this:
![Manifest](https://github.com/ShivangShandilya/armada/assets/101946115/2324787b-978f-4da3-b8b4-e1ee41d8aec0)

- Now try and run this command to check if you can run `armadactl` alongside `kubectl`
```
kubectl armadactl
```
- It should show something like this which will ensure installing of the plugin was a success.
![armadactl plugin](https://github.com/ShivangShandilya/armada/assets/101946115/c73e49f3-1b60-4baa-b0b3-67ddeacf9387)

⚠️ **Before using the Armada CLI, make sure you have working armada enviornment or a armadactl.yaml file that points to a valid armada cluster.**

### Uninstalling the plugin 

In order to uninstall the `armadactl` plugin, just run this command:
```
kubectl krew uninstall armadactl
```
