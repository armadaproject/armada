Armada Python client
=

# Installation:

On Linux Machine
=
1) Open your favorite shell, for example, good old [Bourne Again SHell, aka, bash](https://www.gnu.org/software/bash/), the somewhat newer [Z shell, aka, zsh](https://www.zsh.org/), or shiny new [fish](https://fishshell.com/).
2) Install [Git](https://git-scm.com/) by running `sudo apt-get install git-all` on [Debian-based](https://www.debian.org/) distributions like [Ubuntu](https://ubuntu.com/), or sudo dnf install git on [Fedora](https://getfedora.org/) 
   and closely-related [RPM-Package-Manager](https://rpm.org/) based distributions like [CentOS](https://www.centos.org/). For further information see [Installing Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).
3) Fork the G-Research Armada repository to your personal repository.

4) Clone the forked armada repository on your personal git repository by running git clone [Armada Repo](https://github.com/G-Research/armada.git) and navigate into the new directory Armada project by running cd armada.

### Python / Pyenv and Poetry Installation
#### Note that this repository does not support python 3.10 for now

1) Install [Pyenv](https://github.com/pyenv/pyenv) on [Ubuntu](https://ubuntu.com/) and [Debian-based](https://www.debian.org/) by running `$ sudo apt-get update` , `$ sudo apt-get -y install pyenv`,  `$ sudo apt-get -y install pipenv`.
2) Install Pyenv dependencies by running `$ sudo apt install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl git` or follow instructions [here](https://gist.github.com/jmvrbanac/8793985).
3) Install [Pyenv](https://github.com/pyenv/pyenv) on [Fedora](https://getfedora.org/) by running `$ sudo dnf update`, `$ curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash` or follow the instructions [here](https://joepreludian.medium.com/starting-your-python-dev-environment-with-pyenv-and-pipenv-on-a-redhat-gnu-linux-based-system-d66795377ea).
4) Install a single Python version using Pyenv by running `pyenv install [python version]`.
5) Install multiple Python versions using Pyenv by running `pyenv install --list`.
6) Install [poetry](https://python-poetry.org/) on your local machine by following the instructions on [python poetry documentation](https://python-poetry.org/docs/).

On Mac Machine
=

1) Open your favorite shell, for example, [Homebrew](https://brew.sh/) or [MacPorts](https://www.macports.org/).
2) Install [Git](https://git-scm.com/) by running `$ brew install git` or `$ sudo port install git` or follow the instructions [here](https://www.atlassian.com/git/tutorials/install-git)
7) Fork the G-Research Armada repository to your personal repository.
8) Clone the forked armada repository on your personal git repository by running git clone [Armada Repo](https://github.com/G-Research/armada.git) and navigate into the new directory Armada project by running cd armada.

### Python / Poetry and Pyenv installation
#### Note that this repository does not support python 3.10 for now

1) Install [Pyenv](https://github.com/pyenv/pyenv) by running `$ brew update` `$ brew install pyenv` then follow the steps on [Set up your shell environment for Pyenv](https://github.com/pyenv/pyenv#set-up-your-shell-environment-for-pyenv) for post-installation instructions.
2) Install a single Python version using Pyenv by running `pyenv install [python version]`.
3) Install multiple Python versions using Pyenv by running `pyenv install --list`.
4) Install [poetry](https://python-poetry.org/) on your local machine by following the instructions on [python poetry documentation](https://python-poetry.org/docs/).

On Windows Machine
=
### Note: This repository can only run on Windows Subsystem for Linux (WSL)

1) To install Windows Subsystem for Linux on your local machine follow the instructions [here](https://docs.microsoft.com/en-us/windows/wsl/install) 
2) Install [Git](https://git-scm.com/) by running `$ sudo apt-get install git` or follow the instructions [here](https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-git)
3) Fork the G-Research Armada repository to your personal repository.
4) Clone the forked armada repository in your personal git repository by running git clone [Armada Repo](https://github.com/G-Research/armada.git) and navigate into the new directory Armada project by running cd armada.

### Python / Poetry and Pyenv Installation
#### Note that this repository does not support python 3.10 for now

1) Install [Pyenv](https://github.com/pyenv/pyenv) by running `sudo apt-get update` to update and `sudo apt-get install make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev` to get all pyenv dependencies.
2) Install curl by running `$ sudo apt update && sudo apt upgrade -y` `sudo apt install curl`.
3) To run Pyenv `$curl https://pyenv.run | bash`  
4) Install Python using Pyenv by running `pyenv install [python version]` or follow the instructions [here](https://levelup.gitconnected.com/install-multiple-python-versions-in-wsl2-ba81f21109d6) to install from pyenv to python


# Armada Python client
Python client wrapping the gRPC services defined in `submit.proto` and `events.proto`; allows for

- submitting, cancelling, and re-prioritising jobs, and
- watching for job events.

## Build
Prerequisites:

For building the python client:
In the repo level, run `make python` then `cd client/python` and lastly run `poetry install` to pull all the necessary dependencies.

1) pyenv
    - Sets up local python environment for supporting multiple python environments
    - Set up a local python 3.9 environment 
2) poetry
    - Package is defined by pyproject.toml
    - `poetry install` will pull dependencies and install based on pyproject.toml
3) tox
    - `poetry run tox -e format` will check formatting/linter for your code according to default black settings and flake8
    - `poetry run tox -e py39` will run unit tests with your default python 3.9 environment
4) Auto formatting
    - `poetry run tox -e format-code` will run black formatter on client and testing code.
## CI

We use tox for running our formatting and testing jobs in github actions.


## Testing
gRPC requires a server to start so our unit tests are not true unit tests.  We start a grpc server and then our unit tests run against that server.

`poetry run pytest tests/unit/test_client.py`

This is just a simple test that starts a grpc server in the background and verifies that we can call the client.**  

