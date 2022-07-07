**Armada Python client
-


Getting Started:
-
On Linux Machine
-
1) Open your favorite shell, for example, good old [Bourne Again SHell, aka, bash](https://www.gnu.org/software/bash/), the somewhat newer [Z shell, aka, zsh](https://www.zsh.org/), or shiny new [fish](https://fishshell.com/).

2) Install [Git](https://git-scm.com/) by running
   sudo apt-get install git-all on [Debian-based](https://www.debian.org/)
   distributions like [Ubuntu](https://ubuntu.com/), or
   sudo dnf install git on [Fedora](https://getfedora.org/) and closely-related
  [ RPM-Package-Manager](https://rpm.org/) based distributions like
   [CentOS](https://www.centos.org/). For further information see
   [Installing Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).

3) Install [poetry](https://python-poetry.org/) on your local machine by following the instructions on [python poetry documentation](https://python-poetry.org/docs/). 

4) Fork the G-Research Armada repository to your personal repository.

5) Clone the forked armada repository on your personal git repository by running git clone [Armada Repo](https://github.com/G-Research/armada.git) and navigate into the new directory Armada project by running cd armada.

On Mac Machine
-

1) Open your favorite shell, for example, [Homebrew](https://brew.sh/) or [MacPorts](https://www.macports.org/).

2) Install [Git](https://git-scm.com/) by running `$ brew install git` or `$ sudo port install git` or follow the instructions [here](https://www.atlassian.com/git/tutorials/install-git)

3) Install [Python](https://docs.python-guide.org/starting/install3/osx/)

4) Install [poetry](https://python-poetry.org/) on your local machine by following the instructions on [python poetry documentation](https://python-poetry.org/docs/).

5) Fork the G-Research Armada repository to your personal repository.

6) Clone the forked armada repository on your personal git repository by running git clone [Armada Repo](https://github.com/G-Research/armada.git) and navigate into the new directory Armada project by running cd armada.


On Windows Machine
-

1) Download [Git](https://git-scm.com/) by visiting [Git-SCM](https://git-scm.com/download/win) and the download will start automatically or navigate to [Git for windows](https://gitforwindows.org/).

2) Install [Python](https://www.python.org/downloads/) and then install [Poetry](https://python-poetry.org/docs/).

3) Fork the G-Research Armada repository to your personal repository.

4) Clone the forked armada repository on your personal git repository by running git clone [Armada Repo](https://github.com/G-Research/armada.git) and navigate into the new directory Armada project by running cd armada.


# Armada Python client
Python client wrapping the gRPC services defined in `submit.proto` and `events.proto`; allows for

- submitting, cancelling, and re-prioritising jobs, and
- watching for job events.

## Build
Prerequisites:

For building the python client:
In the repo level, run `make python`
`cd client/python`
`poetry install`

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

