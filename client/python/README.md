=======
# Armada Python Client
<hr />

The Armada Python client wraps the gRPC services defined in `submit.proto` and `events.proto`.

It supports the following Armada features:
- submitting, cancelling, and re-prioritising jobs, and
- watching for job events.

## Installation
## Build
Prerequisites:

For building the python client:
In the repo level, run `make python` then `cd client/python` and lastly run `pip install` to install the client from git with all necessary dependencies.

1) pyenv
   - Sets up local python environment for supporting multiple python environments
   - Set up a local python 3.9 environment
2) pip
   - Package is defined by pyproject.toml
   - `pip install .` will pull dependencies and install based on pyproject.toml
3) tox
   - `tox -e format` will check formatting/linter for your code according to default black settings and flake8
   - `tox -e py39` will run unit tests with your default python 3.9 environment
4) Auto formatting
   - `tox -e format-code` will run black formatter on client and testing code.
## CI

We use tox for running our formatting and testing jobs in github actions.

## Testing
gRPC requires a server to start so our unit tests are not true unit tests.  We start a grpc server and then our unit tests run against that server.

`poetry run pytest tests/unit/test_client.py`
=======
> **These instructions are intended for end-users.** If you wish to develop against armada_client, please see our **[documentation on contributing](CONTRIBUTING.md )**

### PyPI

The Armada python client is available from [armada_client](https://pypi.org/project/armada-client/). It can be installed
with `pip install armada-client`. Documentation and examples of how to use the python can be found on the
[Armada libraries webpage](https://armadaproject.io/libraries). 

### Build from Git
Building from Git is a multi-step process unlike many other Python projects. This project extensively uses generated
code, which is not committed into the repository.

Before beginning, ensure you have:
- A working docker client, or docker-compatible client available under `docker`.
- Network access to fetch docker images and go dependencies.

To generate all needed code, and install the python client:
1) From the root of the repository, run `make python`
2) Install the client using `pip install client/python`. It's strongly recommended you do this inside a virtualenv.
