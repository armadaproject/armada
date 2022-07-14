Armada Python Client
=
<hr />

## How To Use This Application
***

> **Do Not Fork** this application is meant to be used from **[armada_client](https://pypi.org/project/armada-client/)**
1) Click on [armada_client](https://pypi.org/project/armada-client/).
2) Read the file [CONTRIBUTING.md](CONTRIBUTING.md)
3) Click [here](https://armadaproject.io/libraries) get an overview of Armada client libraries

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

This is just a simple test that starts a grpc server in the background and verifies that we can call the client. 

## Releasing

This is to be automated.

Manual Release:

Log into the Armada-GROSS account on PyPI.  
Generate the API tokens and copy those tokens to ~/.pypirc.
`poetry build`
`poetry run twine upload dist/*`