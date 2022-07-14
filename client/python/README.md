# Armada Python client


Python client wrapping the gRPC services defined in `submit.proto` and `events.proto`; allows for

- submitting, cancelling, and reprioritising jobs, and
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
    - `poetry run tox -e format` will check formating/linter for your code according to default black settings and flake8
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
