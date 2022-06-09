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
2) poetry
    - Package is defined by pyproject.toml
    - `poetry install` will pull dependencies and install based on pyproject.toml
3) formatting
    - `poetry run black` will format your code according to default black settings
    - `poetry run pylint` will lint your code
4) Unit Testing
    - `poetry run pytest tests/unit`

## CI

We use tox for running our formatting and testing jobs in github actions.


## Testing
gRPC requires a server to start so our unit tests are not true unit tests.  We start a grpc server and then our unit tests run against that server.

`poetry run pytest tests/unit/test_client.py`

This is just a simple test that starts a grpc server in the background and verifies that we can call the client.  

