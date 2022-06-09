Python Client For Armada

## Overview

The python client wraps the gRPC api from events.proto and submit.proto.  

We expose the public rpc calls from submit.proto and event.proto.


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

# Unit Tests

`poetry run pytest tests/unit/test_client.py`

This is just a simple test that starts a grpc server in the background and verifies that we can call the client.  

# Integration Tests

A design decision we made is allow users of the client to plug their own grpc channels into the class.  This allows one to inject auth/ssl into the client but does not require any code on our part for handling authentication.  

tests/integration has some examples of how to use it with basic and no auth.  


