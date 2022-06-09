# Armada Python client


Python client wrapping the gRPC services defined in `submit.proto` and `events.proto`; allows for

- submitting, cancelling, and reprioritising jobs, and
- watching for job events.



## Build
Prerequisites:

1) pyenv
    - Sets up local python environment for supporting multiple python environments
2) poetry
    - Package is defined by pyproject.toml
    - poetry install will pull dependencies and install based on pyproject.toml
3) formatting
    - poetry run black will format your code according to default black settings
    - poetry run pylint will lint your code


## Testing
gRPC requires a server to start so our unit tests are not true unit tests.  We start a grpc server and then our unit tests run against that server.

Run tests/run/server in one shell.

Run tests/run_unit.sh in another shell to unit test the client.

# Integration Tests

A design decision we made is allow users of the client to plug their own grpc channels into the class.  This allows one to inject auth/ssl into the client but does not require any code on our part for handling authentication.  

tests/integration has some examples of how to use it with basic and no auth.  


