# Python Client Examples

Currently there are three example files:

- [simple.py](#simplepy)
- [general.py](#generalpy)
- [async_logging.py](#asyncloggingpy)


## Setup

Please see the [python client docs](https://github.com/G-Research/armada/blob/master/client/python/README.md) for getting the client setup.

### Running the examples

Each example has three enviromental variables for setup

```bash
export ARMADA_SERVER=localhost
export ARMADA_PORT=443
export DISABLE_SSL=true
```

You can then simply run one of the examples

```bash
python3 simple.py
```

## Example Files

### simple.py

#### Overview

#### Walkthrough

### general.py

#### Overview

#### Walkthrough

### async_logging.py

#### Overview

#### Walkthrough

## Using Basic Auth

The following code shows an example of how basic authentication can be setup

```py
# The python GRPC library requires authentication
#  data to be provided as an AuthMetadataPlugin.
# The username/password are colon-delimted and base64 encoded as per RFC 2617

class GrpcBasicAuth(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        super().__init__()

    def __call__(self, context, callback):
        b64encoded_auth = base64.b64encode(
            bytes(f"{self._username}:{self._password}", "utf-8")
        ).decode("ascii")
        callback((("authorization", f"basic {b64encoded_auth}"),), None)

class BasicAuthTest:
    def __init__(self, host, port, username, password, disable_ssl=False):
        if disable_ssl:
            channel_credentials = grpc.local_channel_credentials()
        else:
            channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{host}:{port}",
            grpc.composite_channel_credentials(
                channel_credentials,
                grpc.metadata_call_credentials(GrpcBasicAuth(username, password)),
            ),
        )
        self.client = ArmadaClient(channel)
```