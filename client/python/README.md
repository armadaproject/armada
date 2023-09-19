# Armada Python Client
<hr />

The Armada Python client wraps the gRPC services defined in `submit.proto` and `events.proto`.

It supports the following Armada features:
- submitting, cancelling, and re-prioritising jobs, and
- watching for job events.

## Installation

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
1) From the root of the repository, run `mage buildPython`
2) Install the client using `pip install client/python`. It's strongly recommended you do this inside a virtualenv.
