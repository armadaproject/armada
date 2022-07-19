ARG PYTHON_VERSION=3.8.10

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

RUN pip install grpcio grpcio-tools mypy-protobuf

ENTRYPOINT ["/bin/bash"]
