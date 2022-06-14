ARG PYTHON_VERSION=3.9.12

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

RUN pip install grpcio grpcio-tools poetry

ENTRYPOINT ["/bin/bash"]