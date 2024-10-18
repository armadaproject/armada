ARG PLATFORM=x86_64
ARG BASE_IMAGE=python:3.13.0b4-bookworm

FROM --platform=$PLATFORM ${BASE_IMAGE}

RUN mkdir /proto

COPY client/python/pyproject.toml /code/pyproject.toml

RUN pip install "/code[test]"

# Creating folders, and files for a project:
COPY client/python /code

ENTRYPOINT ["/bin/bash"]
