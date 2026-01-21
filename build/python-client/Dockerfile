ARG PLATFORM=x86_64
ARG BASE_IMAGE=python:3.9-slim-bookworm
FROM --platform=$PLATFORM ${BASE_IMAGE}

RUN mkdir /proto

# Install dependencies for the python client including test extras
COPY client/python/pyproject.toml /code/pyproject.toml

RUN pip install --no-cache-dir "/code[test]"

# Copy the full client sources after deps are installed for better layer reuse
COPY client/python /code

ENTRYPOINT ["/bin/bash"]
