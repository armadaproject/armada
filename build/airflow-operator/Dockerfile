ARG PLATFORM=x86_64
ARG BASE_IMAGE=python:3.8.18-bookworm
FROM --platform=$PLATFORM ${BASE_IMAGE}

RUN mkdir /proto

COPY third_party/airflow/pyproject.toml /code/pyproject.toml

RUN pip install "/code[test]"

# Creating folders, and files for a project:
COPY third_party/airflow /code

ENTRYPOINT ["/bin/bash"]
