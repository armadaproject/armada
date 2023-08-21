ARG PYTHON_VERSION=3.8.15

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

COPY third_party/airflow/pyproject.toml /code/pyproject.toml

RUN pip install "/code[test]"

# Creating folders, and files for a project:
COPY third_party/airflow /code

ENTRYPOINT ["/bin/bash"]
