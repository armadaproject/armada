ARG PYTHON_VERSION=3.8.10

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

# Project Install
COPY client/python/pyproject.toml pyproject.toml
RUN pip install .

# Creating folders, and files for a project:
COPY client/python /code

ENTRYPOINT ["/bin/bash"]
