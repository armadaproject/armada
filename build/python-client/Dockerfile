ARG PYTHON_VERSION=3.8.10

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

# Project Install
COPY client/python/pyproject.toml /code/pyproject.toml
RUN pip install /code --use-feature=in-tree-build

# Creating folders, and files for a project:
COPY client/python /code

ENTRYPOINT ["/bin/bash"]
