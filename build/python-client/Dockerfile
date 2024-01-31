ARG PYTHON_VERSION=3.8.15

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

COPY client/python/pyproject.toml /code/pyproject.toml

RUN pip install "/code[test]"

# Creating folders, and files for a project:
COPY client/python /code

ENTRYPOINT ["/bin/bash"]
