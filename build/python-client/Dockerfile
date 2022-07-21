ARG PYTHON_VERSION=3.8.10

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

COPY client/python/pyproject.toml /code/pyproject.toml

# Project Install
# Note that --use-feature=in-tree-build is needed until pipn 21.3
# (https://github.com/libAtoms/QUIP/issues/345)
# To make sure that the workdir is not changed (This might break something in the future),
# We pip install from /code
# Then to install the `test` optional dependencies, we pip install from /code[test]
# Backslashes are needed to escape [] which can break in certain shells
RUN pip install /code\[test\] --use-feature=in-tree-build

# Creating folders, and files for a project:
COPY client/python /code

ENTRYPOINT ["/bin/bash"]
