ARG PYTHON_VERSION=3.10

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

COPY third_party/airflow/pyproject.toml /code/pyproject.toml

# Note that --use-feature=in-tree-build is needed until pip 21.3
# (https://github.com/libAtoms/QUIP/issues/345)
#
# To make sure that the workdir is not changed (This might break something in the future),
# We pip install from /code
#
# Then to install the `test` optional dependencies, we pip install from /code[test]
# See https://stackoverflow.com/questions/46775346/what-do-square-brackets-mean-in-pip-install
RUN pip install "/code[test]" --use-feature=in-tree-build

# Creating folders, and files for a project:
COPY third_party/airflow /code

ENTRYPOINT ["/bin/bash"]
