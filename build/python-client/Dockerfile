ARG PYTHON_VERSION=3.9.12

FROM --platform=x86_64 python:${PYTHON_VERSION}-buster

RUN mkdir /proto

RUN pip install grpcio grpcio-tools poetry

WORKDIR /code
COPY client/python/poetry.lock client/python/pyproject.toml /code/

# Project initialization:
RUN poetry config virtualenvs.create false \
  && poetry install $(test "$YOUR_ENV" == production && echo "--no-dev") --no-interaction --no-ansi

# Creating folders, and files for a project:
COPY client/python /code

ENTRYPOINT ["/bin/bash"]