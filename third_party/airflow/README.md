# armada-airflow-operator

An Airflow operator for interfacing with the armada client

## Background

Airflow is an open source project focused on orchestrating Direct Acylic Graphs (DAGs) across different compute platforms.  To interface Airflow with Armada, you should use our armada operator.

## Airflow

The [airflow documentation](https://airflow.apache.org/) was used for setting up a simple test server.  

[setup-local-airflow.sh](./setup-local-airflow.sh) demonstrates how to run airflow locally using Airflow's SequentialExecutor.  This is only used for testing purposes.

Adding custom dags requires you to create a ~/airflow/dags folder and copying the dag files under examples in that location.  This allows you to test the DAG in your airflow test server.

## Examples

For documentation by example, see [hello_armada.py](./examples/hello_armada.py) or [bad_armada.py](./examples/bad_armada.py).

## Operator Documentation

[Armada Operator](../../docs/python_airflow_operator.md)

## Usage

The operator is available on [PyPi](https://pypi.org/project/armada-airflow/)

```
python3.8 -m venv armada38
source armada38/bin/activate
python3.8 -m pip install armada-airflow
```

## Development

From the top level of the repo, you should run `make airflow-operator`.  This will generate proto/grpc files in the jobservice folder.

Airflow with the Armada operator can be run alongside the other Armada services via the docker-compose environment. It is manually started in this way:

```
mage airflow start
```

Airflow's web UI will then be accessible at http://localhost:8081/login/  (login with airflow/airflow).

You can install the package via `pip3 install third_party/airflow`. 

You can use our tox file that streamlines development lifecycle.  For development, you can install black, tox, mypy and flake8.

`python3.8 -m tox -e py38` will run unit tests.

`python3.8 -m tox -e format` will run a format check

`python3.8 -m tox -e format-code` will run black on your code.

`python3.8 -m tox -e docs` will generate a new sphinx doc.

## Releasing the client
Armada-airflow releases are automated via Github Actions, for contributors with sufficient access to run them.

1) Commit and merge a change to `third_party/airflow/pyproject.toml` raising the version number the appropriate amount. We are 
   using [semver](https://semver.org/) for versioning.
2) Navigate to the [airflow operator release workflow](https://github.com/armadaproject/armada/actions/workflows/airflow-operator-release-to-pypi.yml)
   in Github workflows, click the "Run Workflow" button on the right side, and choose "master" as the branch to use the
   workflow from.
3) Once the workflow has completed running, verify the new version of Armada client has been uploaded to
   [PyPI](https://pypi.org/project/armada-airflow/).




