#!/usr/bin/env bash
#
# Set up an isolated venv with a specific Apache Airflow + Python version and run the
# armada_airflow operator's unit tests against it. Used both locally and in CI to verify
# the operator works across its supported Airflow range (apache-airflow>=2.10,<3.3).
#
# Airflow's transitive dependencies (e.g. pendulum) differ sharply between 2.x and 3.x, so
# we install using Airflow's official per-version constraints file. Without it, pip resolves
# incompatible deps (e.g. pendulum 3.x against Airflow 2.6.3) and the environment breaks at
# import time.
#
# Usage:
#   third_party/airflow/scripts/run-tests.sh <airflow-version> <python-version>
#
# Examples:
#   third_party/airflow/scripts/run-tests.sh 2.10.5 3.10   # supported floor
#   third_party/airflow/scripts/run-tests.sh 3.2.1 3.12    # near the 3.x ceiling
#
# Environment overrides:
#   PYTHON_BIN   - python interpreter to use (default: python<python-version>, e.g. python3.11)
#   VENV_DIR     - venv location (default: a fresh temp dir). If set, only this directory
#                  is removed on exit, not its parent.
#   KEEP_VENV    - if set, do not remove the venv on exit (for debugging)

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <airflow-version> <python-version>" >&2
  echo "example: $0 2.10.5 3.11" >&2
  exit 2
fi

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"

# Resolve paths from the script's own location so it works from any CWD.
OPERATOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLIENT_DIR="$(cd "${OPERATOR_DIR}/../.." && pwd)/client/python"

# Some (airflow, python) pairs need overrides because Airflow doesn't publish a
# constraints file for that python version:
#   airflow 3.1.x has no constraints-3.14.txt — keep python 3.14 but pin transitive
#     deps using constraints-3.13.txt.
#   airflow 2.10.5 has no constraints-3.14.txt — keep python 3.14 but pin transitive
#     deps using constraints-3.12.txt.
CONSTRAINTS_PYTHON="${PYTHON_VERSION}"
if [[ "${AIRFLOW_VERSION}" == 3.1.* && "${PYTHON_VERSION}" == "3.14" ]]; then
  echo "==> airflow ${AIRFLOW_VERSION} has no constraints-3.14: using constraints-3.13"
  CONSTRAINTS_PYTHON="3.13"
elif [[ "${AIRFLOW_VERSION}" == "2.10.5" && "${PYTHON_VERSION}" == "3.14" ]]; then
  echo "==> airflow ${AIRFLOW_VERSION} has no constraints-3.14: using constraints-3.12"
  CONSTRAINTS_PYTHON="3.12"
fi

PYTHON_BIN="${PYTHON_BIN:-python${PYTHON_VERSION}}"
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "error: ${PYTHON_BIN} not found on PATH; set PYTHON_BIN to a Python ${PYTHON_VERSION} interpreter" >&2
  exit 1
fi

# Airflow publishes one constraints file per (airflow version, python version); it pins the
# exact transitive deps that version was released and tested against.
CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${CONSTRAINTS_PYTHON}.txt"

# If the caller did not supply VENV_DIR, create the venv inside our own temp dir and remove
# that whole temp dir on exit. If the caller did supply VENV_DIR, only remove the venv
# directory itself on exit, never its parent (which may hold unrelated files).
if [[ -n "${VENV_DIR:-}" ]]; then
  CLEANUP_TARGET="${VENV_DIR}"
else
  TMP_ROOT="$(mktemp -d)"
  VENV_DIR="${TMP_ROOT}/venv"
  CLEANUP_TARGET="${TMP_ROOT}"
fi
cleanup() {
  if [[ -z "${KEEP_VENV:-}" ]]; then
    rm -rf "${CLEANUP_TARGET}"
  fi
}
trap cleanup EXIT

echo "==> Airflow ${AIRFLOW_VERSION} on Python ${PYTHON_VERSION} (${PYTHON_BIN})"
echo "==> constraints: ${CONSTRAINTS_URL}"
echo "==> venv: ${VENV_DIR}"

"${PYTHON_BIN}" -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"
python -m pip install --quiet --upgrade pip

# Install Airflow first, pinned to its constraints file so its transitive deps (pendulum,
# flask, sqlalchemy, ...) match the versions that Airflow release was tested against.
echo "==> installing apache-airflow==${AIRFLOW_VERSION}"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINTS_URL}"

# Install the local armada-client and the operator WITHOUT the Airflow constraints file.
# The constraints file pins versions (e.g. grpcio) that conflict with armada-client's own
# requirements; applying it here would make resolution impossible. Letting pip resolve
# these against the already-installed Airflow is what an end user's `pip install
# armada_airflow` does, so it is the combination we actually want to test. pip will upgrade
# shared deps (e.g. grpcio) as needed; if that breaks Airflow, the import check below fails.
echo "==> installing armada-client (local)"
pip install -e "${CLIENT_DIR}"

echo "==> installing armada_airflow + test extras (local)"
pip install -e "${OPERATOR_DIR}[test]"

# Sanity check: pip installed the Airflow version we asked for. Read it from package
# metadata rather than importing airflow: on some versions (e.g. 3.2) importing airflow
# emits config-deprecation warnings to stdout, which would pollute the parsed version.
INSTALLED="$(python -c 'from importlib.metadata import version; print(version("apache-airflow"))')"
echo "==> installed airflow ${INSTALLED}"
if [[ "${INSTALLED}" != "${AIRFLOW_VERSION}" ]]; then
  echo "error: expected airflow ${AIRFLOW_VERSION} but got ${INSTALLED}" >&2
  exit 1
fi

# Run the unit tests + verify the example DAGs import cleanly (catches operator/Airflow
# API drift and the BashOperator/get_current_context compatibility shims).
echo "==> running unit tests"
( cd "${OPERATOR_DIR}" && python -m pytest test/unit/ -v )

echo "==> verifying example DAGs import"
( cd "${OPERATOR_DIR}" && for dag in examples/*.py; do python "${dag}"; done )

echo "==> OK: armada_airflow passes on Airflow ${AIRFLOW_VERSION} / Python ${PYTHON_VERSION}"
