from armada.operators.utils import JobStateEnum, airflow_error
from airflow.exceptions import AirflowFailException
import pytest

testdata_success = [JobStateEnum.SUCCEEDED]


@pytest.mark.parametrize("state", testdata_success)
def test_airflow_error_successful(state):
    airflow_error(state, "hello", "id")


testdata_error = [
    (JobStateEnum.FAILED, "The Armada job hello:id FAILED"),
    (JobStateEnum.CANCELLED, "The Armada job hello:id CANCELLED"),
    (JobStateEnum.JOB_ID_NOT_FOUND, "The Armada job hello:id JOB_ID_NOT_FOUND"),
]


@pytest.mark.parametrize("state, expected_exception_message", testdata_error)
def test_airflow_error_states(state, expected_exception_message):

    with pytest.raises(AirflowFailException) as airflow:
        airflow_error(state, "hello", "id")
    assert str(airflow.value) == expected_exception_message
