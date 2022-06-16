from armada.operators.utils import airflow_error
from airflow.exceptions import AirflowException
import pytest

testdata_success = [
    "successful", "running", "queued"
]
@pytest.mark.parametrize("state", testdata_success)
def test_airflow_error_successful(state):
    airflow_error(state, 'hello', 'id')


testdata_error = [
    ("failed", "The Armada job hello:id failed"),
    ("cancelled", "The Armada job hello:id cancelled"),
    ("cancelling", "The Armada job hello:id cancelling"),
    ("terminated", "The Armada job hello:id terminated"),
]
@pytest.mark.parametrize("state, expected_exception_message", testdata_error)
def test_airflow_error_states(state, expected_exception_message):

    with pytest.raises(AirflowException) as excinfo:   
        airflow_error(state, "hello", "id")
    assert str(excinfo.value) == expected_exception_message
