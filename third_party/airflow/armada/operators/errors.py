from typing import Any, Dict, Tuple, Union

from airflow.exceptions import AirflowException

from armada_client.typings import JobState

from .._compat import AirflowFailException


class ArmadaOperatorJobFailedError(AirflowException):
    """
    Raised when an ArmadaOperator job has terminated unsuccessfully on Armada.

    :param job_id: The unique identifier of the job.
    :type job_id: str
    :param queue: The queue the job was submitted to.
    :type queue: str
    :param state: The termination state of the job. Accepts the state name as
        a string so Airflow can reconstruct the exception from serialize().
    :type state: Union[JobState, str]
    :param reason: The termination reason, if provided.
    :type reason: str
    """

    def __init__(
        self,
        queue: str,
        job_id: str,
        state: Union[JobState, str],
        reason: str = "",
    ):
        self.job_id = job_id
        self.queue = queue
        self.state = JobState[state] if isinstance(state, str) else state
        self.reason = reason
        self.message = self._generate_message()
        super().__init__(self.message)

    def serialize(self) -> Tuple[str, Tuple[str, str, str, str], Dict[str, Any]]:
        """
        Serialize into (classpath, args, kwargs) as expected by Airflow's
        exception serialization, which reconstructs with cls(*args, **kwargs).
        The state is passed by name since JobState is not serializable.

        :return: Tuple of class path, constructor args and kwargs.
        :rtype: Tuple[str, Tuple[str, str, str, str], Dict[str, Any]]
        """
        cls = self.__class__
        return (
            f"{cls.__module__}.{cls.__name__}",
            (self.queue, self.job_id, self.state.name, self.reason),
            {},
        )

    def _generate_message(self) -> str:
        """
        Generate a user-friendly error message.

        :return: Formatted error message with job details.
        :rtype: str
        """
        message = (
            f"ArmadaOperator job '{self.job_id}' in queue '{self.queue}'"
            f" terminated with state '{self.state.name.capitalize()}'."
        )
        if self.reason:
            message += f" Termination reason: {self.reason}"
        return message

    def __str__(self) -> str:
        """
        Return the error message when the exception is converted to a string.

        :return: The error message.
        :rtype: str
        """
        return self.message


class ArmadaOperatorJobFailedFatalError(
    ArmadaOperatorJobFailedError, AirflowFailException
):
    """
    Raised when an ArmadaOperator job has terminated unsuccessfully on Armada
    and the task must not be retried.

    Subclasses AirflowFailException so Airflow fails the task without
    scheduling further retries, while carrying the same structured job
    context (queue, job_id, state, reason) as ArmadaOperatorJobFailedError.
    """
