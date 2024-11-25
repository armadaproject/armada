from airflow.exceptions import AirflowException

from armada_client.typings import JobState


class ArmadaOperatorJobFailedError(AirflowException):
    """
    Raised when an ArmadaOperator job has terminated unsuccessfully on Armada.

    :param job_id: The unique identifier of the job.
    :type job_id: str
    :param queue: The queue the job was submitted to.
    :type queue: str
    :param state: The termination state of the job.
    :type state: TerminationState
    :param reason: The termination reason, if provided.
    :type reason: str
    """

    def __init__(self, queue: str, job_id: str, state: JobState, reason: str = ""):
        self.job_id = job_id
        self.queue = queue
        self.state = state
        self.reason = reason
        self.message = self._generate_message()
        super().__init__(self.message)

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
