from armada_client.typings import JobState


def is_terminal(self) -> bool:
    """
    Determines if a job state is terminal.

    Terminal states indicate that a job has completed its lifecycle,
     whether successfully or due to failure.

    :param state: The current state of the job.
    :type state: JobState

    :returns: True if the job state is terminal, False if it is active.
    :rtype: bool
    """
    terminal_states = {
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.CANCELLED,
        JobState.PREEMPTED,
    }
    return self in terminal_states


def is_active(self) -> bool:
    """
    Determines if a job state is active.

    Active states indicate that a job is still running or in a non-terminal state.

    :param state: The current state of the job.
    :type state: JobState

    :returns: True if the job state is active, False if it is terminal.
    :rtype: bool
    """
    return not is_terminal(self)
