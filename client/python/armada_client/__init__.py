try:
    from .typings import JobState
    from ._proto_methods import is_active, is_terminal

    JobState.is_active = is_active
    JobState.is_terminal = is_terminal

    del is_active, is_terminal, JobState
except ImportError:
    """
    Import errors occur during proto generation, where certain
    modules import types that don't exist yet. We can safely ignore these failures
    """
    pass
