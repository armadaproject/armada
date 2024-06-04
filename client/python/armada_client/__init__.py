from .typings import JobState
from ._proto_methods import is_active, is_terminal

JobState.is_active = is_active
JobState.is_terminal = is_terminal

del _proto_methods
