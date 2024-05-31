import datetime
from enum import Enum
from functools import cached_property
from typing import Dict, Any, Tuple, Protocol, Optional


class AuthType(Enum):
    """
    Defines the Authentication flows to be used when getting tokens from IDS.
    """

    LOCAL_AGENT = 0
    KUBERNETES = 2


class TokenRetriever(Protocol):
    def get_token(self) -> str: ...

    def serialize(self) -> Tuple[str, Dict[str, Any]]: ...
