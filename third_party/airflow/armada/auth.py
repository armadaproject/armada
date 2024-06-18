from typing import Dict, Any, Tuple, Protocol


""" We use this interface for objects fetching Kubernetes auth tokens. Since
    it's used within the Trigger, it must be serialisable."""


class TokenRetriever(Protocol):
    def get_token(self) -> str: ...

    def serialize(self) -> Tuple[str, Dict[str, Any]]: ...
