from typing import Protocol

""" We use this interface for objects fetching Kubernetes auth tokens. Since
    it's used within the Trigger, it must be serialisable."""


class TokenRetriever(Protocol):
    def get_token(self) -> str: ...
