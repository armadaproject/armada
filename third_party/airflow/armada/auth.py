from typing import Dict, Any, Tuple, Protocol


class TokenRetriever(Protocol):
    def get_token(self) -> str:
        ...

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        ...
