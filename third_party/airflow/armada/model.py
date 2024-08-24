from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Optional, Sequence, Tuple

import grpc
from armada_client.typings import JobState
from pendulum import DateTime

""" This class exists so that we can retain our connection to the Armada Query API
    when using the deferrable Armada Airflow Operator. Airflow requires any state
    within deferrable operators be serialisable, unfortunately grpc.Channel isn't
    itself serialisable."""


class GrpcChannelArgs:
    __version__: ClassVar[int] = 1

    def __init__(
        self,
        target: str,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
        compression: Optional[grpc.Compression] = None,
        auth: Optional[grpc.AuthMetadataPlugin] = None,
    ):
        self.target = target
        self.options = options
        self.compression = compression
        self.auth = auth

    def serialize(self) -> Dict[str, Any]:
        return {
            "target": self.target,
            "options": self.options,
            "compression": self.compression,
            "auth": self.auth,
        }

    @staticmethod
    def deserialize(data: dict[str, Any], version: int) -> GrpcChannelArgs:
        if version > GrpcChannelArgs.__version__:
            raise TypeError("serialized version > class version")
        return GrpcChannelArgs(**data)

    def __eq__(self, value: object) -> bool:
        if type(value) is not GrpcChannelArgs:
            return False
        return (
            self.target == value.target
            and self.options == value.options
            and self.compression == value.compression
            and self.auth == value.auth
        )


@dataclass(frozen=True)
class RunningJobContext:
    armada_queue: str
    job_id: str
    job_set_id: str
    submit_time: DateTime
    cluster: Optional[str] = None
    last_log_time: Optional[DateTime] = None
    job_state: str = JobState.UNKNOWN.name

    @property
    def state(self) -> JobState:
        return JobState[self.job_state]
