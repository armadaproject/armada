from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, ClassVar, Dict

from airflow.serialization.serde import deserialize, serialize
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState
from pendulum import DateTime

from .hooks import ArmadaHook
from .model import GrpcChannelArgs, RunningJobContext
from .utils import log_exceptions


class ArmadaPollJobTrigger(BaseTrigger):
    __version__: ClassVar[int] = 1

    @log_exceptions
    def __init__(
        self,
        moment: DateTime,
        context: RunningJobContext | tuple[str, Dict[str, Any]] | None = None,
        channel_args: GrpcChannelArgs | tuple[str, Dict[str, Any]] | None = None,
    ) -> None:
        super().__init__()

        self.moment = moment

        # TODO: Clean up once migration to using xcom is completed
        self.legacy_mode = False
        if type(context) is RunningJobContext:
            self.context = context
            self.legacy_mode = True
        elif context:
            self.context = deserialize(context)
            self.legacy_mode = True

        # TODO: Clean up once migration to using xcom is completed
        if type(channel_args) is GrpcChannelArgs:
            self.channel_args = channel_args
        elif channel_args:
            self.channel_args = deserialize(channel_args)
        else:
            self.channel_args = None

    @log_exceptions
    def serialize(self) -> tuple[str, dict[str, Any]]:
        if self.legacy_mode:
            return (
                "armada.triggers.ArmadaPollJobTrigger",
                {
                    "moment": self.moment,
                    "context": serialize(self.context),
                    "channel_args": serialize(self.channel_args),
                },
            )
        return (
            "armada.triggers.ArmadaPollJobTrigger",
            {"moment": self.moment},
        )

    def should_cancel_job(self) -> bool:
        """
        We only want to cancel jobs when task is being marked Failed/Succeeded.
        """
        # Database query is needed to get the latest state of the task instance.
        return self.task_instance.current_state() != TaskInstanceState.DEFERRED

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ArmadaPollJobTrigger):
            return False
        return self.moment == value.moment

    @property
    def hook(self) -> ArmadaHook:
        args = self.task_instance.xcom_pull(key="channel_args")
        if args:
            return ArmadaHook(deserialize(args))
        return ArmadaHook(self.channel_args)

    @log_exceptions
    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while self.moment > DateTime.utcnow():
                await asyncio.sleep(1)
            if self.legacy_mode:
                yield TriggerEvent(serialize(self.context))
            else:
                yield TriggerEvent({"moment": self.moment.isoformat()})
        except asyncio.CancelledError:
            if self.should_cancel_job():
                ctx = self.hook.context_from_xcom(self.task_instance, re_attach=False)
                self.hook.cancel_job(ctx)
            raise
