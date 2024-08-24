from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any, AsyncIterator, ClassVar, Dict

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.serde import deserialize, serialize
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState
from pendulum import DateTime
from sqlalchemy.orm.session import Session

from .hooks import ArmadaHook
from .model import GrpcChannelArgs, RunningJobContext
from .utils import log_exceptions


class ArmadaPollJobTrigger(BaseTrigger):
    __version__: ClassVar[int] = 1

    @log_exceptions
    def __init__(
        self,
        moment: timedelta,
        context: RunningJobContext | tuple[str, Dict[str, Any]],
        channel_args: GrpcChannelArgs | tuple[str, Dict[str, Any]],
    ) -> None:
        super().__init__()

        self.moment = moment
        if type(context) is RunningJobContext:
            self.context = context
        else:
            self.context = deserialize(context)

        if type(channel_args) is GrpcChannelArgs:
            self.channel_args = channel_args
        else:
            self.channel_args = deserialize(channel_args)

    @log_exceptions
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "armada.triggers.ArmadaPollJobTrigger",
            {
                "moment": self.moment,
                "context": serialize(self.context),
                "channel_args": serialize(self.channel_args),
            },
        )

    @log_exceptions
    @provide_session
    def get_task_instance(self, session: Session) -> TaskInstance:
        """
        Get the task instance for the current task.
        :param session: Sqlalchemy session
        """
        query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.task_instance.dag_id,
            TaskInstance.task_id == self.task_instance.task_id,
            TaskInstance.run_id == self.task_instance.run_id,
            TaskInstance.map_index == self.task_instance.map_index,
        )
        task_instance = query.one_or_none()
        if task_instance is None:
            raise AirflowException(
                "TaskInstance with dag_id: %s,task_id: %s, "
                "run_id: %s and map_index: %s is not found",
                self.task_instance.dag_id,
                self.task_instance.task_id,
                self.task_instance.run_id,
                self.task_instance.map_index,
            )
        return task_instance

    def should_cancel_job(self) -> bool:
        """
        We only want to cancel jobs when task is being marked Failed/Succeeded.
        """
        # Database query is needed to get the latest state of the task instance.
        task_instance = self.get_task_instance()  # type: ignore[call-arg]
        return task_instance.state != TaskInstanceState.DEFERRED

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ArmadaPollJobTrigger):
            return False
        return (
            self.moment == value.moment
            and self.context == value.context
            and self.channel_args == value.channel_args
        )

    @property
    def hook(self) -> ArmadaHook:
        return ArmadaHook(self.channel_args)

    @log_exceptions
    async def run(self) -> AsyncIterator[TriggerEvent]:
        try:
            while self.moment > DateTime.utcnow():
                await asyncio.sleep(1)
            yield TriggerEvent(serialize(self.context))
        except asyncio.CancelledError:
            if self.should_cancel_job():
                self.hook.cancel_job(self.context)
            raise
