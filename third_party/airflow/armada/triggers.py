from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, ClassVar

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState
from pendulum import DateTime
from asgiref.sync import sync_to_async
from ._compat import AIRFLOW_V_3_0_PLUS, deserialize
from .hooks import ArmadaHook
from .utils import log_exceptions, xcom_pull_for_ti


class ArmadaPollJobTrigger(BaseTrigger):
    __version__: ClassVar[int] = 1

    @log_exceptions
    def __init__(
        self,
        moment: DateTime,
    ) -> None:
        super().__init__()
        self.moment = moment

    @log_exceptions
    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "armada.triggers.ArmadaPollJobTrigger",
            {"moment": self.moment},
        )

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, ArmadaPollJobTrigger):
            return False
        return self.moment == value.moment

    @property
    def hook(self) -> ArmadaHook:
        args = xcom_pull_for_ti(self.task_instance, key="channel_args")
        return ArmadaHook(deserialize(args))

    @log_exceptions
    async def run(self) -> AsyncIterator[TriggerEvent]:
        await asyncio.sleep((self.moment - DateTime.utcnow()).total_seconds())
        yield TriggerEvent({"moment": self.moment.isoformat()})

    async def on_kill(self) -> None:
        await sync_to_async(self._cancel_job)()

    async def cleanup(self) -> None:
        if await self._should_cancel_job():
            await self.on_kill()

    def _cancel_job(self) -> None:
        try:
            ctx = self.hook.context_from_xcom(self.task_instance)
            self.log.info(
                "Cancelling Armada job queue=%s job_id=%s job_set_id=%s",
                ctx.armada_queue,
                ctx.job_id,
                ctx.job_set_id,
            )
            self.hook.cancel_job(ctx)
        except Exception:
            self.log.warning(
                "Could not cancel Armada job during cleanup; "
                "re-raising to fail trigger cleanup.",
                exc_info=True,
            )
            raise

    async def _should_cancel_job(self) -> bool:
        """
        Cancel the Armada job only when the task is no longer DEFERRED.

        A task that is still DEFERRED on trigger exit means the triggerer
        is restarting / handing off the trigger, not that the user killed
        the task — cancelling in that case would kill a perfectly healthy
        job. Morally equivalent to KubernetesPodTrigger.safe_to_cancel().
        """
        try:
            state = await self._get_task_state()
        except Exception:
            self.log.warning(
                "Could not determine task state during cleanup; "
                "re-raising to fail trigger cleanup.",
                exc_info=True,
            )
            raise
        return state != TaskInstanceState.DEFERRED

    async def _get_task_state(self) -> Any:
        if not AIRFLOW_V_3_0_PLUS:
            return await sync_to_async(self.task_instance.current_state)()

        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        ti = self.task_instance
        map_index = getattr(ti, "map_index", -1)
        response = await sync_to_async(RuntimeTaskInstance.get_task_states)(
            dag_id=ti.dag_id,
            task_ids=[ti.task_id],
            run_ids=[ti.run_id],
            map_index=map_index,
        )
        # The /states endpoint suffixes the response key with
        # ``_{map_index}`` for mapped TIs and uses the bare task_id
        # otherwise.
        ti_key = f"{ti.task_id}_{map_index}" if map_index >= 0 else ti.task_id
        try:
            return response[ti.run_id][ti_key]
        except KeyError:
            raise AirflowException(
                "TaskInstance not found for "
                f"dag_id={ti.dag_id}, task_id={ti.task_id}, "
                f"run_id={ti.run_id}, map_index={map_index}"
            )
