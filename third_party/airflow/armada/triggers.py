from __future__ import annotations

import asyncio
from http import HTTPStatus
from typing import Any, AsyncIterator, ClassVar

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState
from pendulum import DateTime
from asgiref.sync import sync_to_async
from ._compat import AIRFLOW_V_3_0_PLUS, deserialize
from .hooks import ArmadaHook
from .utils import log_exceptions, xcom_pull_for_ti

# Terminal task states in which a lingering Armada job must be cancelled on
# trigger cleanup. Any other state means the task did not finish: DEFERRED is the
# triggerer handing off the trigger, and REMOVED is the attempt this trigger
# belongs to having been superseded (cleared). In both cases the job is left alone.
CANCEL_JOBS_WHEN_TASK_IN_STATE: frozenset[TaskInstanceState] = frozenset(
    {TaskInstanceState.SUCCESS, TaskInstanceState.FAILED}
)


def _is_task_instance_not_found(error: Exception) -> bool:
    """
    True when an Execution API error reports the task instance as not found.

    ``AirflowRuntimeError`` carries the failed call's HTTP status in
    ``error.error.detail``; a 404 there is the API server saying it cannot
    resolve the task instance, as opposed to the call itself having gone wrong.
    """
    detail = getattr(getattr(error, "error", None), "detail", None)
    if not isinstance(detail, dict):
        return False
    return detail.get("status_code") == HTTPStatus.NOT_FOUND


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
        Cancel the Armada job only when the task has reached a terminal state.

        A task that is not in a terminal state on trigger exit means the task did
        not finish: DEFERRED is the triggerer restarting / handing off the trigger,
        and REMOVED is this attempt having been cleared. Cancelling in either case
        would kill a job that is either perfectly healthy or already owned by the
        replacement attempt. Morally equivalent to
        KubernetesPodTrigger.safe_to_cancel().
        """
        try:
            state = await self._get_task_state()
            self.log.info("Task state during cleanup: %s", state)
        except Exception:
            self.log.warning(
                "Could not determine task state during cleanup; "
                "re-raising to fail trigger cleanup.",
                exc_info=True,
            )
            raise
        return state in CANCEL_JOBS_WHEN_TASK_IN_STATE

    async def _get_task_state(self) -> Any:
        """
        State of the task instance this trigger was created for.

        Reports REMOVED when the task instance can no longer be resolved.
        Clearing a task supersedes the attempt the trigger belongs to: the
        Execution API then either answers the state lookup with 404 (the task
        instance record is gone) or reports no state for it (its state is reset
        to NULL, or it is absent from the response). That is the normal
        consequence of clearing rather than a failed lookup, and REMOVED is not
        a terminal state, so cleanup logs one line and leaves the job alone.
        """
        if not AIRFLOW_V_3_0_PLUS:
            return await sync_to_async(self.task_instance.current_state)()

        from airflow.sdk.exceptions import AirflowRuntimeError
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        ti = self.task_instance
        map_index = getattr(ti, "map_index", -1)
        ti_description = (
            f"dag_id={ti.dag_id}, task_id={ti.task_id}, "
            f"run_id={ti.run_id}, map_index={map_index}"
        )
        try:
            response = await sync_to_async(RuntimeTaskInstance.get_task_states)(
                dag_id=ti.dag_id,
                task_ids=[ti.task_id],
                run_ids=[ti.run_id],
                map_index=map_index,
            )
        except AirflowRuntimeError as e:
            if not _is_task_instance_not_found(e):
                raise
            return self._removed_task_instance(
                ti_description, "the Execution API answered the state lookup with 404"
            )

        # The /states endpoint suffixes the response key with
        # ``_{map_index}`` for mapped TIs and uses the bare task_id
        # otherwise.
        ti_key = f"{ti.task_id}_{map_index}" if map_index >= 0 else ti.task_id
        state = response.get(ti.run_id, {}).get(ti_key)
        if state is None:
            return self._removed_task_instance(
                ti_description, "the Execution API reports no state for it"
            )
        return state

    def _removed_task_instance(
        self, ti_description: str, reason: str
    ) -> TaskInstanceState:
        self.log.info(
            "TaskInstance (%s) can no longer be resolved: %s. It was most likely "
            "cleared while the trigger was running; treating it as %s.",
            ti_description,
            reason,
            TaskInstanceState.REMOVED,
        )
        return TaskInstanceState.REMOVED
