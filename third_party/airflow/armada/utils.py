import functools
from typing import Any, Callable, Optional, TypeVar

import tenacity
from airflow.configuration import conf


def log_exceptions(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            return method(self, *args, **kwargs)
        except Exception as e:
            if hasattr(self, "log") and hasattr(self.log, "error"):
                self.log.error(f"Exception in {method.__name__}: {e}")
            raise

    return wrapper


@tenacity.retry(
    wait=tenacity.wait_random_exponential(max=3),
    stop=tenacity.stop_after_attempt(5),
    reraise=True,
)
@log_exceptions
def xcom_pull_for_ti(ti: Any, key: str) -> Any:
    # In a trigger on Airflow 3.x, the triggerer sets ``trigger.task_instance``
    # to a ``TaskInstanceDTO`` (a Pydantic data model), not a RuntimeTaskInstance,
    # so ``xcom_pull`` is unavailable. Fall back to the XCom backend, which
    # routes through the triggerer's ``SUPERVISOR_COMMS`` and works from both
    # the loop thread (via greenback) and worker threads.
    if hasattr(ti, "xcom_pull"):
        # On a mapped task in Airflow 3.x, omitting map_indexes makes
        # RuntimeTaskInstance.xcom_pull return a list across all map indexes
        # instead of the calling instance's value. Pass it explicitly so we
        # always get a single value on both 2.x and 3.x.
        return ti.xcom_pull(key=key, task_ids=ti.task_id, map_indexes=ti.map_index)

    from airflow.sdk.execution_time.xcom import XCom

    return XCom.get_one(
        key=key,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=getattr(ti, "map_index", -1),
    )


T = TypeVar("T")


def resolve_parameter_value(
    param_name: str,
    param_value: Optional[T],
    kwargs: dict,
    fallback_value: T,
    type_converter: Callable[[str], T] = lambda x: x,
) -> T:
    if param_value is not None:
        return param_value

    dag = kwargs.get("dag")
    if dag and getattr(dag, "default_args", None):
        default_args = dag.default_args
        if param_name in default_args:
            return default_args[param_name]

    airflow_config_value = conf.get("armada_operator", param_name, fallback=None)
    if airflow_config_value is not None:
        try:
            return type_converter(airflow_config_value)
        except ValueError as e:
            raise ValueError(
                f"Failed to convert '{airflow_config_value}' for '{param_name}': {e}"
            )

    return fallback_value
