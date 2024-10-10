import functools
from typing import Any

import tenacity
from airflow.models import TaskInstance


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
def xcom_pull_for_ti(ti: TaskInstance, key: str) -> Any:
    return ti.xcom_pull(key=key, task_ids=ti.task_id, map_indexes=ti.map_index)
