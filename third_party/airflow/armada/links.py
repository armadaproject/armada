from __future__ import annotations

import re
import attrs

from typing import Dict, Optional, Union
from airflow.models import XCom
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstancekey import TaskInstanceKey


def get_link_value(ti_key: TaskInstanceKey, name: str) -> Optional[str]:
    return XCom.get_value(ti_key=ti_key, key=f"armada_{name.lower()}_url")


def persist_link_value(ti_key: TaskInstanceKey, name: str, value: str):
    XCom.set(
        key=f"armada_{name.lower()}_url",
        value=value,
        dag_id=ti_key.dag_id,
        task_id=ti_key.task_id,
        run_id=ti_key.run_id,
        map_index=ti_key.map_index,
    )


class LookoutLink(BaseOperatorLink):
    name = "Lookout"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        task_state = XCom.get_value(ti_key=ti_key, key="job_context")
        if not task_state:
            return ""

        return task_state.get("armada_lookout_url", "")


@attrs.define(init=True)
class DynamicLink(BaseOperatorLink, LoggingMixin):
    name: str

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        url = get_link_value(ti_key, self.name)
        if not url:
            return ""
        return url


class UrlFromLogsExtractor:
    """Extracts and persists URLs from log messages based on regex patterns."""

    def __init__(self, extra_links: Dict[str, re.Pattern], ti_key: TaskInstanceKey):
        """
        :param extra_links: Dictionary of link names to regex patterns for URLs
        :param ti_key: TaskInstanceKey for XCom
        """
        self._extra_links = extra_links
        self._ti_key = ti_key

    @staticmethod
    def create(
        extra_links: Dict[str, Union[str, re.Pattern]], ti_key: TaskInstanceKey
    ) -> UrlFromLogsExtractor:
        valid_links = {
            name: pattern
            for name, pattern in extra_links.items()
            if isinstance(pattern, re.Pattern) and not get_link_value(ti_key, name)
        }
        return UrlFromLogsExtractor(valid_links, ti_key)

    def extract_and_persist_urls(self, message: str):
        if not self._extra_links:
            return

        matches = []
        for name in self._extra_links:
            pattern = self._extra_links[name]
            match = re.search(pattern, message)
            if match:
                url = match.group(0)
                persist_link_value(self._ti_key, name, url)
                matches.append(name)

        for m in matches:
            del self._extra_links[m]
