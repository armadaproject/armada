from __future__ import annotations

import re
import attrs

from typing import Dict, Optional, Union
from airflow.models import XCom, TaskInstance
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstancekey import TaskInstanceKey


def get_link_value(ti_key: TaskInstanceKey, name: str) -> Optional[str]:
    """
    Get XCom value for operator links.
    Since we only have ti_key (not full ti), we must use XCom.get_value().
    This is the recommended approach for BaseOperatorLink in Airflow 3.0.
    """
    try:
        return XCom.get_value(ti_key=ti_key, key=f"armada_{name.lower()}_url")
    except Exception:
        # Return None if XCom doesn't exist yet (e.g., before job submission)
        return None


def persist_link_value(ti: TaskInstance, name: str, value: str):
    """
    Persist link value to XCom using ti.xcom_push().
    This is the recommended approach per Airflow documentation.
    """
    ti.xcom_push(key=f"armada_{name.lower()}_url", value=value)


class LookoutLink(BaseOperatorLink):
    name = "Lookout"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        """
        Get Lookout URL from XCom.
        Returns empty string if XCom data doesn't exist yet.
        """
        try:
            task_state = XCom.get_value(ti_key=ti_key, key="job_context")
        except Exception:
            # XCom doesn't exist yet (e.g., task not started)
            return ""

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

    def __init__(self, extra_links: Dict[str, re.Pattern], ti: TaskInstance):
        """
        :param extra_links: Dictionary of link names to regex patterns for URLs
        :param ti: TaskInstance for XCom operations
        """
        self._extra_links = extra_links
        self._ti = ti

    @staticmethod
    def create(
        extra_links: Dict[str, Union[str, re.Pattern]], ti: TaskInstance
    ) -> UrlFromLogsExtractor:
        """
        Create a UrlFromLogsExtractor, filtering out non-regex patterns
        and patterns that already have values in XCom.
        """
        # Get ti_key for checking existing XCom values
        ti_key = TaskInstanceKey(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            try_number=ti.try_number,
            map_index=ti.map_index if hasattr(ti, "map_index") else -1,
        )

        valid_links = {
            name: pattern
            for name, pattern in extra_links.items()
            if isinstance(pattern, re.Pattern) and not get_link_value(ti_key, name)
        }
        return UrlFromLogsExtractor(valid_links, ti)

    def extract_and_persist_urls(self, message: str):
        """
        Extract URLs from log message using regex patterns and persist to XCom.
        """
        if not self._extra_links:
            return

        matches = []
        for name in self._extra_links:
            pattern = self._extra_links[name]
            match = re.search(pattern, message)
            if match:
                url = match.group(0)
                persist_link_value(self._ti, name, url)
                matches.append(name)

        # Remove patterns that have been matched to avoid re-extracting
        for m in matches:
            del self._extra_links[m]
