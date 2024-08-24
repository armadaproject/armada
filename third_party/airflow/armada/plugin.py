from airflow.plugins_manager import AirflowPlugin

from .armada.operators.armada import LookoutLink


class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        LookoutLink(),
    ]
