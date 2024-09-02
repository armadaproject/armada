from airflow.serialization.serde import _extra_allowed

_extra_allowed.add("armada.model.RunningJobContext")
_extra_allowed.add("armada.model.GrpcChannelArgs")


def get_provider_info():
    return {
        "package-name": "armada-airflow",
        "name": "Armada Airflow Operator",
        "description": "Armada Airflow Operator.",
        "extra-links": ["armada.operators.armada.LookoutLink"],
        "versions": ["1.0.0"],
    }
