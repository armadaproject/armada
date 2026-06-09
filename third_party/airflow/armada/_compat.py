"""Airflow imports that moved between supported versions (apache-airflow>=2.10,<3.3).

Centralizing the version-divergent imports keeps the drift in one place: the rest of
the package imports these names from here, not from Airflow directly. Each try/except
marks a real relocation between supported Airflow versions. Delete a branch once the
floor moves past the version that needed it.
"""

# serde moved from airflow.serialization.serde to airflow.sdk.serde in Airflow 3.2.
# serialize/deserialize keep the same signatures and _extra_allowed is still the
# module-level set the deserializer consults, so this is a pure import-path change.
try:
    from airflow.sdk.serde import (  # Airflow >= 3.2
        _extra_allowed,
        deserialize,
        serialize,
    )
except ImportError:
    from airflow.serialization.serde import (  # Airflow < 3.2
        _extra_allowed,
        deserialize,
        serialize,
    )

# get_current_context moved to airflow.sdk in Airflow 3.0
# (was airflow.operators.python).
try:
    from airflow.sdk import get_current_context  # Airflow >= 3.0
except ImportError:
    from airflow.operators.python import get_current_context  # Airflow 2.x

__all__ = ["serialize", "deserialize", "_extra_allowed", "get_current_context"]
