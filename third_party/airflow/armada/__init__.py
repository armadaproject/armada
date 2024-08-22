from airflow.serialization.serde import _extra_allowed

_extra_allowed.add("armada.model.RunningJobContext")
_extra_allowed.add("armada.model.GrpcChannelArgs")
