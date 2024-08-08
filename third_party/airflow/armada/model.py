import importlib
from typing import Any, Dict, Optional, Sequence, Tuple

import grpc

""" This class exists so that we can retain our connection to the Armada Query API
    when using the deferrable Armada Airflow Operator. Airflow requires any state
    within deferrable operators be serialisable, unfortunately grpc.Channel isn't
    itself serialisable."""


class GrpcChannelArgs:
    def __init__(
        self,
        target: str,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
        compression: Optional[grpc.Compression] = None,
        auth: Optional[grpc.AuthMetadataPlugin] = None,
        auth_details: Optional[Dict[str, Any]] = None,
    ):
        self.target = target
        self.options = options
        self.compression = compression
        if auth:
            self.auth = auth
        elif auth_details:
            classpath, kwargs = auth_details
            module_path, class_name = classpath.rsplit(
                ".", 1
            )  # Split the classpath to module and class name
            module = importlib.import_module(
                module_path
            )  # Dynamically import the module
            cls = getattr(module, class_name)  # Get the class from the module
            self.auth = cls(
                **kwargs
            )  # Instantiate the class with the deserialized kwargs
        else:
            self.auth = None

    def serialize(self) -> Dict[str, Any]:
        auth_details = self.auth.serialize() if self.auth else None
        return {
            "target": self.target,
            "options": self.options,
            "compression": self.compression,
            "auth_details": auth_details,
        }

    def channel(self) -> grpc.Channel:
        if self.auth is None:
            return grpc.insecure_channel(
                target=self.target, options=self.options, compression=self.compression
            )

        return grpc.secure_channel(
            target=self.target,
            options=self.options,
            compression=self.compression,
            credentials=grpc.composite_channel_credentials(
                grpc.ssl_channel_credentials(),
                grpc.metadata_call_credentials(self.auth),
            ),
        )

    def aio_channel(self) -> grpc.aio.Channel:
        if self.auth is None:
            return grpc.aio.insecure_channel(
                target=self.target,
                options=self.options,
                compression=self.compression,
            )

        return grpc.aio.secure_channel(
            target=self.target,
            options=self.options,
            compression=self.compression,
            credentials=grpc.composite_channel_credentials(
                grpc.ssl_channel_credentials(),
                grpc.metadata_call_credentials(self.auth),
            ),
        )
