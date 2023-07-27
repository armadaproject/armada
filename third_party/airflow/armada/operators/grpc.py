from typing import Optional, Sequence, Tuple, Any, TypedDict

import grpc


class GrpcChannelArgsDict(TypedDict):
    """
    Helper class to provide stronger type checking on Grpc channel arugments.
    """

    target: str
    credentials: Optional[grpc.ChannelCredentials]
    options: Optional[Sequence[Tuple[str, Any]]]
    compression: Optional[grpc.Compression]


class GrpcChannelArguments(object):
    """
    A Serializable GRPC Arguments Object.

    :param target: Target keyword argument used
        when instantiating a grpc channel.
    :param credentials: credentials keyword argument used
        when instantiating a grpc channel.
    :param options: options keyword argument used
        when instantiating a grpc channel.
    :param compression: compression keyword argument used
        when instantiating a grpc channel.
    :return: a GrpcChannelArguments instance
    """

    def __init__(
        self,
        target: str,
        credentials: Optional[grpc.ChannelCredentials] = None,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
        compression: Optional[grpc.Compression] = None,
    ) -> None:
        self.target = target
        self.credentials = credentials
        self.options = options
        self.compression = compression

    def channel(self) -> grpc.Channel:
        """
        Create a grpc.Channel based on arguments supplied to this object.

        :return: Return grpc.insecure_channel if credentials is None. Otherwise
            returns grpc.secure_channel.
        """

        if self.credentials is None:
            return grpc.insecure_channel(
                target=self.target,
                options=self.options,
                compression=self.compression,
            )
        return grpc.secure_channel(
            target=self.target,
            credentials=self.credentials,
            options=self.options,
            compression=self.compression,
        )

    def aio_channel(self) -> grpc.aio.Channel:
        """
        Create a grpc.aio.Channel (asyncio) based on arguments supplied to this object.

        :return: Return grpc.aio.insecure_channel if credentials is None. Otherwise
            returns grpc.aio.secure_channel.
        """

        if self.credentials is None:
            return grpc.aio.insecure_channel(
                target=self.target,
                options=self.options,
                compression=self.compression,
            )
        return grpc.aio.secure_channel(
            target=self.target,
            credentials=self.credentials,
            options=self.options,
            compression=self.compression,
        )

    def serialize(self) -> dict:
        """
        Get a serialized version of this object.

        :return: A dict of keyword arguments used when calling
            a grpc channel or instantiating this object.
        """

        return {
            "target": self.target,
            "credentials": self.credentials,
            "options": self.options,
            "compression": self.compression,
        }
