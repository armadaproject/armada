import importlib
from typing import Optional, Sequence, Tuple, Any, TypedDict

import grpc


class CredentialsCallbackDict(TypedDict):
    """
    Helper class to provide stronger type checking on Credential callback args.
    """

    module_name: str
    function_name: str
    function_kwargs: dict


class GrpcChannelArgsDict(TypedDict):
    """
    Helper class to provide stronger type checking on Grpc channel arugments.
    """

    target: str
    options: Optional[Sequence[Tuple[str, Any]]]
    compression: Optional[grpc.Compression]
    credentials_callback_args: Optional[CredentialsCallbackDict]


class CredentialsCallback(object):
    """
    Allows the use of an arbitrary callback function to get grpc credentials.

    :param module_name: The fully qualified python module name where the
        function is located.
    :param function_name: The name of the function to be called.
    :param function_kwargs: Keyword arguments to function_name in a dictionary.
    """

    def __init__(
        self,
        module_name: str,
        function_name: str,
        function_kwargs: dict,
    ) -> None:
        self.module_name = module_name
        self.function_name = function_name
        self.function_kwargs = function_kwargs

    def call(self):
        """Do the callback to get grpc credentials."""
        module = importlib.import_module(self.module_name)
        func = getattr(module, self.function_name)
        return func(**self.function_kwargs)


class GrpcChannelArguments(object):
    """
    A Serializable GRPC Arguments Object.

    :param target: Target keyword argument used
        when instantiating a grpc channel.
    :param credentials_callback_args: Arguments to CredentialsCallback to use
        when instantiating a grpc channel that takes credentials.
    :param options: options keyword argument used
        when instantiating a grpc channel.
    :param compression: compression keyword argument used
        when instantiating a grpc channel.
    :return: a GrpcChannelArguments instance
    """

    def __init__(
        self,
        target: str,
        options: Optional[Sequence[Tuple[str, Any]]] = None,
        compression: Optional[grpc.Compression] = None,
        credentials_callback_args: CredentialsCallbackDict = None,
    ) -> None:
        self.target = target
        self.options = options
        self.compression = compression
        self.credentials_callback = None

        if credentials_callback_args is not None:
            self.credentials_callback = CredentialsCallback(**credentials_callback_args)

    def channel(self) -> grpc.Channel:
        """
        Create a grpc.Channel based on arguments supplied to this object.

        :return: Return grpc.insecure_channel if credentials is None. Otherwise
            returns grpc.secure_channel.
        """

        if self.credentials_callback is None:
            return grpc.insecure_channel(
                target=self.target,
                options=self.options,
                compression=self.compression,
            )
        return grpc.secure_channel(
            target=self.target,
            credentials=self.credentials_callback.call(),
            options=self.options,
            compression=self.compression,
        )

    def aio_channel(self) -> grpc.aio.Channel:
        """
        Create a grpc.aio.Channel (asyncio) based on arguments supplied to this object.

        :return: Return grpc.aio.insecure_channel if credentials is None. Otherwise
            returns grpc.aio.secure_channel.
        """

        if self.credentials_callback is None:
            return grpc.aio.insecure_channel(
                target=self.target,
                options=self.options,
                compression=self.compression,
            )
        return grpc.aio.secure_channel(
            target=self.target,
            credentials=self.credentials_callback.call(),
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
            "credentials_callback_args": self.credentials_callback_args,
            "options": self.options,
            "compression": self.compression,
        }
