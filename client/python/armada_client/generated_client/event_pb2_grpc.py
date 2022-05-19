# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from armada_client.generated_client import event_pb2 as armada__client_dot_generated__client_dot_event__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


class EventStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.ReportMultiple = channel.unary_unary(
                '/api.Event/ReportMultiple',
                request_serializer=armada__client_dot_generated__client_dot_event__pb2.EventList.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )
        self.Report = channel.unary_unary(
                '/api.Event/Report',
                request_serializer=armada__client_dot_generated__client_dot_event__pb2.EventMessage.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )
        self.GetJobSetEvents = channel.unary_stream(
                '/api.Event/GetJobSetEvents',
                request_serializer=armada__client_dot_generated__client_dot_event__pb2.JobSetRequest.SerializeToString,
                response_deserializer=armada__client_dot_generated__client_dot_event__pb2.EventStreamMessage.FromString,
                )
        self.Watch = channel.unary_stream(
                '/api.Event/Watch',
                request_serializer=armada__client_dot_generated__client_dot_event__pb2.WatchRequest.SerializeToString,
                response_deserializer=armada__client_dot_generated__client_dot_event__pb2.EventStreamMessage.FromString,
                )


class EventServicer(object):
    """Missing associated documentation comment in .proto file."""

    def ReportMultiple(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Report(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetJobSetEvents(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Watch(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_EventServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'ReportMultiple': grpc.unary_unary_rpc_method_handler(
                    servicer.ReportMultiple,
                    request_deserializer=armada__client_dot_generated__client_dot_event__pb2.EventList.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
            'Report': grpc.unary_unary_rpc_method_handler(
                    servicer.Report,
                    request_deserializer=armada__client_dot_generated__client_dot_event__pb2.EventMessage.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
            'GetJobSetEvents': grpc.unary_stream_rpc_method_handler(
                    servicer.GetJobSetEvents,
                    request_deserializer=armada__client_dot_generated__client_dot_event__pb2.JobSetRequest.FromString,
                    response_serializer=armada__client_dot_generated__client_dot_event__pb2.EventStreamMessage.SerializeToString,
            ),
            'Watch': grpc.unary_stream_rpc_method_handler(
                    servicer.Watch,
                    request_deserializer=armada__client_dot_generated__client_dot_event__pb2.WatchRequest.FromString,
                    response_serializer=armada__client_dot_generated__client_dot_event__pb2.EventStreamMessage.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'api.Event', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Event(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def ReportMultiple(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/api.Event/ReportMultiple',
            armada__client_dot_generated__client_dot_event__pb2.EventList.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Report(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/api.Event/Report',
            armada__client_dot_generated__client_dot_event__pb2.EventMessage.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetJobSetEvents(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/api.Event/GetJobSetEvents',
            armada__client_dot_generated__client_dot_event__pb2.JobSetRequest.SerializeToString,
            armada__client_dot_generated__client_dot_event__pb2.EventStreamMessage.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Watch(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/api.Event/Watch',
            armada__client_dot_generated__client_dot_event__pb2.WatchRequest.SerializeToString,
            armada__client_dot_generated__client_dot_event__pb2.EventStreamMessage.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
