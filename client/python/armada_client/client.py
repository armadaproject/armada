import base64
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool
import os
import time
from urllib import response
from armada_client.auth_data import AuthData, AuthMethod
import grpc
import uuid
import threading
from enum import Enum, auto
from typing import Optional

from armada_client.generated_client import event_pb2
from armada_client.generated_client import event_pb2_grpc
from armada_client.generated_client import queue_pb2
from armada_client.generated_client import queue_pb2_grpc
from armada_client.generated_client import usage_pb2
from armada_client.generated_client import usage_pb2_grpc
from armada_client.generated_client import submit_pb2
from armada_client.generated_client import submit_pb2_grpc
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)



# The python GRPC library requires authentication data to be provided as an AuthMetadataPlugin
# The username/password are colon-delimted and base64 encoded as per RFC 2617

class GrpcBasicAuth(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def __call__(self, context, callback):
        b64encoded_auth = base64\
            .b64encode(bytes(f'{self._username}:{self._password}', 'utf-8'))\
            .decode('ascii')
        callback(
            (('authorization', f'basic {b64encoded_auth}'),),
            None
        )


class ArmadaClient:
    def __init__(self, host: str, port: int,
                 auth_data: AuthData = AuthData(),
                 disable_ssl: bool = False,
                 max_workers=os.cpu_count()):

        self.host = host
        self.port = port
        self.disable_ssl = disable_ssl
        self.executor = ThreadPoolExecutor(max_workers=max_workers or 1)

        if auth_data.method == AuthMethod.Anonymous:
            self.channel = grpc.insecure_channel(f'{host}:{port}')
        elif auth_data.method == AuthMethod.Basic:
            if self.disable_ssl:
                channel_credentials = grpc.local_channel_credentials()
            else:
                # TODO pass root certs, private key, cert chain if this is needed
                channel_credentials = grpc.ssl_channel_credentials()

            self.channel = grpc.secure_channel(f'{host}:{port}',
                                               grpc.composite_channel_credentials(
                                                   channel_credentials,
                                                   grpc.metadata_call_credentials(
                                                       GrpcBasicAuth(
                                                           auth_data.username, auth_data.password)
                                                   )
                                               )
                                               )
        elif auth_data.method == AuthMethod.OpenId:
            pass
        elif auth_data.method == AuthMethod.Kerberos:
            pass

        self.submit_stub = submit_pb2_grpc.SubmitStub(self.channel)
        self.event_stub = event_pb2_grpc.EventStub(self.channel)
        self.usage_stub = usage_pb2_grpc.UsageStub(self.channel)

    def get_job_events_stream(self, queue, job_set_id, from_message_id=None, watch=False):
        jsr = event_pb2.JobSetRequest(
            queue=queue, from_message_id=from_message_id, watch=False)
        jse = self.event_stub.GetJobSetEvents(queue, job_set_id, jsr)

    def submit_jobs(self, queue, job_set_id, job_request_items):
        request = submit_pb2.JobSubmitRequest(
            queue=queue,
            job_set_id=job_set_id,
            job_request_items=job_request_items
        )
        response = self.submit_stub.SubmitJobs(request)
        return response

    def cancel_jobs(self, queue=None, job_id=None, job_set_id=None):
        request = submit_pb2.JobCancelRequest(
            queue=queue, job_id=job_id, job_set_id=job_set_id
        )
        response = self.submit_stub.CancelJobs(request)
        return response

    def reprioritize_jobs(self, new_priority, job_ids=None, job_set_id=None, queue=None):
        request = submit_pb2.JobReprioritizeRequest(
            job_ids=job_ids, job_set_id=job_set_id, queue=queue,
            new_priority=new_priority
        )
        response = self.submit_stub.ReprioritizeJobs(request)
        return response

    def create_queue(self, name, **queue_params):
        request = submit_pb2.Queue(name=name, **queue_params)
        response = self.submit_stub.CreateQueue(request)
        return response

    def update_queue(self, name, **queue_params):
        request = submit_pb2.Queue(name=name, **queue_params)
        response = self.submit_stub.UpdateQueue(request)
        return response

    def delete_queue(self, name):
        request = submit_pb2.QueueDeleteRequest(name=name)
        response = self.submit_stub.DeleteQueue(request)
        return response

    def get_queue(self, name):
        request = submit_pb2.QueueGetRequest(name=name)
        response = self.submit_stub.GetQueue(request)
        return response

    def get_queue_info(self, name):
        request = submit_pb2.QueueInfoRequest(name=name)
        response = self.submit_stub.GetQueueInfo(request)
        return response

    def watch_events(self, on_event, queue, job_set_id, from_message_id=None):
        jsr = event_pb2.JobSetRequest(
            queue=queue, id=job_set_id, from_message_id=from_message_id, watch=True, errorIfMissing=True)
        event_stream = self.event_stub.GetJobSetEvents(jsr)

        def event_counter():
            try:
                nonlocal event_stream
                for event in event_stream:
                    on_event(event)
            except grpc._channel._MultiThreadedRendezvous as e:
                if e.code() == grpc.StatusCode.CANCELLED:
                    pass
                # process cancelled status
                elif e.code() == grpc.StatusCode.UNAVAILABLE and 'Connection reset by peer' in e.details():
                    pass
                # process unavailable status
                else:
                    raise

        self.executor.submit(event_counter)
        return event_stream

    def unwatch_events(self, event_stream):
        event_stream.cancel()
