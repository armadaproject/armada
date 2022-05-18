import base64
import time
from urllib import response
import grpc
import uuid
import threading
from enum import Enum, auto
from typing import Optional

from armada.client import event_pb2
from armada.client import event_pb2_grpc
from armada.client import queue_pb2
from armada.client import queue_pb2_grpc
from armada.client import usage_pb2
from armada.client import usage_pb2_grpc
from armada.client import submit_pb2
from armada.client import submit_pb2_grpc
from k8s.io.api.core.v1 import generated_pb2 as core_v1
from k8s.io.apimachinery.pkg.api.resource import generated_pb2 as api_resource

class AuthMethod(Enum):
        Anonymous = auto()
        Basic = auto()
        OpenId = auto()
        Kerberos = auto()

class AuthData:
    def __init__(self, method: AuthMethod = AuthMethod.Anonymous,
        username: Optional[str] = None, password: Optional[str] = None, # for BasicAuth
        oidc_token: Optional[str] = None,
        # TODO add options for kerberos authentication
    ):

        self.method = method

        if self.method == AuthMethod.Anonymous:
            pass
        elif self.method == AuthMethod.Basic:
            if not (username and password):
                raise Exception("need username and password for Basic auth")
            self.username = username
            self.password = password
        elif self.method == AuthMethod.OpenId:
            if not oidc_token:
                raise Exception("need oidc_token for OpenId auth")
        elif self.method == AuthMethod.Kerberos:
            pass

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
        testing: bool = False):

        self.host = host
        self.port = port
        self.testing = testing

        if auth_data.method == AuthMethod.Anonymous:
            self.channel = grpc.insecure_channel(f'{host}:{port}')
        elif auth_data.method == AuthMethod.Basic:
            if self.testing:
                channel_credentials = grpc.local_channel_credentials()
            else:
                # TODO pass root certs, private key, cert chain if this is needed
                channel_credentials = grpc.ssl_channel_credentials()

            self.channel = grpc.secure_channel(f'{host}:{port}',
                grpc.composite_channel_credentials(
                    channel_credentials,
                    grpc.metadata_call_credentials(
                        GrpcBasicAuth(auth_data.username, auth_data.password)
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
        jsr = event_pb2.JobSetRequest(queue=queue, from_message_id=from_message_id, watch=False)
        jse = self.event_stub.GetJobSetEvents(queue, job_set_id, jsr)
        print(jse)

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
        jsr = event_pb2.JobSetRequest(queue=queue, id=job_set_id, from_message_id=from_message_id, watch=True, errorIfMissing=True)
        event_stream = self.event_stub.GetJobSetEvents(jsr)
        def thread_func():
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

        th = threading.Thread(target=thread_func)
        th.start()
        return event_stream

    def unwatch_events(self, event_stream):
        event_stream.cancel()

class ArmadaClientTest:
    def __init__(self, host, port):
        # TODO: generalize this so tests can be run with a variety of auth schemas

        basic_auth_data = AuthData(AuthMethod.Basic, username='testuser', password='asdfasdf')
        self.client = ArmadaClient(host, port, basic_auth_data, testing=True)

    # private static ApiJobSubmitRequest CreateJobRequest(string jobSet)
    def job_submit_request_items_for_test(self, queue, job_set_id):
        pod = core_v1.PodSpec(
            volumes=[
                core_v1.Volume(
                    name="root-dir",
                    volumeSource=
                        core_v1.VolumeSource(flexVolume=
                            core_v1.FlexVolumeSource(
                                driver="gr/cifs",
                                fsType="cifs",
                                secretRef=core_v1.LocalObjectReference(name="secret-name"),
                                options={'networkPath': ""}
                            )
                        )
                )
            ],
            containers=[
                core_v1.Container(
                    name="Container1",
                    image="index.docker.io/library/ubuntu:latest",
                    args=["sleep", "10s"],
                    securityContext=core_v1.SecurityContext(runAsUser=1000),
                    resources=core_v1.ResourceRequirements(
                        requests = {
                            "cpu": api_resource.Quantity(string="120m"),
                            "memory": api_resource.Quantity(string="510Mi")
                        },
                        limits = {
                            "cpu": api_resource.Quantity(string="120m"),
                            "memory": api_resource.Quantity(string="510Mi")
                        }
                    )
                )
            ]
        )

        return [
                submit_pb2.JobSubmitRequestItem(
                    priority = 1,
                    pod_spec = pod
                )
            ]

    def submit_test_job(self, queue, job_set_id):
        jsr_items = self.job_submit_request_items_for_test(queue, job_set_id)
        request = submit_pb2.JobSubmitRequest(
            queue = "test",
            job_set_id = job_set_id,
            job_request_items = jsr_items
        )
        self.client.submit_jobs(queue, job_set_id, jsr_items)

    def test_watch_events(self):
        queue_name = "test"
        job_set_id = f"set-{uuid.uuid1()}"

        self.client.delete_queue(name=queue_name)
        self.client.create_queue(name=queue_name, priority_factor=200)
        self.submit_test_job(queue=queue_name, job_set_id=job_set_id)
        self.client.cancel_jobs(queue=queue_name, job_set_id=job_set_id)

        count = 0
        def event_counter(e):
            nonlocal count
            count += 1

        event_stream = self.client.watch_events(
            on_event=event_counter,
            queue=queue_name,
            job_set_id=job_set_id
        )
        time.sleep(1)

        print(count)
        self.client.unwatch_events(event_stream)

                # public async Task TestSimpleJobSubmitFlow()
    def test_simple_job_submit_flow(self):
        queue_name = "test"
        job_set_id = f"set-{uuid.uuid1()}"

        self.client.create_queue(name=queue_name, priority_factor=200)

        jsr = self.job_submit_request_items_for_test(
            queue=queue_name, job_set_id=job_set_id
        )

        self.client.submit_jobs(jsr)
        cancel_response =  self.client.cancel_jobs(
            queue=queue_name, job_set_id=job_set_id
        )


    #  public async Task TestProcessingUnknownEvents()
    def test_processing_unknown_events(self):
        pass


tester = ArmadaClientTest("127.0.0.1", 50051)
tester.test_watch_events()