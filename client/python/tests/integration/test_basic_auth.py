import base64
import time
import uuid
from armada_client.client import unwatch_events
import grpc
from armada_client.armada import (
    event_pb2,
    event_pb2_grpc,
    queue_pb2,
    queue_pb2_grpc,
    usage_pb2,
    usage_pb2_grpc,
    submit_pb2,
    submit_pb2_grpc,
)
from armada_client.client import ArmadaClient
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
        b64encoded_auth = base64.b64encode(
            bytes(f"{self._username}:{self._password}", "utf-8")
        ).decode("ascii")
        callback((("authorization", f"basic {b64encoded_auth}"),), None)


class BasicAuthTest:
    def __init__(self, host, port, username, password, disable_ssl=True):
        # TODO: generalize this so tests can be run with a variety of auth schemas
        if disable_ssl:
            channel_credentials = grpc.local_channel_credentials()
        else:
            # TODO pass root certs, private key, cert chain if this is needed
            channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{host}:{port}",
            grpc.composite_channel_credentials(
                channel_credentials,
                grpc.metadata_call_credentials(GrpcBasicAuth(username, password)),
            ),
        )
        self.client = ArmadaClient(host, port, channel)

    # private static ApiJobSubmitRequest CreateJobRequest(string jobSet)
    def job_submit_request_items_for_test(self, queue, job_set_id):
        pod = core_v1.PodSpec(
            containers=[
                core_v1.Container(
                    name="Container1",
                    image="index.docker.io/library/ubuntu:latest",
                    args=["sleep", "10s"],
                    securityContext=core_v1.SecurityContext(runAsUser=1000),
                    resources=core_v1.ResourceRequirements(
                        requests={
                            "cpu": api_resource.Quantity(string="120m"),
                            "memory": api_resource.Quantity(string="510Mi"),
                        },
                        limits={
                            "cpu": api_resource.Quantity(string="120m"),
                            "memory": api_resource.Quantity(string="510Mi"),
                        },
                    ),
                )
            ],
        )

        return [submit_pb2.JobSubmitRequestItem(priority=1, pod_spec=pod)]

    def submit_test_job(self, queue, job_set_id):
        jsr_items = self.job_submit_request_items_for_test(queue, job_set_id)
        request = submit_pb2.JobSubmitRequest(
            queue="test", job_set_id=job_set_id, job_request_items=jsr_items
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
            on_event=event_counter, queue=queue_name, job_set_id=job_set_id
        )
        time.sleep(1)

        print(count)
        unwatch_events(event_stream)

        # public async Task TestSimpleJobSubmitFlow()

    def test_simple_job_submit_flow(self):
        queue_name = "test"
        job_set_id = f"set-{uuid.uuid1()}"

        self.client.create_queue(name=queue_name, priority_factor=200)

        jsr = self.job_submit_request_items_for_test(
            queue=queue_name, job_set_id=job_set_id
        )

        self.client.submit_jobs(jsr)
        cancel_response = self.client.cancel_jobs(
            queue=queue_name, job_set_id=job_set_id
        )


def test_basic_auth():
    tester = BasicAuthTest(host="127.0.0.1", port=50051, username='test', password='asdfasdf')
    tester.test_watch_events()
