from armada_client.client import ArmadaClient
import grpc

from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

from armada_client.armada import (
    submit_pb2,
)
from armada_client.client import unwatch_events


def submit_sleep_job():
    pod = core_v1.PodSpec(containers=[
        core_v1.Container(
            name="sleep",
            image="busybox",
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

def submit_echo_job():
    pod = core_v1.PodSpec(containers=[
        core_v1.Container(
            name="echo",
            image="busybox",
            args=["echo", "hello"],
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

def handle_finished_job(event, job_name: str, completed_job: str):
    for element in event:
        if element.message.succeeded.job_id == completed_job:
            print(f'{job_name} is finished')
            break
    return
if __name__ == "__main__":
    tester = ArmadaClient(
        grpc.insecure_channel(target="127.0.0.1:50051")
    )
    sleep_job = tester.submit_jobs(queue='test', job_set_id='job-set-1',
                       job_request_items=submit_sleep_job())

    sleep_id = sleep_job.job_response_items[0].job_id

    sleep_event = tester.get_job_events_stream(queue='test', job_set_id='job-set-1')
    handle_finished_job(sleep_event, "sleep", sleep_id)
    unwatch_events(sleep_event)

    job_echo = tester.submit_jobs(queue='test', job_set_id='job-set-1', job_request_items=submit_echo_job())
    echo_id = job_echo.job_response_items[0].job_id

    echo_event = tester.get_job_events_stream(queue='test', job_set_id='job-set-1')
    handle_finished_job(echo_event, "echo", echo_id)
    unwatch_events(echo_event)
