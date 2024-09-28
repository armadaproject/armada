from concurrent import futures

import grpc
import pytest

from armada_client.typings import JobState
from armada_client.armada.job_pb2 import JobRunState
from server_mock import EventService, SubmitService, QueryAPIService, QueueService

from armada_client.armada import (
    event_pb2_grpc,
    submit_pb2_grpc,
    submit_pb2,
    health_pb2,
    job_pb2_grpc,
)
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)

from armada_client.permissions import Permissions, Subject


@pytest.fixture(scope="session", autouse=True)
def server_mock():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    submit_pb2_grpc.add_SubmitServicer_to_server(SubmitService(), server)
    submit_pb2_grpc.add_QueueServiceServicer_to_server(QueueService(), server)
    event_pb2_grpc.add_EventServicer_to_server(EventService(), server)
    job_pb2_grpc.add_JobsServicer_to_server(QueryAPIService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()

    yield
    server.stop(False)


channel = grpc.insecure_channel(target="127.0.0.1:50051")
tester = ArmadaClient(
    grpc.insecure_channel(
        target="127.0.0.1:50051",
        options={
            "grpc.keepalive_time_ms": 30000,
        }.items(),
    )
)


def test_submit_job():
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

    labels = {
        "app": "test",
    }
    annotations = {
        "test": "test",
    }
    required_node_labels = {
        "test": "test",
    }

    ingress = submit_pb2.IngressConfig()
    services = submit_pb2.ServiceConfig()

    request_item = tester.create_job_request_item(
        priority=1,
        pod_spec=pod,
        namespace="test",
        client_id="test",
        labels=labels,
        annotations=annotations,
        required_node_labels=required_node_labels,
        ingress=[ingress],
        services=[services],
    )

    resp = tester.submit_jobs(
        queue="test",
        job_set_id="test",
        job_request_items=[request_item],
    )

    assert resp.job_response_items[0].job_id == "job-1"


def test_create_queue():
    queue = tester.create_queue_request(name="test", priority_factor=1)
    tester.create_queue(queue)


def test_create_queue_full():
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = tester.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )

    tester.create_queue(queue)


def test_create_queues():
    queue = tester.create_queue_request(name="test", priority_factor=1)
    queue2 = tester.create_queue_request(name="test2", priority_factor=1)

    resp = tester.create_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


def test_create_queues_full():
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = tester.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )

    queue2 = tester.create_queue_request(name="test2", priority_factor=1)

    resp = tester.create_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


def test_get_queue():
    assert tester.get_queue("test").name == "test"


def test_delete_queue():
    tester.delete_queue("test")


def test_preempt_jobs():
    test_create_queue()
    test_submit_job()

    tester.preempt_jobs(queue="test", job_id="job-1", job_set_id="job-set-1")


def test_cancel_jobs():
    test_create_queue()
    test_submit_job()

    resp = tester.cancel_jobs(queue="test", job_id="job-1", job_set_id="job-set-1")

    assert resp.cancelled_ids[0] == "job-1"


def test_cancel_jobset():
    test_create_queue()
    test_submit_job()
    tester.cancel_jobset(
        queue="test",
        job_set_id="job-set-1",
        filter_states=[JobState.RUNNING, JobState.PENDING],
    )


def test_update_queue():
    queue = tester.create_queue_request(name="test", priority_factor=1)
    tester.update_queue(queue)


def test_update_queue_full():
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = tester.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )
    tester.update_queue(queue)


def test_update_queues():
    queue = tester.create_queue_request(name="test", priority_factor=1)
    queue2 = tester.create_queue_request(name="test2", priority_factor=1)

    resp = tester.update_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


def test_update_queues_full():
    resource_limits = {
        "cpu": 0.2,
    }

    sub = Subject("Group", "group1")
    permissions = Permissions([sub], ["get", "post"])

    queue = tester.create_queue_request(
        name="test",
        priority_factor=1,
        user_owners=["test"],
        group_owners=["test"],
        resource_limits=resource_limits,
        permissions=[permissions],
    )
    queue2 = tester.create_queue_request(name="test2", priority_factor=1)

    resp = tester.update_queues([queue, queue2])

    assert len(resp.failed_queues) == 2


def test_reprioritize_jobs():
    resp = tester.reprioritize_jobs(
        queue="test",
        job_ids=["job-1"],
        job_set_id="job-set-1",
        new_priority=1,
    )

    assert resp.reprioritization_results == {"job-1": "1.0"}

    resp = tester.reprioritize_jobs(
        queue="test",
        job_ids=None,
        job_set_id="job-set-1",
        new_priority=1,
    )

    assert resp.reprioritization_results == {"test/job-set-1": "1.0"}


def test_get_job_events_stream():
    events = tester.get_job_events_stream(queue="test", job_set_id="job-set-1")

    for _ in events:
        pass


def test_health_submit():
    health = tester.submit_health()
    assert health.SERVING == health_pb2.HealthCheckResponse.SERVING


def test_health_event():
    health = tester.event_health()
    assert health.SERVING == health_pb2.HealthCheckResponse.SERVING


def test_job_status():
    test_create_queue()
    test_submit_job()

    job_status_response = tester.get_job_status(["job-1"])
    assert job_status_response.job_states["job-1"] == submit_pb2.JobState.RUNNING


def test_job_details():
    test_create_queue()
    test_submit_job()

    job_details = tester.get_job_details(["job-1"]).job_details
    assert job_details["job-1"].state == submit_pb2.JobState.RUNNING
    assert job_details["job-1"].job_id == "job-1"
    assert job_details["job-1"].queue == "test_queue"


def test_job_run_details():
    test_create_queue()
    test_submit_job()

    run_details = tester.get_job_run_details(["run-1"]).job_run_details
    assert run_details["run-1"].state == JobRunState.RUN_STATE_RUNNING
    assert run_details["run-1"].run_id == "run-1"
    assert run_details["run-1"].cluster == "test_cluster"
