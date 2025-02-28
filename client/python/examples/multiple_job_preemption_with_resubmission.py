"""
A larger, prettier example of preemption.

This script will schedule some low-priority tasks, and then after 30 seconds it will
schedule some high-priority tasks. These tasks will jump the queue, cause evictions
of the low-priority tasks and start running.

The evicted tasks will be automatically requeued, and then proceed to complete normally.

In order for this to demonstrate nicely, provide a node with a "node_pool" label that
matches the namespace name and queue you are running in, by default "armada". It should
be sized to have around 32 cores, for example an e2-standard-32 on GKE is perfect.
"""

import argparse
import logging
import math
import time
import threading
import time
import uuid
from collections import Counter

import grpc
from armada_client.client import ArmadaClient
from armada_client.k8s.io.api.core.v1 import generated_pb2 as core_v1
from armada_client.k8s.io.apimachinery.pkg.api.resource import (
    generated_pb2 as api_resource,
)
from armada_client.typings import EventType

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


class Armada:
    """Wrapper around most Armada functionality for this example."""

    def __init__(self, client, queue, job_set, node_pool):
        # Init Kubernetes
        self.client = client
        self.queue = queue
        self.job_set = f"{job_set}-{str(uuid.uuid1())[0:8]}"
        self.node_pool = node_pool
        self.default_priority_class = "armada-preemptible"
        self.submitted_jobs = {}

    def create_container(self, image, name, args):
        return core_v1.Container(
            name=name,
            image=image,
            args=args,
            resources=core_v1.ResourceRequirements(
                requests={
                    "cpu": api_resource.Quantity(string="1000m"),
                    "memory": api_resource.Quantity(string="200Mi"),
                    "ephemeral-storage": api_resource.Quantity(string="100Mi"),
                },
                limits={
                    "cpu": api_resource.Quantity(string="1000m"),
                    "memory": api_resource.Quantity(string="200Mi"),
                    "ephemeral-storage": api_resource.Quantity(string="100Mi"),
                },
            ),
        )

    def create_pod_spec(self, container, priority_class=None, restart_policy=None):
        return core_v1.PodSpec(
            nodeSelector={"node_pool": self.node_pool},
            terminationGracePeriodSeconds=5,
            restartPolicy="Never",
            priorityClassName=(
                priority_class if priority_class else self.default_priority_class
            ),
            containers=[container],
        )

    def create_job_requests(self, name, pod_spec, replicas=1, priority=1):
        return [
            self.client.create_job_request_item(
                priority=priority,
                pod_spec=pod_spec,
                namespace=self.queue,
            )
            for _ in range(replicas)
        ]

    def submit_batch(self, batch, job_set_suffix, delay=0):
        time.sleep(delay)
        jobs = self.client.submit_jobs(
            queue=self.queue,
            job_set_id=f"{self.job_set}-{job_set_suffix}",
            job_request_items=batch,
        )
        i = 0
        for job in jobs.job_response_items:
            self.submitted_jobs[job.job_id] = batch[i]
            i += 1

    def create_job(self, replicas, priority, priority_class, sleep):
        return self.create_job_requests(
            priority_class,
            self.create_pod_spec(
                self.create_container(
                    "index.docker.io/library/ubuntu:latest",
                    "container",
                    ["sleep", sleep],
                ),
                priority_class=priority_class,
                restart_policy="Never",
            ),
            replicas=replicas,
            priority=priority,
        )

    def wait_for_job_completion(
        self, job_set_suffix, no_of_jobs, timeout_s, sleep, displayer
    ):
        """
        Tries to latch on to the job set and print out the status.

        This is a blocking call, so it will never return until no_of_jobs finish,
            the timeout expires, or there are 10 failed GRPC calls.
        """
        time.sleep(sleep)
        attempts = 0
        total_finished = 0
        start_time = time.time()
        job_set_id = f"{self.job_set}-{job_set_suffix}"
        job_status = {}

        # Continuously try and reconnect to the job set
        while True:
            try:
                event_stream = self.client.get_job_events_stream(
                    queue=self.queue, job_set_id=job_set_id
                )

                # For each event, check if it is one we are interested in
                # and print out the message if it is
                for event_grpc in event_stream:
                    event = self.client.unmarshal_event_response(event_grpc)

                    if event.type in [EventType.succeeded, EventType.cancelled]:
                        total_finished += 1
                        job_status[event.message.job_id] = "T"

                    elif event.type in [EventType.failed]:
                        # Job has failed for some reason, so clean up the job_status
                        # and resubmit the job to the queue.
                        job_status[event.message.job_id] = "F"
                        displayer.display(job_set_suffix, Counter(job_status.values()))
                        del job_status[event.message.job_id]

                        self.submit_batch(
                            [self.submitted_jobs[event.message.job_id]], job_set_suffix
                        )
                        del self.submitted_jobs[event.message.job_id]

                    elif event.type in [EventType.submitted]:
                        job_status[event.message.job_id] = "S"
                    elif event.type in [EventType.queued]:
                        job_status[event.message.job_id] = "Q"
                    elif event.type in [EventType.leased]:
                        job_status[event.message.job_id] = "L"
                    elif event.type in [EventType.pending]:
                        job_status[event.message.job_id] = "P"
                    elif event.type in [EventType.preempted]:
                        job_status[event.message.job_id] = "E"
                    elif event.type in [EventType.running]:
                        job_status[event.message.job_id] = "R"
                    else:
                        print(event.type)

                    displayer.display(job_set_suffix, Counter(job_status.values()))

                    if total_finished >= no_of_jobs:
                        return

            except grpc.RpcError as e:
                if e.code() != grpc.StatusCode.NOT_FOUND:
                    logging.warning("Unexpected RPC error:", e)
                    return

            attempts += 1
            if time.time() - start_time > timeout_s:
                logging.warning(
                    "Job %s did not complete within %ds.",
                    job_set_id,
                    timeout_s,
                )
                return
            time.sleep(1)

            if attempts > 10:
                logging.warning(
                    "Job %s had too many RPC failures... Exiting.", job_set_id
                )
                return


class Displayer:
    """Convenience class for displaying output from multiple threads."""

    headings = ["S", "Q", "L", "P", "R", "T", "E", "F"]
    output = ""
    colors = {
        "S": "\033[35m",
        "Q": "\033[95m",
        "L": "\033[34m",
        "P": "\033[94m",
        "R": "\033[92m",
        "T": "\033[93m",
        "E": "\033[41m",
        "F": "\033[91m",
    }
    endc = "\033[0m"

    def __init__(self):
        self.output = {"lo": "", "hi": ""}
        self.job_statuses = {}
        self.start_time = time.time()

    def display(self, job, status_counts):
        self.job_statuses[job] = status_counts
        new_output = "".join(
            [
                self.colors[x] + x * status_counts.get(x, 0) + self.endc
                for x in self.headings
            ]
        )
        if new_output != self.output[job]:
            elapsed = math.floor(time.time() - self.start_time)
            self.output[job] = new_output
            print(f"{elapsed:3} {self.output['lo']}    {self.output['hi']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", default="armada")
    parser.add_argument("--armada-server", default="localhost")
    parser.add_argument("--armada-port", default=50051)
    parser.add_argument("--job-set-prefix", default="example")
    parser.add_argument("--disable-ssl", default=True)
    args = parser.parse_args()

    if args.disable_ssl:
        channel = grpc.insecure_channel(f"{args.armada_server}:{args.armada_port}")
    else:
        channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{args.armada_server}:{args.armada_port}",
            channel_credentials,
        )
    client = ArmadaClient(channel)
    armada = Armada(client, args.queue, args.job_set_prefix, args.queue)
    print(f"Job set: {armada.job_set}")

    # Submit three sets of jobs:
    # lo: 30 replicas immediately that will run for 120s
    # lo: 10 replicas after 10s that will run for 30s
    # hi: 5 replicas after 30s that will run for 30s
    #
    # The lo jobs should be scaled to fill up total capacity and then queue some.
    # I found it convenient to use a node pool with exactly one small node.
    # Consequently when the hi jobs submit they should jump the queue and then evict some of the lo jobs.
    lo_priority_job_a = armada.create_job(
        replicas=30, priority=1000, priority_class="armada-preemptible", sleep="30s"
    )
    lo_priority_job_b = armada.create_job(
        replicas=10, priority=1001, priority_class="armada-preemptible", sleep="30s"
    )
    hi_priority_job = armada.create_job(
        replicas=5, priority=1, priority_class="armada-default", sleep="20s"
    )

    displayer = Displayer()

    threads = [
        threading.Thread(target=armada.submit_batch, args=(lo_priority_job_a, "lo", 1)),
        threading.Thread(
            target=armada.submit_batch, args=(lo_priority_job_b, "lo", 10)
        ),
        threading.Thread(target=armada.submit_batch, args=(hi_priority_job, "hi", 31)),
        threading.Thread(
            target=armada.wait_for_job_completion, args=("lo", 40, 240, 0, displayer)
        ),
        threading.Thread(
            target=armada.wait_for_job_completion, args=("hi", 5, 240, 30, displayer)
        ),
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
