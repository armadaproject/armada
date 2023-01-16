# Python Client Examples

Currently there are five examples:

- [Intro to using the Python Client - simple.py](#simplepy)
- [Interacting with Queues and Jobs - general.py](#generalpy)
- [A more detailed look at Queues - queues.py](#queuespy)
- [Methods for cancelling Jobs - cancelling.py ](#cancellingpy)
- [Monitoring the state of Jobs in a Job Set - monitor.py](#monitorpy)

There is also a section on [Using Basic Auth](#using-basic-auth)

## Preqrequisites

Please see the [python client docs](https://github.com/armadaproject/armada/blob/master/client/python/README.md) for getting the client setup.

### Running the examples

> Each example has three enviromental variables for setup
> ```bash
> export ARMADA_SERVER=localhost
> export ARMADA_PORT=443
> export DISABLE_SSL=true
>```

> You can then simply run one of the examples
> ```bash
> python3 simple.py
> ```

## Example Files

### simple.py

[Link to File](./simple.py)

Example of using the Armada client to create a queue, jobset and job,
then watch for the job to succeed or fail.

#### Walkthrough

> Armada uses Kubernetes to control jobs, so we need to define a podspec that our jobs will use.
> ```py
> def create_dummy_job(client: ArmadaClient):
> ```

> If you are using a remote server, secure_channel should be used to ensure that all infomation is encrypted.
> ```py
> if DISABLE_SSL:
>     channel = grpc.insecure_channel(f"{HOST}:{PORT}")
> else:
>     channel_credentials = grpc.ssl_channel_credentials()
>     channel = grpc.secure_channel(
>         f"{HOST}:{PORT}",
>         channel_credentials,
>     )
> ```


> All queues need a priority factor to be created, which is used to determine the order in which jobs are run.
> ```py
> client.create_queue(name=queue, priority_factor=1)
> ```


### general.py

[Link to File](./general.py)

A more fledged out version of `simple.py` where we create a queue only if it doesn't exist, and then create a jobset and job, and wait until the job succeeds or fails.

#### Walkthrough


> We use this function to check the status of a job, return false if the job failed and otherwise wait for the event state we are interested in.
> **Please note** that this is shown for demonstration purposes only. Subscribing to events like this to watch individual events like this will not scale well.
> ```py
> def wait_for_job_event(client, event_stream, job_id: str, event_state: EventType)
> ```

> We can use this code to get the job_id we are interested in from the event_stream.
> ```py
> job_id = resp.job_response_items[0].job_id
> ```

> As the code documents, we need the sleep to wait for the job_set to be created, allowing for `client.get_job_events_stream` to not fail.
> ```py
> client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)
>
> # Needed to allow for the delay in the job_set being created
> time.sleep(2)
> ```

> For any grpc-related errors, more detail can be found by checking the grpc.RpcError object. In this case, we use it to ignore if a queue has already been created or not.
> ```py
> try:
>     client.create_queue(name=queue, priority_factor=1)
>
> # Handle the error we expect to maybe occur
> except grpc.RpcError as e:
>     code = e.code()
>     if code == grpc.StatusCode.ALREADY_EXISTS:
>         print(f"Queue {queue} already exists")
>         client.update_queue(name=queue, priority_factor=1)
>     else:
>         raise e
> ```


### queues.py

[Link to File](./queues.py)

A full example of creating a queue with all options.

#### Walkthrough

> We use `create_queue_request` to create the initial request for the queue.
> ```py
> def create_queue_request(client, queue):
> ```

> Permissions are used to control who can do what with the queue.
> ```py
> subject = Subject(type="Group", name="group1")
> permissions = Permissions(subjects=[subject], verbs=["cancel", "reprioritize"])
> ```

> Resource limits are set similarly to [Kubernetes](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-requests-and-limits-of-pod-and-container)
> ```py
> resource_limits = {"cpu": 1.0, "memory": 1.0}
> ```

We then have two functions:

> `creating_full_queue_example()` which is used to create a single queue using `create_queue()`
> ```py
> def creating_full_queue_example(client, queue):
> ```

> `creating_multiple_queues_example()` which is used to create a multiple queues using `create_queues()`
> ```py
> def creating_multiple_queues_example(client, queue):
> ```
>
> We also use the response of `create_queues()` to detect any errors
> ```py
> resp = client.create_queues([queue_req1, queue_req2])
> ```

### cancelling.py

[Link to File](./cancelling.py)

A full example of cancelling jobs, either with their job id, or
the job-set id.

#### Walkthrough

> To cancel a specific job, its job_id must be read from the `client.submit_jobs` response, and then it can be cancelled.
> ```py
> job_id = resp1.job_response_items[0].job_id
> client.cancel_jobs(job_id=job_id)
> ```

> You can also cancel all jobs on a job_set with its queue and job_set id.
> ```py
> client.cancel_jobs(queue=queue, job_set_id=job_set_id1)
> ```

> If you want to cancel a jobset, but only jobs in certain states, you can use `cancel_jobset()`
> ```py
> client.cancel_jobset(
>     queue=queue, job_set_id=job_set_id, filter=[JobState.PENDING, JobState.RUNNING]
> )
> ```
>
> You can set `ilter_states` to either \[JobState.Queued] or [JobState.PENDING, JobState.RUNNING]

### monitor.py

[Link to File](./monitor.py)

Demonstrates how to run jobs, and also log all changes to that job or job_set in realtime concurrently.

3 Jobs are created, with the first one being cancelled.

#### Walkthrough

> Watches all changes on a job_set. It attempts to connect multiple times instead of sleeping like in `general.py`. It also demonstrates using the clients `unmarshal_event_response()` method to better access the event object.
> ```py
> def watch_job_set(client: ArmadaClient, queue: str, job_set_id):
> ```

> `workflow` is where the queue, job_set and job are created and run from. The other functions are watching the changes created by this workflow. It is very similar to `general.py`.
> ```py
> def workflow(client, queue, job_set_id):
> ```


> We use threading to run the different parts of the code at the same time.
> ```py
> thread = threading.Thread(target=workflow, args=(client, queue, job_set_id))
>
> watch_jobs = threading.Thread(
>     target=watch_job_set, args=(client, queue, job_set_id)
> )
> ```
> You should end up with an output that looks like this
>
> ```
> Queue test-general already exists
> Job 01g9j3wmggqmw2vmmj96cfkzh9 - EventType.submitted
> Job 01g9j3wmggqmw2vmmj96cfkzh9 - EventType.queued
> Job 01g9j3wmggqmw2vmmj96cfkzh9 - EventType.cancelling
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.reprioritizing
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.reprioritized
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.submitted
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.queued
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.reprioritizing
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.updated
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.submitted
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.queued
> Job 01g9j3wmggqmw2vmmj96cfkzh9 - EventType.cancelled
> Job 01g9j3wmggqmw2vmmj96cfkzh9 Terminated
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.updated
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.reprioritized
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.pending
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.leased
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.pending
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.leased
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.running
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.running
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 - EventType.succeeded
> Job 01g9j3wmggqmw2vmmj9bfdnrm8 Terminated
> Job 01g9j3wmggqmw2vmmj98f3bs57 - EventType.succeeded
> Job 01g9j3wmggqmw2vmmj98f3bs57 Terminated
> 3 jobs were terminated
> ```

## Using Basic Auth

The following code shows an example of how basic authentication can be setup

```py

# The python GRPC library requires authentication
#  data to be provided as an AuthMetadataPlugin.
# The username/password are colon-delimted and base64 encoded as per RFC 2617

class GrpcBasicAuth(grpc.AuthMetadataPlugin):
    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password
        super().__init__()

    def __call__(self, context, callback):
        b64encoded_auth = base64.b64encode(
            bytes(f"{self._username}:{self._password}", "utf-8")
        ).decode("ascii")
        callback((("authorization", f"basic {b64encoded_auth}"),), None)

class BasicAuthTest:
    def __init__(self, host, port, username, password, disable_ssl=False):
        if disable_ssl:
            channel_credentials = grpc.local_channel_credentials()
        else:
            channel_credentials = grpc.ssl_channel_credentials()
        channel = grpc.secure_channel(
            f"{host}:{port}",
            grpc.composite_channel_credentials(
                channel_credentials,
                grpc.metadata_call_credentials(GrpcBasicAuth(username, password)),
            ),
        )
        self.client = ArmadaClient(channel)
```

# Using OIDC Client Credentials

```py
# This is an example of how you can use OIDC and GRPC with a ClientCredential flow.

class GrpcAuth(grpc.AuthMetadataPlugin):
    def __init__(self, key):
        self._key = key

    def __call__(self, context, callback):
        callback((("authorization", self._key),), None)


def get_jwt():
    client_id = os.environ.get("OIDC_CLIENT_ID")
    client_secret = os.environ.get("OIDC_CLIENT_SECRET")
    oidc_provider = os.environ.get("OIDC_PROVIDER_URL")
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }

    response = requests.post(f"{oidc_provider}", data=params)

    if response.status_code != 200:
        raise ValueError("Error accessing the API token via OAuth")
    return response.json()["access_token"]


def get_grpc_channel(jwt):
    channel = grpc.secure_channel(
        "localhost:50051",
        grpc.composite_channel_credentials(
            grpc.local_channel_credentials(),
            grpc.metadata_call_credentials(GrpcAuth("Bearer " + jwt)),
        ),
    )

    return channel


armada_client = ArmadaClient(channel=get_grpc_channel(get_jwt()))
```
