# Python Client Examples

Currently there are three example files:

- [simple.py](#simple.py)
- [general.py](#general.py)
- [async_logging.py](#async_logging.py)

There are also some codeblocks with useful code snippets:

- [Using Basic Auth](#using-basic-auth)


## Setup

Please see the [python client docs](https://github.com/G-Research/armada/blob/master/client/python/README.md) for getting the client setup.

### Running the examples

Each example has three enviromental variables for setup

```bash
export ARMADA_SERVER=localhost
export ARMADA_PORT=443
export DISABLE_SSL=true
```

You can then simply run one of the examples

```bash
python3 simple.py
```

## Example Files

### simple.py

Example of using the Armada client to create a queue, jobset and job,
then watch for the job to succeed or fail.

#### Walkthrough

```py
def create_dummy_job(client: ArmadaClient):
```
Armada uses Kubernetes to control jobs, so we need to define a podspec that are job will use.

```py
if DISABLE_SSL:
    channel = grpc.insecure_channel(f"{HOST}:{PORT}")
else:
    channel_credentials = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel(
        f"{HOST}:{PORT}",
        channel_credentials,
    )
```

If you are using a remote server, secure_channel should be used to ensure that all infomation is encrypted.


```py
client.create_queue(name=queue, priority_factor=1)
```

All queues need a priority factor to be created, which is used to determine the order in which jobs are run.

### general.py

A more fledged out version of `simple.py` where we create a queue only if it doesn't exist, and then create a jobset and job, and wait until the job succeeds or fails.

#### Walkthrough

```py
def wait_for_job_event(client, event_stream, job_id: str, event_state: EventType)
```

We use this function to check the status of a job, return false if the job failed and otherwise wait for the event state we are interested in.

```py
job_id = resp.job_response_items[0].job_id
```

We can use this code to get the job_id we are interested in from the event_stream.


```py
client.reprioritize_jobs(new_priority=2, queue=queue, job_set_id=job_set_id)

# Needed to allow for the delay in the job_set being created
time.sleep(2)
```

As the code documents, we need the sleep to wait for the job_set to be created, allowing for `client.get_job_events_stream` to not fail.

```py
try:
    client.create_queue(name=queue, priority_factor=1)

# Handle the error we expect to maybe occur
except grpc.RpcError as e:
    code = e.code()
    if code == grpc.StatusCode.ALREADY_EXISTS:
        print(f"Queue {queue} already exists")
        client.update_queue(name=queue, priority_factor=1)
    else:
        raise e
```

For any grpc-related errors, more detail can be found by checking the grpc.RpcError object. In this case, we use it to ignore if a queue has already been created or not.

### async_logging.py

Demonstrates how to run jobs, and also log all changes to that job or job_set in realtime concurrently.

#### Walkthrough

```py
def watch_job_set(client: ArmadaClient, queue: str, job_set_id):
```

Watches all changes on a job_set. It attempts to connect multiple times instead of sleeping like in `general.py`. It also demonstrates using the clients `unmarshal_event_response()` method to better access the event object.


```py
def workflow(client, queue, job_set_id):
```

`workflow` is where the queue, job_set and job are created and run from. The other functions are watching the changes created by this workflow. It is very similar to `general.py`.

```py
thread = threading.Thread(target=workflow, args=(client, queue, job_set_id))

watch_jobs = threading.Thread(
    target=watch_job_set, args=(client, queue, job_set_id)
)

watch_queues = threading.Thread(target=watch_queue, args=(client, queue))
```

We use threading to run the different parts of the code at the same time.

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