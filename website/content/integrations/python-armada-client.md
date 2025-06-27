---
title: Python Armada Client
---

# armada_client package

## armada_client.client module

Armada Python GRPC Client

For the api definitions:
[https://armadaproject.io/api](https://armadaproject.io/api)

### _class_ armada_client.client.ArmadaClient(channel, event_timeout=datetime.timedelta(seconds=900))

Client for accessing Armada over gRPC.

- **Parameters**
  - **channel** – gRPC channel used for authentication. See
    [https://grpc.github.io/grpc/python/grpc.html](https://grpc.github.io/grpc/python/grpc.html)
    for more information.

  - **event_timeout** (_datetime.timedelta_) –

- **Returns**

  an Armada client instance

#### cancel_jobs(queue, job_set_id, job_id=None)

Cancel jobs in a given queue.

Uses the CancelJobs RPC to cancel jobs.

- **Parameters**
  - **queue** (_str_) – The name of the queue

  - **job_set_id** (_str_) – The name of the job set id

  - **job_id** (_str** | **None_) – The name of the job id (optional), if empty - cancel all jobs

- **Returns**

  A CancellationResult object.

- **Return type**

  armada.submit_pb2.CancellationResult

#### cancel_jobset(queue, job_set_id, filter_states)

Cancel jobs in a given queue.

Uses the CancelJobSet RPC to cancel jobs.
A filter is used to only cancel jobs in certain states.

- **Parameters**
  - **queue** (_str_) – The name of the queue

  - **job_set_id** (_str_) – An array of JobSubmitRequestItems.

  - **filter_states** (_List**[**armada_client.typings.JobState\*\*]_) – A list of states to filter by.

- **Returns**

  An empty response.

- **Return type**

  google.protobuf.empty_pb2.Empty

#### create_job_request_item(priority=1.0, pod_spec=None, pod_specs=None, namespace=None, client_id=None, labels=None, annotations=None, required_node_labels=None, ingress=None, services=None)

Create a job request.

- **Parameters**
  - **priority** (_float_) – The priority of the job

  - **pod_spec** (_Optional**[**armada_client.k8s.io.api.core.v1.generated_pb2.PodSpec\*\*]_) – The k8s pod spec of the job

  - **pod_specs** (_Optional**[**List**[**armada_client.k8s.io.api.core.v1.generated_pb2.PodSpec**]**]_) – List of k8s pod specs of the job

  - **namespace** (_str** | **None_) – The namespace of the job

  - **client_id** (_str** | **None_) – The client id of the job

  - **labels** (_Dict**[**str**, **str**] **| \*\*None_) – The labels of the job

  - **annotations** (_Dict**[**str**, **str**] **| \*\*None_) – The annotations of the job

  - **required_node_labels** (_Dict**[**str**, **str**] **| \*\*None_) – The required node labels of the job

  - **ingress** (_List**[**armada.submit_pb2.IngressConfig**] **| \*\*None_) – The ingress of the job

  - **services** (_List**[**armada.submit_pb2.ServiceConfig**] **| \*\*None_) – The services of the job

- **Returns**

  A job item request object. See the api definition.

- **Return type**

  armada.submit_pb2.JobSubmitRequestItem

#### create_queue(queue)

Uses the CreateQueue RPC to create a queue.

- **Parameters**

  **queue** (_armada.submit_pb2.Queue_) – A queue to create.

- **Return type**

  google.protobuf.empty_pb2.Empty

#### create_queue_request(name, priority_factor, user_owners=None, group_owners=None, resource_limits=None, permissions=None)

Create a queue request object.

- **Parameters**
  - **name** (_str_) – The name of the queue

  - **priority_factor** (_float** | **None_) – The priority factor for the queue

  - **user_owners** (_List**[**str**] **| \*\*None_) – The user owners for the queue

  - **group_owners** (_List**[**str**] **| \*\*None_) – The group owners for the queue

  - **resource_limits** (_Dict**[**str**, **float**] **| \*\*None_) – The resource limits for the queue

  - **permissions** (_List**[**armada_client.permissions.Permissions**] **| \*\*None_) – The permissions for the queue

- **Returns**

  A queue request object.

- **Return type**

  armada.submit_pb2.Queue

#### create_queues(queues)

Uses the CreateQueues RPC to create a list of queues.

- **Parameters**

  **queues** (_List**[**armada.submit_pb2.Queue\*\*]_) – A list of queues to create.

- **Return type**

  armada.submit_pb2.BatchQueueCreateResponse

#### delete_queue(name)

Delete an empty queue by name.

Uses the DeleteQueue RPC to delete the queue.

- **Parameters**

  **name** (_str_) – The name of an empty queue

- **Returns**

  None

- **Return type**

  None

#### event_health()

Health check for Event Service.
:return: A HealthCheckResponse object.

- **Return type**

  armada.health_pb2.HealthCheckResponse

#### get_job_details(job_ids)

Retrieves the details of a job from Armada.

- **Parameters**

  **job_ids** (_List**[**str\*\*]_) – A list of unique job identifiers.

- **Returns**

  The Armada job details response.

- **Return type**

  armada.job_pb2.JobDetailsResponse

#### get_job_errors(job_ids)

Retrieves termination reason from query api.

- **Parameters**
  - **queue** – The name of the queue

  - **job_set_id** – The name of the job set (a grouping of jobs)

  - **external_job_uri** – externalJobUri annotation value

  - **job_ids** (_List**[**str\*\*]_) –

- **Returns**

  The response from the server containing the job errors.

- **Return type**

  JobErrorsResponse

#### get_job_events_stream(queue, job_set_id, from_message_id=None)

Get event stream for a job set.

Uses the GetJobSetEvents rpc to get a stream of events relating
to the provided job_set_id.

Usage:

```python
events = client.get_job_events_stream(...)
for event in events:
    event = client.unmarshal_event_response(event)
    print(event)
```

- **Parameters**
  - **queue** (_str_) – The name of the queue

  - **job_set_id** (_str_) – The name of the job set (a grouping of jobs)

  - **from_message_id** (_str** | **None_) – The from message id.

- **Returns**

  A job events stream for the job_set_id provided.

- **Return type**

  _Iterator_[armada.event_pb2.EventStreamMessage]

#### get_job_run_details(run_ids)

Retrieves the details of a job run from Armada.

- **Parameters**

  **run_ids** (_List**[**str\*\*]_) – A list of unique job run identifiers.

- **Returns**

  The Armada run details response.

- **Return type**

  armada.job_pb2.JobRunDetailsResponse

#### get_job_status(job_ids)

Retrieves the status of a list of jobs from Armada.

- **Parameters**

  **job_ids** (_List**[**str\*\*]_) – A list of unique job identifiers.

- **Returns**

  The response from the server containing the job status.

- **Return type**

  JobStatusResponse

#### get_job_status_by_external_job_uri(queue, job_set_id, external_job_uri)

Retrieves the status of a job based on externalJobUri annotation.

- **Parameters**
  - **queue** (_str_) – The name of the queue

  - **job_set_id** (_str_) – The name of the job set (a grouping of jobs)

  - **external_job_uri** (_str_) – externalJobUri annotation value

- **Returns**

  The response from the server containing the job status.

- **Return type**

  JobStatusResponse

#### get_queue(name)

Get the queue by name.

Uses the GetQueue RPC to get the queue.

- **Parameters**

  **name** (_str_) – The name of the queue

- **Returns**

  A queue object. See the api definition.

- **Return type**

  armada.submit_pb2.Queue

#### preempt_jobs(queue, job_set_id, job_id)

Preempt jobs in a given queue.

Uses the PreemptJobs RPC to preempt jobs.

- **Parameters**
  - **queue** (_str_) – The name of the queue

  - **job_set_id** (_str_) – The name of the job set id

  - **job_id** (_str_) – The id the job

- **Returns**

  An empty response.

- **Return type**

  google.protobuf.empty_pb2.Empty

#### reprioritize_jobs(new_priority, job_ids, job_set_id, queue)

Reprioritize jobs with new_priority value.

Uses ReprioritizeJobs RPC to set a new priority on a list of jobs
or job set (if job_ids are set to None or empty).

- **Parameters**
  - **new_priority** (_float_) – The new priority value for the jobs

  - **job_ids** (_List**[**str**] **| \*\*None_) – A list of job ids to change priority of

  - **job_set_id** (_str_) – A job set id including jobs to change priority of

  - **queue** (_str_) – The queue the jobs are in

- **Returns**

  JobReprioritizeResponse object. It is a map of strings.

- **Return type**

  armada.submit_pb2.JobReprioritizeResponse

#### submit_health()

Health check for Submit Service.
:return: A HealthCheckResponse object.

- **Return type**

  armada.health_pb2.HealthCheckResponse

#### submit_jobs(queue, job_set_id, job_request_items)

Submit an armada job.

Uses SubmitJobs RPC to submit a job.

- **Parameters**
  - **queue** (_str_) – The name of the queue

  - **job_set_id** (_str_) – The name of the job set (a grouping of jobs)

  - **job_request_items** – List[JobSubmitRequestItem]
    An array of JobSubmitRequestItems.

- **Returns**

  A JobSubmitResponse object.

- **Return type**

  armada.submit_pb2.JobSubmitResponse

#### _static_ unmarshal_event_response(event)

Unmarshal an event response from the gRPC server.

- **Parameters**

  **event** (_armada.event_pb2.EventStreamMessage_) – The event response from the gRPC server.

- **Returns**

  An Event object.

- **Return type**

  armada_client.event.Event

#### _static_ unwatch_events(event_stream)

Closes gRPC event streams

Closes the provided event_stream.queue

- **Parameters**

  **event_stream** – a gRPC event stream

- **Returns**

  nothing

- **Return type**

  None

#### update_queue(queue)

Uses the UpdateQueue RPC to update a queue.

- **Parameters**

  **queue** (_armada.submit_pb2.Queue_) – A queue to update.

- **Return type**

  google.protobuf.empty_pb2.Empty

#### update_queues(queues)

Uses the UpdateQueues RPC to update a list of queues.

- **Parameters**

  **queues** (_List**[**armada.submit_pb2.Queue\*\*]_) – A list of queues to update.

- **Return type**

  armada.submit_pb2.BatchQueueUpdateResponse

## armada_client.event module

### _class_ armada_client.event.Event(event)

Represents a gRPC proto event

Definition can be found at:
[https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto#L284](https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto#L284)

- **Parameters**

  **event** (_armada.event_pb2.EventStreamMessage_) – The gRPC proto event

## armada_client.permissions module

### _class_ armada_client.permissions.Permissions(subjects, verbs)

Permissions including Subjects and Verbs

```python
permissions = Permissions(...)
client = ArmadaClient(...)

queue = client.create_queue(
    permissions=[permissions],
)
```

- **Parameters**
  - **subjects** (_List**[**armada_client.permissions.Subject\*\*]_) –

  - **verbs** (_List**[**str\*\*]_) –

#### to_grpc()

Convert to grpc object

- **Return type**

  armada.submit_pb2.Permissions

### _namedtuple_ armada_client.permissions.Subject(kind, name)

Subject is a NamedTuple that represents a subject in the permission system.

- **Fields**
  1.   **kind** (`str`) – Alias for field number 0

  2.   **name** (`str`) – Alias for field number 1

- **Parameters**
  - **kind** (_str_) –

  - **name** (_str_) –

#### to_grpc()

Convert this Subject to a grpc Subject.

- **Return type**

  armada.submit_pb2.Subject

## armada_client.log_client module

### _class_ armada_client.log_client.JobLogClient(url, job_id, disable_ssl=False)

Client for retrieving logs for a given job.

- **Parameters**
  - **url** (_str_) – The url to use for retreiving logs.

  - **job_id** (_str_) – The ID of the job.

  - **disable_ssl** (_bool_) –

- **Returns**

  A JobLogClient instance.

#### logs(since_time='')

Retrieve logs for the job associated with this client.

- **Parameters**

  **since_time** (_str** | **None_) – Logs will be retrieved starting at the time
  specified in this str. Must conform to RFC3339 date time format.

- **Returns**

  A list of LogLine objects.

### _class_ armada_client.log_client.LogLine(line, timestamp)

Represents a single line from a log.

- **Parameters**
  - **line** (_str_) –

  - **timestamp** (_str_) –
