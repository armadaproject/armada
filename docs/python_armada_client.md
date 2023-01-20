---
docname: python_armada_client
images: {}
path: /python-armada-client
title: armada_client package
---

# armada_client package

## armada_client.client module

Armada Python GRPC Client

For the api definitions:
[https://armadaproject.io/api](https://armadaproject.io/api)


### _class_ armada_client.client.ArmadaClient(channel, max_workers=None)
Client for accessing Armada over gRPC.


* **Parameters**

    
    * **channel** – gRPC channel used for authentication. See
    [https://grpc.github.io/grpc/python/grpc.html](https://grpc.github.io/grpc/python/grpc.html)
    for more information.


    * **max_workers** (*int** | **None*) – number of cores for thread pools, if unset, defaults
    to number of CPUs



* **Returns**

    an Armada client instance



#### cancel_jobs(queue=None, job_id=None, job_set_id=None)
Cancel jobs in a given queue.

Uses the CancelJobs RPC to cancel jobs. Either job_id or
job_set_id is required.


* **Parameters**

    
    * **queue** (*str** | **None*) – The name of the queue


    * **job_id** (*str** | **None*) – The name of the job id (this or job_set_id required)


    * **job_set_id** (*str** | **None*) – An array of JobSubmitRequestItems. (this or job_id required)



* **Returns**

    A CancellationResult object.



* **Return type**

    armada.submit_pb2.CancellationResult



#### cancel_jobset(queue, job_set_id, filter_states)
Cancel jobs in a given queue.

Uses the CancelJobSet RPC to cancel jobs.
A filter is used to only cancel jobs in certain states.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – An array of JobSubmitRequestItems.


    * **filter_states** (*List**[**armada_client.typings.JobState**]*) – A list of states to filter by.



* **Returns**

    An empty response.



* **Return type**

    google.protobuf.empty_pb2.Empty



#### create_job_request_item(priority=1.0, pod_spec=None, pod_specs=None, namespace=None, client_id=None, labels=None, annotations=None, required_node_labels=None, ingress=None, services=None)
Create a job request.


* **Parameters**

    
    * **priority** (*float*) – The priority of the job


    * **pod_spec** (*Optional**[**armada_client.k8s.io.api.core.v1.generated_pb2.PodSpec**]*) – The k8s pod spec of the job


    * **pod_specs** (*Optional**[**List**[**armada_client.k8s.io.api.core.v1.generated_pb2.PodSpec**]**]*) – List of k8s pod specs of the job


    * **namespace** (*str** | **None*) – The namespace of the job


    * **client_id** (*str** | **None*) – The client id of the job


    * **labels** (*Dict**[**str**, **str**] **| **None*) – The labels of the job


    * **annotations** (*Dict**[**str**, **str**] **| **None*) – The annotations of the job


    * **required_node_labels** (*Dict**[**str**, **str**] **| **None*) – The required node labels of the job


    * **ingress** (*List**[**armada.submit_pb2.IngressConfig**] **| **None*) – The ingress of the job


    * **services** (*List**[**armada.submit_pb2.ServiceConfig**] **| **None*) – The services of the job



* **Returns**

    A job item request object. See the api definition.



* **Return type**

    armada.submit_pb2.JobSubmitRequestItem



#### create_queue(queue)
Uses the CreateQueue RPC to create a queue.


* **Parameters**

    **queue** (*armada.submit_pb2.Queue*) – A queue to create.



* **Return type**

    google.protobuf.empty_pb2.Empty



#### create_queue_request(name, priority_factor, user_owners=None, group_owners=None, resource_limits=None, permissions=None)
Create a queue request object.


* **Parameters**

    
    * **name** (*str*) – The name of the queue


    * **priority_factor** (*float** | **None*) – The priority factor for the queue


    * **user_owners** (*List**[**str**] **| **None*) – The user owners for the queue


    * **group_owners** (*List**[**str**] **| **None*) – The group owners for the queue


    * **resource_limits** (*Dict**[**str**, **float**] **| **None*) – The resource limits for the queue


    * **permissions** (*List**[**armada_client.permissions.Permissions**] **| **None*) – The permissions for the queue



* **Returns**

    A queue request object.



* **Return type**

    armada.submit_pb2.Queue



#### create_queues(queues)
Uses the CreateQueues RPC to create a list of queues.


* **Parameters**

    **queues** (*List**[**armada.submit_pb2.Queue**]*) – A list of queues to create.



* **Return type**

    armada.submit_pb2.BatchQueueCreateResponse



#### delete_queue(name)
Delete an empty queue by name.

Uses the DeleteQueue RPC to delete the queue.


* **Parameters**

    **name** (*str*) – The name of an empty queue



* **Returns**

    None



* **Return type**

    None



#### event_health()
Health check for Event Service.
:return: A HealthCheckResponse object.


* **Return type**

    armada.health_pb2.HealthCheckResponse



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


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **from_message_id** (*str** | **None*) – The from message id.



* **Returns**

    A job events stream for the job_set_id provided.



* **Return type**

    *Iterator*[armada.event_pb2.EventStreamMessage]



#### get_queue(name)
Get the queue by name.

Uses the GetQueue RPC to get the queue.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue object. See the api definition.



* **Return type**

    armada.submit_pb2.Queue



#### get_queue_info(name)
Get the queue info by name.

Uses the GetQueueInfo RPC to get queue info.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue info object.  See the api definition.



* **Return type**

    armada.submit_pb2.QueueInfo



#### reprioritize_jobs(new_priority, job_ids=None, job_set_id=None, queue=None)
Reprioritize jobs with new_priority value.

Uses ReprioritizeJobs RPC to set a new priority on a list of jobs
or job set.


* **Parameters**

    
    * **new_priority** (*float*) – The new priority value for the jobs


    * **job_ids** (*List**[**str**] **| **None*) – A list of job ids to change priority of


    * **job_set_id** (*str** | **None*) – A job set id including jobs to change priority of


    * **queue** (*str** | **None*) – The queue the jobs are in



* **Returns**

    JobReprioritizeResponse object. It is a map of strings.



* **Return type**

    armada.submit_pb2.JobReprioritizeResponse



#### submit_health()
Health check for Submit Service.
:return: A HealthCheckResponse object.


* **Return type**

    armada.health_pb2.HealthCheckResponse



#### submit_jobs(queue, job_set_id, job_request_items)
Submit a armada job.

Uses SubmitJobs RPC to submit a job.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **job_request_items** – List[JobSubmitRequestItem]
    An array of JobSubmitRequestItems.



* **Returns**

    A JobSubmitResponse object.



* **Return type**

    armada.submit_pb2.JobSubmitResponse



#### _static_ unmarshal_event_response(event)
Unmarshal an event response from the gRPC server.


* **Parameters**

    **event** (*armada.event_pb2.EventStreamMessage*) – The event response from the gRPC server.



* **Returns**

    An Event object.



* **Return type**

    armada_client.event.Event



#### _static_ unwatch_events(event_stream)
Closes gRPC event streams

Closes the provided event_stream.queue


* **Parameters**

    **event_stream** – a gRPC event stream



* **Returns**

    nothing



* **Return type**

    None



#### update_queue(queue)
Uses the UpdateQueue RPC to update a queue.


* **Parameters**

    **queue** (*armada.submit_pb2.Queue*) – A queue to update.



* **Return type**

    google.protobuf.empty_pb2.Empty



#### update_queues(queues)
Uses the UpdateQueues RPC to update a list of queues.


* **Parameters**

    **queues** (*List**[**armada.submit_pb2.Queue**]*) – A list of queues to update.



* **Return type**

    armada.submit_pb2.BatchQueueUpdateResponse


## armada_client.event module


### _class_ armada_client.event.Event(event)
Represents a gRPC proto event

Definition can be found at:
[https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto#L284](https://github.com/armadaproject/armada/blob/master/pkg/api/event.proto#L284)


* **Parameters**

    **event** (*armada.event_pb2.EventStreamMessage*) – The gRPC proto event


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


* **Parameters**

    
    * **subjects** (*List**[**armada_client.permissions.Subject**]*) – 


    * **verbs** (*List**[**str**]*) – 



#### to_grpc()
Convert to grpc object


* **Return type**

    armada.submit_pb2.Permissions



### _namedtuple_ armada_client.permissions.Subject(kind, name)
Subject is a NamedTuple that represents a subject in the permission system.


* **Fields**

    
    1.  **kind** (`str`) – Alias for field number 0


    2.  **name** (`str`) – Alias for field number 1



* **Parameters**

    
    * **kind** (*str*) – 


    * **name** (*str*) – 



#### to_grpc()
Convert this Subject to a grpc Subject.


* **Return type**

    armada.submit_pb2.Subject
