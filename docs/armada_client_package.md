# Armada client package
- [Armada client package](#armada-client-package)
  - [armada\_client.client module](#armada_clientclient-module)
    - [_class_ armada\_client.client.ArmadaClient(channel, event\_timeout=datetime.timedelta(seconds=900))](#class-armada_clientclientarmadaclientchannel-event_timeoutdatetimetimedeltaseconds900)
      - [cancel\_jobs(queue, job\_set\_id, job\_id=None)](#cancel_jobsqueue-job_set_id-job_idnone)
      - [cancel\_jobset(queue, job\_set\_id, filter\_states)](#cancel_jobsetqueue-job_set_id-filter_states)
      - [create\_job\_request\_item(priority=1.0, pod\_spec=None, pod\_specs=None, namespace=None, client\_id=None, labels=None, annotations=None, required\_node\_labels=None, ingress=None, services=None)](#create_job_request_itempriority10-pod_specnone-pod_specsnone-namespacenone-client_idnone-labelsnone-annotationsnone-required_node_labelsnone-ingressnone-servicesnone)
      - [create\_queue(queue)](#create_queuequeue)
      - [create\_queue\_request(name, priority\_factor, user\_owners=None, group\_owners=None, resource\_limits=None, permissions=None)](#create_queue_requestname-priority_factor-user_ownersnone-group_ownersnone-resource_limitsnone-permissionsnone)
      - [create\_queues(queues)](#create_queuesqueues)
      - [delete\_queue(name)](#delete_queuename)
      - [event\_health()](#event_health)
      - [get\_job\_details(job\_ids)](#get_job_detailsjob_ids)
      - [get\_job\_errors(job\_ids)](#get_job_errorsjob_ids)
      - [get\_job\_events\_stream(queue, job\_set\_id, from\_message\_id=None)](#get_job_events_streamqueue-job_set_id-from_message_idnone)
      - [get\_job\_run\_details(run\_ids)](#get_job_run_detailsrun_ids)
      - [get\_job\_status(job\_ids)](#get_job_statusjob_ids)
      - [get\_job\_status\_by\_external\_job\_uri(queue, job\_set\_id, external\_job\_uri)](#get_job_status_by_external_job_uriqueue-job_set_id-external_job_uri)
      - [get\_queue(name)](#get_queuename)
      - [get\_queues()](#get_queues)
      - [preempt\_jobs(queue, job\_set\_id, job\_id)](#preempt_jobsqueue-job_set_id-job_id)
      - [reprioritize\_jobs(new\_priority, job\_ids, job\_set\_id, queue)](#reprioritize_jobsnew_priority-job_ids-job_set_id-queue)
      - [submit\_health()](#submit_health)
      - [submit\_jobs(queue, job\_set\_id, job\_request\_items)](#submit_jobsqueue-job_set_id-job_request_items)
      - [_static_ unmarshal\_event\_response(event)](#static-unmarshal_event_responseevent)
      - [_static_ unwatch\_events(event\_stream)](#static-unwatch_eventsevent_stream)
      - [update\_queue(queue)](#update_queuequeue)
      - [update\_queues(queues)](#update_queuesqueues)
  - [armada\_client.event module](#armada_clientevent-module)
    - [_class_ armada\_client.event.Event(event)](#class-armada_clienteventeventevent)
  - [armada\_client.permissions module](#armada_clientpermissions-module)
    - [_class_ armada\_client.permissions.Permissions(subjects, verbs)](#class-armada_clientpermissionspermissionssubjects-verbs)
      - [to\_grpc()](#to_grpc)
    - [_namedtuple_ armada\_client.permissions.Subject(kind, name)](#namedtuple-armada_clientpermissionssubjectkind-name)
      - [to\_grpc()](#to_grpc-1)
  - [armada\_client.log\_client module](#armada_clientlog_client-module)
    - [_class_ armada\_client.log\_client.JobLogClient(url, job\_id, disable\_ssl=False)](#class-armada_clientlog_clientjoblogclienturl-job_id-disable_sslfalse)
      - [logs(since\_time='')](#logssince_time)
    - [_class_ armada\_client.log\_client.LogLine(line, timestamp)](#class-armada_clientlog_clientloglineline-timestamp)

## armada_client.client module

Armada Python GRPC Client

For the API definitions:
[https://armadaproject.io/armada_api](https://armadaproject.io/armada_api)


### _class_ armada_client.client.ArmadaClient(channel, event_timeout=datetime.timedelta(seconds=900))
Client for accessing Armada over gRPC.


* **Parameters**

    
    * **channel** – gRPC channel used for authentication. [See the gRPC documentation](https://grpc.github.io/grpc/python/grpc.html).


    * **event_timeout** (*datetime.timedelta*) – 



* **Returns**

    an Armada client instance



#### cancel_jobs(queue, job_set_id, job_id=None)
Cancel jobs in a given queue.

Uses the CancelJobs RPC to cancel jobs.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set id


    * **job_id** (*str** | **None*) – The name of the job id (optional), if empty - cancel all jobs



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



#### get_job_details(job_ids)
Retrieves the details of a job from Armada.


* **Parameters**

    **job_ids** (*List**[**str**]*) – A list of unique job identifiers.



* **Returns**

    The Armada job details response.



* **Return type**

    armada.job_pb2.JobDetailsResponse



#### get_job_errors(job_ids)
Retrieves termination reason from query api.


* **Parameters**

    
    * **queue** – The name of the queue


    * **job_set_id** – The name of the job set (a grouping of jobs)


    * **external_job_uri** – externalJobUri annotation value


    * **job_ids** (*List**[**str**]*) – 



* **Returns**

    The response from the server containing the job errors.



* **Return type**

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


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **from_message_id** (*str** | **None*) – The from message id.



* **Returns**

    A job events stream for the job_set_id provided.



* **Return type**

    *Iterator*[armada.event_pb2.EventStreamMessage]



#### get_job_run_details(run_ids)
Retrieves the details of a job run from Armada.


* **Parameters**

    **run_ids** (*List**[**str**]*) – A list of unique job run identifiers.



* **Returns**

    The Armada run details response.



* **Return type**

    armada.job_pb2.JobRunDetailsResponse



#### get_job_status(job_ids)
Retrieves the status of a list of jobs from Armada.


* **Parameters**

    **job_ids** (*List**[**str**]*) – A list of unique job identifiers.



* **Returns**

    The response from the server containing the job status.



* **Return type**

    JobStatusResponse



#### get_job_status_by_external_job_uri(queue, job_set_id, external_job_uri)
Retrieves the status of a job based on externalJobUri annotation.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **external_job_uri** (*str*) – externalJobUri annotation value



* **Returns**

    The response from the server containing the job status.



* **Return type**

    JobStatusResponse



#### get_queue(name)
Get the queue by name.

Uses the GetQueue RPC to get the queue.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue object. See the api definition.



* **Return type**

    armada.submit_pb2.Queue



#### get_queues()
Get all queues.

Uses the GetQueues RPC to get the queues.


* **Returns**

    list containing all queues



* **Return type**

    *List*[armada.submit_pb2.Queue]



#### preempt_jobs(queue, job_set_id, job_id)
Preempt jobs in a given queue.

Uses the PreemptJobs RPC to preempt jobs.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set id


    * **job_id** (*str*) – The id the job



* **Returns**

    An empty response.



* **Return type**

    google.protobuf.empty_pb2.Empty



#### reprioritize_jobs(new_priority, job_ids, job_set_id, queue)
Reprioritize jobs with new_priority value.

Uses ReprioritizeJobs RPC to set a new priority on a list of jobs
or job set (if job_ids are set to None or empty).


* **Parameters**

    
    * **new_priority** (*float*) – The new priority value for the jobs


    * **job_ids** (*List**[**str**] **| **None*) – A list of job ids to change priority of


    * **job_set_id** (*str*) – A job set id including jobs to change priority of


    * **queue** (*str*) – The queue the jobs are in



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
Submit an armada job.

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


## armada_client.log_client module


### _class_ armada_client.log_client.JobLogClient(url, job_id, disable_ssl=False)
Client for retrieving logs for a given job.


* **Parameters**

    
    * **url** (*str*) – The url to use for retreiving logs.


    * **job_id** (*str*) – The ID of the job.


    * **disable_ssl** (*bool*) – 



* **Returns**

    A JobLogClient instance.



#### logs(since_time='')
Retrieve logs for the job associated with this client.


* **Parameters**

    **since_time** (*str** | **None*) – Logs will be retrieved starting at the time
    specified in this str. Must conform to RFC3339 date time format.



* **Returns**

    A list of LogLine objects.



### _class_ armada_client.log_client.LogLine(line, timestamp)
Represents a single line from a log.


* **Parameters**

    
    * **line** (*str*) – 


    * **timestamp** (*str*) –
