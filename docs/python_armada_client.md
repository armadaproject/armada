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
Bases: `object`

Client for accessing Armada over gRPC.


* **Parameters**

    
    * **channel** – gRPC channel used for authentication. See
    [https://grpc.github.io/grpc/python/grpc.html](https://grpc.github.io/grpc/python/grpc.html)
    for more information.


    * **max_workers** (*Optional**[**int**]*) – number of cores for thread pools, if unset, defaults
    to number of CPUs



* **Returns**

    an Armada client instance



#### cancel_jobs(queue=None, job_id=None, job_set_id=None)
Cancel jobs in a given queue.

Uses the CancelJobs RPC to cancel jobs. Either job_id or
job_set_id is required.


* **Parameters**

    
    * **queue** (*Optional**[**str**]*) – The name of the queue


    * **job_id** (*Optional**[**str**]*) – The name of the job id (this or job_set_id required)


    * **job_set_id** (*Optional**[**str**]*) – An array of JobSubmitRequestItems. (this or job_id required)



* **Returns**

    A JobSubmitResponse object.



* **Return type**

    *JobCancelRequest*



#### create_job_request_item(pod_spec, priority=1.0, pod_specs=None, namespace=None, client_id=None, labels=None, annotations=None, required_node_labels=None, ingress=None, services=None)
Create a job request.


* **Parameters**

    
    * **priority** (*float*) – The priority of the job


    * **pod_spec** (*PodSpec*) – The k8s pod spec of the job


    * **pod_specs** (*Optional**[**List**[**PodSpec**]**]*) – List of k8s pod specs of the job


    * **namespace** (*Optional**[**str**]*) – The namespace of the job


    * **client_id** (*Optional**[**str**]*) – The client id of the job


    * **labels** (*Optional**[**Dict**[**str**, **str**]**]*) – The labels of the job


    * **annotations** (*Optional**[**Dict**[**str**, **str**]**]*) – The annotations of the job


    * **required_node_labels** (*Optional**[**Dict**[**str**, **str**]**]*) – The required node labels of the job


    * **ingress** (*Optional**[**List**[**IngressConfig**]**]*) – The ingress of the job


    * **services** (*Optional**[**List**[**ServiceConfig**]**]*) – The services of the job



* **Returns**

    A job item request object. See the api definition.



* **Return type**

    *JobSubmitRequestItem*



#### create_queue(name, priority_factor, user_owners=None, group_owners=None, resource_limits=None, permissions=None)
Create the queue by name.

Uses the CreateQueue RPC to create a queue.


* **Parameters**

    
    * **name** (*str*) – The name of the queue


    * **priority_factor** (*Optional**[**float**]*) – The priority factor for the queue


    * **user_owners** (*Optional**[**List**[**str**]**]*) – The user owners for the queue


    * **group_owners** (*Optional**[**List**[**str**]**]*) – The group owners for the queue


    * **resource_limits** (*Optional**[**Dict**[**str**, **float**]**]*) – The resource limits for the queue


    * **permissions** (*Optional**[**List**[**Permissions**]**]*) – The permissions for the queue



* **Returns**

    A queue object per the Armada api definition.



* **Return type**

    *Empty*



#### delete_queue(name)
Delete an empty queue by name.

Uses the DeleteQueue RPC to delete the queue.


* **Parameters**

    **name** (*str*) – The name of an empty queue



* **Returns**

    None



* **Return type**

    None



#### get_job_events_stream(queue, job_set_id, from_message_id=None)
Get event stream for a job set.

Uses the GetJobSetEvents rpc to get a stream of events relating
to the provided job_set_id.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **from_message_id** (*Optional**[**str**]*) – The from message id.



* **Returns**

    A job events stream for the job_set_id provided.



* **Return type**

    *Generator*[*EventMessage*, None, None]



#### get_queue(name)
Get the queue by name.

Uses the GetQueue RPC to get the queue.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue object. See the api definition.



* **Return type**

    *Queue*



#### get_queue_info(name)
Get the queue info by name.

Uses the GetQueueInfo RPC to get queue info.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue info object.  See the api definition.



* **Return type**

    *QueueInfo*



#### reprioritize_jobs(new_priority, job_ids=None, job_set_id=None, queue=None)
Reprioritize jobs with new_priority value.

Uses ReprioritizeJobs RPC to set a new priority on a list of jobs
or job set.


* **Parameters**

    
    * **new_priority** (*float*) – The new priority value for the jobs


    * **job_ids** (*Optional**[**List**[**str**]**]*) – A list of job ids to change priority of


    * **job_set_id** (*Optional**[**str**]*) – A job set id including jobs to change priority of


    * **queue** (*Optional**[**str**]*) – The queue the jobs are in



* **Returns**

    ReprioritizeJobsResponse object. It is a map of strings.



* **Return type**

    *JobReprioritizeResponse*



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

    *JobSubmitResponse*



#### _static_ unmarshal_event_response(event)
Unmarshal an event response from the gRPC server.


* **Parameters**

    **event** (*EventStreamMessage*) – The event response from the gRPC server.



* **Returns**

    An Event object.



* **Return type**

    *Event*



#### _static_ unwatch_events(event_stream)
Closes gRPC event streams

Closes the provided event_stream.queue


* **Parameters**

    **event_stream** – a gRPC event stream



* **Returns**

    nothing



* **Return type**

    None



#### update_queue(name, priority_factor, user_owners=None, group_owners=None, resource_limits=None, permissions=None)
Update the queue of name with values in queue_params

Uses UpdateQueue RPC to update the parameters on the queue.


* **Parameters**

    
    * **name** (*str*) – The name of the queue


    * **priority_factor** (*Optional**[**float**]*) – The priority factor for the queue


    * **user_owners** (*Optional**[**List**[**str**]**]*) – The user owners for the queue


    * **group_owners** (*Optional**[**List**[**str**]**]*) – The group owners for the queue


    * **resource_limits** (*Optional**[**Dict**[**str**, **float**]**]*) – The resource limits for the queue


    * **permissions** (*Optional**[**List**[**Permissions**]**]*) – The permissions for the queue



* **Returns**

    None



* **Return type**

    None


## armada_client.event module


### _class_ armada_client.event.Event(event)
Represents a gRPC proto event

Definition can be found at:
[https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284](https://github.com/G-Research/armada/blob/master/pkg/api/event.proto#L284)


* **Parameters**

    **event** (*EventStreamMessage*) – The gRPC proto event


## armada_client.permissions module


### _class_ armada_client.permissions.Permissions(subjects, verbs)
Permissions including Subjects and Verbs


* **Parameters**

    
    * **subjects** (*List**[**Subject**]*) – 


    * **verbs** (*List**[**str**]*) – 



#### to_grpc()
Convert to grpc object


* **Return type**

    *Permissions*



### _class_ armada_client.permissions.Subject(kind, name)
Subject is a NamedTuple that represents a subject in the permission system.


* **Parameters**

    
    * **kind** (*str*) – 


    * **name** (*str*) – 



#### kind(_: st_ )
Alias for field number 0


#### name(_: st_ )
Alias for field number 1


#### to_grpc()
Convert this Subject to a grpc Subject.


* **Return type**

    *Subject*
