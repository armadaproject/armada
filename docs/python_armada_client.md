---
docname: python_armada_client
images: {}
path: /python-armada-client
title: armada_client package
---

# armada_client package

## armada_client.client module

Armada Python GRPC Client


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



#### create_job_request(queue, job_set_id, job_request_items)
Create a job request.


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **job_request_items** (*List**[**JobSubmitRequestItem**]*) – List of Job Request Items



* **Returns**

    A job request object. See the api definition.



#### create_job_request_item(pod_spec, priority=1, \*\*job_item_params)
Create a job request.


* **Parameters**

    
    * **priority** (*int*) – The priority of the job


    * **pod_spec** (*PodSpec*) – The k8s pod spec of the job


    * **job_item_params** – All other job_item kwaarg
    arguments as specified in the api definition.



* **Returns**

    A job item request object. See the api definition.



#### create_queue(name, \*\*queue_params)
Create the queue by name.

Uses the CreateQueue RPC to create a queue.


* **Parameters**

    
    * **name** (*str*) – The name of the queue


    * **queue_params** – Queue Object



* **Returns**

    A queue object per the Armada api definition.



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



#### get_queue(name)
Get the queue by name.

Uses the GetQueue RPC to get the queue.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue object. See the api definition.



#### get_queue_info(name)
Get the queue info by name.

Uses the GetQueueInfo RPC to get queue info.


* **Parameters**

    **name** (*str*) – The name of the queue



* **Returns**

    A queue info object.  See the api definition.



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



#### _static_ unwatch_events(event_stream)
Closes gRPC event streams

Closes the provided event_stream.queue


* **Parameters**

    **event_stream** – a gRPC event stream



* **Returns**

    nothing



* **Return type**

    None



#### update_queue(name, \*\*queue_params)
Update the queue of name with values in queue_params

Uses UpdateQueue RPC to update the parameters on the queue.


* **Parameters**

    
    * **name** (*str*) – The name of the queue


    * **queue_params** – Queue Object



* **Returns**

    None



* **Return type**

    None
