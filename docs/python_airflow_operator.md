---
docname: python_airflow_operator
images: {}
path: /python-airflow-operator
title: Armada Airflow Operator
---

# Armada Airflow Operator

This class provides integration with Airflow and Armada

## armada.operators.armada module


### _class_ armada.operators.armada.ArmadaOperator(name, armada_client, job_service_client, armada_queue, job_request_items, lookout_url_template=None, \*\*kwargs)
Bases: `BaseOperator`

Implementation of an ArmadaOperator for airflow.

Airflow operators inherit from BaseOperator.


* **Parameters**

    
    * **name** (*str*) – The name of the airflow task


    * **armada_client** (*ArmadaClient*) – The Armada Python GRPC client
    that is used for interacting with Armada


    * **job_service_client** (*JobServiceClient*) – The JobServiceClient that is used for polling


    * **armada_queue** (*str*) – The queue name for Armada.


    * **job_request_items** – A PodSpec that is used by Armada for submitting a job


    * **lookout_url_template** (*str** | **None*) – A URL template to be used to provide users
    a valid link to the related lookout job in this operator’s log.
    The format should be:
    “[https://lookout.armada.domain/jobs](https://lookout.armada.domain/jobs)?job_id=<job_id>” where <job_id> will
    be replaced with the actual job ID.



* **Returns**

    a job service client instance



#### execute(context)
Executes the Armada Operator.

Runs an Armada job and calls the job_service_client for polling.


* **Parameters**

    **context** – The airflow context.



* **Returns**

    None



* **Return type**

    None



### armada.operators.armada.annotate_job_request_items(context, job_request_items)
Annotates the inbound job request items with Airflow context elements


* **Parameters**

    
    * **context** – The airflow context.


    * **job_request_items** (*List**[**JobSubmitRequestItem**]*) – The job request items to be sent to armada



* **Returns**

    annotated job request items for armada



* **Return type**

    *List*[*JobSubmitRequestItem*]



### armada.operators.armada.get_annotation_key_prefix()
Provides the annotation key perfix,
which can be specified in env var ANNOTATION_KEY_PREFIX.
A default is provided if the env var is not defined


* **Returns**

    string annotation key prefix



* **Return type**

    str


## armada.operators.jobservice module


### _class_ armada.operators.jobservice.JobServiceClient(channel, max_workers=None)
Bases: `object`

The JobService Client

Implementation of gRPC stubs from JobService


* **Parameters**

    
    * **channel** – gRPC channel used for authentication. See
    [https://grpc.github.io/grpc/python/grpc.html](https://grpc.github.io/grpc/python/grpc.html)
    for more information.


    * **max_workers** (*int** | **None*) – number of cores for thread pools, if unset, defaults
    to number of CPUs



* **Returns**

    a job service client instance



#### get_job_status(queue, job_set_id, job_id)
Get job status of a given job in a queue and job_set_id.

Uses the GetJobStatus rpc to get a status of your job


* **Parameters**

    
    * **queue** (*str*) – The name of the queue


    * **job_set_id** (*str*) – The name of the job set (a grouping of jobs)


    * **job_id** (*str*) – The id of the job



* **Returns**

    A Job Service Request (State, Error)



* **Return type**

    *JobServiceResponse*



#### health()
Health Check for GRPC Request


* **Return type**

    *HealthCheckResponse*


## armada.operators.utils module


### _class_ armada.operators.utils.JobState(value)
Bases: `Enum`

An enumeration.


#### CANCELLED(_ = _ )

#### CONNECTION_ERR(_ = _ )

#### DUPLICATE_FOUND(_ = _ )

#### FAILED(_ = _ )

#### JOB_ID_NOT_FOUND(_ = _ )

#### RUNNING(_ = _ )

#### SUBMITTED(_ = _ )

#### SUCCEEDED(_ = _ )

### armada.operators.utils.airflow_error(job_state, name, job_id)
Throw an error on a terminal event if job errored out


* **Parameters**

    
    * **job_state** (*JobState*) – A JobState enum class


    * **name** (*str*) – The name of your armada job


    * **job_id** (*str*) – The job id that armada assigns to it



* **Returns**

    No Return or an AirflowFailException.


AirflowFailException tells Airflow Schedule to not reschedule the task


### armada.operators.utils.default_job_status_callable(armada_queue, job_set_id, job_id, job_service_client)

* **Parameters**

    
    * **armada_queue** (*str*) – 


    * **job_set_id** (*str*) – 


    * **job_id** (*str*) – 


    * **job_service_client** (*JobServiceClient*) – 



* **Return type**

    *JobServiceResponse*



### armada.operators.utils.job_state_from_pb(state)

* **Return type**

    *JobState*



### armada.operators.utils.search_for_job_complete(armada_queue, job_set_id, airflow_task_name, job_id, job_service_client=None, job_status_callable=<function default_job_status_callable>, time_out_for_failure=7200)
Poll JobService cache until you get a terminated event.

A terminated event is SUCCEEDED, FAILED or CANCELLED


* **Parameters**

    
    * **armada_queue** (*str*) – The queue for armada


    * **job_set_id** (*str*) – Your job_set_id


    * **airflow_task_name** (*str*) – The name of your armada job


    * **job_id** (*str*) – The name of the job id that armada assigns to it


    * **job_service_client** (*JobServiceClient** | **None*) – A JobServiceClient that is used for polling.
    It is optional only for testing


    * **job_status_callable** – A callable object for test injection.


    * **time_out_for_failure** (*int*) – The amount of time a job
    can be in job_id_not_found
    before we decide it was a invalid job



* **Returns**

    A tuple of JobStateEnum, message



* **Return type**

    *Tuple*[*JobState*, str]
