---
docname: python_airflow_operator
images: {}
path: /python-airflow-operator
title: Armada Airflow Operator
---

# Armada Airflow Operator

This class provides integration with Airflow and Armada

## armada.operators.armada module


### _class_ armada.operators.armada.ArmadaOperator(name, armada_client, job_service_client, queue, job_set_id, job_request_items, \*\*kwargs)
Bases: `BaseOperator`

Implementation of an ArmadaOperator for airflow.

Airflow operators inherit from BaseOperator.


* **Parameters**

    
    * **name** (*str*) – The name of the airflow task


    * **armada_client** (*ArmadaClient*) – The Armada Python GRPC client
    that is used for interacting with Armada


    * **job_service_client** (*JobServiceClient*) – The JobServiceClient that is used for polling


    * **queue** (*str*) – The queue name


    * **job_set_id** (*str*) – The job_set_id. Should be set at dag level for all jobs


    * **job_request_items** – A PodSpec that is used by Armada for submitting a job



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


## armada.operators.jobservice module


### _class_ armada.operators.jobservice.JobServiceClient(channel, max_workers=None)
Bases: `object`

The JobService Client

Implementation of gRPC stubs from JobService


* **Parameters**

    
    * **channel** – gRPC channel used for authentication. See
    [https://grpc.github.io/grpc/python/grpc.html](https://grpc.github.io/grpc/python/grpc.html)
    for more information.


    * **max_workers** (*Optional**[**int**]*) – number of cores for thread pools, if unset, defaults
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


## armada.operators.utils module


### _class_ armada.operators.utils.JobState(value)
Bases: `Enum`

An enumeration.


#### CANCELLED(_ = _ )

#### FAILED(_ = _ )

#### JOB_ID_NOT_FOUND(_ = _ )

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


### armada.operators.utils.default_job_status_callable(queue, job_set_id, job_id, job_service_client)

* **Parameters**

    
    * **queue** (*str*) – 


    * **job_set_id** (*str*) – 


    * **job_id** (*str*) – 


    * **job_service_client** (*JobServiceClient*) – 



* **Return type**

    *JobServiceResponse*



### armada.operators.utils.search_for_job_complete(queue, job_set_id, airflow_task_name, job_id, job_service_client=None, job_status_callable=<function default_job_status_callable>, time_out_for_failure=7200)
Poll JobService cache until you get a terminated event.

A terminated event is SUCCEEDED, FAILED or CANCELLED


* **Parameters**

    
    * **job_set_id** (*str*) – Your job_set_id


    * **airflow_task_name** (*str*) – The name of your armada job


    * **job_id** (*str*) – The name of the job id that armada assigns to it


    * **job_service_client** (*Optional**[**JobServiceClient**]*) – A JobServiceClient that is used for polling.
    It is optional only for testing


    * **job_status_callable** – A callable object for test injection.


    * **time_out_for_failure** (*int*) – The amount of time a job
    can be in job_id_not_found
    before we decide it was a invalid job


    * **queue** (*str*) – 



* **Returns**

    A tuple of JobStateEnum, message



* **Return type**

    *Tuple*[*JobState*, str]
