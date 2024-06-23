---
docname: python_airflow_operator
images: {}
path: /python-airflow-operator
title: Armada Airflow Operator
---

# Armada Airflow Operator

This class provides integration with Airflow and Armada

## armada.operators.armada module


### _class_ armada.operators.armada.ArmadaOperator(name, channel_args, armada_queue, job_request, job_set_prefix='', lookout_url_template=None, poll_interval=30, container_logs=None, k8s_token_retriever=None, deferrable=False, job_acknowledgement_timeout=300, \*\*kwargs)
Bases: `BaseOperator`, `LoggingMixin`

An Airflow operator that manages Job submission to Armada.

This operator submits a job to an Armada cluster, polls for its completion,
and handles job cancellation if the Airflow task is killed.


* **Parameters**

    
    * **name** (*str*) – 


    * **channel_args** (*GrpcChannelArgs*) – 


    * **armada_queue** (*str*) – 


    * **job_request** (*JobSubmitRequestItem*) – 


    * **job_set_prefix** (*str** | **None*) – 


    * **lookout_url_template** (*str** | **None*) – 


    * **poll_interval** (*int*) – 


    * **container_logs** (*str** | **None*) – 


    * **k8s_token_retriever** (*TokenRetriever** | **None*) – 


    * **deferrable** (*bool*) – 


    * **job_acknowledgement_timeout** (*int*) – 



#### _property_ client(_: ArmadaClien_ )

#### execute(context)
Submits the job to Armada and polls for completion.


* **Parameters**

    **context** (*Context*) – The execution context provided by Airflow.



* **Return type**

    None



#### on_kill()
Override this method to clean up subprocesses when a task instance gets killed.

Any use of the threading, subprocess or multiprocessing module within an
operator needs to be cleaned up, or it will leave ghost processes behind.


* **Return type**

    None



#### pod_manager(k8s_context)

* **Parameters**

    **k8s_context** (*str*) – 



* **Return type**

    *PodLogManager*



#### render_template_fields(context, jinja_env=None)
Template all attributes listed in self.template_fields.
This mutates the attributes in-place and is irreversible.

Args:

    context (Context): The execution context provided by Airflow.


* **Parameters**

    
    * **context** (*Context*) – Airflow Context dict wi1th values to apply on content


    * **jinja_env** (*Environment** | **None*) – jinja’s environment to use for rendering.



* **Return type**

    None



#### template_fields(_: Sequence[str_ _ = ('job_request', 'job_set_prefix'_ )
Initializes a new ArmadaOperator.


* **Parameters**

    
    * **name** (*str*) – The name of the job to be submitted.


    * **channel_args** (*GrpcChannelArgs*) – The gRPC channel arguments for connecting to the Armada server.


    * **armada_queue** (*str*) – The name of the Armada queue to which the job will be submitted.


    * **job_request** (*JobSubmitRequestItem*) – The job to be submitted to Armada.


    * **job_set_prefix** (*Optional**[**str**]*) – A string to prepend to the jobSet name


    * **lookout_url_template** – Template for creating lookout links. If not specified


then no tracking information will be logged.
:type lookout_url_template: Optional[str]
:param poll_interval: The interval in seconds between polling for job status updates.
:type poll_interval: int
:param container_logs: Name of container whose logs will be published to stdout.
:type container_logs: Optional[str]
:param k8s_token_retriever: A serialisable Kubernetes token retriever object. We use
this to read logs from Kubernetes pods.
:type k8s_token_retriever: Optional[TokenRetriever]
:param deferrable: Whether the operator should run in a deferrable mode, allowing
for asynchronous execution.
:type deferrable: bool
:param job_acknowledgement_timeout: The timeout in seconds to wait for a job to be
acknowledged by Armada.
:type job_acknowledgement_timeout: int
:param kwargs: Additional keyword arguments to pass to the BaseOperator.

## armada.operators.armada_deferrable module

## armada.operators.jobservice module

## armada.operators.jobservice_asyncio module

## armada.operators.utils module
