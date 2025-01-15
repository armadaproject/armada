---
docname: python_airflow_operator
images: {}
path: /python-airflow-operator
title: Armada Airflow Operator
---

# Armada Airflow Operator

This class provides integration with Airflow and Armada

## armada.operators.armada module


### _class_ armada.operators.armada.ArmadaOperator(name, channel_args, armada_queue, job_request, job_set_prefix='', lookout_url_template=None, poll_interval=30, container_logs=None, k8s_token_retriever=None, deferrable=False, job_acknowledgement_timeout=300, dry_run=False, reattach_policy=None, extra_links=None, \*\*kwargs)
Bases: `BaseOperator`, `LoggingMixin`

An Airflow operator that manages Job submission to Armada.

This operator submits a job to an Armada cluster, polls for its completion,
and handles job cancellation if the Airflow task is killed.


* **Parameters**

    
    * **name** (*str*) – 


    * **channel_args** (*GrpcChannelArgs*) – 


    * **armada_queue** (*str*) – 


    * **job_request** (*JobSubmitRequestItem** | **Callable**[**[**Context**, **jinja2.Environment**]**, **JobSubmitRequestItem**]*) – 


    * **job_set_prefix** (*Optional**[**str**]*) – 


    * **lookout_url_template** (*Optional**[**str**]*) – 


    * **poll_interval** (*int*) – 


    * **container_logs** (*Optional**[**str**]*) – 


    * **k8s_token_retriever** (*Optional**[**TokenRetriever**]*) – 


    * **deferrable** (*bool*) – 


    * **job_acknowledgement_timeout** (*int*) – 


    * **dry_run** (*bool*) – 


    * **reattach_policy** (*Optional**[**str**] **| **Callable**[**[**JobState**, **str**]**, **bool**]*) – 


    * **extra_links** (*Optional**[**Dict**[**str**, **str**]**]*) – 



#### execute(context)
Submits the job to Armada and polls for completion.


* **Parameters**

    **context** (*Context*) – The execution context provided by Airflow.



* **Return type**

    None



#### _property_ hook(_: ArmadaHoo_ )

#### lookout_url(job_id)

#### on_kill()
Override this method to clean up subprocesses when a task instance gets killed.

Any use of the threading, subprocess or multiprocessing module within an
operator needs to be cleaned up, or it will leave ghost processes behind.


* **Return type**

    None



#### _property_ pod_manager(_: KubernetesPodLogManage_ )

#### render_extra_links_urls(context, jinja_env=None)
Template all URLs listed in self.extra_links.
This pushes all URL values to xcom for values to be picked up by UI.

Args:

    context (Context): The execution context provided by Airflow.


* **Parameters**

    
    * **context** (*Context*) – Airflow Context dict wi1th values to apply on content


    * **jinja_env** (*Environment** | **None*) – jinja’s environment to use for rendering.



* **Return type**

    None



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

#### template_fields_renderers(_: Dict[str, str_ _ = {'job_request': 'py'_ )
Initializes a new ArmadaOperator.


* **Parameters**

    
    * **name** (*str*) – The name of the job to be submitted.


    * **channel_args** (*GrpcChannelArgs*) – The gRPC channel arguments for connecting to the Armada server.


    * **armada_queue** (*str*) – The name of the Armada queue to which the job will be submitted.


    * **job_request** (*JobSubmitRequestItem** | **Callable**[**[**Context**, **jinja2.Environment**]**, **JobSubmitRequestItem**]*) – The job to be submitted to Armada.


    * **job_set_prefix** (*Optional**[**str**]*) – A string to prepend to the jobSet name.


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
:param dry_run: Run Operator in dry-run mode - render Armada request and terminate.
:type dry_run: bool
:param reattach_policy: Operator reattach policy to use (defaults to: never)
:type reattach_policy: Optional[str] | Callable[[JobState, str], bool]
:param kwargs: Additional keyword arguments to pass to the BaseOperator.
:param extra_links: Extra links to be shown in addition to Lookout URL.
:type extra_links: Optional[Dict[str, str]]
:param kwargs: Additional keyword arguments to pass to the BaseOperator.


### _class_ armada.operators.armada.DynamicLink(name)
Bases: `BaseOperatorLink`, `LoggingMixin`


* **Parameters**

    **name** (*str*) – 



#### get_link(operator, \*, ti_key)
Link to external system.

Note: The old signature of this function was `(self, operator, dttm: datetime)`. That is still
supported at runtime but is deprecated.


* **Parameters**

    
    * **operator** (*BaseOperator*) – The Airflow operator object this link is associated to.


    * **ti_key** (*TaskInstanceKey*) – TaskInstance ID to return link for.



* **Returns**

    link to external system



#### name(_: st_ )

### _class_ armada.operators.armada.LookoutLink()
Bases: `BaseOperatorLink`


#### get_link(operator, \*, ti_key)
Link to external system.

Note: The old signature of this function was `(self, operator, dttm: datetime)`. That is still
supported at runtime but is deprecated.


* **Parameters**

    
    * **operator** (*BaseOperator*) – The Airflow operator object this link is associated to.


    * **ti_key** (*TaskInstanceKey*) – TaskInstance ID to return link for.



* **Returns**

    link to external system



#### name(_ = 'Lookout_ )
## armada.triggers.armada module

## armada.auth module


### _class_ armada.auth.TokenRetriever(\*args, \*\*kwargs)
Bases: `Protocol`


#### get_token()

* **Return type**

    str


## armada.model module


### _class_ armada.model.GrpcChannelArgs(target, options=None, compression=None, auth=None)
Bases: `object`


* **Parameters**

    
    * **target** (*str*) – 


    * **options** (*Optional**[**Sequence**[**Tuple**[**str**, **Any**]**]**]*) – 


    * **compression** (*Optional**[**grpc.Compression**]*) – 


    * **auth** (*Optional**[**grpc.AuthMetadataPlugin**]*) – 



#### _static_ deserialize(data, version)

* **Parameters**

    
    * **data** (*dict**[**str**, **Any**]*) – 


    * **version** (*int*) – 



* **Return type**

    *GrpcChannelArgs*



#### serialize()

* **Return type**

    *Dict*[str, *Any*]



### _class_ armada.model.RunningJobContext(armada_queue: 'str', job_id: 'str', job_set_id: 'str', submit_time: 'DateTime', cluster: 'Optional[str]' = None, last_log_time: 'Optional[DateTime]' = None, job_state: 'str' = 'UNKNOWN')
Bases: `object`


* **Parameters**

    
    * **armada_queue** (*str*) – 


    * **job_id** (*str*) – 


    * **job_set_id** (*str*) – 


    * **submit_time** (*DateTime*) – 


    * **cluster** (*str** | **None*) – 


    * **last_log_time** (*DateTime** | **None*) – 


    * **job_state** (*str*) – 



#### armada_queue(_: st_ )

#### cluster(_: str | Non_ _ = Non_ )

#### job_id(_: st_ )

#### job_set_id(_: st_ )

#### job_state(_: st_ _ = 'UNKNOWN_ )

#### last_log_time(_: DateTime | Non_ _ = Non_ )

#### _property_ state(_: JobStat_ )

#### submit_time(_: DateTim_ )
