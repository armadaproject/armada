---
title: Armada Airflow Operator
---

This class provides integration with Airflow and Armada

## armada.operators.armada module

### _class_ armada.operators.armada.ArmadaOperator(name, channel_args, armada_queue, job_request, job_set_prefix='', lookout_url_template=None, poll_interval=30, container_logs=None, k8s_token_retriever=None, deferrable=False, job_acknowledgement_timeout=300, dry_run=False, reattach_policy=None, extra_links=None, \*\*kwargs)

Bases: `BaseOperator`, `LoggingMixin`

An Airflow operator that manages Job submission to Armada.

This operator submits a job to an Armada cluster, polls for its completion,
and handles job cancellation if the Airflow task is killed.

- **Parameters**
  - **name** (_str_) –

  - **channel_args** (_GrpcChannelArgs_) –

  - **armada_queue** (_str_) –

  - **job_request** (_JobSubmitRequestItem** | **Callable**[**[**Context**, **jinja2.Environment**]**, **JobSubmitRequestItem\*\*]_) –

  - **job_set_prefix** (_Optional**[**str\*\*]_) –

  - **lookout_url_template** (_Optional**[**str\*\*]_) –

  - **poll_interval** (_int_) –

  - **container_logs** (_Optional**[**str\*\*]_) –

  - **k8s_token_retriever** (_Optional**[**TokenRetriever\*\*]_) –

  - **deferrable** (_bool_) –

  - **job_acknowledgement_timeout** (_int_) –

  - **dry_run** (_bool_) –

  - **reattach_policy** (_Optional**[**str**] **| **Callable**[**[**JobState**, **str**]**, **bool**]_) –

  - **extra_links** (_Optional**[**Dict**[**str**, **Union**[**str**, **re.Pattern**]**]\*\*]_) –

#### execute(context)

Submits the job to Armada and polls for completion.

- **Parameters**

  **context** (_Context_) – The execution context provided by Airflow.

- **Return type**

  None

#### _property_ hook(_: ArmadaHoo_ )

#### lookout_url(job_id)

#### on_kill()

Override this method to clean up subprocesses when a task instance gets killed.

Any use of the threading, subprocess, or multiprocessing module within an
operator needs to be cleaned up, or it will leave ghost processes behind.

- **Return type**

  None

#### _property_ pod*manager(*: KubernetesPodLogManage\_ )

#### render_extra_links_urls(context, jinja_env=None)

Template all URLs listed in self.extra_links.
This pushes all URL values to xcom for values to be picked up by UI.

Args:

    context (Context): The execution context provided by Airflow.

- **Parameters**
  - **context** (_Context_) – Airflow Context dict wi1th values to apply on content

  - **jinja_env** (_Environment** | **None_) – jinja’s environment to use for rendering.

- **Return type**

  None

#### render_template_fields(context, jinja_env=None)

Template all attributes listed in self.template_fields.
This mutates the attributes in-place and is irreversible.

Args:

    context (Context): The execution context provided by Airflow.

- **Parameters**
  - **context** (_Context_) – Airflow Context dict wi1th values to apply on content

  - **jinja_env** (_Environment** | **None_) – jinja’s environment to use for rendering.

- **Return type**

  None

#### template*fields(*: Sequence[str\_ _ = ('job_request', 'job_set_prefix'_ )

#### template*fields_renderers(*: Dict[str, str\_ _ = {'job_request': 'py'_ )

Initializes a new ArmadaOperator.

- **Parameters**
  - **name** (_str_) – The name of the job to be submitted.

  - **channel_args** (_GrpcChannelArgs_) – The gRPC channel arguments for connecting to the Armada server.

  - **armada_queue** (_str_) – The name of the Armada queue to which the job will be submitted.

  - **job_request** (_JobSubmitRequestItem** | **Callable**[**[**Context**, **jinja2.Environment**]**, **JobSubmitRequestItem\*\*]_) – The job to be submitted to Armada.

  - **job_set_prefix** (_Optional**[**str\*\*]_) – A string to prepend to the jobSet name.

  - **lookout_url_template** – Template for creating lookout links. If not specified

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
:param extra_links: Extra links to be shown in addition to Lookout URL. Regex patterns will be extracted from container logs (taking first match).
:type extra_links: Optional[Dict[str, Union[str, re.Pattern]]]
:param kwargs: Additional keyword arguments to pass to the BaseOperator.

## armada.triggers.armada module

## armada.auth module

### _class_ armada.auth.TokenRetriever(\*args, \*\*kwargs)

Bases: `Protocol`

#### get_token()

- **Return type**

  str

## armada.model module

### _class_ armada.model.GrpcChannelArgs(target, options=None, compression=None, auth=None)

Bases: `object`

- **Parameters**
  - **target** (_str_) –

  - **options** (_Optional**[**Sequence**[**Tuple**[**str**, **Any**]**]\*\*]_) –

  - **compression** (_Optional**[**grpc.Compression\*\*]_) –

  - **auth** (_Optional**[**grpc.AuthMetadataPlugin\*\*]_) –

#### _static_ deserialize(data, version)

- **Parameters**
  - **data** (_dict**[**str**, **Any\*\*]_) –

  - **version** (_int_) –

- **Return type**

  _GrpcChannelArgs_

#### serialize()

- **Return type**

  _Dict_[str, *Any*]

### _class_ armada.model.RunningJobContext(armada_queue: 'str', job_id: 'str', job_set_id: 'str', submit_time: 'DateTime', cluster: 'Optional[str]' = None, last_log_time: 'Optional[DateTime]' = None, job_state: 'str' = 'UNKNOWN')

Bases: `object`

- **Parameters**
  - **armada_queue** (_str_) –

  - **job_id** (_str_) –

  - **job_set_id** (_str_) –

  - **submit_time** (_DateTime_) –

  - **cluster** (_str** | **None_) –

  - **last_log_time** (_DateTime** | **None_) –

  - **job_state** (_str_) –

#### armada*queue(*: st\_ )

#### cluster(_: str | Non_ _ = Non_ )

#### job*id(*: st\_ )

#### job*set_id(*: st\_ )

#### job*state(*: st\_ _ = 'UNKNOWN_ )

#### last*log_time(*: DateTime | Non\_ _ = Non_ )

#### _property_ state(_: JobStat_ )

#### submit*time(*: DateTim\_ )
