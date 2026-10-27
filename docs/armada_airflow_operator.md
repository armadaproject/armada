# Armada Airflow Operator

This class provides integration with Airflow and Armada

## armada.operators.armada module

### *class* armada.operators.armada.ArmadaOperator(name, channel_args, armada_queue, job_request, job_set_prefix='', lookout_url_template=None, poll_interval=30, container_logs=None, k8s_token_retriever=None, deferrable=False, job_acknowledgement_timeout=300, dry_run=False, reattach_policy=None, extra_links=None, \*\*kwargs)

Bases: `BaseOperator`, `LoggingMixin`

An Airflow operator that manages Job submission to Armada.

This operator submits a job to an Armada cluster, polls for its completion, and handles job cancellation if the Airflow task is killed.

Initializes a new ArmadaOperator.

* **Parameters:**
  * **name** (*str*) – The name of the job to be submitted.
  * **channel_args** ([*GrpcChannelArgs*](#armada.model.GrpcChannelArgs)) – The gRPC channel arguments for connecting to the Armada server.
  * **armada_queue** (*str*) – The name of the Armada queue to which the job will be submitted.
  * **job_request** (*JobSubmitRequestItem* *|* *Callable* *[* *[**Context* *,* *jinja2.Environment* *]* *,* *JobSubmitRequestItem* *]*) – The job to be submitted to Armada.
  * **job_set_prefix** (*Optional* *[**str* *]*) – A string to prepend to the jobSet name.
  * **lookout_url_template** (*Optional* *[**str* *]*) – Template for creating lookout links. If not specified, no tracking information will be logged.
  * **poll_interval** (*int*) – The interval in seconds between polling for job status updates.
  * **container_logs** (*Optional* *[**str* *]*) – Name of container whose logs will be published to stdout.
  * **k8s_token_retriever** (*Optional* *[*[*TokenRetriever*](#armada.auth.TokenRetriever) *]*) – A serialisable Kubernetes token retriever object. We use this to read logs from Kubernetes pods.
  * **deferrable** (*bool*) – Whether the operator should run in a deferrable mode, allowing for asynchronous execution.
  * **job_acknowledgement_timeout** (*int*) – The timeout in seconds to wait for a job to be acknowledged by Armada.
  * **dry_run** (*bool*) – Run Operator in dry-run mode - render Armada request and terminate.
  * **reattach_policy** (*Optional* *[**str* *]*  *|* *Callable* *[* *[**JobState* *,* *str* *]* *,* *bool* *]*) – Operator reattach policy to use (defaults to `never`).
  * **extra_links** (*Optional* *[**Dict* *[**str* *,* *Union* *[**str* *,* *re.Pattern* *]* *]* *]*) – Extra links to be shown in addition to the Lookout URL. Regex patterns will be extracted from container logs (taking the first match).
  * **kwargs** – Additional keyword arguments to pass to the BaseOperator.

#### execute(context)

Submits the job to Armada and polls for completion.

* **Parameters:**
  **context** (*Context*) – The execution context provided by Airflow.
* **Returns:**
  Dictionary containing job information and links
* **Return type:**
  Dict[str, Any]

#### *property* hook *: ArmadaHook*

#### lookout_url(job_id)

#### on_kill()

Override this method to clean up subprocesses when a task instance gets killed.

Any use of the threading, subprocess or multiprocessing module within an operator needs to be cleaned up, or it will leave ghost processes behind.

* **Return type:**
  None

#### *property* pod_manager *: KubernetesPodLogManager*

#### render_extra_links_urls(context, jinja_env=None)

Template all URLs listed in self.extra_links. This pushes all URL values to xcom for values to be picked up by UI.

* **Parameters:**
  * **context** (*Context*) – Airflow Context dict with values to apply on content.
  * **jinja_env** (*Environment* *|* *None*) – Jinja environment to use for rendering.
* **Return type:**
  None

#### render_template_fields(context, jinja_env=None)

Template all attributes listed in self.template_fields. This mutates the attributes in-place and is irreversible.

* **Parameters:**
  * **context** (*Context*) – Airflow Context dict with values to apply on content.
  * **jinja_env** (*Environment* *|* *None*) – Jinja environment to use for rendering.
* **Return type:**
  None

#### template_fields *: Sequence[str]* *= ('job_request', 'job_set_prefix')*

#### template_fields_renderers *: Dict[str, str]* *= {'job_request': 'py'}*

## armada.triggers module

### *class* armada.triggers.ArmadaPollJobTrigger(moment, context=None, channel_args=None)

Bases: `BaseTrigger`

* **Parameters:**
  * **moment** (*DateTime*)
  * **context** ([*RunningJobContext*](#armada.model.RunningJobContext) *|* *tuple* *[**str* *,* *Dict* *[**str* *,* *Any* *]* *]*  *|* *None*)
  * **channel_args** ([*GrpcChannelArgs*](#armada.model.GrpcChannelArgs) *|* *tuple* *[**str* *,* *Dict* *[**str* *,* *Any* *]* *]*  *|* *None*)

#### *property* hook *: ArmadaHook*

#### run()

Run the trigger in an asynchronous context.

The trigger should yield an Event whenever it wants to fire off an event, and return None if it is finished. Single-event triggers should thus yield and then immediately return.

If it yields, it is likely that it will be resumed very quickly, but it may not be (e.g. if the workload is being moved to another triggerer process, or a multi-event trigger was being used for a single-event task defer).

In either case, Trigger classes should assume they will be persisted, and then rely on cleanup() being called when they are no longer needed.

* **Return type:**
  *AsyncIterator*[*TriggerEvent*]

#### serialize()

Return the information needed to reconstruct this Trigger.

* **Returns:**
  Tuple of (class path, keyword arguments needed to re-instantiate).
* **Return type:**
  tuple[str, dict[str, *Any*]]

#### should_cancel_job()

We only want to cancel jobs when task is being marked Failed/Succeeded.

* **Return type:**
  bool

## armada.auth module

### *class* armada.auth.TokenRetriever(\*args, \*\*kwargs)

Bases: `Protocol`

#### get_token()

* **Return type:**
  str

## armada.model module

### *class* armada.model.GrpcChannelArgs(target, options=None, compression=None, auth=None)

Bases: `object`

* **Parameters:**
  * **target** (*str*)
  * **options** (*Optional* *[**Sequence* *[**Tuple* *[**str* *,* *Any* *]* *]* *]*)
  * **compression** (*Optional* *[**grpc.Compression* *]*)
  * **auth** (*Optional* *[**grpc.AuthMetadataPlugin* *]*)

#### *static* deserialize(data, version)

* **Parameters:**
  * **data** (*dict* *[**str* *,* *Any* *]*)
  * **version** (*int*)
* **Return type:**
  [*GrpcChannelArgs*](#armada.model.GrpcChannelArgs)

#### serialize()

* **Return type:**
  *Dict*[str, *Any*]

### *class* armada.model.RunningJobContext(armada_queue: 'str', job_id: 'str', job_set_id: 'str', submit_time: 'DateTime', cluster: 'Optional[str]' = None, last_log_time: 'Optional[DateTime]' = None, job_state: 'str' = 'UNKNOWN')

Bases: `object`

* **Parameters:**
  * **armada_queue** (*str*)
  * **job_id** (*str*)
  * **job_set_id** (*str*)
  * **submit_time** (*DateTime*)
  * **cluster** (*str* *|* *None*)
  * **last_log_time** (*DateTime* *|* *None*)
  * **job_state** (*str*)

#### armada_queue *: str*

#### cluster *: str | None* *= None*

#### job_id *: str*

#### job_set_id *: str*

#### job_state *: str* *= 'UNKNOWN'*

#### last_log_time *: DateTime | None* *= None*

#### *property* state *: JobState*

#### submit_time *: DateTime*
