# Pod names

- [Configure the format](#configure-the-format)
- [Configure the prefix](#configure-the-prefix)
- [Find the pod of a run](#find-the-pod-of-a-run)
- [Before you turn on run-scoped names](#before-you-turn-on-run-scoped-names)
- [Services and ingresses](#services-and-ingresses)

The executor creates one Kubernetes pod for each run of a job. It names the pod in one of two formats:

| Format | Pod name | Setting |
| --- | --- | --- |
| Job-scoped, the default | `<prefix>-<jobId>-0` | `submission.runScopedPodNames: false` |
| Run-scoped | `<prefix>-<runId>` | `submission.runScopedPodNames: true` |

The prefix is `armada` by default. See [Configure the prefix](#configure-the-prefix).

With job-scoped names, only one pod of a job can exist on a cluster. A retry on the same cluster must wait until the failed pod of the previous run is gone. Retry policies therefore need `action: Delete` on the categories that they retry. See [Retry policies](retry_policies.md#pod-naming-and-collision-avoidance). With run-scoped names, each run has its own pod. The failed pod of a run can stay for debugging while the retry runs.

## Configure the format

```yaml
submission:
  runScopedPodNames: true
```

The server stores the setting in each job when you submit it, and the executor reads it from the job. All runs of a job thus use one format. A change to the setting applies to jobs that you submit after the change. The executor finds its pods by label, so it manages pods of both formats.

Upgrade the scheduler, the scheduler ingester and all executors before you turn on run-scoped names. An older scheduler or scheduler ingester drops the setting from the job, and an older executor ignores it. In both cases the executor names every pod `armada-<jobId>-0`.

If you turn the setting off again, new jobs get job-scoped names, and existing jobs keep their format. Retried categories then need `action: Delete` again. See [Retry policies](retry_policies.md#pod-naming-and-collision-avoidance).

## Configure the prefix

The server sets one prefix for the names of the pod, the services, the ingresses and the ingress hosts of a job:

```yaml
submission:
  objectNamePrefix: armada
```

The prefix must be a DNS-1035 label of at most 20 characters. The server does not start with a prefix that is not valid. The limit keeps a service name, `<prefix>-<jobId>-0-service-<index>`, within the 63 characters of a DNS label. An ingress host starts with `<container>-<port>-<prefix>-<jobId>-0`, so its length also depends on the container name and the port. Kubernetes rejects an ingress whose first host label is longer than 63 characters. A long prefix can thus stop a job from exposing its ingress.

The server stores the prefix in the job when you submit it, and the executor reads it from the job for the pod name. All objects of a job thus use the same prefix. When a job has no stored prefix, the executor uses `armada`. A change to the setting applies to jobs that you submit after the change. Jobs that exist keep their prefix.

Upgrade the scheduler, the scheduler ingester and all executors before you change the prefix. An older scheduler or scheduler ingester drops the prefix from the job, and an older executor always names the pod with the prefix `armada`. Also check the items in [Before you turn on run-scoped names](#before-you-turn-on-run-scoped-names), because some of them build names with the prefix `armada`.

## Find the pod of a run

Armada reports the name of the pod of each run. Use the reported name, and do not build it from the job ID or the run ID. The name is empty while the run has no pod. For a run with a pod that Lookout stored before it stored pod names, the Lookout API and the Query API return `armada-<jobId>-0`, because every pod of such a run has that name.

| Source | Run ID | Job ID | Pod name |
| --- | --- | --- | --- |
| Lookout API, `run` | `runId` | `jobId`, on the job | `podName` |
| Query API, `GetJobRunDetails` and `GetJobDetails` | `run_id` | `job_id` | `pod_name` |
| Event API | no, except `JobPreemptedEvent` | `job_id` | `pod_name`, on the events that report a pod |
| Pod labels | `armada_job_run_id` | `armada_job_id` | the pod itself |
| Container environment | `ARMADA_JOB_RUN_ID` | `ARMADA_JOB_ID` | `$HOSTNAME`, unless the pod spec sets `hostname` |

The Lookout logs tab sends the pod name of the selected run to binoculars. The Lookout commands and the Airflow operator use the pod name of the latest run.

With job-scoped names, all runs of a job have the same pod name. When two runs ran on one cluster, the name of the earlier run therefore points to the pod of the later run.

## Before you turn on run-scoped names

Upgrade the server, the scheduler, the scheduler ingester, all executors, Lookout, the Lookout ingester, binoculars and `armadactl` first, and the Airflow operator if you use it. The server runs the Query API that the Airflow operator reads. Older versions build the job-scoped name or do not store the pod name, so logs and `kubectl` commands fail for runs with run-scoped names. The runs themselves continue to work.

Then check these items:

- Custom Lookout `commandSpecs` that build `armada-{{ jobId }}-0`. Use the pod name of the run, for example `logs {{ runs[runs.length - 1].podName }}`.
- Go code that calls `pkg/client.GetKubectlCommand`. It builds the job-scoped name with the prefix `armada`. Use `GetKubectlCommandForPod` with the pod name from the job events.
- Binoculars requests without a pod name. Binoculars then builds the job-scoped name with the prefix `armada`. Lookout sends the pod name.
- Tools, log pipelines and alert rules that build or parse the job-scoped name. A tool that reads the ID after the prefix gets the run ID and uses it as the job ID, without an error. An alert rule such as `pod=~"armada-.*-0"` stops firing.
- Workloads that use `$HOSTNAME` as a stable key, for example for checkpoints. The hostname changes on each run. Use `ARMADA_JOB_ID` as the key for a job, and `ARMADA_JOB_RUN_ID` for one run.
- The number of terminated pods. The failed pod of each run stays until it expires (`failedPodExpiry`) or until the number of terminated pods is more than `maxTerminatedPods`. A pod in a category with `action: Delete` does not stay, because the executor deletes it when it categorizes the failure.

## Services and ingresses

Services, ingresses and ingress host names keep one name per job, for example `<prefix>-<jobId>-0-service-0`. Clients can thus use the same service name or ingress host name in every run. Each run creates its own service and ingress objects with these names, and a service selects only the pod of its run.

The executor removes the service and the ingress of a run when its pod ends, also when the pod stays for debugging. Kubernetes also removes them when it deletes the pod, because the pod owns them. The next run creates them again, so a `ClusterIP` service gets a new cluster IP.

When the service or the ingress of an earlier run still exists, for example while Kubernetes removes it, the executor deletes the new pod and returns the lease, and Armada starts a new run. The returned run counts toward the attempt limit of the job (`scheduling.maxRetries`). Jobs with job-scoped pod names have the same behaviour when a service or an ingress of an earlier run still exists.
