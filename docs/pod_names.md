# Pod names

- [Configure the executor](#configure-the-executor)
- [Configure the prefix](#configure-the-prefix)
- [Find the pod of a run](#find-the-pod-of-a-run)
- [Before you turn on run-scoped names](#before-you-turn-on-run-scoped-names)
- [Services and ingresses](#services-and-ingresses)

The executor creates one Kubernetes pod for each run of a job. It names the pod in one of two formats:

| Format | Pod name | Setting |
| --- | --- | --- |
| Job-scoped, the default | `<prefix>-<jobId>-0` | `runScopedPodNames: false` |
| Run-scoped | `<prefix>-<runId>` | `runScopedPodNames: true` |

The prefix is `armada` by default, see [Configure the prefix](#configure-the-prefix). With job-scoped names, only one pod of a job can exist on a cluster. A retry on the same cluster must wait until the failed pod of the previous run is gone. Retry policies therefore need `action: Delete` on the categories that they retry, see [Retry policies](retry_policies.md#pod-naming-and-collision-avoidance). With run-scoped names, each run has its own pod. The failed pod of a run can stay for debugging while the retry runs.

## Configure the executor

```yaml
kubernetes:
  runScopedPodNames: true
```

Each executor reads its own setting, so you can turn on run-scoped names one cluster at a time. The executor finds its pods by label, so it manages pods of both formats.

If you turn the setting off again, new pods get job-scoped names and existing pods keep their names. Retried categories then need `action: Delete` again, see [Retry policies](retry_policies.md#pod-naming-and-collision-avoidance).

## Configure the prefix

The server sets one prefix for the names of the pod, the services, the ingresses and the ingress hosts of a job:

```yaml
submission:
  objectNamePrefix: armada
```

The prefix must be a DNS-1035 label of at most 20 characters. The server does not start with a prefix that is not valid. The limit keeps a service name, `<prefix>-<jobId>-0-service-<index>`, within the 63 characters of a DNS label. An ingress host starts with `<container>-<port>-<prefix>-<jobId>-0`, so its length also depends on the container name and the port.

The server writes the prefix to the job in the annotation `armadaproject.io/objectNamePrefix`, and overwrites a value that the user sets. The executor reads the annotation for the pod name, so all objects of a job use the same prefix. A job without the annotation gets the prefix `armada`. A change to the setting applies to jobs that you submit after the change. Jobs that exist keep their prefix.

Upgrade all executors before you change the prefix, because an older executor always names the pod with the prefix `armada`. Also check the items in [Before you turn on run-scoped names](#before-you-turn-on-run-scoped-names), because some of them build names with the prefix `armada`.

## Find the pod of a run

Treat the pod name as opaque. Find a pod by its labels, and use both the job ID and the run ID:

```bash
kubectl -n <namespace> logs -l armada_job_id=<jobId>,armada_job_run_id=<runId> --tail=-1
kubectl -n <namespace> exec -it $(kubectl -n <namespace> get pod -l armada_job_id=<jobId>,armada_job_run_id=<runId> -o name) -- /bin/sh
```

The namespace is the namespace of the job. With a selector, `kubectl logs` shows only the last 10 lines, unless you set `--tail=-1`. The selector `armada_job_id=<jobId>` alone matches the pods of all runs of a job.

A label is not proof that Armada created the pod. A user who can create pods in the namespace can give a pod the same labels. Binoculars returns an error when more than one pod matches, and the `exec` command fails in that case.

| Source | Run ID | Job ID | Pod name |
| --- | --- | --- | --- |
| Event API | no, except `JobPreemptedEvent` | `job_id` | `pod_name` |
| Query API, `GetJobRunDetails` | `run_id` | `job_id` | no |
| Pod labels | `armada_job_run_id` | `armada_job_id` | the pod itself |
| Container environment | `ARMADA_JOB_RUN_ID` | `ARMADA_JOB_ID` | `$HOSTNAME`, unless the pod spec sets `hostname` |

## Upgrade

Binoculars, the Lookout commands and the Airflow operator find the pod of a run by its labels, also when run-scoped names are off. So they need permission to list pods after the upgrade:

- The binoculars service account lists pods in all namespaces and gets full pod objects. The Helm chart gives it this permission. A deployment with its own RBAC or its own service account must add it before the upgrade.
- The Lookout commands and the Airflow operator list pods by label. A user or a credential that can only get a pod and its logs must also get permission to list pods in the namespace.

Without this permission, the Lookout logs tab, the Lookout commands and the logs of the Airflow operator fail.

## Before you turn on run-scoped names

Upgrade binoculars, Lookout and `armadactl` first, and the Airflow operator if you use it. Binoculars, the Lookout commands and the Airflow operator find the pod of a run by its labels. The `armadactl watch` hint uses the pod name from the failed event. Older versions build the job-scoped name, so logs and `kubectl` commands fail for new runs. The runs themselves continue to work.

Then check these items:

- Custom Lookout `commandSpecs` that build `armada-{{ jobId }}-0`. Use the label selector, for example `logs -l armada_job_id={{ jobId }},armada_job_run_id={{ runs[runs.length - 1].runId }} --tail=-1`.
- Go code that calls `pkg/client.GetKubectlCommand`. It builds the job-scoped name with the prefix `armada`. Use `GetKubectlCommandForPod` with the pod name from the job events.
- Binoculars requests without a run ID. Binoculars then builds the job-scoped name with the prefix `armada`. Lookout sends the run ID.
- Tools, log pipelines and alert rules that build or parse the job-scoped name. A tool that reads the ID after the prefix gets the run ID and uses it as the job ID, without an error. An alert rule such as `pod=~"armada-.*-0"` stops firing.
- Workloads that use `$HOSTNAME` as a stable key, for example for checkpoints. The hostname changes on each run. Use `ARMADA_JOB_ID` as the key for a job, and `ARMADA_JOB_RUN_ID` for one run.
- The number of terminated pods. The failed pod of each run stays until it expires (`failedPodExpiry`) or until the number of terminated pods is more than `maxTerminatedPods`. A pod in a category with `action: Delete` does not stay, because the executor deletes it when it categorizes the failure.

## Services and ingresses

Services, ingresses and ingress host names keep one name per job, for example `<prefix>-<jobId>-0-service-0`, so clients can use the same service name or ingress host name in every run. Each run creates its own service and ingress objects with these names. With run-scoped pod names, a service selects only the pod of its run.

The executor removes the service and the ingress of a run when its pod ends, also when the pod stays for debugging. Kubernetes also removes them when it deletes the pod, because the pod owns them. The next run creates them again, so a `ClusterIP` service gets a new cluster IP.

When the service or the ingress of an earlier run still exists, for example while Kubernetes removes it, the executor deletes the new pod and returns the lease, and Armada starts a new run. The returned run counts toward the attempt limit of the job (`scheduling.maxRetries`). Job-scoped pod names have the same behaviour.
