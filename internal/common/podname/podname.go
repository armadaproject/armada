// Package podname holds the rules for the names of the pods of Armada runs. The job-scoped name is also the start of
// the names of the services and the ingresses of a job.
package podname

// DefaultPrefix is the default prefix of the names of the pod, the services and the ingresses of a job.
const DefaultPrefix = "armada"

// JobScoped returns <prefix>-<jobId>-0. It is the job-scoped pod name. The names of the services and the ingresses of
// the job start with it, and the ingress hosts contain it.
func JobScoped(prefix string, jobId string) string {
	return prefix + "-" + jobId + "-0"
}

// ForRun returns the name that the executor gives to the pod of a run: <prefix>-<runId> with run-scoped names, else
// the job-scoped name. An empty prefix means DefaultPrefix, because a job that the server stored before it stored the
// prefix has none, and the services and the ingresses of that job use the default prefix.
func ForRun(prefix string, runScoped bool, jobId string, runId string) string {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if runScoped {
		return prefix + "-" + runId
	}
	return JobScoped(prefix, jobId)
}

// Default returns the name of the pod of a job with the default settings. A component that does not know the reported
// name of a pod uses it, because the executor gave every pod this name before Armada reported pod names.
func Default(jobId string) string {
	return JobScoped(DefaultPrefix, jobId)
}

// Resolve returns the name of the pod of a run that Lookout stores, or "" when the run has no pod. Lookout stored runs
// before it stored pod names, and the executor named every pod of those runs Default(jobId). A run has a pod when it
// has a pending time or a start time, because the ingester sets them only from the events of the pod.
func Resolve(jobId string, stored *string, pending bool, started bool) string {
	if stored != nil {
		return *stored
	}
	if pending || started {
		return Default(jobId)
	}
	return ""
}
