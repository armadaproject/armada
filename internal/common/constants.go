package common

// DefaultObjectNamePrefix is the default prefix of the names of the pod, the services and the ingresses of a job.
const DefaultObjectNamePrefix = "armada"

// PodNamePrefix is the start of a pod name with the default prefix.
const PodNamePrefix string = DefaultObjectNamePrefix + "-"

// JobScopedName returns <prefix>-<jobId>-0. It is the job-scoped pod name, and the names of the services,
// the ingresses and the ingress hosts of the job start with it.
func JobScopedName(prefix string, jobId string) string {
	return prefix + "-" + jobId + "-0"
}
