// Package podname holds the rules for the names of the pods of Armada runs.
package podname

import "github.com/armadaproject/armada/internal/common"

// Default returns the name of the pod of a job with the default settings. A component that does not know the reported
// name of a pod uses it, because the executor gave every pod this name before Armada reported pod names.
func Default(jobId string) string {
	return common.PodName(jobId)
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
