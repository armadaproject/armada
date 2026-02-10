package common

import "fmt"

const PodNamePrefix string = "armada-"

// PodName returns the legacy pod name format: armada-<jobId>-0
// Deprecated: Use BuildPodName for new code. Deprecated when native retries were introduced
// in https://github.com/armadaproject/armada/issues/4683
func PodName(jobId string) string {
	return PodNamePrefix + jobId + "-0"
}

// BuildPodName constructs a pod name with support for retry run index.
// If runIndex is nil, returns format: armada-<jobId>-<podIndex>
// If runIndex is non-nil, returns format: armada-<jobId>-<podIndex>-<runIndex>
// The run index format was introduced for native retries in https://github.com/armadaproject/armada/issues/4683
func BuildPodName(jobId string, podIndex int, runIndex *int) string {
	if runIndex != nil {
		return fmt.Sprintf("%s%s-%d-%d", PodNamePrefix, jobId, podIndex, *runIndex)
	}
	return fmt.Sprintf("%s%s-%d", PodNamePrefix, jobId, podIndex)
}
