package common

import "strconv"

const PodNamePrefix string = "armada-"

// PodName returns the canonical pod name for a job. When runIndex is non-nil
// (retry-policy engine populated JobRunIndex), it is appended as a suffix to
// disambiguate retry attempts; otherwise the legacy single-attempt format is
// used. This is the only place that owns the pod-name format.
func PodName(jobId string, runIndex *uint32) string {
	name := PodNamePrefix + jobId + "-0"
	if runIndex != nil {
		name = name + "-" + strconv.FormatUint(uint64(*runIndex), 10)
	}
	return name
}
