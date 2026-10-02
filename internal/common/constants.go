package common

import "strconv"

const PodNamePrefix string = "armada-"

func PodName(jobId string) string {
	return PodNamePrefix + jobId + "-0"
}

// PodNameForRun returns the pod name for one run of a job. The run index takes
// the place of the pod number, so no two runs of a job share a pod name. Index
// 0 gives the same name as PodName.
func PodNameForRun(jobId string, runIndex uint32) string {
	return PodNamePrefix + jobId + "-" + strconv.FormatUint(uint64(runIndex), 10)
}
