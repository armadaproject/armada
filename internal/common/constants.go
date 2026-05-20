package common

const PodNamePrefix string = "armada-"
const RunPreemptedFallback string = "Run preempted"

func PodName(jobId string) string {
	return PodNamePrefix + jobId + "-0"
}
