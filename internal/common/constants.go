package common

const PodNamePrefix string = "armada-"

func PodName(jobId string) string {
	return PodNamePrefix + jobId + "-0"
}
