package service

import (
	"github.com/G-Research/armada/internal/executor/reporter"
	v1 "k8s.io/api/core/v1"

	commonUtil "github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/util"
)

func extractJobIds(jobs []*job.RunningJob) []string {
	ids := []string{}
	for _, job := range jobs {
		ids = append(ids, job.JobId)
	}
	return ids
}

func filterRunningJobs(jobs []*job.RunningJob, filter func(*job.RunningJob) bool) []*job.RunningJob {
	result := make([]*job.RunningJob, 0)
	for _, job := range jobs {
		if filter(job) {
			result = append(result, job)
		}
	}
	return result
}

func filterRunningJobsByIds(jobs []*job.RunningJob, ids []string) []*job.RunningJob {
	idSet := commonUtil.StringListToSet(ids)
	return filterRunningJobs(jobs, func(j *job.RunningJob) bool { return idSet[j.JobId] })
}

func shouldBeRenewed(pod *v1.Pod) bool {
	return !isReportedDone(pod)
}

func jobShouldBeRenewed(job *job.RunningJob) bool {
	for _, pod := range job.ActivePods {
		if shouldBeRenewed(pod) {
			return true
		}
	}
	return false
}

func shouldBeReportedDone(job *job.RunningJob) bool {
	for _, pod := range job.ActivePods {
		if util.IsInTerminalState(pod) && !isReportedDone(pod) {
			return true
		}
	}
	return false
}

func isReportedDone(pod *v1.Pod) bool {
	_, exists := pod.Annotations[jobDoneAnnotation]
	return exists
}

func extractPods(jobs []*job.RunningJob) []*v1.Pod {
	pods := []*v1.Pod{}
	for _, job := range jobs {
		pods = append(pods, job.ActivePods...)
	}
	return pods
}

func chunkJobs(jobs []*job.RunningJob, size int) [][]*job.RunningJob {
	chunks := [][]*job.RunningJob{}
	for start := 0; start < len(jobs); start += size {
		end := start + size
		if end > len(jobs) {
			end = len(jobs)
		}
		chunks = append(chunks, jobs[start:end])
	}
	return chunks
}

func IsPodFinishedAndReported(pod *v1.Pod) bool {
	if !util.IsInTerminalState(pod) ||
		!isReportedDone(pod) ||
		!reporter.HasCurrentStateBeenReported(pod) {
		return false
	}
	return true
}
