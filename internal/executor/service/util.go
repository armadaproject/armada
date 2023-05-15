package service

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	commonUtil "github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
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
	return !util.IsReportedDone(pod)
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
		if util.IsInTerminalState(pod) && util.HasCurrentStateBeenReported(pod) && !util.IsReportedDone(pod) {
			return true
		}
	}
	return false
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

func ExtractEssentialJobMetadata(jobRun *executorapi.JobRunLease) (*job.RunMeta, error) {
	if jobRun.Job == nil {
		return nil, fmt.Errorf("job is invalid, job field is nil")
	}
	jobId, err := armadaevents.UlidStringFromProtoUuid(jobRun.Job.JobId)
	if err != nil {
		return nil, fmt.Errorf("unable to extract jobId because %s", err)
	}
	runId, err := armadaevents.UuidStringFromProtoUuid(jobRun.JobRunId)
	if err != nil {
		return nil, fmt.Errorf("unable to extract runId because %s", err)
	}
	if jobRun.Queue == "" {
		return nil, fmt.Errorf("job is invalid, queue is empty")
	}
	if jobRun.Jobset == "" {
		return nil, fmt.Errorf("job is invalid, jobset is empty")
	}

	return &job.RunMeta{
		JobId:  jobId,
		RunId:  runId,
		Queue:  jobRun.Queue,
		JobSet: jobRun.Jobset,
	}, nil
}
