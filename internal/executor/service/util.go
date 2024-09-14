package service

import (
	"fmt"

	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func ExtractEssentialJobMetadata(jobRun *executorapi.JobRunLease) (*job.RunMeta, error) {
	if jobRun.Job == nil {
		return nil, fmt.Errorf("job is invalid, job field is nil")
	}
	jobId := jobRun.Job.JobIdStr
	if jobId == "" {
		return nil, fmt.Errorf("job is invalid, jobId is empty")
	}
	runId := jobRun.JobRunIdStr
	if runId == "" {
		return nil, fmt.Errorf("job is invalid, jobRunId is empty")
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
