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
	if jobRun.Queue == "" {
		return nil, fmt.Errorf("job is invalid, queue is empty")
	}
	if jobRun.Jobset == "" {
		return nil, fmt.Errorf("job is invalid, jobset is empty")
	}

	return &job.RunMeta{
		JobId:  jobRun.Job.JobIdStr,
		RunId:  jobRun.JobRunIdStr,
		Queue:  jobRun.Queue,
		JobSet: jobRun.Jobset,
	}, nil
}
