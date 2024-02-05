package service

import (
	"fmt"

	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

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
