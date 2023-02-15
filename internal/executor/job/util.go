package job

import (
	"github.com/armadaproject/armada/internal/executor/configuration"
	util2 "github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

func CreateSubmitJobsFromApiJobs(apiJobs []*api.Job, podDefaults *configuration.PodDefaults) []*SubmitJob {
	result := make([]*SubmitJob, 0, len(apiJobs))
	for _, apiJob := range apiJobs {
		result = append(result, CreateSubmitJobFromApiJob(apiJob, podDefaults))
	}
	return result
}

func CreateSubmitJobFromApiJob(apiJob *api.Job, podDefaults *configuration.PodDefaults) *SubmitJob {
	pod := util2.CreatePod(apiJob, podDefaults, 0)

	runMeta := &RunMetaInfo{
		JobId:  apiJob.Id,
		RunId:  "",
		JobSet: apiJob.JobSetId,
		Queue:  apiJob.Queue,
	}

	return &SubmitJob{
		Meta: SubmitJobMetaInfo{
			JobRunMeta:      runMeta,
			Owner:           apiJob.Owner,
			OwnershipGroups: apiJob.QueueOwnershipUserGroups,
		},
		Pod:       pod,
		Ingresses: apiJob.K8SIngress,
		Services:  apiJob.K8SService,
	}
}

func CreateSubmitJobFromExecutorApiJobRunLease(
	jobRunLease *executorapi.JobRunLease,
	podDefaults *configuration.PodDefaults,
) (*SubmitJob, error) {
	pod, err := util2.CreatePodFromExecutorApiJob(jobRunLease, podDefaults)
	if err != nil {
		return nil, err
	}

	jobId, err := armadaevents.UlidStringFromProtoUuid(jobRunLease.Job.JobId)
	if err != nil {
		return nil, err
	}

	runId, err := armadaevents.UuidStringFromProtoUuid(jobRunLease.JobRunId)
	if err != nil {
		return nil, err
	}

	runMeta := &RunMetaInfo{
		JobId:  jobId,
		RunId:  runId,
		JobSet: jobRunLease.Jobset,
		Queue:  jobRunLease.Queue,
	}
	return &SubmitJob{
		Meta: SubmitJobMetaInfo{
			JobRunMeta:      runMeta,
			Owner:           jobRunLease.User,
			OwnershipGroups: jobRunLease.Groups,
		},
		Pod:       pod,
		Ingresses: util2.ExtractIngresses(jobRunLease, pod, podDefaults.Ingress),
		Services:  util2.ExtractServices(jobRunLease, pod),
	}, nil
}
