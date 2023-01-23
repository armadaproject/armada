package job

import (
	"github.com/armadaproject/armada/internal/executor/configuration"
	util2 "github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
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

	return &SubmitJob{
		Meta: SubmitJobMetaInfo{
			JobId:           apiJob.Id,
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
	podDefaults *configuration.PodDefaults) (*SubmitJob, error) {
	pod, err := util2.CreatePodFromExecutorApiJob(jobRunLease, podDefaults)
	if err != nil {
		return nil, err
	}

	return &SubmitJob{
		Meta: SubmitJobMetaInfo{
			JobId:           jobRunLease.Job.JobId.String(),
			Owner:           jobRunLease.User,
			OwnershipGroups: jobRunLease.Groups,
		},
		Pod:       pod,
		Ingresses: util2.ExtractIngresses(jobRunLease, pod, podDefaults.Ingress),
		Services:  util2.ExtractServices(jobRunLease, pod),
	}, nil
}
