package job

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

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

	runMeta := &RunMeta{
		JobId:  apiJob.Id,
		RunId:  "",
		JobSet: apiJob.JobSetId,
		Queue:  apiJob.Queue,
	}

	return &SubmitJob{
		Meta: SubmitJobMeta{
			RunMeta:         runMeta,
			Owner:           apiJob.Owner,
			OwnershipGroups: apiJob.QueueOwnershipUserGroups,
			JobMeta:         apiJob.Meta,
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

	runMeta := &RunMeta{
		JobId:  jobId,
		RunId:  runId,
		JobSet: jobRunLease.Jobset,
		Queue:  jobRunLease.Queue,
	}
	return &SubmitJob{
		Meta: SubmitJobMeta{
			RunMeta:         runMeta,
			Owner:           jobRunLease.User,
			OwnershipGroups: jobRunLease.Groups,
		},
		Pod:       pod,
		Ingresses: util2.ExtractIngresses(jobRunLease, pod, podDefaults.Ingress),
		Services:  util2.ExtractServices(jobRunLease, pod),
	}, nil
}

func ExtractJobRunMeta(pod *v1.Pod) (*RunMeta, error) {
	runId := util2.ExtractJobRunId(pod)
	if runId == "" {
		return nil, fmt.Errorf("job run id is missing")
	}
	jobId := util2.ExtractJobId(pod)
	if jobId == "" {
		return nil, fmt.Errorf("job id is missing")
	}
	queue := util2.ExtractQueue(pod)
	if queue == "" {
		return nil, fmt.Errorf("queue is missing")
	}
	jobSet := util2.ExtractJobSet(pod)
	if jobSet == "" {
		return nil, fmt.Errorf("job set is missing")
	}
	return &RunMeta{
		RunId:  runId,
		JobId:  jobId,
		JobSet: jobSet,
		Queue:  queue,
	}, nil
}
