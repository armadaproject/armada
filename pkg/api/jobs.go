package api

import (
	time "time"

	v1 "k8s.io/api/core/v1"
)

type JobsFromSubmitRequestFn func(request *JobSubmitRequest, owner string, ownershipGroups []string) []*Job

type updatePodSpec func(*v1.PodSpec)
type newJobID func() string
type newTime func() time.Time

func JobsFromSubmitRequest(updatePodSpec updatePodSpec, newJobID newJobID, now newTime) JobsFromSubmitRequestFn {
	return func(request *JobSubmitRequest, owner string, ownershipGroups []string) []*Job {
		jobs := make([]*Job, 0, len(request.JobRequestItems))

		for _, item := range request.JobRequestItems {
			if item.Namespace == "" {
				item.Namespace = "default"
			}

			for _, podSpec := range item.GetAllPodSpecs() {
				updatePodSpec(podSpec)
			}

			jobs = append(jobs, &Job{
				Id:                       newJobID(),
				ClientId:                 item.ClientId,
				Queue:                    request.Queue,
				JobSetId:                 request.JobSetId,
				Namespace:                item.Namespace,
				Labels:                   item.Labels,
				Annotations:              item.Annotations,
				RequiredNodeLabels:       item.RequiredNodeLabels,
				Ingress:                  item.Ingress,
				Priority:                 item.Priority,
				PodSpec:                  item.PodSpec,
				PodSpecs:                 item.PodSpecs,
				Created:                  now(),
				Owner:                    owner,
				QueueOwnershipUserGroups: ownershipGroups,
			})
		}

		return jobs
	}
}
