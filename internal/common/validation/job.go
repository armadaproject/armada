package validation

import (
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func CreateJobs(request *api.JobSubmitRequest, principal authorization.Principal) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0, len(request.JobRequestItems))

	if request.JobSetId == "" {
		return nil, fmt.Errorf("job set is not specified")
	}

	if request.Queue == "" {
		return nil, fmt.Errorf("queue is not specified")
	}

	for i, item := range request.JobRequestItems {

		e := ValidatePodSpec(item.PodSpec)
		if e != nil {
			return nil, fmt.Errorf("error validating pod spec of job with index %v: %v", i, e)
		}

		namespace := item.Namespace
		if namespace == "" {
			namespace = "default"
		}

		j := &api.Job{
			Id:       util.NewULID(),
			Queue:    request.Queue,
			JobSetId: request.JobSetId,

			Namespace:   namespace,
			Labels:      item.Labels,
			Annotations: item.Annotations,

			RequiredNodeLabels: item.RequiredNodeLabels,

			Priority: item.Priority,

			PodSpec: item.PodSpec,
			Created: time.Now(),
			Owner:   principal.GetName(),
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}
