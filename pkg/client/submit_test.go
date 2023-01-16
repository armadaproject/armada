package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/api"
)

func TestCreateChunkedSubmitRequests(t *testing.T) {
	requestItems := createJobRequestItems(MaxJobsPerRequest + 1)

	result := CreateChunkedSubmitRequests("queue", "jobsetid", requestItems)

	assert.Equal(t, len(result), 2)
	assert.Equal(t, len(result[0].JobRequestItems), MaxJobsPerRequest)
	assert.Equal(t, len(result[1].JobRequestItems), 1)
}

func TestCreateChunkedSubmitRequests_MaintainsOrderOfJobs(t *testing.T) {
	requestItems := createJobRequestItems(MaxJobsPerRequest + 1)

	result := CreateChunkedSubmitRequests("queue", "jobsetid", requestItems)

	position := 0
	for _, request := range result {
		for i := 0; i < len(request.JobRequestItems); i++ {
			assert.Equal(t, request.JobRequestItems[i], requestItems[position])
			position++
		}
	}
}

func createJobRequestItems(numberOfItems int) []*api.JobSubmitRequestItem {
	requestItems := make([]*api.JobSubmitRequestItem, 0, numberOfItems)

	for i := 0; i < numberOfItems; i++ {
		requestItem := &api.JobSubmitRequestItem{
			Priority: 0,
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Name: fmt.Sprintf("Container%d", i),
					},
				},
			},
		}

		requestItems = append(requestItems, requestItem)
	}

	return requestItems
}
