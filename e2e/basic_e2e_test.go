package e2e

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/client/service"
	util2 "github.com/G-Research/k8s-batch/internal/client/util"
	"github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"
)

func TestCanSubmitJob_ReceivingAllExpectedEvents(t *testing.T) {
	jobRequest := createJobRequest()
	connectionDetails := &domain.ArmadaApiConnectionDetails{
		Url: "localhost:50051",
	}

	util2.WithConnection(connectionDetails, func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)

		err := service.CreateQueue(submitClient, &api.Queue{Name: jobRequest.Queue, Priority: 1})
		assert.Empty(t, err)

		_, err = service.SubmitJob(submitClient, jobRequest)
		assert.Empty(t, err)

		receivedEvents := make(map[service.JobStatus] bool)

		eventsClient := api.NewEventClient(connection)

		//TODO add timeout

		service.WatchJobSet(eventsClient, jobRequest.JobSetId, func(state map[string]*service.JobInfo, e api.Event) bool {
			currentStatus := state[e.GetJobId()].Status
			receivedEvents[currentStatus] = true

			if currentStatus == service.Succeeded || currentStatus == service.Failed || currentStatus == service.Cancelled {
				return true
			}

			return false
		})

		assert.True(t, receivedEvents[service.Succeeded])
	})
}

func createJobRequest() *api.JobRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	return &api.JobRequest{
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "container1",
				Image: "mcr.microsoft.com/dotnet/core/runtime:2.2",
				Args:  []string{"sleep", "10s"},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
					Limits: v1.ResourceList{"cpu": cpu, "memory": memory},
				},
			},
			},
		},
		JobSetId: util.NewULID(),
		Priority: 0,
		Queue:    "test",
	}
}