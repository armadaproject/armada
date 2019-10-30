package test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/client/domain"
	"github.com/G-Research/armada/internal/client/service"
	util2 "github.com/G-Research/armada/internal/client/util"
	"github.com/G-Research/armada/internal/common/util"
)

const integrationEnabledEnvVar = "INTEGRATION_ENABLED"

func TestCanSubmitJob_ReceivingAllExpectedEvents(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	jobRequest := createJobRequest()
	connectionDetails := &domain.ArmadaApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}

	util2.WithConnection(connectionDetails, func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)

		err := service.CreateQueue(submitClient, &api.Queue{Name: jobRequest.Queue, PriorityFactor: 1})
		assert.Nil(t, err)

		_, err = service.SubmitJob(submitClient, jobRequest)
		assert.Nil(t, err)

		receivedEvents := make(map[service.JobStatus]bool)

		eventsClient := api.NewEventClient(connection)

		timeout, _ := context.WithTimeout(context.Background(), 30*time.Second)

		service.WatchJobSet(eventsClient, jobRequest.JobSetId, true, timeout, func(state map[string]*service.JobInfo, e api.Event) bool {
			currentStatus := state[e.GetJobId()].Status
			receivedEvents[currentStatus] = true

			if currentStatus == service.Succeeded || currentStatus == service.Failed || currentStatus == service.Cancelled {
				return true
			}

			return false
		})
		assert.False(t, hasTimedOut(timeout), "Test timed out waiting for expected events")

		assert.True(t, receivedEvents[service.Queued])
		assert.True(t, receivedEvents[service.Leased])
		assert.True(t, receivedEvents[service.Running])
		assert.True(t, receivedEvents[service.Succeeded])
	})
}

func skipIfIntegrationEnvNotPresent(t *testing.T) {
	value, exists := os.LookupEnv(integrationEnabledEnvVar)
	if !exists {
		t.Skip(fmt.Sprintf("Skipping as integration enabled env variable (%s) not passed", integrationEnabledEnvVar))
	}

	isEnabled, err := strconv.ParseBool(value)
	if err != nil {
		t.Skip(fmt.Sprintf("Skipping as integration enabled env variable (%s) not with invalid value, must be true or false", integrationEnabledEnvVar))
	}

	if !isEnabled {
		t.Skip(fmt.Sprintf("Skipping as integration enabled env variable (%s) is not set to true", integrationEnabledEnvVar))
	}
}

func hasTimedOut(context context.Context) bool {
	select {
	case <-context.Done():
		return true
	default:
		return false
	}
}

func createJobRequest() *api.JobRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	return &api.JobRequest{
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{{
				Name:  "container1",
				Image: "alpine:3.10",
				Args:  []string{"sleep", "5s"},
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
					Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
				},
			},
			},
		},
		JobSetId: util.NewULID(),
		Priority: 0,
		Queue:    "test",
	}
}
