package basic_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
)

const integrationEnabledEnvVar = "INTEGRATION_ENABLED"

// Location of the armadactl executable, which is needed by some tests.
// Path is relative to this file.
const armadactlExecutable = "../../bin/armadactl"

func TestCanSubmitJob_ReceivingAllExpectedEvents(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		createQueue(submitClient, jobRequest, t)

		statusEvents, _ := submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)

		assert.True(t, statusEvents[domain.Queued])
		assert.True(t, statusEvents[domain.Leased])
		assert.True(t, statusEvents[domain.Running])
		assert.True(t, statusEvents[domain.Succeeded])
		return nil
	})
	assert.NoError(t, err)
}

func TestCanSubmitJob_IncorrectImage_FailsWithoutRetry(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		pod := jobRequest.JobRequestItems[0].PodSpec
		pod.Containers[0].Image = "https://wrongimagename.com" // Image names should be not a url. This will fail immediately with container state InvalidImageName.

		createQueue(submitClient, jobRequest, t)

		statusEvents, allEvents := submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)

		numLeasedEvents := 0
		for _, e := range allEvents {
			_, isLeasedEvent := e.(*api.JobLeasedEvent)
			if isLeasedEvent {
				numLeasedEvents++
			}
		}

		assert.True(t, statusEvents[domain.Failed])
		assert.Equal(t, 1, numLeasedEvents)
		return nil
	})
	assert.NoError(t, err)
}

func TestCanNotSubmitJobToDeletedQueue(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	err := client.WithSubmitClient(connectionDetails(), func(client api.SubmitClient) error {
		jobRequest := createJobRequest("personal-anonymous")
		createQueue(client, jobRequest, t)

		_, err := client.DeleteQueue(context.Background(), &api.QueueDeleteRequest{
			Name: jobRequest.Queue,
		})
		assert.NoError(t, err)

		_, err = client.SubmitJobs(context.Background(), jobRequest)
		assert.Error(t, err)
		return nil
	})
	assert.NoError(t, err)
}

// TODO: Should be an armadactl test.
func TestCanSubmitJob_ArmdactlWatchExitOnInactive(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)
	connDetails := connectionDetails()
	err := client.WithConnection(connDetails, func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		createQueue(submitClient, jobRequest, t)

		cmd := exec.Command(armadactlExecutable, "--armadaUrl="+connDetails.ArmadaUrl, "watch", "--exit-if-inactive", jobRequest.Queue, jobRequest.JobSetId)
		err := cmd.Start()
		assert.NoError(t, err)

		submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)
		err = cmd.Wait()
		assert.NoError(t, err)
		return nil
	})
	assert.NoError(t, err)
}

func TestCanSubmitJob_KubernetesNamespacePermissionsAreRespected(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("default")
		createQueue(submitClient, jobRequest, t)

		statusEvents, _ := submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)
		assert.True(t, statusEvents[domain.Failed], "Anonymous user should not have access to default namespace.")
		return nil
	})
	assert.NoError(t, err)
}

func connectionDetails() *client.ApiConnectionDetails {
	connectionDetails := &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}
	return connectionDetails
}

func submitJobsAndWatch(
	t *testing.T,
	submitClient api.SubmitClient,
	eventsClient api.EventClient,
	jobRequest *api.JobSubmitRequest,
) (map[domain.JobStatus]bool, []api.Event) {
	_, err := client.SubmitJobs(submitClient, jobRequest)
	assert.NoError(t, err)
	statusEvents := make(map[domain.JobStatus]bool)
	timeout, _ := context.WithTimeout(context.Background(), 60*time.Second)
	allEvents := []api.Event{}
	client.WatchJobSet(eventsClient, jobRequest.Queue, jobRequest.JobSetId, true, false, false, false, timeout, func(state *domain.WatchContext, e api.Event) bool {
		allEvents = append(allEvents, e)
		currentStatus := state.GetJobInfo(e.GetJobId()).Status
		statusEvents[currentStatus] = true

		fmt.Printf("Got event %v\n", e)

		if currentStatus == domain.Succeeded || currentStatus == domain.Failed || currentStatus == domain.Cancelled {
			return true
		}

		return false
	})
	assert.False(t, hasTimedOut(timeout), "Test timed out waiting for expected events")
	return statusEvents, allEvents
}

func createQueue(submitClient api.SubmitClient, jobRequest *api.JobSubmitRequest, t *testing.T) {
	err := client.CreateQueue(submitClient, &api.Queue{Name: jobRequest.Queue, PriorityFactor: 1})
	assert.NoError(t, err)
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

func createJobRequest(namespace string) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	return &api.JobSubmitRequest{
		Queue:    "test" + util.NewULID(),
		JobSetId: util.NewULID(),
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Namespace: namespace,
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
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
				Priority: 1,
			},
		},
	}
}
