package test

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

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
)

const integrationEnabledEnvVar = "INTEGRATION_ENABLED"

func TestCanSubmitJob_ReceivingAllExpectedEvents(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		createQueue(submitClient, jobRequest, t)

		receivedEvents := submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)

		assert.True(t, receivedEvents[domain.Queued])
		assert.True(t, receivedEvents[domain.Leased])
		assert.True(t, receivedEvents[domain.Running])
		assert.True(t, receivedEvents[domain.Succeeded])
	})
}

func TestCanSubmitJob_IncorrectJobMountFails(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		pod := jobRequest.JobRequestItems[0].PodSpec
		pod.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "config", MountPath: "/test"}}
		pod.Volumes = []v1.Volume{{
			Name: "config",
			VolumeSource: v1.VolumeSource{
				ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: "missing"}}}}}

		createQueue(submitClient, jobRequest, t)

		receivedEvents := submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)
		assert.True(t, receivedEvents[domain.Failed])
	})
}

func TestCanNotSubmitJobToDeletedQueue(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		createQueue(submitClient, jobRequest, t)

		err := client.DeleteQueue(submitClient, jobRequest.Queue)
		assert.NoError(t, err)

		_, err = client.SubmitJobs(submitClient, jobRequest)
		assert.Error(t, err)
	})
}

func TestCanSubmitJob_ArmdactlWatchExitOnInactive(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)
	connDetails := connectionDetails()
	client.WithConnection(connDetails, func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("personal-anonymous")
		createQueue(submitClient, jobRequest, t)

		cmd := exec.Command("armadactl", "--armadaUrl="+connDetails.ArmadaUrl, "watch", "--exit-if-inactive", jobRequest.Queue, jobRequest.JobSetId)
		err := cmd.Start()
		assert.NoError(t, err)

		submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)
		err = cmd.Wait()
		assert.NoError(t, err)
	})
}

func TestCanSubmitJob_KubernetesNamespacePermissionsAreRespected(t *testing.T) {
	skipIfIntegrationEnvNotPresent(t)

	client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) {
		submitClient := api.NewSubmitClient(connection)
		eventsClient := api.NewEventClient(connection)

		jobRequest := createJobRequest("default")
		createQueue(submitClient, jobRequest, t)

		receivedEvents := submitJobsAndWatch(t, submitClient, eventsClient, jobRequest)
		assert.True(t, receivedEvents[domain.Failed], "Anonymous user should not have access to default namespace.")
	})
}

func connectionDetails() *client.ApiConnectionDetails {
	connectionDetails := &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}
	return connectionDetails
}

func submitJobsAndWatch(t *testing.T, submitClient api.SubmitClient, eventsClient api.EventClient, jobRequest *api.JobSubmitRequest) map[domain.JobStatus]bool {
	_, err := client.SubmitJobs(submitClient, jobRequest)
	assert.NoError(t, err)
	receivedEvents := make(map[domain.JobStatus]bool)
	timeout, _ := context.WithTimeout(context.Background(), 60*time.Second)
	client.WatchJobSet(eventsClient, jobRequest.Queue, jobRequest.JobSetId, true, timeout, func(state *domain.WatchContext, e api.Event) bool {
		currentStatus := state.GetJobInfo(e.GetJobId()).Status
		receivedEvents[currentStatus] = true

		if currentStatus == domain.Succeeded || currentStatus == domain.Failed || currentStatus == domain.Cancelled {
			return true
		}

		return false
	})
	assert.False(t, hasTimedOut(timeout), "Test timed out waiting for expected events")
	return receivedEvents
}

func createQueue(submitClient api.SubmitClient, jobRequest *api.JobSubmitRequest, t *testing.T) {
	err := client.CreateQueue(submitClient, &api.Queue{Name: jobRequest.Queue, PriorityFactor: 1})
	assert.Nil(t, err)
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
				Priority: 0,
			},
		},
	}
}
