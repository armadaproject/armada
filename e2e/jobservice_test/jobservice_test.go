package lookout_ingester_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/api/jobservice"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getJobserviceConn() (*grpc.ClientConn, error) {
	return grpc.Dial("dns:localhost:60003", grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func getArmadaApiConnectionDetails() *client.ApiConnectionDetails {
	return &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}
}

func createJobRequest(namespace string, args []string) *api.JobSubmitRequest {
	cpu, _ := resource.ParseQuantity("80m")
	memory, _ := resource.ParseQuantity("50Mi")
	return &api.JobSubmitRequest{
		Queue:    "personal-anonymous",
		JobSetId: util.NewULID(),
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Namespace: namespace,
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "container1",
							Image:   "alpine:3.10",
							Command: []string{"/bin/sh", "-c"},
							Args:    args,
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

// TODO: Copy paste of func from e2e/basic_test/basic_test.go, refactor
func createQueue(submitClient api.SubmitClient, jobRequest *api.JobSubmitRequest, t *testing.T) {
	err := client.CreateQueue(submitClient, &api.Queue{Name: jobRequest.Queue, PriorityFactor: 1})
	require.NoError(t, err)
}

func TestJobserviceJobStatus(t *testing.T) {
	err := client.WithConnection(getArmadaApiConnectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)

		err := client.CreateQueue(submitClient, &api.Queue{Name: "personal-anonymous", PriorityFactor: 1})
		if err != nil {
			assert.Contains(t, err.Error(), "already exists")
		}

		cc, err := getJobserviceConn()
		assert.Nil(t, err)
		assert.NotNil(t, cc)

		numJobs := 20

		wg := sync.WaitGroup{}
		wg.Add(numJobs)

		jobWatchSleepTime, err := time.ParseDuration("3s")
		assert.Nil(t, err)
		timeToWait, err := time.ParseDuration("30s")
		assert.Nil(t, err)

		type JobInfo struct {
			JobId    string
			JobSetId string
		}
		jobInfo := make([]JobInfo, numJobs)

		client.WithJobServiceClient(cc, func(jsClient jobservice.JobServiceClient) error {
			// Submit several jobs to armada.
			for i := 0; i < numJobs; i++ {
				jobRequest := createJobRequest(
					"personal-anonymous",
					[]string{"sleep 2"},
				)

				submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
				require.NoError(t, err)
				assert.Equal(t, len(submitResponse.JobResponseItems), 1)
				jobInfo[i].JobId = submitResponse.JobResponseItems[0].JobId
				jobInfo[i].JobSetId = jobRequest.JobSetId
				fmt.Printf("%+v\n", jobInfo[i])
				assert.Empty(t, submitResponse.JobResponseItems[0].Error)
			}

			start := time.Now()
			deadline := start.Add(timeToWait)

			// Launch a goroutine to query the jobservice about each job.
			for i := 0; i < numJobs; i++ {
				go func(jobNum int) {
					defer wg.Done()
					for {
						resp, err := jsClient.GetJobStatus(context.Background(), &jobservice.JobServiceRequest{
							JobId:    jobInfo[jobNum].JobId,
							JobSetId: jobInfo[jobNum].JobSetId,
							Queue:    "personal-anonymous",
						})
						assert.Nil(t, err)
						if err != nil {
							fmt.Println(err.Error())
						}
						if resp != nil {
							fmt.Printf("Job #%d: %s\n", jobNum, resp.State.String())
						}
						time.Sleep(jobWatchSleepTime)
						if time.Now().After(deadline) {
							resp, err := jsClient.GetJobStatus(context.Background(), &jobservice.JobServiceRequest{
								JobId:    jobInfo[jobNum].JobId,
								JobSetId: jobInfo[jobNum].JobSetId,
								Queue:    "personal-anonymous",
							})
							assert.Nil(t, err)
							assert.Equal(t, jobservice.JobServiceResponse_SUCCEEDED, resp.State)
							fmt.Println(resp.State.String())
							break
						}
					}
				}(i)
			}
			wg.Wait()
			return nil
		})
		return nil
	})
	assert.Nil(t, err)
}
