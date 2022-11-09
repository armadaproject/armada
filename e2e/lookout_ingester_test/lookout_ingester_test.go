package lookout_ingester_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestLookoutIngesterUpdatesPostgres(t *testing.T) {
	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		jobRequest := createJobRequest("personal-anonymous")

		// createQueue
		createQueue(submitClient, jobRequest, t)

		// Submit job
		submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
		assert.NoError(t, err)
		assert.Equal(t, len(submitResponse.JobResponseItems), 1)

		// wait for rows to show up in psql db. to some limit.
		// jobRows, err := awaitJobEntries()
		db, err := openPgDbTestConnection()
		assert.NoError(t, err)

		jobId := submitResponse.JobResponseItems[0].JobId
		errStr := submitResponse.JobResponseItems[0].Error
		assert.Empty(t, errStr)
		assert.NotEmpty(t, jobId)

		pJob, err := awaitJobEntry(t, db, jobId)
		assert.NotNil(t, pJob)
		assert.NoError(t, err)
		assert.Equal(t, pJob.JobId, submitResponse.JobResponseItems[0].JobId)

		return nil
	})
	assert.NoError(t, err)
}

type PartialJob struct {
	JobId string
	State int
}

func awaitJobEntry(t *testing.T, db *sql.DB, jobId string) (*PartialJob, error) {
	oneMinute, err := time.ParseDuration("1m")
	assert.NoError(t, err)
	oneSecond, err := time.ParseDuration("1s")
	assert.NoError(t, err)

	timeout := time.Now().Add(oneMinute)
	pJob := &PartialJob{}

	for {
		row := db.QueryRow("SELECT job_id,state FROM job WHERE job_id = $1", jobId)
		if err = row.Scan(&pJob.JobId, &pJob.State); err != nil {
			if err == sql.ErrNoRows && time.Now().After(timeout) {
				return nil, fmt.Errorf("Timed out waiting for job to appear in postgres")
			} else if err == sql.ErrNoRows {
				time.Sleep(oneSecond)
				continue
			}
			return nil, err
		}
		return pJob, nil
	}
}

func connectionDetails() *client.ApiConnectionDetails {
	connectionDetails := &client.ApiConnectionDetails{
		ArmadaUrl: "localhost:50051",
	}
	return connectionDetails
}

func openPgDbTestConnection() (*sql.DB, error) {
	return sql.Open("postgres", "postgresql://postgres:psw@localhost:5432/postgres?sslmode=disable")
}

// TODO: Copy paste of func from e2e/basic_test/basic_test.go, refactor
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
							Args:  []string{"sleep", "1s"},
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
	assert.NoError(t, err)
}
