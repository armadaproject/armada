package lookout_ingester_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

var defaultWaits struct {
	PollWait     time.Duration
	ExpiresAfter time.Duration
}

func init() {
	pollWait, _ := time.ParseDuration("1s")
	expiresAfter, _ := time.ParseDuration("1m")

	defaultWaits.PollWait = pollWait
	defaultWaits.ExpiresAfter = expiresAfter
}

func TestLookoutIngesterUpdatesPostgresWithJobInfo(t *testing.T) {
	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		jobRequest := createJobRequest("personal-anonymous")

		createQueue(submitClient, jobRequest, t)

		submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
		assert.NoError(t, err)
		assert.Equal(t, len(submitResponse.JobResponseItems), 1)

		jobId := submitResponse.JobResponseItems[0].JobId
		errStr := submitResponse.JobResponseItems[0].Error
		assert.Empty(t, errStr)
		assert.NotEmpty(t, jobId)

		db, err := openPgDbTestConnection()
		assert.NoError(t, err)

		pJobRun, err := awaitJobRunEntry(db, jobId, defaultWaits.PollWait, defaultWaits.ExpiresAfter)
		assert.NoError(t, err)
		assert.NotNil(t, pJobRun)
		assert.Equal(t, pJobRun.JobId, submitResponse.JobResponseItems[0].JobId)

		pJob, err := awaitJobEntry(db, jobId, defaultWaits.PollWait, defaultWaits.ExpiresAfter, repository.JobSucceededOrdinal)
		assert.NoError(t, err)
		assert.NotNil(t, pJob)
		assert.Equal(t, pJob.JobId, submitResponse.JobResponseItems[0].JobId)

		return nil
	})
	assert.NoError(t, err)
}

func TestLookoutIngesterUpdatesPostgresWithJobInfoFailedJob(t *testing.T) {
	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		jobRequest := createJobRequest("personal-anonymous")
		jobRequest.JobRequestItems[0].PodSpec.Containers[0].Image = "https://wrongimagename/"

		createQueue(submitClient, jobRequest, t)

		submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
		assert.NoError(t, err)
		assert.Equal(t, len(submitResponse.JobResponseItems), 1)

		jobId := submitResponse.JobResponseItems[0].JobId
		errStr := submitResponse.JobResponseItems[0].Error
		assert.Empty(t, errStr)
		assert.NotEmpty(t, jobId)

		db, err := openPgDbTestConnection()
		assert.NoError(t, err)

		pJobRun, err := awaitJobRunEntry(db, jobId, defaultWaits.PollWait, defaultWaits.ExpiresAfter)
		assert.NoError(t, err)
		assert.NotNil(t, pJobRun)
		assert.Equal(t, pJobRun.JobId, submitResponse.JobResponseItems[0].JobId)
		assert.Equal(t, pJobRun.Succeeded, false)

		pJob, err := awaitJobEntry(db, jobId, defaultWaits.PollWait, defaultWaits.ExpiresAfter, repository.JobFailedOrdinal)
		assert.NoError(t, err)
		assert.NotNil(t, pJob)
		assert.Equal(t, pJob.JobId, submitResponse.JobResponseItems[0].JobId)
		assert.Equal(t, repository.JobFailedOrdinal, pJob.State)

		return nil
	})
	assert.NoError(t, err)
}

type PartialJob struct {
	JobId string
	State int
}

func awaitJobEntry(db *sql.DB, jobId string, pollWait time.Duration, expiresAfter time.Duration, expectedState int) (*PartialJob, error) {
	timeout := time.Now().Add(expiresAfter)
	pJob := &PartialJob{}

	for {
		row := db.QueryRow("SELECT job_id,state FROM job WHERE job_id = $1 AND state = $2", jobId, expectedState)
		if err := row.Scan(&pJob.JobId, &pJob.State); err != nil {
			if err == sql.ErrNoRows && time.Now().After(timeout) {
				return nil, fmt.Errorf("Timed out waiting for job to appear in postgres in the expected state")
			} else if err == sql.ErrNoRows {
				time.Sleep(pollWait)
				continue
			}
			return nil, err
		}
		return pJob, nil
	}
}

type PartialJobRun struct {
	RunId     string
	JobId     string
	Cluster   string
	Succeeded bool
	PodNumber int
}

func awaitJobRunEntry(db *sql.DB, jobId string, pollWait time.Duration, expiresAfter time.Duration) (*PartialJobRun, error) {
	timeout := time.Now().Add(expiresAfter)
	pJobRun := &PartialJobRun{}

	for {
		row := db.QueryRow("SELECT run_id,job_id,cluster,succeeded,pod_number FROM job_run WHERE job_id = $1 AND finished IS NOT NULL", jobId)
		if err := row.Scan(&pJobRun.RunId, &pJobRun.JobId, &pJobRun.Cluster, &pJobRun.Succeeded, &pJobRun.PodNumber); err != nil {
			if err == sql.ErrNoRows && time.Now().After(timeout) {
				return nil, fmt.Errorf("Timed out waiting for job run to appear in postgres")
			} else if err == sql.ErrNoRows {
				time.Sleep(pollWait)
				continue
			}
			return nil, err
		}
		return pJobRun, nil
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
