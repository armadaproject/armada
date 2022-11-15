package lookout_ingester_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
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

		db, err := openPgDbTestConnection()
		assert.NoError(t, err)
		dbTestSetup(db)
		defer dbTestTeardown(db)

		jobUpdateListener := openTestListener(t, "job_update", func(event pq.ListenerEventType, err error) {
			// TODO
		})
		defer jobUpdateListener.Close()

		jobRunUpdateListener := openTestListener(t, "job_run_update", func(event pq.ListenerEventType, err error) {
			// TODO
		})
		defer jobRunUpdateListener.Close()

		submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
		assert.NoError(t, err)
		assert.Equal(t, len(submitResponse.JobResponseItems), 1)

		jobId := submitResponse.JobResponseItems[0].JobId
		errStr := submitResponse.JobResponseItems[0].Error
		assert.Empty(t, errStr)
		assert.NotEmpty(t, jobId)

		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute)
		defer cancelFunc()

		var jobGood bool
		var jobRunGood bool

		for {
			select {
			case <-jobUpdateListener.NotificationChannel():
				// TODO: Abstract this a bit.
				pJob := &PartialJob{}
				row := db.QueryRow("SELECT job_id,state FROM job WHERE job_id = $1 AND state = $2", jobId, repository.JobSucceededOrdinal)
				if err := row.Scan(&pJob.JobId, &pJob.State); err != nil {
					continue
				}
				assert.Equal(t, pJob.JobId, submitResponse.JobResponseItems[0].JobId)
				jobGood = true
			case <-jobRunUpdateListener.NotificationChannel():
				pJobRun := &PartialJobRun{}
				row := db.QueryRow("SELECT run_id,job_id,cluster,succeeded,pod_number FROM job_run WHERE job_id = $1 AND finished IS NOT NULL", jobId)
				if err := row.Scan(&pJobRun.RunId, &pJobRun.JobId, &pJobRun.Cluster, &pJobRun.Succeeded, &pJobRun.PodNumber); err != nil {
					continue
				}
				assert.Equal(t, pJobRun.JobId, submitResponse.JobResponseItems[0].JobId)
				assert.Equal(t, pJobRun.Succeeded, true)
				jobRunGood = true
			case <-ctx.Done():
				return ctx.Err()
			}

			if jobGood && jobRunGood {
				return nil
			}
		}
	})
	assert.NoError(t, err)
}

func TestLookoutIngesterUpdatesPostgresWithJobInfoFailedJob(t *testing.T) {
	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		jobRequest := createJobRequest("personal-anonymous")
		jobRequest.JobRequestItems[0].PodSpec.Containers[0].Image = "https://wrongimagename/"

		createQueue(submitClient, jobRequest, t)

		db, err := openPgDbTestConnection()
		assert.NoError(t, err)
		dbTestSetup(db)
		defer dbTestTeardown(db)

		jobUpdateListener := openTestListener(t, "job_update", func(event pq.ListenerEventType, err error) {
			// TODO
		})
		defer jobUpdateListener.Close()

		jobRunUpdateListener := openTestListener(t, "job_run_update", func(event pq.ListenerEventType, err error) {
			// TODO
		})
		defer jobRunUpdateListener.Close()

		submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
		assert.NoError(t, err)
		assert.Equal(t, len(submitResponse.JobResponseItems), 1)

		jobId := submitResponse.JobResponseItems[0].JobId
		errStr := submitResponse.JobResponseItems[0].Error
		assert.Empty(t, errStr)
		assert.NotEmpty(t, jobId)

		ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute)
		defer cancelFunc()

		var jobGood bool
		var jobRunGood bool

		for {
			select {
			case <-jobUpdateListener.NotificationChannel():
				// TODO: Abstract this a bit.
				pJob := &PartialJob{}
				row := db.QueryRow("SELECT job_id,state FROM job WHERE job_id = $1 AND state = $2", jobId, repository.JobFailedOrdinal)
				if err := row.Scan(&pJob.JobId, &pJob.State); err != nil {
					continue
				}
				assert.Equal(t, pJob.JobId, submitResponse.JobResponseItems[0].JobId)
				jobGood = true
			case <-jobRunUpdateListener.NotificationChannel():
				pJobRun := &PartialJobRun{}
				row := db.QueryRow("SELECT run_id,job_id,cluster,succeeded,pod_number FROM job_run WHERE job_id = $1 AND finished IS NOT NULL", jobId)
				if err := row.Scan(&pJobRun.RunId, &pJobRun.JobId, &pJobRun.Cluster, &pJobRun.Succeeded, &pJobRun.PodNumber); err != nil {
					continue
				}
				assert.Equal(t, pJobRun.JobId, submitResponse.JobResponseItems[0].JobId)
				assert.Equal(t, pJobRun.Succeeded, false)
				jobRunGood = true
			case <-ctx.Done():
				return ctx.Err()
			}

			if jobGood && jobRunGood {
				return nil
			}
		}
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

const testPGConnectionString = "postgresql://postgres:psw@localhost:5432/postgres?sslmode=disable"

func openPgDbTestConnection() (*sql.DB, error) {
	return sql.Open("postgres", testPGConnectionString)
}

func openTestListener(t *testing.T, channelName string, callback func(pq.ListenerEventType, error)) *pq.Listener {
	listener := pq.NewListener(testPGConnectionString, 10*time.Second, time.Minute, callback)
	err := listener.Listen(channelName)
	assert.NoError(t, err)
	return listener
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

// TODO: Copy paste of func from e2e/basic_test/basic_test.go, refactor
func createQueue(submitClient api.SubmitClient, jobRequest *api.JobSubmitRequest, t *testing.T) {
	err := client.CreateQueue(submitClient, &api.Queue{Name: jobRequest.Queue, PriorityFactor: 1})
	assert.NoError(t, err)
}

// NOTE Some important points on trugger functions
// They must:
//	- return trigger in their function signature
//	- return OLD, NEW, or NULL (In the case of AFTER triggers, NULL is most logical)
//  Also note that PERFORM discards results we don't want/use, and is only available
//  when using plpgsql language.
const triggerNotifyFuncSql = `CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$ 
	BEGIN 
		PERFORM pg_notify('%s', 'update') as notify; 
		RETURN NULL; 
	END; 
	$$ LANGUAGE plpgsql;
`

func createNotifyFuncs(db *sql.DB) {
	for _, trigger := range testTriggers {
		_, err := db.Exec(fmt.Sprintf(triggerNotifyFuncSql, trigger.Func(), trigger.NotifyChannel()))
		if err != nil {
			panic(err)
		}
	}
}

// This will trigger once for each statement that updates a given table.
const updateTriggerSql = `CREATE TRIGGER %s
	AFTER UPDATE ON %s
	FOR EACH STATEMENT
	EXECUTE FUNCTION %s();`

type triggerInfo struct {
	Table string
}

func (ti *triggerInfo) Name() string {
	return fmt.Sprintf("%s_update_trigger", ti.Table)
}

func (ti *triggerInfo) Func() string {
	return fmt.Sprintf("notify_%s_update", ti.Table)
}

func (ti *triggerInfo) NotifyChannel() string {
	return fmt.Sprintf("%s_update", ti.Table)
}

var testTriggers = []triggerInfo{
	{"job"},
	{"job_run"},
}

func dbTestSetup(db *sql.DB) {
	// Just in case they're hanging around from a previous test run.
	dropTestTriggers(db)
	createNotifyFuncs(db)
	createTestTriggers(db)
}

func dbTestTeardown(db *sql.DB) {
	dropTestTriggers(db)
}

func createTestTriggers(db *sql.DB) {
	for _, trigger := range testTriggers {
		_, err := db.Exec(fmt.Sprintf(updateTriggerSql, trigger.Name(), trigger.Table, trigger.Func()))
		if err != nil {
			panic(err)
		}
	}
}

func dropTestTriggers(db *sql.DB) {
	for _, trigger := range testTriggers {
		// We don't care about the result. Just drop them.
		db.Exec(fmt.Sprintf("DROP TRIGGER %s on %s", trigger.Name(), trigger.Table))
	}
}
