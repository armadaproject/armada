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
		jobRequest := createJobRequest("personal-anonymous", []string{"sleep", "5"})

		createQueue(submitClient, jobRequest, t)

		db, err := openPgDbTestConnection()
		assert.NoError(t, err)
		dbTestSetup(db)
		defer dbTestTeardown(db)

		jobUpdateListener := openTestListener(t, "job_update")
		defer jobUpdateListener.Close()

		jobRunUpdateListener := openTestListener(t, "job_run_update")
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

		jst := NewJobStateTracker(jobId, db)
		jrt := NewJobRunTracker(jobId, db)

		for {
			select {
			case notification := <-jobUpdateListener.NotificationChannel():
				if notification.Extra == jobId {
					err := jst.Scan()
					assert.NoError(t, err)
				}
			case notification := <-jobRunUpdateListener.NotificationChannel():
				if notification.Extra == jobId {
					err := jrt.Scan()
					assert.NoError(t, err)
				}
			case <-ctx.Done():
				return ctx.Err()
			}

			// We're gating on things that should be satisfied by entries in
			// both job and job_run tables.
			if jst.SeenStates[repository.JobSucceededOrdinal] && jrt.OneRun().Finished.Valid {
				assert.True(t, jst.SeenStates[repository.JobQueuedOrdinal])
				assert.True(t, jst.SeenStates[repository.JobPendingOrdinal])
				assert.True(t, jst.SeenStates[repository.JobRunningOrdinal])
				assert.True(t, jst.SeenStates[repository.JobSucceededOrdinal])

				assert.Len(t, jrt.JobRuns, 1)
				pJobRun := jrt.OneRun()
				assert.True(t, pJobRun.Succeeded.Valid)
				assert.True(t, pJobRun.Succeeded.Bool)
				assert.True(t, pJobRun.Finished.Valid)
				return nil
			}
		}
	})
	assert.NoError(t, err)
}

func TestLookoutIngesterUpdatesPostgresWithJobInfoFailedJob(t *testing.T) {
	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		jobRequest := createJobRequest("personal-anonymous", []string{"sleep", "5"})
		jobRequest.JobRequestItems[0].PodSpec.Containers[0].Image = "https://wrongimagename/"

		createQueue(submitClient, jobRequest, t)

		db, err := openPgDbTestConnection()
		assert.NoError(t, err)
		dbTestSetup(db)
		defer dbTestTeardown(db)

		jobUpdateListener := openTestListener(t, "job_update")
		defer jobUpdateListener.Close()

		jobRunUpdateListener := openTestListener(t, "job_run_update")
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

		jst := NewJobStateTracker(jobId, db)
		jrt := NewJobRunTracker(jobId, db)

		for {
			select {
			case notification := <-jobUpdateListener.NotificationChannel():
				if notification.Extra == jobId {
					err := jst.Scan()
					assert.NoError(t, err)
				}
			case notification := <-jobRunUpdateListener.NotificationChannel():
				if notification.Extra == jobId {
					err := jrt.Scan()
					assert.NoError(t, err)
				}
			case <-ctx.Done():
				return ctx.Err()
			}

			// We're gating on things that should be satisfied by entries in
			// both job and job_run tables.
			if jst.SeenStates[repository.JobFailedOrdinal] && jrt.OneRun().Finished.Valid {
				assert.True(t, jst.SeenStates[repository.JobQueuedOrdinal])
				assert.True(t, jst.SeenStates[repository.JobPendingOrdinal])
				assert.True(t, jst.SeenStates[repository.JobFailedOrdinal])

				assert.Len(t, jrt.JobRuns, 1)
				pJobRun := jrt.OneRun()
				assert.True(t, pJobRun.Succeeded.Valid)
				assert.False(t, pJobRun.Succeeded.Bool)
				assert.True(t, pJobRun.Finished.Valid)

				return nil
			}
		}
	})
	assert.NoError(t, err)
}

type JobStateTracker struct {
	JobId      string
	SeenStates map[int]bool

	db *sql.DB
}

func NewJobStateTracker(jobId string, db *sql.DB) *JobStateTracker {
	return &JobStateTracker{
		JobId:      jobId,
		SeenStates: make(map[int]bool),
		db:         db,
	}
}

func (jst *JobStateTracker) Scan() error {
	pJob := &PartialJob{}
	row := jst.db.QueryRow("SELECT job_id,state FROM job WHERE job_id = $1", jst.JobId)
	if err := row.Scan(&pJob.JobId, &pJob.State); err != nil {
		return err
	}
	jst.AddState(pJob.State)
	return nil
}

func (jst *JobStateTracker) AddState(state int) {
	jst.SeenStates[state] = true
}

type PartialJob struct {
	JobId string
	State int
}

type PartialJobRun struct {
	RunId     string
	JobId     string
	Cluster   string
	Succeeded sql.NullBool
	PodNumber int
	Finished  sql.NullTime
}

type JobRunTracker struct {
	JobId   string
	JobRuns map[string]*PartialJobRun

	db *sql.DB
}

func NewJobRunTracker(jobId string, db *sql.DB) *JobRunTracker {
	return &JobRunTracker{
		JobId:   jobId,
		JobRuns: make(map[string]*PartialJobRun),
		db:      db,
	}
}

func (jrt *JobRunTracker) Scan() error {
	pJobRun := &PartialJobRun{}
	rows, err := jrt.db.Query("SELECT run_id,job_id,cluster,succeeded,pod_number,finished FROM job_run WHERE job_id = $1", jrt.JobId)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		pJobRun = &PartialJobRun{}
		if err := rows.Scan(&pJobRun.RunId, &pJobRun.JobId, &pJobRun.Cluster, &pJobRun.Succeeded, &pJobRun.PodNumber, &pJobRun.Finished); err != nil {
			return err
		}
		jrt.JobRuns[pJobRun.RunId] = pJobRun
	}

	return nil
}

func (jrt *JobRunTracker) OneRun() *PartialJobRun {
	for _, pjr := range jrt.JobRuns {
		return pjr
	}
	return nil
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

func openTestListener(t *testing.T, channelName string) *pq.Listener {
	listener := pq.NewListener(testPGConnectionString, 10*time.Second, time.Minute,
		generateNotificationConnectionEventHandler(channelName))
	err := listener.Listen(channelName)
	assert.NoError(t, err)
	return listener
}

// TODO: Copy paste of func from e2e/basic_test/basic_test.go, refactor
func createJobRequest(namespace string, args []string) *api.JobSubmitRequest {
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
							Args:  args, //[]string{"sleep", "5s"},
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

// NOTE Some important points on trigger functions
// They must:
//	- return trigger in their function signature
//	- return OLD, NEW, or NULL (In the case of AFTER triggers, NULL is most logical)
//  Also note that PERFORM discards results we don't want/use, and is only available
//  when using plpgsql language.
const triggerNotifyFuncSql = `CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$ 
	BEGIN 
		PERFORM pg_notify('%s', NEW.job_id) as notify; 
		RETURN NEW; 
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

// This will trigger for each row updated on a table.
const updateTriggerSql = `CREATE TRIGGER %s
	AFTER INSERT OR UPDATE ON %s
	FOR EACH ROW
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

func generateNotificationConnectionEventHandler(channelName string) func(pq.ListenerEventType, error) {
	return func(event pq.ListenerEventType, err error) {
		switch event {
		case pq.ListenerEventDisconnected:
			panic(fmt.Sprintf("Listener %q got disconnected: %s", channelName, err.Error()))
		case pq.ListenerEventConnectionAttemptFailed:
			panic(fmt.Sprintf("Listener %q could not connect: %s", channelName, err.Error()))
		}
	}
}
