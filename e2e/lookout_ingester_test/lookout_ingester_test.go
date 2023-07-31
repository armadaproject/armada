package lookout_ingester_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/lookout/repository"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

type JobRequestArgs struct {
	Namespace string
	Args      []string
}

type lookoutIngesterPGTestCase struct {
	Name              string
	JobRequestArgs    *JobRequestArgs
	JobRequestMutator func(*api.JobSubmitRequest) *api.JobSubmitRequest
	TerminalCondition func(*JobStateTracker, *JobRunTracker, *UserAnnotationLookupTracker) bool
	TestAssertions    func(*testing.T, *JobStateTracker, *JobRunTracker, *UserAnnotationLookupTracker)
}

var lookoutIngesterPostgresTestCases = []*lookoutIngesterPGTestCase{
	{
		"JobSucceeds",
		&JobRequestArgs{
			"personal-anonymous",
			[]string{"sleep 5"},
		},
		func(jr *api.JobSubmitRequest) *api.JobSubmitRequest {
			jr.JobRequestItems[0].Annotations = make(map[string]string)
			jr.JobRequestItems[0].Annotations["test"] = "asdf"
			return jr
		},
		func(jst *JobStateTracker, jrt *JobRunTracker, ualt *UserAnnotationLookupTracker) bool {
			return jst.SeenStates[repository.JobSucceededOrdinal] && jrt.OneRun().Finished.Valid
		},
		func(t *testing.T, jst *JobStateTracker, jrt *JobRunTracker, ualt *UserAnnotationLookupTracker) {
			assert.True(t, jst.SeenStates[repository.JobQueuedOrdinal])
			assert.True(t, jst.SeenStates[repository.JobPendingOrdinal])
			assert.True(t, jst.SeenStates[repository.JobRunningOrdinal])
			assert.True(t, jst.SeenStates[repository.JobSucceededOrdinal])

			assert.Len(t, jrt.JobRuns, 1)
			pJobRun := jrt.OneRun()
			assert.True(t, pJobRun.Succeeded.Valid)
			assert.True(t, pJobRun.Succeeded.Bool)
			assert.True(t, pJobRun.Finished.Valid)

			assert.Contains(t, ualt.Annotations, "test")
			assert.Equal(t, ualt.Annotations["test"], "asdf")
		},
	},
	{
		"JobFails",
		&JobRequestArgs{
			"personal-anonymous",
			[]string{"sleep 10; exit 57"},
		},
		func(jr *api.JobSubmitRequest) *api.JobSubmitRequest {
			return jr
		},
		func(jst *JobStateTracker, jrt *JobRunTracker, ualt *UserAnnotationLookupTracker) bool {
			return jst.SeenStates[repository.JobFailedOrdinal] && jrt.OneRun().Finished.Valid
		},
		func(t *testing.T, jst *JobStateTracker, jrt *JobRunTracker, ualt *UserAnnotationLookupTracker) {
			assert.True(t, jst.SeenStates[repository.JobQueuedOrdinal])
			assert.True(t, jst.SeenStates[repository.JobPendingOrdinal])
			assert.True(t, jst.SeenStates[repository.JobRunningOrdinal])
			assert.True(t, jst.SeenStates[repository.JobFailedOrdinal])

			assert.Len(t, jrt.JobRuns, 1)
			pJobRun := jrt.OneRun()
			assert.True(t, pJobRun.Succeeded.Valid)
			assert.False(t, pJobRun.Succeeded.Bool)
			assert.True(t, pJobRun.Finished.Valid)
			assert.True(t, pJobRun.Error.Valid)
			assert.Contains(t, pJobRun.Error.String, "Container container1 failed with exit code 57")
		},
	},
	{
		"UnrunnableJobFails",
		&JobRequestArgs{
			"personal-anonymous",
			[]string{"sleep 5"},
		},
		func(jr *api.JobSubmitRequest) *api.JobSubmitRequest {
			jr.JobRequestItems[0].PodSpec.Containers[0].Image = "https://wrongimagename/"
			return jr
		},
		func(jst *JobStateTracker, jrt *JobRunTracker, ualt *UserAnnotationLookupTracker) bool {
			return jst.SeenStates[repository.JobFailedOrdinal] && jrt.OneRun().Finished.Valid
		},
		func(t *testing.T, jst *JobStateTracker, jrt *JobRunTracker, ualt *UserAnnotationLookupTracker) {
			assert.True(t, jst.SeenStates[repository.JobQueuedOrdinal])
			assert.True(t, jst.SeenStates[repository.JobPendingOrdinal])
			assert.False(t, jst.SeenStates[repository.JobRunningOrdinal])
			assert.True(t, jst.SeenStates[repository.JobFailedOrdinal])

			assert.Len(t, jrt.JobRuns, 1)
			pJobRun := jrt.OneRun()
			assert.True(t, pJobRun.Succeeded.Valid)
			assert.False(t, pJobRun.Succeeded.Bool)
			assert.True(t, pJobRun.Finished.Valid)
		},
	},
}

func TestLookoutIngesterUpdatesPostgresWithJobInfo(t *testing.T) {
	err := client.WithConnection(connectionDetails(), func(connection *grpc.ClientConn) error {
		submitClient := api.NewSubmitClient(connection)
		db, err := openPgDbTestConnection()
		require.NoError(t, err)
		dbTestSetup(db)
		defer dbTestTeardown(db)

		jobUpdateListener := openTestListener(t, "job_update")
		defer jobUpdateListener.Close()

		jobRunUpdateListener := openTestListener(t, "job_run_update")
		defer jobRunUpdateListener.Close()

		annotationUpdateLisener := openTestListener(t, "user_annotation_lookup_update")
		defer annotationUpdateLisener.Close()

		for _, tc := range lookoutIngesterPostgresTestCases {
			t.Run(tc.Name, func(t *testing.T) {
				jobRequest := createJobRequest(
					tc.JobRequestArgs.Namespace,
					tc.JobRequestArgs.Args)

				jobRequest = tc.JobRequestMutator(jobRequest)

				createQueue(submitClient, jobRequest, t)

				submitResponse, err := client.SubmitJobs(submitClient, jobRequest)
				require.NoError(t, err)
				assert.Equal(t, len(submitResponse.JobResponseItems), 1)

				jobId := submitResponse.JobResponseItems[0].JobId
				errStr := submitResponse.JobResponseItems[0].Error
				assert.Empty(t, errStr)
				assert.NotEmpty(t, jobId)

				ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute)
				defer cancelFunc()

				jst := NewJobStateTracker(jobId, db)
				jrt := NewJobRunTracker(jobId, db)
				uslt := NewUserAnnotationLookupTracker(jobId, db)

				for {
					select {
					case notification := <-jobUpdateListener.NotificationChannel():
						if notification.Extra == jobId {
							err := jst.Scan()
							require.NoError(t, err)
						}
					case notification := <-jobRunUpdateListener.NotificationChannel():
						if notification.Extra == jobId {
							err := jrt.Scan()
							require.NoError(t, err)
						}
					case notification := <-annotationUpdateLisener.NotificationChannel():
						if notification.Extra == jobId {
							err := uslt.Scan()
							require.NoError(t, err)
						}
					case <-ctx.Done():
						require.NoError(t, ctx.Err())
						return
					}

					if tc.TerminalCondition(jst, jrt, uslt) {
						tc.TestAssertions(t, jst, jrt, uslt)
						return
					}
				}
			})
		}
		return nil
	})
	require.NoError(t, err)
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
	rows, err := jst.db.Query("SELECT job_id,state FROM job_shadow WHERE job_id = $1", jst.JobId)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&pJob.JobId, &pJob.State); err != nil {
			return err
		}
		jst.AddState(pJob.State)
	}
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
	Error     sql.NullString
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
	rows, err := jrt.db.Query("SELECT run_id,job_id,cluster,succeeded,pod_number,finished,error FROM job_run WHERE job_id = $1", jrt.JobId)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		pJobRun = &PartialJobRun{}
		if err := rows.Scan(&pJobRun.RunId, &pJobRun.JobId, &pJobRun.Cluster, &pJobRun.Succeeded, &pJobRun.PodNumber, &pJobRun.Finished, &pJobRun.Error); err != nil {
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

type UserAnnotationLookupTracker struct {
	JobId       string
	Annotations map[string]string
	db          *sql.DB
}

func NewUserAnnotationLookupTracker(jobId string, db *sql.DB) *UserAnnotationLookupTracker {
	return &UserAnnotationLookupTracker{
		JobId:       jobId,
		Annotations: make(map[string]string),
		db:          db,
	}
}

func (ualt *UserAnnotationLookupTracker) Scan() error {
	rows, err := ualt.db.Query("SELECT key,value FROM user_annotation_lookup WHERE job_id = $1", ualt.JobId)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		if err = rows.Scan(&key, &value); err != nil {
			return err
		}
		ualt.Annotations[key] = value
	}

	return nil
}

const testPGConnectionString = "postgresql://postgres:psw@localhost:5432/postgres?sslmode=disable"

func openPgDbTestConnection() (*sql.DB, error) {
	return sql.Open("postgres", testPGConnectionString)
}

func openTestListener(t *testing.T, channelName string) *pq.Listener {
	listener := pq.NewListener(testPGConnectionString, 10*time.Second, time.Minute,
		generateNotificationConnectionEventHandler(channelName))
	err := listener.Listen(channelName)
	require.NoError(t, err)
	return listener
}

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
							Name:    "container1",
							Image:   "alpine:3",
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

// NOTE Some important points on trigger functions
// They must:
// - return trigger in their function signature
// - return OLD, NEW, or NULL (In the case of AFTER triggers, NULL is most logical)
// Also note that PERFORM discards results we don't want/use, and is only available
// when using plpgsql language.
const triggerNotifyFuncSql = `CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $$ 
	BEGIN 
		INSERT into %s_shadow SELECT NEW.*;
		PERFORM pg_notify('%s', NEW.job_id) as notify; 
		RETURN NEW; 
	END; 
	$$ LANGUAGE plpgsql;
`

func createNotifyFuncs(db *sql.DB) {
	for _, trigger := range testTriggers {
		_, err := db.Exec(fmt.Sprintf(triggerNotifyFuncSql,
			trigger.Func(), trigger.Table, trigger.NotifyChannel()))
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
	{"user_annotation_lookup"},
}

func dbTestSetup(db *sql.DB) {
	// Just in case they're hanging around from a previous test run.
	dropTestTriggers(db)
	dropShadowTables(db)

	createNotifyFuncs(db)
	createTestTriggers(db)
	createShadowTables(db)
}

func dbTestTeardown(db *sql.DB) {
	dropTestTriggers(db)
	dropShadowTables(db)
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
		_, err := db.Exec(fmt.Sprintf("DROP TRIGGER IF EXISTS %s on %s;", trigger.Name(), trigger.Table))
		if err != nil {
			panic(fmt.Sprintf("Encountered error trying to drop trigger: %s", err.Error()))
		}
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

var (
	createShadowTableSql = "CREATE TABLE %s_shadow (LIKE %s INCLUDING ALL, update_num SERIAL);"
	alterShadowTableSql  = "ALTER TABLE %s_shadow DROP CONSTRAINT %s_shadow_pkey, ADD PRIMARY KEY (job_id, update_num);"
)

func createShadowTables(db *sql.DB) {
	for _, trigger := range testTriggers {
		_, err := db.Exec(fmt.Sprintf(createShadowTableSql, trigger.Table, trigger.Table))
		if err != nil {
			panic(err)
		}
		_, err = db.Exec(fmt.Sprintf(alterShadowTableSql, trigger.Table, trigger.Table))
		if err != nil {
			panic(err)
		}
	}
}

func dropShadowTables(db *sql.DB) {
	for _, trigger := range testTriggers {
		db.Exec(fmt.Sprintf("DROP TABLE %s_shadow", trigger.Table)) //nolint:errcheck
	}
}
