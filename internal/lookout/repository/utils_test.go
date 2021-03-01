package repository

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/doug-martin/goqu/v9"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
)

var (
	queue        = "queue"
	queue2       = "queue2"
	cluster      = "cluster"
	k8sId1       = util.NewULID()
	k8sId2       = util.NewULID()
	k8sId3       = util.NewULID()
	k8sId4       = util.NewULID()
	k8sId5       = util.NewULID()
	node         = "node"
	someTimeUnix = int64(1612546858)
	someTime     = time.Unix(someTimeUnix, 0)
	ctx          = context.Background()
)

func AssertJobsAreEquivalent(t *testing.T, expected *api.Job, actual *api.Job) {
	t.Helper()
	assert.Equal(t, expected.Id, actual.Id)
	assert.Equal(t, expected.JobSetId, actual.JobSetId)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.Equal(t, expected.Priority, actual.Priority)
	assert.Equal(t, expected.Queue, actual.Queue)
	AssertTimesApproxEqual(t, &expected.Created, &actual.Created)
}

func AssertRunInfosEquivalent(t *testing.T, expected *lookout.RunInfo, actual *lookout.RunInfo) {
	assert.Equal(t, expected.K8SId, actual.K8SId)
	assert.Equal(t, expected.Cluster, actual.Cluster)
	assert.Equal(t, expected.Node, actual.Node)
	assert.Equal(t, expected.Succeeded, actual.Succeeded)
	AssertTimesApproxEqual(t, expected.Created, actual.Created)
	AssertTimesApproxEqual(t, expected.Started, actual.Started)
	AssertTimesApproxEqual(t, expected.Finished, actual.Finished)
	assert.Equal(t, expected.Error, actual.Error)
}

// asserts that two times are equivalent in UTC rounded to nearest millisecond
func AssertTimesApproxEqual(t *testing.T, expected *time.Time, actual *time.Time) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	assert.Equal(t, expected.Round(time.Second).UTC(), actual.Round(time.Second).UTC())
}

func AssertProtoDurationsApproxEqual(t *testing.T, expected *types.Duration, actual *types.Duration) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	expectedDuration, err := types.DurationFromProto(expected)
	assert.Nil(t, err)
	actualDuration, err := types.DurationFromProto(actual)
	assert.Nil(t, err)

	assert.Equal(t, expectedDuration.Round(time.Second), actualDuration.Round(time.Second))
}

type JobSimulator struct {
	t        *testing.T
	jobStore JobRecorder
	job      *api.Job
}

type DummyClock struct {
	t time.Time
}

func (c *DummyClock) Now() time.Time {
	return c.t
}

func NewJobSimulator(t *testing.T, jobStore JobRecorder) *JobSimulator {
	return &JobSimulator{
		t:        t,
		jobStore: jobStore,
		job:      nil,
	}
}

func (js *JobSimulator) CreateJob(queue string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set", time.Now())
}

func (js *JobSimulator) CreateJobWithId(queue string, id string) *JobSimulator {
	return js.CreateJobWithOpts(queue, id, "job-set", time.Now())
}

func (js *JobSimulator) CreateJobWithJobSet(queue string, jobSetId string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), jobSetId, time.Now())
}

func (js *JobSimulator) CreateJobAtTime(queue string, time time.Time) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set", time)
}

func (js *JobSimulator) CreateJobWithOpts(queue string, jobId string, jobSetId string, time time.Time) *JobSimulator {
	js.job = &api.Job{
		Id:          jobId,
		JobSetId:    jobSetId,
		Queue:       queue,
		Namespace:   "nameSpace",
		Labels:      nil,
		Annotations: nil,
		Owner:       "user",
		Priority:    10,
		PodSpec:     &v1.PodSpec{},
		Created:     time,
	}
	assert.NoError(js.t, js.jobStore.RecordJob(js.job))
	return js
}

func (js *JobSimulator) Pending(cluster string, k8sId string) *JobSimulator {
	return js.PendingAtTime(cluster, k8sId, time.Now())
}

func (js *JobSimulator) PendingAtTime(cluster string, k8sId string, time time.Time) *JobSimulator {
	event := &api.JobPendingEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time,
		ClusterId:    cluster,
		KubernetesId: k8sId,
	}
	assert.NoError(js.t, js.jobStore.RecordJobPending(event))
	return js
}

func (js *JobSimulator) Running(cluster string, k8sId string, node string) *JobSimulator {
	return js.RunningAtTime(cluster, k8sId, node, time.Now())
}

func (js *JobSimulator) RunningAtTime(cluster string, k8sId string, node string, time time.Time) *JobSimulator {
	event := &api.JobRunningEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time,
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobRunning(event))
	return js
}

func (js *JobSimulator) Succeeded(cluster string, k8sId string, node string) *JobSimulator {
	return js.SucceededAtTime(cluster, k8sId, node, time.Now())
}

func (js *JobSimulator) SucceededAtTime(cluster string, k8sId string, node string, time time.Time) *JobSimulator {
	event := &api.JobSucceededEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time,
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobSucceeded(event))
	return js
}

func (js *JobSimulator) Failed(cluster string, k8sId string, node string, error string) *JobSimulator {
	return js.FailedAtTime(cluster, k8sId, node, error, time.Now())
}

func (js *JobSimulator) FailedAtTime(cluster string, k8sId string, node string, error string, time time.Time) *JobSimulator {
	failedEvent := &api.JobFailedEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time,
		ClusterId:    cluster,
		Reason:       error,
		ExitCodes:    nil,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobFailed(failedEvent))
	return js
}

func (js *JobSimulator) Cancelled() *JobSimulator {
	return js.CancelledAtTime(time.Now())
}

func (js *JobSimulator) CancelledAtTime(time time.Time) *JobSimulator {
	cancelledEvent := &api.JobCancelledEvent{
		JobId:    js.job.Id,
		JobSetId: js.job.JobSetId,
		Queue:    js.job.Queue,
		Created:  time,
	}
	assert.NoError(js.t, js.jobStore.MarkCancelled(cancelledEvent))
	return js
}

func (js *JobSimulator) UnableToSchedule(cluster string, k8sId string, node string) *JobSimulator {
	return js.UnableToScheduleAtTime(cluster, k8sId, node, time.Now())
}

func (js *JobSimulator) UnableToScheduleAtTime(cluster string, k8sId string, node string, time time.Time) *JobSimulator {
	unableToScheduleEvent := &api.JobUnableToScheduleEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time,
		ClusterId:    cluster,
		Reason:       "unable to schedule reason",
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobUnableToSchedule(unableToScheduleEvent))
	return js
}

func withDatabaseNamed(t *testing.T, dbName string, action func(db *goqu.Database)) {
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()

	assert.Nil(t, err)

	testDb, err := sql.Open("postgres", connectionString+" dbname="+dbName)
	assert.Nil(t, err)

	err = schema.UpdateDatabase(testDb)
	assert.Nil(t, err)

	action(goqu.New("postgres", testDb))
}

func Test_PopulateDb(t *testing.T) {
	withDatabaseNamed(t, "postgres", func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		nJobs := 10000

		wg := &sync.WaitGroup{}
		wg.Add(6)

		runConcurrently(wg, partialApplyMakeJobs(makeQueued, t, jobStore, nJobs))
		runConcurrently(wg, partialApplyMakeJobs(makePending, t, jobStore, nJobs))
		runConcurrently(wg, partialApplyMakeJobs(makeRunning, t, jobStore, nJobs))
		runConcurrently(wg, partialApplyMakeJobs(makeSucceeded, t, jobStore, nJobs))
		runConcurrently(wg, partialApplyMakeJobs(makeFailed, t, jobStore, nJobs))
		runConcurrently(wg, partialApplyMakeJobs(makeCancelled, t, jobStore, nJobs))

		wg.Wait()
	})
}

type fn func()

func runConcurrently(wg *sync.WaitGroup, f fn) {
	go func() {
		defer wg.Done()
		fmt.Println("Starting worker")
		f()
		fmt.Println("Stopping worker")
	}()
}

type makeJobsFn func(t *testing.T, jobStore JobRecorder, nJobs int)

func partialApplyMakeJobs(f makeJobsFn, t *testing.T, jobStore JobRecorder, nJobs int) fn {
	return func() {
		f(t, jobStore, nJobs)
	}
}

func makeQueued(t *testing.T, jobStore JobRecorder, nJobs int) {
	for i := 0; i < nJobs; i++ {
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, util.NewULID())
	}
}

func makePending(t *testing.T, jobStore JobRecorder, nJobs int) {
	for i := 0; i < nJobs; i++ {
		id1 := util.NewULID()
		id2 := util.NewULID()
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, util.NewULID()).
			Pending(cluster, id1).
			UnableToSchedule(cluster, id1, node).
			Pending(cluster, id2)
	}
}

func makeRunning(t *testing.T, jobStore JobRecorder, nJobs int) {
	for i := 0; i < nJobs; i++ {
		id1 := util.NewULID()
		id2 := util.NewULID()
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, util.NewULID()).
			Pending(cluster, id1).
			UnableToSchedule(cluster, id1, node).
			Running(cluster, id2, node)
	}
}

func makeSucceeded(t *testing.T, jobStore JobRecorder, nJobs int) {
	for i := 0; i < nJobs; i++ {
		id1 := util.NewULID()
		id2 := util.NewULID()
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, util.NewULID()).
			Pending(cluster, id1).
			UnableToSchedule(cluster, id1, node).
			Running(cluster, id2, node).
			Succeeded(cluster, id2, node)
	}
}

func makeFailed(t *testing.T, jobStore JobRecorder, nJobs int) {
	for i := 0; i < nJobs; i++ {
		id1 := util.NewULID()
		id2 := util.NewULID()
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, util.NewULID()).
			Pending(cluster, id1).
			UnableToSchedule(cluster, id1, node).
			Running(cluster, id2, node).
			Failed(cluster, id2, node, "error")
	}
}

func makeCancelled(t *testing.T, jobStore JobRecorder, nJobs int) {
	for i := 0; i < nJobs; i++ {
		id1 := util.NewULID()
		id2 := util.NewULID()
		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, util.NewULID()).
			Pending(cluster, id1).
			UnableToSchedule(cluster, id1, node).
			Running(cluster, id2, node).
			Cancelled()
	}
}
