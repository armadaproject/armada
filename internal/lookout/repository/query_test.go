package repository

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
)

func Test_QueueStats(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queue := "queue"

		cluster := "cluster"
		k8sId1 := util.NewULID()
		k8sId2 := util.NewULID()
		node := "node"

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			running(cluster, k8sId2, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		stats, err := jobRepo.GetQueueStats()
		assert.NoError(t, err)
		assert.Equal(t, []*lookout.QueueInfo{{
			Queue:       queue,
			JobsQueued:  1,
			JobsPending: 1,
			JobsRunning: 1,
		}}, stats)
	})
}

func Test_GetNoJobsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)

		cluster := "cluster"
		k8sId1 := util.NewULID()
		k8sId2 := util.NewULID()
		node := "node"

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob("queue-1")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob("queue-2").
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob("queue-2").
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db)

		jobInfos, err := jobRepo.GetJobsInQueue("queue", GetJobsInQueueOpts{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobInfos))
	})
}

func Test_GetSucceededJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()
		queue := "queue"

		cluster := "cluster"
		k8sId := util.NewULID()
		node := "node"

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId).
			running(cluster, k8sId, node).
			succeeded(cluster, k8sId, node)

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, 1, len(jobInfo.Runs))
		runInfo := jobInfo.Runs[0]
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Created:   increment(startTime, 1),
			Started:   increment(startTime, 2),
			Finished:  increment(startTime, 3),
		}, runInfo)
	})
}

func Test_GetFailedJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()
		queue := "queue"

		cluster := "cluster"
		k8sId := util.NewULID()
		node := "node"
		failureReason := "Something bad happened"

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId).
			running(cluster, k8sId, node).
			failed(cluster, k8sId, node, failureReason)

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, 1, len(jobInfo.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   increment(startTime, 1),
			Started:   increment(startTime, 2),
			Finished:  increment(startTime, 3),
			Error:     failureReason,
		}, jobInfo.Runs[0])
	})
}

func Test_GetCancelledJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()
		queue := "queue"

		cluster := "cluster"
		k8sId := util.NewULID()
		node := "node"

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId).
			running(cluster, k8sId, node).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assertTimesApproxEqual(t, increment(startTime, 3), jobInfo.Cancelled)

		assert.Equal(t, 1, len(jobInfo.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   increment(startTime, 1),
			Started:   increment(startTime, 2),
		}, jobInfo.Runs[0])
	})
}

func Test_GetMultipleRunJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()
		queue := "queue"

		k8sId1 := util.NewULID()
		k8sId2 := util.NewULID()
		cluster := "cluster"
		node := "node"

		retried := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node).
			succeeded(cluster, k8sId2, node)

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, retried.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, 2, len(jobInfo.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   increment(startTime, 1),
		}, jobInfo.Runs[0])
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId2,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Created:   increment(startTime, 2),
			Started:   increment(startTime, 3),
			Finished:  increment(startTime, 4),
		}, jobInfo.Runs[1])
	})
}

func Test_GetJobsOrderedFromOldestToNewest(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queue := "queue"

		// Should be sorted by ULID
		jobId1 := util.NewULID()
		jobId2 := util.NewULID()
		jobId3 := util.NewULID()

		cluster := "cluster"
		node := "node"

		third := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithId(queue, jobId3).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		second := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithId(queue, jobId2).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		first := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithId(queue, jobId1).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		assertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, third.job, jobInfos[2].Job)
	})
}

func Test_GetJobsOrderedFromNewestToOldest(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queue := "queue"

		// Should be sorted by ULID
		jobId1 := util.NewULID()
		jobId2 := util.NewULID()
		jobId3 := util.NewULID()

		cluster := "cluster"
		node := "node"

		first := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithId(queue, jobId1).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		second := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithId(queue, jobId2).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		third := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithId(queue, jobId3).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{
			NewestFirst: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		assertJobsAreEquivalent(t, third.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, first.job, jobInfos[2].Job)
	})
}

func Test_GetOnlySubmittedJobs(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queue := "queue"

		cluster := "cluster"
		node := "node"

		queued := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		k8sId1 := util.NewULID()
		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		k8sId2 := util.NewULID()
		k8sId3 := util.NewULID()
		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		k8sId4 := util.NewULID()
		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		k8sId5 := util.NewULID()
		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(queue, GetJobsInQueueOpts{
			FilterStates: []JobState{Submitted},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
	})
}

func assertJobsAreEquivalent(t *testing.T, expected *api.Job, actual *api.Job) {
	t.Helper()
	assert.Equal(t, expected.Id, actual.Id)
	assert.Equal(t, expected.JobSetId, actual.JobSetId)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.Equal(t, expected.Priority, actual.Priority)
	assertTimesApproxEqual(t, &expected.Created, &actual.Created)
}

func assertRunInfosEquivalent(t *testing.T, expected *lookout.RunInfo, actual *lookout.RunInfo) {
	assert.Equal(t, expected.K8SId, actual.K8SId)
	assert.Equal(t, expected.Cluster, actual.Cluster)
	assert.Equal(t, expected.Node, expected.Node)
	assert.Equal(t, expected.Succeeded, actual.Succeeded)
	assertTimesApproxEqual(t, expected.Created, actual.Created)
	assertTimesApproxEqual(t, expected.Started, actual.Started)
	assertTimesApproxEqual(t, expected.Finished, expected.Finished)
	assert.Equal(t, expected.Error, actual.Error)
}

// asserts that two times are equivalent in UTC rounded to nearest millisecond
func assertTimesApproxEqual(t *testing.T, expected *time.Time, actual *time.Time) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	assert.Equal(t, expected.Round(time.Millisecond).UTC(), actual.Round(time.Millisecond).UTC())
}

// increment given time by a number of minutes, returns pointer to new time
func increment(t time.Time, x int) *time.Time {
	i := t.Add(time.Minute * time.Duration(x))
	return &i
}

type clock interface {
	now() time.Time
}

type defaultClock struct{}

func (c *defaultClock) now() time.Time { return time.Now() }

type jobSimulator struct {
	t        *testing.T
	jobStore JobRecorder
	clock    clock
	job      *api.Job
}

type incrementClock struct {
	startTime  time.Time
	increments int
}

func newIncrementClock(startTime time.Time) *incrementClock {
	return &incrementClock{
		startTime:  startTime,
		increments: 0,
	}
}

func (c *incrementClock) now() time.Time {
	t := increment(c.startTime, c.increments)
	c.increments++
	return *t
}

func newJobSimulator(t *testing.T, jobStore JobRecorder, timeProvider clock) *jobSimulator {
	return &jobSimulator{
		t:        t,
		jobStore: jobStore,
		clock:    timeProvider,
		job:      nil,
	}
}

func (js *jobSimulator) createJob(queue string) *jobSimulator {
	return js.createJobWithId(queue, util.NewULID())
}

func (js *jobSimulator) createJobWithId(queue string, id string) *jobSimulator {
	js.job = &api.Job{
		Id:          id,
		JobSetId:    "job-set",
		Queue:       queue,
		Namespace:   "nameSpace",
		Labels:      nil,
		Annotations: nil,
		Owner:       "user",
		Priority:    0,
		PodSpec:     &v1.PodSpec{},
		Created:     js.clock.now(),
	}
	assert.NoError(js.t, js.jobStore.RecordJob(js.job))
	return js
}

func (js *jobSimulator) pending(cluster string, k8sId string) *jobSimulator {
	event := &api.JobPendingEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
	}
	assert.NoError(js.t, js.jobStore.RecordJobPending(event))
	return js
}

func (js *jobSimulator) running(cluster string, k8sId string, node string) *jobSimulator {
	event := &api.JobRunningEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobRunning(event))
	return js
}

func (js *jobSimulator) succeeded(cluster string, k8sId string, node string) *jobSimulator {
	event := &api.JobSucceededEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobSucceeded(event))
	return js
}

func (js *jobSimulator) failed(cluster string, k8sId string, node string, error string) *jobSimulator {
	failedEvent := &api.JobFailedEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		Reason:       error,
		ExitCodes:    nil,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobFailed(failedEvent))
	return js
}

func (js *jobSimulator) cancelled() *jobSimulator {
	cancelledEvent := &api.JobCancelledEvent{
		JobId:    js.job.Id,
		JobSetId: js.job.JobSetId,
		Queue:    js.job.Queue,
		Created:  js.clock.now(),
	}
	assert.NoError(js.t, js.jobStore.MarkCancelled(cancelledEvent))
	return js
}
