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

		newJobSimulator(t, jobStore, &defaultClock{time.Now()}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{time.Now()}).
			createJob(queue).
			running(cluster, k8sId2, node)

		newJobSimulator(t, jobStore, &defaultClock{time.Now()}).
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

		newJobSimulator(t, jobStore, &defaultClock{time.Now()}).
			createJob("queue-1")

		newJobSimulator(t, jobStore, &defaultClock{time.Now()}).
			createJob("queue-2").
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{time.Now()}).
			createJob("queue-2").
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db)

		jobInfos, err := jobRepo.GetQueuedJobs("queue")
		assert.NoError(t, err)
		assert.Equal(t, []*lookout.JobInfo{}, jobInfos)
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

		jobInfos, err := jobRepo.GetQueuedJobs(queue)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		jobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Equal(t, 1, len(jobInfo.Runs))
		runInfo := jobInfo.Runs[0]
		assert.Equal(t, cluster, runInfo.Cluster)
		assert.Equal(t, node, runInfo.Node)
		assert.True(t, runInfo.Succeeded)
	})
}

func Test_GetMultipleRunJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queue := "queue"

		k8sId1 := util.NewULID()
		k8sId2 := util.NewULID()

		cluster := "cluster"
		node := "node"

		retried := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1).
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node).
			succeeded(cluster, k8sId2, node)

		jobInfos, err := jobRepo.GetQueuedJobs(queue)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		jobsAreEquivalent(t, retried.job, jobInfo.Job)

		assert.Equal(t, 2, len(jobInfo.Runs))
		runInfo := jobInfo.Runs[0]
		assert.Equal(t, cluster, runInfo.Cluster)
	})
}

func createPendingEvent(cluster string, k8sId string, job *api.Job) *api.JobPendingEvent {
	return &api.JobPendingEvent{
		JobId:        job.Id,
		JobSetId:     job.JobSetId,
		Queue:        job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
	}
}

func createRunningEvent(cluster string, k8sId string, node string, job *api.Job) *api.JobRunningEvent {
	return &api.JobRunningEvent{
		JobId:        job.Id,
		JobSetId:     job.JobSetId,
		Queue:        job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
}

func createSucceededEvent(cluster string, k8sId string, node string, job *api.Job) *api.JobSucceededEvent {
	return &api.JobSucceededEvent{
		JobId:        job.Id,
		JobSetId:     job.JobSetId,
		Queue:        job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
}

func createJob(queue string) *api.Job {
	return &api.Job{
		Id:          util.NewULID(),
		JobSetId:    "job-set",
		Queue:       queue,
		Namespace:   "nameSpace",
		Labels:      nil,
		Annotations: nil,
		Owner:       "user",
		Priority:    0,
		PodSpec:     &v1.PodSpec{},
		Created:     time.Now(),
	}
}

func jobsAreEquivalent(t *testing.T, expected *api.Job, actual *api.Job) {
	t.Helper()
	assert.Equal(t, expected.Id, actual.Id)
	assert.Equal(t, expected.JobSetId, actual.JobSetId)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.Equal(t, expected.Priority, actual.Priority)
	assert.Equal(t, expected.Created, actual.Created)
}

// increment given time by a number of minutes
func increment(t time.Time, x int) time.Time {
	return t.Add(time.Minute * time.Duration(x))
}

type clock interface {
	now() time.Time
}

type defaultClock struct{
	t time.Time
}

func (c *defaultClock) now() time.Time { return c.t }

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
		startTime: startTime,
		increments: 0,
	}
}

func (c *incrementClock) now() time.Time {
	t := increment(c.startTime, c.increments)
	c.increments++
	return t
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
	js.job = createJob(queue)
	assert.NoError(js.t, js.jobStore.RecordJob(js.job))
	return js
}

func (js *jobSimulator) pending(cluster string, k8sId string) *jobSimulator {
	assert.NoError(js.t, js.jobStore.RecordJobPending(createPendingEvent(cluster, k8sId, js.job)))
	return js
}

func (js *jobSimulator) running(cluster string, k8sId string, node string) *jobSimulator {
	assert.NoError(js.t, js.jobStore.RecordJobRunning(createRunningEvent(cluster, k8sId, node, js.job)))
	return js
}

func (js *jobSimulator) succeeded(cluster string, k8sId string, node string) *jobSimulator {
	assert.NoError(js.t, js.jobStore.RecordJobSucceeded(createSucceededEvent(cluster, k8sId, node, js.job)))
	return js
}

func (js *jobSimulator) failed(cluster string, k8sId string, node string) *jobSimulator {
	failedEvent := &api.JobFailedEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		Reason:       "",
		ExitCodes:    nil,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobFailed(failedEvent))
	return js
}
