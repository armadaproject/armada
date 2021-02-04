package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
)

var (
	queue   = "queue"
	queue2  = "queue2"
	cluster = "cluster"
	k8sId1  = util.NewULID()
	k8sId2  = util.NewULID()
	k8sId3  = util.NewULID()
	k8sId4  = util.NewULID()
	k8sId5  = util.NewULID()
	node    = "node"
	ctx     = context.Background()
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
	AssertTimesApproxEqual(t, expected.Finished, expected.Finished)
	assert.Equal(t, expected.Error, actual.Error)
}

// asserts that two times are equivalent in UTC rounded to nearest millisecond
func AssertTimesApproxEqual(t *testing.T, expected *time.Time, actual *time.Time) {
	t.Helper()
	if expected == nil {
		assert.Nil(t, actual)
		return
	}
	assert.Equal(t, expected.Round(time.Millisecond).UTC(), actual.Round(time.Millisecond).UTC())
}

// Increment given time by a number of minutes, returns pointer to new time
func Increment(t time.Time, x int) *time.Time {
	i := t.Add(time.Minute * time.Duration(x))
	return &i
}

type JobSimulator struct {
	t        *testing.T
	jobStore JobRecorder
	clock    Clock
	job      *api.Job
}

type DummyClock struct {
	t time.Time
}

func (c *DummyClock) Now() time.Time {
	return c.t
}

type incrementClock struct {
	startTime  time.Time
	increments int
}

func NewIncrementClock(startTime time.Time) *incrementClock {
	return &incrementClock{
		startTime:  startTime,
		increments: 0,
	}
}

func (c *incrementClock) Now() time.Time {
	t := Increment(c.startTime, c.increments)
	c.increments++
	return *t
}

func NewJobSimulator(t *testing.T, jobStore JobRecorder, timeProvider Clock) *JobSimulator {
	return &JobSimulator{
		t:        t,
		jobStore: jobStore,
		clock:    timeProvider,
		job:      nil,
	}
}

func (js *JobSimulator) CreateJob(queue string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set")
}

func (js *JobSimulator) CreateJobWithId(queue string, id string) *JobSimulator {
	return js.CreateJobWithOpts(queue, id, "job-set")
}

func (js *JobSimulator) CreateJobWithJobSet(queue string, jobSetId string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), jobSetId)
}

func (js *JobSimulator) CreateJobWithOpts(queue string, jobId string, jobSetId string) *JobSimulator {
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
		Created:     js.clock.Now(),
	}
	assert.NoError(js.t, js.jobStore.RecordJob(js.job))
	return js
}

func (js *JobSimulator) Pending(cluster string, k8sId string) *JobSimulator {
	event := &api.JobPendingEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
	}
	assert.NoError(js.t, js.jobStore.RecordJobPending(event))
	return js
}

func (js *JobSimulator) Running(cluster string, k8sId string, node string) *JobSimulator {
	event := &api.JobRunningEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobRunning(event))
	return js
}

func (js *JobSimulator) Succeeded(cluster string, k8sId string, node string) *JobSimulator {
	event := &api.JobSucceededEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobSucceeded(event))
	return js
}

func (js *JobSimulator) Failed(cluster string, k8sId string, node string, error string) *JobSimulator {
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

func (js *JobSimulator) Cancelled() *JobSimulator {
	cancelledEvent := &api.JobCancelledEvent{
		JobId:    js.job.Id,
		JobSetId: js.job.JobSetId,
		Queue:    js.job.Queue,
		Created:  js.clock.Now(),
	}
	assert.NoError(js.t, js.jobStore.MarkCancelled(cancelledEvent))
	return js
}

func (js *JobSimulator) UnableToSchedule(cluster string, k8sId string, node string) *JobSimulator {
	unableToScheduleEvent := &api.JobUnableToScheduleEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      js.clock.Now(),
		ClusterId:    cluster,
		Reason:       "unable to schedule reason",
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobUnableToSchedule(unableToScheduleEvent))
	return js
}
