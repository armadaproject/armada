package repository

import (
	"fmt"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/api/lookout"
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
	ctx          = armadacontext.Background()
)

func AssertJobsAreEquivalent(t *testing.T, expected *api.Job, actual *api.Job) {
	t.Helper()
	assert.Equal(t, expected.Id, actual.Id)
	assert.Equal(t, expected.JobSetId, actual.JobSetId)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.Equal(t, expected.Priority, actual.Priority)
	assert.Equal(t, expected.Queue, actual.Queue)
	assert.Equal(t, expected.Annotations, actual.Annotations)
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

func NewJobSimulator(t *testing.T, jobStore JobRecorder) *JobSimulator {
	return &JobSimulator{
		t:        t,
		jobStore: jobStore,
		job:      nil,
	}
}

func (js *JobSimulator) CreateJob(queue string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", time.Now(), nil)
}

func (js *JobSimulator) CreateJobWithId(queue string, id string) *JobSimulator {
	return js.CreateJobWithOpts(queue, id, "job-set", "user", time.Now(), nil)
}

func (js *JobSimulator) CreateJobWithJobSet(queue string, jobSetId string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), jobSetId, "user", time.Now(), nil)
}

func (js *JobSimulator) CreateJobWithOwner(queue string, owner string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set", owner, time.Now(), nil)
}

func (js *JobSimulator) CreateJobAtTime(queue string, time time.Time) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", time, nil)
}

func (js *JobSimulator) CreateJobWithAnnotations(queue string, annotations map[string]string) *JobSimulator {
	return js.CreateJobWithOpts(queue, util.NewULID(), "job-set", "user", time.Now(), annotations)
}

func (js *JobSimulator) CreateJobWithOpts(
	queue string,
	jobId string,
	jobSetId string,
	owner string,
	time time.Time,
	annotations map[string]string,
) *JobSimulator {
	js.job = &api.Job{
		Id:          jobId,
		JobSetId:    jobSetId,
		Queue:       queue,
		Namespace:   "nameSpace",
		Labels:      nil,
		Annotations: annotations,
		Owner:       owner,
		Priority:    10,
		PodSpec:     &v1.PodSpec{},
		Created:     time,
	}
	assert.NoError(js.t, js.jobStore.RecordJob(js.job, time))
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
	return js.UnableToScheduleAtTime(cluster, k8sId, node, time.Now(), "unable to schedule reason")
}

func (js *JobSimulator) UnableToScheduleAtTime(cluster string, k8sId string, node string, time time.Time, reason string) *JobSimulator {
	unableToScheduleEvent := &api.JobUnableToScheduleEvent{
		JobId:        js.job.Id,
		JobSetId:     js.job.JobSetId,
		Queue:        js.job.Queue,
		Created:      time,
		ClusterId:    cluster,
		Reason:       reason,
		KubernetesId: k8sId,
		NodeName:     node,
	}
	assert.NoError(js.t, js.jobStore.RecordJobUnableToSchedule(unableToScheduleEvent))
	return js
}

func (js *JobSimulator) Duplicate(originalJobId string) *JobSimulator {
	duplicateFoundEvent := &api.JobDuplicateFoundEvent{
		JobId:         js.job.Id,
		JobSetId:      js.job.JobSetId,
		Queue:         js.job.Queue,
		Created:       time.Now(),
		OriginalJobId: originalJobId,
	}
	assert.NoError(js.t, js.jobStore.RecordJobDuplicate(duplicateFoundEvent))
	return js
}

func TestGlobSearchOrExact(t *testing.T) {
	testCases := []struct {
		field              exp.IdentifierExpression
		pattern            string
		expectedExpression goqu.Expression
	}{
		{job_queue, "test", job_queue.Eq("test")},
		{job_queue, "test*", job_queue.Like("test%")},
		{job_queue, "*test*", job_queue.Like("%test%")},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("TestGlobSearchOrExact(%q,%q)", tc.field, tc.pattern),
			func(t *testing.T) {
				result := GlobSearchOrExact(tc.field, tc.pattern)
				assert.Equal(t, tc.expectedExpression, result)
			})
	}
}

func SaveJobWithNullObj(db *goqu.Database, jobId, queue, jobSet string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	return tx.Wrap(func() error {
		ds := tx.Insert(jobTable).
			Rows(goqu.Record{
				"job_id": jobId,
				"queue":  queue,
				"jobset": jobSet,
				"state":  JobStateToIntMap[JobQueued],
			}).
			OnConflict(goqu.DoNothing())

		_, err := ds.Prepared(true).Executor().Exec()
		if err != nil {
			return err
		}

		return nil
	})
}
