package repository

import (
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

func TestGetQueueInfos_WithNoJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node).
			Succeeded(cluster, k8sId3, node)

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(queueInfos))
	})
}

func TestGetQueueInfos_Counts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, "a1").
			Pending(cluster, "a2")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, "b1").
			UnableToSchedule(cluster, "b1", node).
			Running(cluster, "b2", node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, "c1").
			Running(cluster, "c2", node).
			Succeeded(cluster, "c2", node)

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		assertQueueInfoCountsAreEqual(t, &lookout.QueueInfo{
			Queue:       queue,
			JobsQueued:  1,
			JobsPending: 1,
			JobsRunning: 1,
		}, queueInfos[0])
	})
}

func TestGetQueueInfos_OldestQueuedJobIsNilIfNoJobsAreQueued(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "c", someTime.Add(6*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", someTime.Add(time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(10*time.Second)).
			PendingAtTime(cluster, "a", someTime.Add(11*time.Second)).
			RunningAtTime(cluster, "a", node, someTime.Add(12*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "b", someTime.Add(6*time.Second)).
			RunningAtTime(cluster, "b", node, someTime.Add(7*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(time.Second)).
			RunningAtTime(cluster, "d", node, someTime.Add(2*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e", node, someTime.Add(time.Second)).
			SucceededAtTime(cluster, "e", node, someTime.Add(2*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "g", node, someTime.Add(time.Second)).
			CancelledAtTime(someTime.Add(2 * time.Second))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].OldestQueuedJob)
	})
}

func TestGetQueueInfos_IncludeOldestQueuedJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		someTime2 := someTime.Add(5*time.Hour + 4*time.Minute + 3*time.Second)

		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{T: someTime2})

		submissionTime := someTime.Add(time.Second)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second))

		oldestQueued := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, submissionTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(10*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "c", someTime.Add(6*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", someTime.Add(1*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(10*time.Second)).
			PendingAtTime(cluster, "a", someTime.Add(11*time.Second)).
			RunningAtTime(cluster, "a", node, someTime.Add(12*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "b", someTime.Add(6*time.Second)).
			RunningAtTime(cluster, "b", node, someTime.Add(7*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "d", node, someTime.Add(time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e", node, someTime.Add(time.Second)).
			SucceededAtTime(cluster, "e", node, someTime.Add(2*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "g", node, someTime.Add(time.Second)).
			CancelledAtTime(someTime.Add(2 * time.Second))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		AssertJobsAreEquivalent(t, oldestQueued.job, queueInfos[0].OldestQueuedJob.Job)

		assert.Nil(t, queueInfos[0].OldestQueuedJob.Cancelled)
		assert.Equal(t, string(JobQueued), queueInfos[0].OldestQueuedJob.JobState)

		assert.Equal(t, 0, len(queueInfos[0].OldestQueuedJob.Runs))

		AssertProtoDurationsApproxEqual(t, types.DurationProto(someTime2.Sub(submissionTime)), queueInfos[0].OldestQueuedDuration)
	})
}

func TestGetQueueInfos_LongestRunningJobIsNilIfNoJobsAreRunning(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", someTime.Add(time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "c", someTime.Add(6*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e", node, someTime.Add(time.Second)).
			SucceededAtTime(cluster, "e", node, someTime.Add(2*time.Second))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].LongestRunningJob)
	})
}

func TestGetQueueInfos_IncludeLongestRunningJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		someTime2 := someTime.Add(5*time.Hour + 4*time.Minute + 3*time.Second)

		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime2})

		pendingTime := someTime.Add(2 * time.Second)
		unableToScheduleTime := someTime.Add(3 * time.Second)
		runningTime := someTime.Add(4 * time.Second)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "c", someTime.Add(6*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", someTime.Add(time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(10*time.Second)).
			PendingAtTime(cluster, "a1", someTime.Add(11*time.Second)).
			UnableToScheduleAtTime(cluster, "a1", node, someTime.Add(12*time.Second), "error").
			RunningAtTime(cluster, "a2", node, someTime.Add(13*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "b1", someTime.Add(6*time.Second)).
			UnableToScheduleAtTime(cluster, "b1", node, someTime.Add(7*time.Second), "error").
			RunningAtTime(cluster, "b2", node, someTime.Add(8*time.Second))

		longestRunning := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(time.Second)).
			PendingAtTime(cluster, "d1", pendingTime).
			UnableToScheduleAtTime(cluster, "d1", node, unableToScheduleTime, "error").
			RunningAtTime(cluster, "d2", node, runningTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e1", node, someTime.Add(time.Second)).
			UnableToScheduleAtTime(cluster, "e1", node, someTime.Add(2*time.Second), "error").
			SucceededAtTime(cluster, "e2", node, someTime.Add(3*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "g", node, someTime.Add(time.Second)).
			CancelledAtTime(someTime.Add(2 * time.Second))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		AssertJobsAreEquivalent(t, longestRunning.job, queueInfos[0].LongestRunningJob.Job)

		assert.Nil(t, queueInfos[0].LongestRunningJob.Cancelled)
		assert.Equal(t, string(JobRunning), queueInfos[0].LongestRunningJob.JobState)

		assert.Equal(t, 2, len(queueInfos[0].LongestRunningJob.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "d1",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   &pendingTime,
			Started:   nil,
			Finished:  &unableToScheduleTime,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])

		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "d2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   &runningTime,
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[1])

		AssertProtoDurationsApproxEqual(t, types.DurationProto(someTime2.Sub(runningTime)), queueInfos[0].LongestRunningDuration)
	})
}

func TestGetQueueInfos_MultipleQueues(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		queue1PendingTime := someTime.Add(time.Second)
		queue1UnableToScheduleTime := someTime.Add(2 * time.Second)
		queue1RunningTime := someTime.Add(3 * time.Second)

		queue2RunningTime := someTime.Add(time.Second)

		// Queue 1
		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second))

		oldestQueued1 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "a", someTime.Add(time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime.Add(5*time.Second)).
			PendingAtTime(cluster, "b1", someTime.Add(6*time.Second)).
			UnableToScheduleAtTime(cluster, "b1", node, someTime.Add(7*time.Second), "error").
			RunningAtTime(cluster, "b2", node, someTime.Add(8*time.Second))

		longestRunning1 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "c1", queue1PendingTime).
			UnableToScheduleAtTime(cluster, "c1", node, queue1UnableToScheduleTime, "error").
			RunningAtTime(cluster, "c2", node, queue1RunningTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "d1", someTime.Add(time.Second)).
			UnableToScheduleAtTime(cluster, "d1", node, someTime.Add(2*time.Second), "error").
			RunningAtTime(cluster, "d2", node, someTime.Add(3*time.Second)).
			SucceededAtTime(cluster, "d2", node, someTime.Add(4*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			CancelledAtTime(someTime.Add(time.Second))

		// Queue 2
		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime.Add(5*time.Second))

		oldestQueued2 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime).
			PendingAtTime(cluster, "e", someTime.Add(1*time.Second))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime.Add(5*time.Second)).
			RunningAtTime(cluster, "f", node, someTime.Add(6*time.Second))

		longestRunning2 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime).
			RunningAtTime(cluster, "g", node, queue2RunningTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime.Add(time.Second)).
			PendingAtTime(cluster, "h1", someTime.Add(2*time.Second)).
			UnableToScheduleAtTime(cluster, "h1", node, someTime.Add(3*time.Second), "error").
			RunningAtTime(cluster, "h2", node, someTime.Add(4*time.Second))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(queueInfos))
		assertQueueInfoCountsAreEqual(t, &lookout.QueueInfo{
			Queue:       queue,
			JobsQueued:  2,
			JobsPending: 1,
			JobsRunning: 2,
		}, queueInfos[0])
		assertQueueInfoCountsAreEqual(t, &lookout.QueueInfo{
			Queue:       queue2,
			JobsQueued:  2,
			JobsPending: 1,
			JobsRunning: 3,
		}, queueInfos[1])

		AssertJobsAreEquivalent(t, oldestQueued1.job, queueInfos[0].OldestQueuedJob.Job)
		assert.Nil(t, queueInfos[0].OldestQueuedJob.Cancelled)
		assert.Equal(t, string(JobQueued), queueInfos[0].OldestQueuedJob.JobState)
		assert.Equal(t, 0, len(queueInfos[0].OldestQueuedJob.Runs))

		AssertJobsAreEquivalent(t, longestRunning1.job, queueInfos[0].LongestRunningJob.Job)
		assert.Nil(t, queueInfos[0].LongestRunningJob.Cancelled)
		assert.Equal(t, string(JobRunning), queueInfos[0].LongestRunningJob.JobState)
		assert.Equal(t, 2, len(queueInfos[0].LongestRunningJob.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:    "c1",
			Cluster:  cluster,
			Node:     node,
			Created:  &queue1PendingTime,
			Finished: &queue1UnableToScheduleTime,
		}, queueInfos[0].LongestRunningJob.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:   "c2",
			Cluster: cluster,
			Node:    node,
			Started: &queue1RunningTime,
		}, queueInfos[0].LongestRunningJob.Runs[1])

		AssertJobsAreEquivalent(t, oldestQueued2.job, queueInfos[1].OldestQueuedJob.Job)
		assert.Nil(t, queueInfos[1].OldestQueuedJob.Cancelled)
		assert.Equal(t, string(JobQueued), queueInfos[1].OldestQueuedJob.JobState)
		assert.Equal(t, 0, len(queueInfos[1].OldestQueuedJob.Runs))

		AssertJobsAreEquivalent(t, longestRunning2.job, queueInfos[1].LongestRunningJob.Job)
		assert.Nil(t, queueInfos[1].LongestRunningJob.Cancelled)
		assert.Equal(t, string(JobRunning), queueInfos[1].LongestRunningJob.JobState)
		assert.Equal(t, 1, len(queueInfos[1].LongestRunningJob.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:   "g",
			Cluster: cluster,
			Node:    node,
			Started: &queue2RunningTime,
		}, queueInfos[1].LongestRunningJob.Runs[0])
	})
}

func assertQueueInfoCountsAreEqual(t *testing.T, expected *lookout.QueueInfo, actual *lookout.QueueInfo) {
	t.Helper()
	assert.Equal(t, expected.Queue, actual.Queue)
	assert.Equal(t, expected.JobsQueued, actual.JobsQueued)
	assert.Equal(t, expected.JobsPending, actual.JobsPending)
	assert.Equal(t, expected.JobsRunning, actual.JobsRunning)
}
