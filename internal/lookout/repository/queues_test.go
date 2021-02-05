package repository

import (
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api/lookout"
)

func TestGetQueueInfos_WithNoJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

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
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, "a1").
			UnableToSchedule(cluster, "a1", node).
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
			UnableToSchedule(cluster, "c1", node).
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
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "c", *Increment(someTime, 6))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 10)).
			PendingAtTime(cluster, "a", *Increment(someTime, 11)).
			RunningAtTime(cluster, "a", node, *Increment(someTime, 12))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "b", *Increment(someTime, 6)).
			RunningAtTime(cluster, "b", node, *Increment(someTime, 7))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 1)).
			RunningAtTime(cluster, "d", node, *Increment(someTime, 2))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e", node, *Increment(someTime, 1)).
			SucceededAtTime(cluster, "e", node, *Increment(someTime, 2))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "g", node, *Increment(someTime, 1)).
			CancelledAtTime(*Increment(someTime, 2))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].OldestQueuedJob)
	})
}

func TestGetQueueInfos_IncludeOldestQueuedJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		someTime2 := someTime.Add(5*time.Hour + 4*time.Minute + 3*time.Second)

		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DummyClock{someTime2})

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5))

		oldestQueued := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 10))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "c", *Increment(someTime, 6))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f",*Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 10)).
			PendingAtTime(cluster, "a", *Increment(someTime, 11)).
			RunningAtTime(cluster, "a", node,*Increment(someTime, 12))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "b", *Increment(someTime, 6)).
			RunningAtTime(cluster, "b", node, *Increment(someTime, 7))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "d", node, *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e", node, *Increment(someTime, 1)).
			SucceededAtTime(cluster, "e", node, *Increment(someTime, 2))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "g", node, *Increment(someTime, 1)).
			CancelledAtTime(*Increment(someTime, 2))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		AssertJobsAreEquivalent(t, oldestQueued.job, queueInfos[0].OldestQueuedJob.Job)

		assert.Nil(t, queueInfos[0].OldestQueuedJob.Cancelled)
		assert.Equal(t, string(JobQueued), queueInfos[0].OldestQueuedJob.JobState)

		assert.Equal(t, 0, len(queueInfos[0].OldestQueuedJob.Runs))

		submissionTime := *Increment(someTime, 1)
		assert.Equal(t, types.DurationProto(someTime2.Sub(submissionTime).Round(time.Second)), queueInfos[0].OldestQueuedDuration)
	})
}

func TestGetQueueInfos_LongestRunningJobIsNilIfNoJobsAreRunning(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "c", *Increment(someTime, 6))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e", node, *Increment(someTime, 1)).
			SucceededAtTime(cluster, "e", node, *Increment(someTime, 2))

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].LongestRunningJob)
	})
}

func TestGetQueueInfos_IncludeLongestRunningJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		someTime2 := someTime.Add(5*time.Hour + 4*time.Minute + 3*time.Second)

		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DummyClock{someTime2})

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "c", *Increment(someTime, 6))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "f", *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 10)).
			PendingAtTime(cluster, "a1", *Increment(someTime, 11)).
			UnableToScheduleAtTime(cluster, "a1", node, *Increment(someTime, 12)).
			RunningAtTime(cluster, "a2", node, *Increment(someTime, 13))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "b1", *Increment(someTime, 6)).
			UnableToScheduleAtTime(cluster, "b1", node, *Increment(someTime, 7)).
			RunningAtTime(cluster, "b2", node, *Increment(someTime, 8))

		longestRunning := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 1)).
			PendingAtTime(cluster, "d1", *Increment(someTime, 2)).
			UnableToScheduleAtTime(cluster, "d1", node, *Increment(someTime, 3)).
			RunningAtTime(cluster, "d2", node, *Increment(someTime, 4))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "e1", node, *Increment(someTime, 1)).
			UnableToScheduleAtTime(cluster, "e1", node, *Increment(someTime, 2)).
			SucceededAtTime(cluster, "e2", node, *Increment(someTime, 3))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			RunningAtTime(cluster, "g", node, *Increment(someTime, 1)).
			CancelledAtTime(*Increment(someTime, 2))

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
			Created:   Increment(someTime, 2),
			Started:   nil,
			Finished:  Increment(someTime, 3),
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])

		startRunningTime := *Increment(someTime, 4)
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "d2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   &startRunningTime,
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[1])

		assert.Equal(t, types.DurationProto(someTime2.Sub(startRunningTime).Round(time.Second)), queueInfos[0].LongestRunningDuration)
	})
}

func TestGetQueueInfos_MultipleQueues(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Queue 1
		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5))

		oldestQueued1 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "a", *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, *Increment(someTime, 5)).
			PendingAtTime(cluster, "b1", *Increment(someTime, 6)).
			UnableToScheduleAtTime(cluster, "b1", node, *Increment(someTime, 7)).
			RunningAtTime(cluster, "b2", node, *Increment(someTime, 8))

		longestRunning1 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "c1", *Increment(someTime, 1)).
			UnableToScheduleAtTime(cluster, "c1", node, *Increment(someTime, 2)).
			RunningAtTime(cluster, "c2", node, *Increment(someTime, 3))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, "d1", *Increment(someTime, 1)).
			UnableToScheduleAtTime(cluster, "d1", node, *Increment(someTime, 2)).
			RunningAtTime(cluster, "d2", node, *Increment(someTime, 3)).
			SucceededAtTime(cluster, "d2", node, *Increment(someTime, 4))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			CancelledAtTime(*Increment(someTime, 1))

		// Queue 2
		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, *Increment(someTime, 5))

		oldestQueued2 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime)

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime).
			PendingAtTime(cluster, "e", *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, *Increment(someTime, 5)).
			RunningAtTime(cluster, "f", node, *Increment(someTime, 6))

		longestRunning2 := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, someTime).
			RunningAtTime(cluster, "g", node, *Increment(someTime, 1))

		NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue2, *Increment(someTime, 1)).
			PendingAtTime(cluster, "h1", *Increment(someTime, 2)).
			UnableToScheduleAtTime(cluster, "h1", node, *Increment(someTime, 3)).
			RunningAtTime(cluster, "h2", node, *Increment(someTime, 4))

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
			K8SId:     "c1",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   Increment(someTime, 1),
			Started:   nil,
			Finished:  Increment(someTime, 2),
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "c2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   Increment(someTime, 3),
			Finished:  nil,
			Error:     "",
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
			K8SId:     "g",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   Increment(someTime, 1),
			Finished:  nil,
			Error:     "",
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
