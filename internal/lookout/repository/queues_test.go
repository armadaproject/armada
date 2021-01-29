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

		NewJobSimulator(t, jobStore, &DefaultClock{}).
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

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, "a1").
			Pending(cluster, "a2")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, "b1").
			Running(cluster, "b2", node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
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
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue).
			Pending(cluster, "c")

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, "f")

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 10))).
			CreateJob(queue).
			Pending(cluster, "a").
			Running(cluster, "a", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue).
			Pending(cluster, "b").
			Running(cluster, "b", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 1))).
			CreateJob(queue).
			Running(cluster, "d", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Running(cluster, "e", node).
			Succeeded(cluster, "e", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Running(cluster, "g", node).
			Cancelled()

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].OldestQueuedJob)
	})
}

func TestGetQueueInfos_IncludeOldestQueuedJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		time1 := time.Now()
		time2 := time1.Add(5*time.Hour + 4*time.Minute + 3*time.Second + 300*time.Millisecond)

		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DummyClock{time2})

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 5))).
			CreateJob(queue)

		oldestQueued := NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 1))).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 10))).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 5))).
			CreateJob(queue).
			Pending(cluster, "c")

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Pending(cluster, "f")

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 10))).
			CreateJob(queue).
			Pending(cluster, "a").
			Running(cluster, "a", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 5))).
			CreateJob(queue).
			Pending(cluster, "b").
			Running(cluster, "b", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Running(cluster, "d", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Running(cluster, "e", node).
			Succeeded(cluster, "e", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Running(cluster, "g", node).
			Cancelled()

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		AssertJobsAreEquivalent(t, oldestQueued.job, queueInfos[0].OldestQueuedJob.Job)

		assert.Nil(t, queueInfos[0].OldestQueuedJob.Cancelled)
		assert.Equal(t, string(JobQueued), queueInfos[0].OldestQueuedJob.JobState)

		assert.Equal(t, 0, len(queueInfos[0].OldestQueuedJob.Runs))

		submissionTime := *Increment(time1, 1)
		assert.Equal(t, types.DurationProto(time2.Sub(submissionTime).Round(time.Second)), queueInfos[0].OldestQueuedDuration)
	})
}

func TestGetQueueInfos_LongestRunningJobIsNilIfNoJobsAreRunning(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, "f")

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue).
			Pending(cluster, "c")

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Running(cluster, "e", node).
			Succeeded(cluster, "e", node)

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].LongestRunningJob)
	})
}

func TestGetQueueInfos_IncludeLongestRunningJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		time1 := time.Now()
		time2 := time1.Add(5*time.Hour + 4*time.Minute + 3*time.Second + 300*time.Millisecond)

		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DummyClock{time2})

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 5))).
			CreateJob(queue).
			Pending(cluster, "c")

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Pending(cluster, "f")

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 10))).
			CreateJob(queue).
			Pending(cluster, "a1").
			Running(cluster, "a2", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 5))).
			CreateJob(queue).
			Pending(cluster, "b1").
			Running(cluster, "b2", node)

		longestRunning := NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(time1, 1))).
			CreateJob(queue).
			Pending(cluster, "d1").
			Running(cluster, "d2", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Running(cluster, "e1", node).
			Succeeded(cluster, "e2", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(time1)).
			CreateJob(queue).
			Running(cluster, "g", node).
			Cancelled()

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
			Node:      "",
			Succeeded: false,
			Created:   Increment(time1, 2),
			Started:   nil,
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "d2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   Increment(time1, 3),
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[1])

		startRunningTime := *Increment(time1, 3)
		assert.Equal(t, types.DurationProto(time2.Sub(startRunningTime).Round(time.Second)), queueInfos[0].LongestRunningDuration)
	})
}

func TestGetQueueInfos_MultipleQueues(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		// Queue 1
		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue)

		oldestQueued1 := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, "a")

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue).
			Pending(cluster, "b1").
			Running(cluster, "b2", node)

		longestRunning1 := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, "c1").
			Running(cluster, "c2", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, "d1").
			Running(cluster, "d2", node).
			Succeeded(cluster, "d2", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Cancelled()

		// Queue 2
		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue2)

		oldestQueued2 := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue2)

		NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue2).
			Pending(cluster, "e")

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 5))).
			CreateJob(queue2).
			Running(cluster, "f", node)

		longestRunning2 := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue2).
			Running(cluster, "g", node)

		NewJobSimulator(t, jobStore, NewIncrementClock(*Increment(startTime, 1))).
			CreateJob(queue2).
			Pending(cluster, "h1").
			Running(cluster, "h2", node)

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
			Node:      "",
			Succeeded: false,
			Created:   Increment(startTime, 1),
			Started:   nil,
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "c2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   Increment(startTime, 2),
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
			Started:   Increment(startTime, 1),
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
