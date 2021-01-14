package repository

import (
	"context"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/gogo/protobuf/types"
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

func Test_QueueInfosWithNoJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node).
			succeeded(cluster, k8sId3, node)

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(queueInfos))
	})
}

func Test_QueueInfoCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, "a1").
			pending(cluster, "a2")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, "b1").
			running(cluster, "b2", node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, "c1").
			running(cluster, "c2", node).
			succeeded(cluster, "c2", node)

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

func Test_QueueInfosOldestQueuedJobIsNilIfNoJobsAreQueued(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue).
			pending(cluster, "c")

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, "f")

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 10))).
			createJob(queue).
			pending(cluster, "a").
			running(cluster, "a", node)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue).
			pending(cluster, "b").
			running(cluster, "b", node)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 1))).
			createJob(queue).
			running(cluster, "d", node)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			running(cluster, "e", node).
			succeeded(cluster, "e", node)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			running(cluster, "g", node).
			cancelled()

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].OldestQueuedJob)
	})
}

func Test_QueueInfosIncludeOldestQueuedJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		time1 := time.Now()
		time2 := time1.Add(5*time.Hour + 4*time.Minute + 3*time.Second + 300*time.Millisecond)

		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &dummyClock{time2})

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 5))).
			createJob(queue)

		oldestQueued := newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 1))).
			createJob(queue)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 10))).
			createJob(queue)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 5))).
			createJob(queue).
			pending(cluster, "c")

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			pending(cluster, "f")

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 10))).
			createJob(queue).
			pending(cluster, "a").
			running(cluster, "a", node)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 5))).
			createJob(queue).
			pending(cluster, "b").
			running(cluster, "b", node)

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			running(cluster, "d", node)

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			running(cluster, "e", node).
			succeeded(cluster, "e", node)

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			running(cluster, "g", node).
			cancelled()

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		assertJobsAreEquivalent(t, oldestQueued.job, queueInfos[0].OldestQueuedJob.Job)

		assert.Nil(t, queueInfos[0].OldestQueuedJob.Cancelled)
		assert.Equal(t, JobStates.Queued, queueInfos[0].OldestQueuedJob.JobState)

		assert.Equal(t, 0, len(queueInfos[0].OldestQueuedJob.Runs))

		submissionTime := *increment(time1, 1)
		assert.Equal(t, types.DurationProto(time2.Sub(submissionTime).Round(time.Second)), queueInfos[0].OldestQueuedDuration)
	})
}

func Test_QueueInfosLongestRunningJobIsNilIfNoJobsAreRunning(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, "f")

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue).
			pending(cluster, "c")

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			running(cluster, "e", node).
			succeeded(cluster, "e", node)

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))

		assert.Nil(t, queueInfos[0].LongestRunningJob)
	})
}

func Test_QueueInfosIncludeLongestRunningJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		time1 := time.Now()
		time2 := time1.Add(5*time.Hour + 4*time.Minute + 3*time.Second + 300*time.Millisecond)

		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &dummyClock{time2})

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 5))).
			createJob(queue).
			pending(cluster, "c")

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			pending(cluster, "f")

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 10))).
			createJob(queue).
			pending(cluster, "a1").
			running(cluster, "a2", node)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 5))).
			createJob(queue).
			pending(cluster, "b1").
			running(cluster, "b2", node)

		longestRunning := newJobSimulator(t, jobStore, newIncrementClock(*increment(time1, 1))).
			createJob(queue).
			pending(cluster, "d1").
			running(cluster, "d2", node)

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			running(cluster, "e1", node).
			succeeded(cluster, "e2", node)

		newJobSimulator(t, jobStore, newIncrementClock(time1)).
			createJob(queue).
			running(cluster, "g", node).
			cancelled()

		queueInfos, err := jobRepo.GetQueueInfos(ctx)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(queueInfos))
		assertJobsAreEquivalent(t, longestRunning.job, queueInfos[0].LongestRunningJob.Job)

		assert.Nil(t, queueInfos[0].LongestRunningJob.Cancelled)
		assert.Equal(t, JobStates.Running, queueInfos[0].LongestRunningJob.JobState)

		assert.Equal(t, 2, len(queueInfos[0].LongestRunningJob.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "d1",
			Cluster:   cluster,
			Node:      "",
			Succeeded: false,
			Created:   increment(time1, 2),
			Started:   nil,
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "d2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   increment(time1, 3),
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[1])

		startRunningTime := *increment(time1, 3)
		assert.Equal(t, types.DurationProto(time2.Sub(startRunningTime).Round(time.Second)), queueInfos[0].LongestRunningDuration)
	})
}

func Test_QueueInfosMultipleQueues(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		// Queue 1
		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue)

		oldestQueued1 := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, "a")

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue).
			pending(cluster, "b1").
			running(cluster, "b2", node)

		longestRunning1 := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, "c1").
			running(cluster, "c2", node)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, "d1").
			running(cluster, "d2", node).
			succeeded(cluster, "d2", node)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			cancelled()

		// Queue 2
		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue2)

		oldestQueued2 := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue2)

		newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue2).
			pending(cluster, "e")

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 5))).
			createJob(queue2).
			running(cluster, "f", node)

		longestRunning2 := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue2).
			running(cluster, "g", node)

		newJobSimulator(t, jobStore, newIncrementClock(*increment(startTime, 1))).
			createJob(queue2).
			pending(cluster, "h1").
			running(cluster, "h2", node)

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

		assertJobsAreEquivalent(t, oldestQueued1.job, queueInfos[0].OldestQueuedJob.Job)
		assert.Nil(t, queueInfos[0].OldestQueuedJob.Cancelled)
		assert.Equal(t, JobStates.Queued, queueInfos[0].OldestQueuedJob.JobState)
		assert.Equal(t, 0, len(queueInfos[0].OldestQueuedJob.Runs))

		assertJobsAreEquivalent(t, longestRunning1.job, queueInfos[0].LongestRunningJob.Job)
		assert.Nil(t, queueInfos[0].LongestRunningJob.Cancelled)
		assert.Equal(t, JobStates.Running, queueInfos[0].LongestRunningJob.JobState)
		assert.Equal(t, 2, len(queueInfos[0].LongestRunningJob.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "c1",
			Cluster:   cluster,
			Node:      "",
			Succeeded: false,
			Created:   increment(startTime, 1),
			Started:   nil,
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[0])
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "c2",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   increment(startTime, 2),
			Finished:  nil,
			Error:     "",
		}, queueInfos[0].LongestRunningJob.Runs[1])

		assertJobsAreEquivalent(t, oldestQueued2.job, queueInfos[1].OldestQueuedJob.Job)
		assert.Nil(t, queueInfos[1].OldestQueuedJob.Cancelled)
		assert.Equal(t, JobStates.Queued, queueInfos[1].OldestQueuedJob.JobState)
		assert.Equal(t, 0, len(queueInfos[1].OldestQueuedJob.Runs))

		assertJobsAreEquivalent(t, longestRunning2.job, queueInfos[1].LongestRunningJob.Job)
		assert.Nil(t, queueInfos[1].LongestRunningJob.Cancelled)
		assert.Equal(t, JobStates.Running, queueInfos[1].LongestRunningJob.JobState)
		assert.Equal(t, 1, len(queueInfos[1].LongestRunningJob.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     "g",
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   nil,
			Started:   increment(startTime, 1),
			Finished:  nil,
			Error:     "",
		}, queueInfos[1].LongestRunningJob.Runs[0])
	})
}

func Test_GetNoJobSetsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob("queue-1")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob("queue-2").
			pending(cluster, k8sId1)

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Empty(t, jobSetInfos)
	})
}

func Test_GetsJobSetWithOnlyFinishedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set").
			running(cluster, k8sId1, node).
			succeeded(cluster, k8sId1, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set").
			pending(cluster, k8sId2).
			failed(cluster, k8sId2, node, "some error")

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set",
			JobsQueued:    0,
			JobsPending:   0,
			JobsRunning:   0,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[0])
	})
}

func Test_JobSetsCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "a1").
			pending(cluster, "a2")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "b1").
			running(cluster, "b2", node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "c1").
			running(cluster, "c2", node).
			succeeded(cluster, "c2", node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "d1").
			running(cluster, "d2", node).
			failed(cluster, "d2", node, "something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "e1").
			running(cluster, "e2", node).
			cancelled()

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobSetInfos))
		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-1",
			JobsQueued:    1,
			JobsPending:   1,
			JobsRunning:   1,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[0])
	})
}

func Test_MultipleJobSetsCounts(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Job set 1
		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "a1").
			pending(cluster, "a2")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "b1").
			running(cluster, "b2", node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "c1").
			running(cluster, "c2", node).
			succeeded(cluster, "c2", node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-1").
			pending(cluster, "d1").
			running(cluster, "d2", node).
			failed(cluster, "d2", node, "something bad")

		// Job set 2
		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-2")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-2").
			pending(cluster, "e1")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-2").
			pending(cluster, "f1").
			running(cluster, "f2", node).
			succeeded(cluster, "f2", node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, "job-set-2").
			pending(cluster, "h1").
			running(cluster, "h2", node).
			failed(cluster, "h2", node, "something bad")

		jobSetInfos, err := jobRepo.GetJobSetInfos(ctx, &lookout.GetJobSetsRequest{Queue: queue})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobSetInfos))

		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-1",
			JobsQueued:    2,
			JobsPending:   1,
			JobsRunning:   1,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[0])

		assertJobSetInfosAreEqual(t, &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        "job-set-2",
			JobsQueued:    1,
			JobsPending:   1,
			JobsRunning:   0,
			JobsSucceeded: 1,
			JobsFailed:    1,
		}, jobSetInfos[1])
	})
}

func Test_GetNoJobsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob("queue-1")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob("queue-2").
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob("queue-2").
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: "queue",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Empty(t, jobInfos)
	})
}

func Test_GetSucceededJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			running(cluster, k8sId1, node).
			succeeded(cluster, k8sId1, node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, JobStates.Succeeded, jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		runInfo := jobInfo.Runs[0]
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
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
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()
		failureReason := "Something bad happened"

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			running(cluster, k8sId1, node).
			failed(cluster, k8sId1, node, failureReason)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, JobStates.Failed, jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
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
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			running(cluster, k8sId1, node).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assertTimesApproxEqual(t, increment(startTime, 3), jobInfo.Cancelled)

		assert.Equal(t, JobStates.Cancelled, jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   increment(startTime, 1),
			Started:   increment(startTime, 2),
		}, jobInfo.Runs[0])
	})
}

func Test_GetMultipleRunJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		retried := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node).
			succeeded(cluster, k8sId2, node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, retried.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, 2, len(jobInfo.Runs))
		assertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      "",
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
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Should be sorted by ULID
		jobId1 := util.NewULID()
		jobId2 := util.NewULID()
		jobId3 := util.NewULID()

		third := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithId(queue, jobId3).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		second := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithId(queue, jobId2).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		first := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithId(queue, jobId1).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		assertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, third.job, jobInfos[2].Job)
	})
}

func Test_GetJobsOrderedFromNewestToOldest(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Should be sorted by ULID
		jobId1 := util.NewULID()
		jobId2 := util.NewULID()
		jobId3 := util.NewULID()

		first := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithId(queue, jobId1).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		second := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithId(queue, jobId2).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		third := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithId(queue, jobId3).
			pending(cluster, util.NewULID()).
			running(cluster, util.NewULID(), node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:       queue,
			Take:        10,
			NewestFirst: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		assertJobsAreEquivalent(t, third.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, first.job, jobInfos[2].Job)
	})
}

func Test_FilterQueuedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		queued := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Queued},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Queued, jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func Test_FilterPendingJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		pending := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Pending},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Pending, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func Test_FilterRunningJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		running := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Running},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, running.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Running, jobInfos[0].JobState)
		assert.Equal(t, 2, len(jobInfos[0].Runs))
	})
}

func Test_FilterSucceededJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		succeeded := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Succeeded},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, succeeded.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Succeeded, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func Test_FilterFailedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		failed := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Failed},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, failed.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Failed, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func Test_FilterCancelledJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		cancelled := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Cancelled},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, cancelled.job, jobInfos[0].Job)
		assert.NotNil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Cancelled, jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func Test_ErrorsIfUnknownStateIsGiven(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		_, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{"Unknown"},
		})

		assert.Error(t, err)
	})
}

func Test_FilterMultipleStates(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		queued := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue)

		pending := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		running := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		succeeded := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		failed := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		cancelled := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Queued, JobStates.Running, JobStates.Failed},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		assertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, running.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, failed.job, jobInfos[2].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Pending, JobStates.Succeeded, JobStates.Cancelled},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		assertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, succeeded.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, cancelled.job, jobInfos[2].Job)
	})
}

func Test_FilterBySingleJobSet(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		job5 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		job6 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet1},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		assertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet2},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		assertJobsAreEquivalent(t, job3.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job4.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet3},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		assertJobsAreEquivalent(t, job5.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job6.job, jobInfos[1].Job)
	})
}

func Test_FilterByMultipleJobSets(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet1, jobSet2},
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(jobInfos))
		assertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, job3.job, jobInfos[2].Job)
		assertJobsAreEquivalent(t, job4.job, jobInfos[3].Job)
	})
}

func Test_FilterByJobSetStartingWith(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		job5 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		job6 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{"job-se"},
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, len(jobInfos))
		assertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, job3.job, jobInfos[2].Job)
		assertJobsAreEquivalent(t, job4.job, jobInfos[3].Job)
		assertJobsAreEquivalent(t, job5.job, jobInfos[4].Job)
		assertJobsAreEquivalent(t, job6.job, jobInfos[5].Job)
	})
}

func Test_FilterByMultipleJobSetStartingWith(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "hello-1"
		jobSet2 := "world-2"
		jobSet3 := "world-3"
		jobSet4 := "other-job-set"

		job1 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		job5 := newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &DefaultClock{}).
			createJobWithJobSet(queue, jobSet4).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{"hello", "world"},
		})
		assert.NoError(t, err)
		assert.Equal(t, 5, len(jobInfos))
		assertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, job3.job, jobInfos[2].Job)
		assertJobsAreEquivalent(t, job4.job, jobInfos[3].Job)
		assertJobsAreEquivalent(t, job5.job, jobInfos[4].Job)
	})
}

func Test_TakeOldestJobsFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &DefaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  uint32(take),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			assertJobsAreEquivalent(t, allJobs[i].job, jobInfos[i].Job)
		}
	})
}

func Test_TakeNewestJobsFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &DefaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			NewestFirst: true,
			Queue:       queue,
			Take:        uint32(take),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			assertJobsAreEquivalent(t, allJobs[nJobs-i-1].job, jobInfos[i].Job)
		}
	})
}

func Test_SkipFirstOldestJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &DefaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  uint32(take),
			Skip:  uint32(skip),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			assertJobsAreEquivalent(t, allJobs[skip+i].job, jobInfos[i].Job)
		}
	})
}

func Test_SkipFirstNewestJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &DefaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			NewestFirst: true,
			Queue:       queue,
			Take:        uint32(take),
			Skip:        uint32(skip),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			assertJobsAreEquivalent(t, allJobs[nJobs-skip-i-1].job, jobInfos[i].Job)
		}
	})
}

func assertQueueInfoCountsAreEqual(t *testing.T, expected *lookout.QueueInfo, actual *lookout.QueueInfo) {
	t.Helper()
	assert.Equal(t, expected.Queue, actual.Queue)
	assert.Equal(t, expected.JobsQueued, actual.JobsQueued)
	assert.Equal(t, expected.JobsPending, actual.JobsPending)
	assert.Equal(t, expected.JobsRunning, actual.JobsRunning)
}

func assertJobSetInfosAreEqual(t *testing.T, expected *lookout.JobSetInfo, actual *lookout.JobSetInfo) {
	t.Helper()
	assert.Equal(t, expected.JobSet, actual.JobSet)
	assert.Equal(t, expected.Queue, actual.Queue)
	assert.Equal(t, expected.JobsQueued, actual.JobsQueued)
	assert.Equal(t, expected.JobsPending, actual.JobsPending)
	assert.Equal(t, expected.JobsRunning, actual.JobsRunning)
	assert.Equal(t, expected.JobsSucceeded, actual.JobsSucceeded)
	assert.Equal(t, expected.JobsFailed, actual.JobsFailed)

}

func assertJobsAreEquivalent(t *testing.T, expected *api.Job, actual *api.Job) {
	t.Helper()
	assert.Equal(t, expected.Id, actual.Id)
	assert.Equal(t, expected.JobSetId, actual.JobSetId)
	assert.Equal(t, expected.Owner, actual.Owner)
	assert.Equal(t, expected.Priority, actual.Priority)
	assert.Equal(t, expected.Queue, actual.Queue)
	assertTimesApproxEqual(t, &expected.Created, &actual.Created)
}

func assertRunInfosEquivalent(t *testing.T, expected *lookout.RunInfo, actual *lookout.RunInfo) {
	assert.Equal(t, expected.K8SId, actual.K8SId)
	assert.Equal(t, expected.Cluster, actual.Cluster)
	assert.Equal(t, expected.Node, actual.Node)
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

type jobSimulator struct {
	t        *testing.T
	jobStore JobRecorder
	clock    Clock
	job      *api.Job
}

type dummyClock struct {
	t time.Time
}

func (c *dummyClock) Now() time.Time {
	return c.t
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

func (c *incrementClock) Now() time.Time {
	t := increment(c.startTime, c.increments)
	c.increments++
	return *t
}

func newJobSimulator(t *testing.T, jobStore JobRecorder, timeProvider Clock) *jobSimulator {
	return &jobSimulator{
		t:        t,
		jobStore: jobStore,
		clock:    timeProvider,
		job:      nil,
	}
}

func (js *jobSimulator) createJob(queue string) *jobSimulator {
	return js.createJobWithOpts(queue, util.NewULID(), "job-set")
}

func (js *jobSimulator) createJobWithId(queue string, id string) *jobSimulator {
	return js.createJobWithOpts(queue, id, "job-set")
}

func (js *jobSimulator) createJobWithJobSet(queue string, jobSetId string) *jobSimulator {
	return js.createJobWithOpts(queue, util.NewULID(), jobSetId)
}

func (js *jobSimulator) createJobWithOpts(queue string, jobId string, jobSetId string) *jobSimulator {
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

func (js *jobSimulator) pending(cluster string, k8sId string) *jobSimulator {
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

func (js *jobSimulator) running(cluster string, k8sId string, node string) *jobSimulator {
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

func (js *jobSimulator) succeeded(cluster string, k8sId string, node string) *jobSimulator {
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
		Created:  js.clock.Now(),
	}
	assert.NoError(js.t, js.jobStore.MarkCancelled(cancelledEvent))
	return js
}
