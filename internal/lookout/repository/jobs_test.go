package repository

import (
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api/lookout"
)

func TestGetJobsInQueue_GetNoJobsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: "queue",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Empty(t, jobInfos)
	})
}

func TestGetJobsInQueue_GetSucceededJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		succeeded := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, k8sId1).
			Running(cluster, k8sId1, node).
			Succeeded(cluster, k8sId1, node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, JobStates.Succeeded, jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		runInfo := jobInfo.Runs[0]
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Created:   Increment(startTime, 1),
			Started:   Increment(startTime, 2),
			Finished:  Increment(startTime, 3),
		}, runInfo)
	})
}

func TestGetJobsInQueue_GetFailedJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()
		failureReason := "Something bad happened"

		failed := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, k8sId1).
			Running(cluster, k8sId1, node).
			Failed(cluster, k8sId1, node, failureReason)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, failed.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, JobStates.Failed, jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   Increment(startTime, 1),
			Started:   Increment(startTime, 2),
			Finished:  Increment(startTime, 3),
			Error:     failureReason,
		}, jobInfo.Runs[0])
	})
}

func TestGetJobsInQueue_GetCancelledJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		cancelled := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, k8sId1).
			Running(cluster, k8sId1, node).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, cancelled.job, jobInfo.Job)

		AssertTimesApproxEqual(t, Increment(startTime, 3), jobInfo.Cancelled)

		assert.Equal(t, JobStates.Cancelled, jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   Increment(startTime, 1),
			Started:   Increment(startTime, 2),
		}, jobInfo.Runs[0])
	})
}

func TestGetJobsInQueue_GetMultipleRunJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		retried := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Pending(cluster, k8sId1).
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node).
			Succeeded(cluster, k8sId2, node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, retried.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, 2, len(jobInfo.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      "",
			Succeeded: false,
			Created:   Increment(startTime, 1),
		}, jobInfo.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId2,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Created:   Increment(startTime, 2),
			Started:   Increment(startTime, 3),
			Finished:  Increment(startTime, 4),
		}, jobInfo.Runs[1])
	})
}

func TestGetJobsInQueue_GetJobsOrderedFromOldestToNewest(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Should be sorted by ULID
		jobId1 := "a"
		jobId2 := "b"
		jobId3 := "c"

		third := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithId(queue, jobId3).
			Pending(cluster, util.NewULID()).
			Running(cluster, util.NewULID(), node)

		second := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithId(queue, jobId2).
			Pending(cluster, util.NewULID()).
			Running(cluster, util.NewULID(), node)

		first := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithId(queue, jobId1).
			Pending(cluster, util.NewULID()).
			Running(cluster, util.NewULID(), node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, third.job, jobInfos[2].Job)
	})
}

func TestGetJobsInQueue_GetJobsOrderedFromNewestToOldest(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		// Should be sorted by ULID
		jobId1 := "a"
		jobId2 := "b"
		jobId3 := "c"

		first := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithId(queue, jobId1).
			Pending(cluster, util.NewULID()).
			Running(cluster, util.NewULID(), node)

		second := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithId(queue, jobId2).
			Pending(cluster, util.NewULID()).
			Running(cluster, util.NewULID(), node)

		third := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithId(queue, jobId3).
			Pending(cluster, util.NewULID()).
			Running(cluster, util.NewULID(), node)

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:       queue,
			Take:        10,
			NewestFirst: true,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		AssertJobsAreEquivalent(t, third.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, first.job, jobInfos[2].Job)
	})
}

func TestGetJobsInQueue_FilterQueuedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		queued := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Queued},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Queued, jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func TestGetJobsInQueue_FilterPendingJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		pending := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Pending},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Pending, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func TestGetJobsInQueue_FilterRunningJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		running := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Running},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, running.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Running, jobInfos[0].JobState)
		assert.Equal(t, 2, len(jobInfos[0].Runs))
	})
}

func TestGetJobsInQueue_FilterSucceededJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		succeeded := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Succeeded},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, succeeded.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Succeeded, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func TestGetJobsInQueue_FilterFailedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		failed := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Failed},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, failed.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Failed, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func TestGetJobsInQueue_FilterCancelledJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		cancelled := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Cancelled},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, cancelled.job, jobInfos[0].Job)
		assert.NotNil(t, jobInfos[0].Cancelled)
		assert.Equal(t, JobStates.Cancelled, jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func TestGetJobsInQueue_ErrorsIfUnknownStateIsGiven(t *testing.T) {
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

func TestGetJobsInQueue_FilterMultipleStates(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		queued := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue)

		pending := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		running := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		succeeded := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		failed := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		cancelled := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Queued, JobStates.Running, JobStates.Failed},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		AssertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, running.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, failed.job, jobInfos[2].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{JobStates.Pending, JobStates.Succeeded, JobStates.Cancelled},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		AssertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, succeeded.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, cancelled.job, jobInfos[2].Job)
	})
}

func TestGetJobsInQueue_FilterBySingleJobSet(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job5 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		job6 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet1},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet2},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, job3.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job4.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet3},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, job5.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job6.job, jobInfos[1].Job)
	})
}

func TestGetJobsInQueue_FilterByMultipleJobSets(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet1, jobSet2},
		})
		assert.NoError(t, err)
		assert.Equal(t, 4, len(jobInfos))
		AssertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, job3.job, jobInfos[2].Job)
		AssertJobsAreEquivalent(t, job4.job, jobInfos[3].Job)
	})
}

func TestGetJobsInQueue_FilterByJobSetStartingWith(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job5 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		job6 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{"job-se"},
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, len(jobInfos))
		AssertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, job3.job, jobInfos[2].Job)
		AssertJobsAreEquivalent(t, job4.job, jobInfos[3].Job)
		AssertJobsAreEquivalent(t, job5.job, jobInfos[4].Job)
		AssertJobsAreEquivalent(t, job6.job, jobInfos[5].Job)
	})
}

func TestGetJobsInQueue_FilterByMultipleJobSetStartingWith(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		jobSet1 := "hello-1"
		jobSet2 := "world-2"
		jobSet3 := "world-3"
		jobSet4 := "other-job-set"

		job1 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job5 := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, jobSet4).
			Cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{"hello", "world"},
		})
		assert.NoError(t, err)
		assert.Equal(t, 5, len(jobInfos))
		AssertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, job3.job, jobInfos[2].Job)
		AssertJobsAreEquivalent(t, job4.job, jobInfos[3].Job)
		AssertJobsAreEquivalent(t, job5.job, jobInfos[4].Job)
	})
}

func TestGetJobsInQueue_TakeOldestJobsFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore, &DefaultClock{}).
				CreateJob(queue).
				Pending(cluster, util.NewULID()).
				Pending(cluster, k8sId).
				Running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  uint32(take),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			AssertJobsAreEquivalent(t, allJobs[i].job, jobInfos[i].Job)
		}
	})
}

func TestGetJobsInQueue_TakeNewestJobsFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore, &DefaultClock{}).
				CreateJob(queue).
				Pending(cluster, util.NewULID()).
				Pending(cluster, k8sId).
				Running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			NewestFirst: true,
			Queue:       queue,
			Take:        uint32(take),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			AssertJobsAreEquivalent(t, allJobs[nJobs-i-1].job, jobInfos[i].Job)
		}
	})
}

func TestGetJobsInQueue_SkipFirstOldestJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore, &DefaultClock{}).
				CreateJob(queue).
				Pending(cluster, util.NewULID()).
				Pending(cluster, k8sId).
				Running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(ctx, &lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  uint32(take),
			Skip:  uint32(skip),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			AssertJobsAreEquivalent(t, allJobs[skip+i].job, jobInfos[i].Job)
		}
	})
}

func TestGetJobsInQueue_SkipFirstNewestJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore, &DefaultClock{}).
				CreateJob(queue).
				Pending(cluster, util.NewULID()).
				Pending(cluster, k8sId).
				Running(cluster, k8sId, node)
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
			AssertJobsAreEquivalent(t, allJobs[nJobs-skip-i-1].job, jobInfos[i].Job)
		}
	})
}

func TestGetJob_ReturnsNoJobIfItDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		job, err := jobRepo.GetJob(ctx, "some-id")

		assert.NoError(t, err)
		assert.Nil(t, job)
	})
}

func TestGetJob_Queued(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		queued := NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-3").
			Pending("cluster-3", k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		job, err := jobRepo.GetJob(ctx, queued.job.Id)
		assert.NoError(t, err)
		AssertJobsAreEquivalent(t, queued.job, job.Job)
		assert.Nil(t, job.Cancelled)
		assert.Equal(t, JobStates.Queued, job.JobState)
		assert.Equal(t, 0, len(job.Runs))
	})
}

func TestGetJob_Pending(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		pending := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-3").
			Pending("cluster-3", k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		job, err := jobRepo.GetJob(ctx, pending.job.Id)
		assert.NoError(t, err)
		AssertJobsAreEquivalent(t, pending.job, job.Job)
		assert.Nil(t, job.Cancelled)
		assert.Equal(t, JobStates.Pending, job.JobState)

		assert.Equal(t, 1, len(job.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      "",
			Succeeded: false,
			Error:     "",
			Created:   Increment(startTime, 1),
			Started:   nil,
			Finished:  nil,
		}, job.Runs[0])
	})
}

func TestGetJob_Running(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		running := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob("queue-3").
			Pending("cluster-3", k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		job, err := jobRepo.GetJob(ctx, running.job.Id)
		assert.NoError(t, err)
		AssertJobsAreEquivalent(t, running.job, job.Job)
		assert.Nil(t, job.Cancelled)
		assert.Equal(t, JobStates.Running, job.JobState)

		assert.Equal(t, 2, len(job.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId2,
			Cluster:   "cluster-3",
			Node:      "",
			Succeeded: false,
			Error:     "",
			Created:   Increment(startTime, 1),
			Started:   nil,
			Finished:  nil,
		}, job.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId3,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Error:     "",
			Created:   Increment(startTime, 2),
			Started:   Increment(startTime, 3),
			Finished:  nil,
		}, job.Runs[1])
	})
}

func TestGetJob_Succeeded(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-3").
			Pending("cluster-3", k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		succeeded := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		job, err := jobRepo.GetJob(ctx, succeeded.job.Id)
		assert.NoError(t, err)
		AssertJobsAreEquivalent(t, succeeded.job, job.Job)
		assert.Nil(t, job.Cancelled)
		assert.Equal(t, JobStates.Succeeded, job.JobState)

		assert.Equal(t, 1, len(job.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId4,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Error:     "",
			Created:   Increment(startTime, 1),
			Started:   Increment(startTime, 2),
			Finished:  Increment(startTime, 3),
		}, job.Runs[0])
	})
}

func TestGetJob_Failed(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-3").
			Pending("cluster-3", k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		failed := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Cancelled()

		job, err := jobRepo.GetJob(ctx, failed.job.Id)
		assert.NoError(t, err)
		AssertJobsAreEquivalent(t, failed.job, job.Job)
		assert.Nil(t, job.Cancelled)
		assert.Equal(t, JobStates.Failed, job.JobState)

		assert.Equal(t, 1, len(job.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId5,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Error:     "Something bad",
			Created:   nil,
			Started:   nil,
			Finished:  Increment(startTime, 1),
		}, job.Runs[0])
	})
}

func TestGetJob_Cancelled(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		startTime := time.Now()

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob("queue-3").
			Pending("cluster-3", k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJobWithJobSet(queue, "job-set-2").
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore, &DefaultClock{}).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		cancelled := NewJobSimulator(t, jobStore, NewIncrementClock(startTime)).
			CreateJob(queue).
			Cancelled()

		job, err := jobRepo.GetJob(ctx, cancelled.job.Id)
		assert.NoError(t, err)
		AssertJobsAreEquivalent(t, cancelled.job, job.Job)
		AssertTimesApproxEqual(t, Increment(startTime, 1), job.Cancelled)
		assert.Equal(t, JobStates.Cancelled, job.JobState)

		assert.Equal(t, 0, len(job.Runs))
	})
}
