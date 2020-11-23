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

var (
	queue   = "queue"
	cluster = "cluster"
	k8sId1  = util.NewULID()
	k8sId2  = util.NewULID()
	k8sId3  = util.NewULID()
	k8sId4  = util.NewULID()
	k8sId5  = util.NewULID()
	node    = "node"
)

func Test_QueueStats(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

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

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue: "queue",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Empty(t, jobInfos)
	})
}

func Test_GetSucceededJobFromQueue(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			running(cluster, k8sId1, node).
			succeeded(cluster, k8sId1, node)

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, lookout.JobState_SUCCEEDED, jobInfo.JobState)

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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()
		failureReason := "Something bad happened"

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			running(cluster, k8sId1, node).
			failed(cluster, k8sId1, node, failureReason)

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, lookout.JobState_FAILED, jobInfo.JobState)

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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()

		succeeded := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			running(cluster, k8sId1, node).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue: queue,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		assertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)

		assertTimesApproxEqual(t, increment(startTime, 3), jobInfo.Cancelled)

		assert.Equal(t, lookout.JobState_CANCELLED, jobInfo.JobState)

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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		startTime := time.Now()

		retried := newJobSimulator(t, jobStore, newIncrementClock(startTime)).
			createJob(queue).
			pending(cluster, k8sId1).
			pending(cluster, k8sId2).
			running(cluster, k8sId2, node).
			succeeded(cluster, k8sId2, node)

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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

		// Should be sorted by ULID
		jobId1 := util.NewULID()
		jobId2 := util.NewULID()
		jobId3 := util.NewULID()

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

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		// Should be sorted by ULID
		jobId1 := util.NewULID()
		jobId2 := util.NewULID()
		jobId3 := util.NewULID()

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

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queued := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_QUEUED},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, lookout.JobState_QUEUED, jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func Test_FilterPendingJobs(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		pending := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_PENDING},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, lookout.JobState_PENDING, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func Test_FilterRunningJobs(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		running := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_RUNNING},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, running.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, lookout.JobState_RUNNING, jobInfos[0].JobState)
		assert.Equal(t, 2, len(jobInfos[0].Runs))
	})
}

func Test_FilterSucceededJobs(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		succeeded := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_SUCCEEDED},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, succeeded.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, lookout.JobState_SUCCEEDED, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func Test_FilterFailedJobs(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		failed := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_FAILED},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, failed.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, lookout.JobState_FAILED, jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func Test_FilterCancelledJobs(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		cancelled := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_CANCELLED},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assertJobsAreEquivalent(t, cancelled.job, jobInfos[0].Job)
		assert.NotNil(t, jobInfos[0].Cancelled)
		assert.Equal(t, lookout.JobState_CANCELLED, jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func Test_FilterMultipleStates(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		queued := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue)

		pending := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId1)

		running := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		succeeded := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		failed := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			failed(cluster, k8sId5, node, "Something bad")

		cancelled := newJobSimulator(t, jobStore, &defaultClock{}).
			createJob(queue).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_QUEUED, lookout.JobState_RUNNING, lookout.JobState_FAILED},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		assertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, running.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, failed.job, jobInfos[2].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []lookout.JobState{lookout.JobState_PENDING, lookout.JobState_SUCCEEDED, lookout.JobState_CANCELLED},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		assertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, succeeded.job, jobInfos[1].Job)
		assertJobsAreEquivalent(t, cancelled.job, jobInfos[2].Job)
	})
}

func Test_FilterBySingleJobSet(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		job5 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		job6 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet1},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		assertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet2},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		assertJobsAreEquivalent(t, job3.job, jobInfos[0].Job)
		assertJobsAreEquivalent(t, job4.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		job5 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		job6 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		jobSet1 := "hello-1"
		jobSet2 := "world-2"
		jobSet3 := "world-3"
		jobSet4 := "other-job-set"

		job1 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1)

		job2 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet1).
			pending(cluster, k8sId1)

		job3 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId2).
			pending(cluster, k8sId3).
			running(cluster, k8sId3, node)

		job4 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet2).
			pending(cluster, k8sId4).
			running(cluster, k8sId4, node).
			succeeded(cluster, k8sId4, node)

		job5 := newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet3).
			failed(cluster, k8sId5, node, "Something bad")

		newJobSimulator(t, jobStore, &defaultClock{}).
			createJobWithJobSet(queue, jobSet4).
			cancelled()

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		nJobs := 100
		take := 10

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &defaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		nJobs := 100
		take := 10

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &defaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &defaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db)

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*jobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			allJobs[i] = newJobSimulator(t, jobStore, &defaultClock{}).
				createJob(queue).
				pending(cluster, util.NewULID()).
				pending(cluster, k8sId).
				running(cluster, k8sId, node)
		}

		jobInfos, err := jobRepo.GetJobsInQueue(&lookout.GetJobsInQueueRequest{
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
