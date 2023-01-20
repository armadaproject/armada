package repository

import (
	"sort"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api/lookout"
)

func TestGetJobs_GetQueued(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime.Add(5 * time.Second)})

		queuedTime := someTime.Add(time.Second)

		_ = NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, queuedTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assert.Equal(t, "4s", jobInfos[0].JobStateDuration)
	})
}

func TestGetJobs_GetPending(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime.Add(5 * time.Second)})

		pendingTime := someTime.Add(time.Second)

		_ = NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, k8sId1, pendingTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assert.Equal(t, "4s", jobInfos[0].JobStateDuration)
	})
}

func TestGetJobs_GetRunning(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime.Add(5 * time.Second)})

		pendingTime := someTime.Add(time.Second)
		runningTime := someTime.Add(2 * time.Second)

		_ = NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, k8sId1, pendingTime).
			RunningAtTime(cluster, k8sId1, node, runningTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assert.Equal(t, "3s", jobInfos[0].JobStateDuration)
	})
}

func TestGetJobs_GetSucceededJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime.Add(5 * time.Second)})

		pendingTime := someTime.Add(time.Second)
		runningTime := someTime.Add(2 * time.Second)
		succeededTime := someTime.Add(3 * time.Second)

		succeeded := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, k8sId1, pendingTime).
			RunningAtTime(cluster, k8sId1, node, runningTime).
			SucceededAtTime(cluster, k8sId1, node, succeededTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, succeeded.job, jobInfo.Job)
		assert.Equal(t, "2s", jobInfo.JobStateDuration)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, string(JobSucceeded), jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		runInfo := jobInfo.Runs[0]
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Created:   &pendingTime,
			Started:   &runningTime,
			Finished:  &succeededTime,
		}, runInfo)
	})
}

func TestGetJobs_GetFailedJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime.Add(5 * time.Second)})

		pendingTime := someTime.Add(time.Second)
		runningTime := someTime.Add(2 * time.Second)
		failedTime := someTime.Add(3 * time.Second)
		failureReason := "Something bad happened"

		failed := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, k8sId1, pendingTime).
			RunningAtTime(cluster, k8sId1, node, runningTime).
			FailedAtTime(cluster, k8sId1, node, failureReason, failedTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, failed.job, jobInfo.Job)
		assert.Equal(t, "2s", jobInfo.JobStateDuration)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, string(JobFailed), jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   &pendingTime,
			Started:   &runningTime,
			Finished:  &failedTime,
			Error:     failureReason,
		}, jobInfo.Runs[0])
	})
}

func TestGetJobs_GetCancelledJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DummyClock{someTime.Add(5 * time.Second)})

		pendingTime := someTime.Add(time.Second)
		runningTime := someTime.Add(2 * time.Second)
		cancelledTime := someTime.Add(3 * time.Second)

		cancelled := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, k8sId1, pendingTime).
			RunningAtTime(cluster, k8sId1, node, runningTime).
			CancelledAtTime(cancelledTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, cancelled.job, jobInfo.Job)
		assert.Equal(t, "2s", jobInfo.JobStateDuration)

		AssertTimesApproxEqual(t, &cancelledTime, jobInfo.Cancelled)

		assert.Equal(t, string(JobCancelled), jobInfo.JobState)

		assert.Equal(t, 1, len(jobInfo.Runs))
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   &pendingTime,
			Started:   &runningTime,
		}, jobInfo.Runs[0])
	})
}

func TestGetJobs_GetMultipleRunJob(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		pendingTime1 := someTime.Add(time.Second)
		unableToScheduleTime := someTime.Add(2 * time.Second)
		unableToScheduleReason := "unable to schedule reason"
		pendingTime2 := someTime.Add(3 * time.Second)
		runningTime := someTime.Add(4 * time.Second)
		succeededTime := someTime.Add(5 * time.Second)

		retried := NewJobSimulator(t, jobStore).
			CreateJobAtTime(queue, someTime).
			PendingAtTime(cluster, k8sId1, pendingTime1).
			UnableToScheduleAtTime(cluster, k8sId1, node, unableToScheduleTime, unableToScheduleReason).
			PendingAtTime(cluster, k8sId2, pendingTime2).
			RunningAtTime(cluster, k8sId2, node, runningTime).
			SucceededAtTime(cluster, k8sId2, node, succeededTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{Take: 10})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		jobInfo := jobInfos[0]
		AssertJobsAreEquivalent(t, retried.job, jobInfo.Job)

		assert.Nil(t, jobInfo.Cancelled)

		assert.Equal(t, 2, len(jobInfo.Runs))
		// Run order is not guaranteed
		sort.SliceStable(jobInfo.Runs, func(i, j int) bool {
			return jobInfo.Runs[i].Created.Before(*jobInfo.Runs[j].Created)
		})
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId1,
			Cluster:   cluster,
			Node:      node,
			Succeeded: false,
			Created:   &pendingTime1,
			Finished:  &unableToScheduleTime,
			Error:     unableToScheduleReason,
		}, jobInfo.Runs[0])
		AssertRunInfosEquivalent(t, &lookout.RunInfo{
			K8SId:     k8sId2,
			Cluster:   cluster,
			Node:      node,
			Succeeded: true,
			Created:   &pendingTime2,
			Started:   &runningTime,
			Finished:  &succeededTime,
		}, jobInfo.Runs[1])
	})
}

func TestGetJobs_GetJobJson(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		queued := NewJobSimulator(t, jobStore).
			CreateJob(queue)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			NewestFirst: true,
			Take:        10,
			Skip:        0,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))

		AssertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
	})
}

func TestGetJobs_GetNoJobsIfQueueDoesNotExist(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		NewJobSimulator(t, jobStore).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob("queue-2").
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "other-queue",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Empty(t, jobInfos)
	})
}

func TestGetJobs_FilterByQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		NewJobSimulator(t, jobStore).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		third := NewJobSimulator(t, jobStore).
			CreateJob("queue-3").
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "queue-3",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, third.job, jobInfos[0].Job)
	})
}

func TestGetJobs_FilterByNoQueueReturnsAll(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		NewJobSimulator(t, jobStore).
			CreateJob("queue-1")

		NewJobSimulator(t, jobStore).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob("queue-3").
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
	})
}

func TestGetJobs_FilterByQueueGlobSearchOrExact(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)

		first := NewJobSimulator(t, jobStore).
			CreateJob("queue-1")

		second := NewJobSimulator(t, jobStore).
			CreateJob("queue-2").
			Pending(cluster, k8sId1)

		third := NewJobSimulator(t, jobStore).
			CreateJob("queue-3").
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "queue*",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, third.job, jobInfos[2].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "queue-1",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "queue-3",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, third.job, jobInfos[0].Job)
	})
}

func TestGetJobs_FilterQueuedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		queued := NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobQueued)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, string(JobQueued), jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func TestGetJobs_FilterPendingJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		pending := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobPending)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, string(JobPending), jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func TestGetJobs_FilterRunningJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		running := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobRunning)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, running.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, string(JobRunning), jobInfos[0].JobState)
		assert.Equal(t, 2, len(jobInfos[0].Runs))
	})
}

func TestGetJobs_FilterSucceededJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		succeeded := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobSucceeded)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, succeeded.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, string(JobSucceeded), jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func TestGetJobs_FilterFailedJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		failed := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobFailed)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, failed.job, jobInfos[0].Job)
		assert.Nil(t, jobInfos[0].Cancelled)
		assert.Equal(t, string(JobFailed), jobInfos[0].JobState)
		assert.Equal(t, 1, len(jobInfos[0].Runs))
	})
}

func TestGetJobs_FilterCancelledJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		cancelled := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobCancelled)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, cancelled.job, jobInfos[0].Job)
		assert.NotNil(t, jobInfos[0].Cancelled)
		assert.Equal(t, string(JobCancelled), jobInfos[0].JobState)
		assert.Empty(t, jobInfos[0].Runs)
	})
}

func TestGetJobs_ErrorsIfUnknownStateIsGiven(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		_, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue:     queue,
			Take:      10,
			JobStates: []string{"Unknown"},
		})

		assert.Error(t, err)
	})
}

func TestGetJobs_FilterMultipleStates(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		queued := NewJobSimulator(t, jobStore).
			CreateJob(queue)

		pending := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		running := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		succeeded := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		failed := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		cancelled := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobQueued), string(JobRunning), string(JobFailed)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		AssertJobsAreEquivalent(t, queued.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, running.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, failed.job, jobInfos[2].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobStates: []string{string(JobPending), string(JobSucceeded), string(JobCancelled)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))
		AssertJobsAreEquivalent(t, pending.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, succeeded.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, cancelled.job, jobInfos[2].Job)
	})
}

func TestGetJobs_FilterBySingleJobSet(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job5 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		job6 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobSetIds: []string{jobSet1},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, job1.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job2.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue:     queue,
			Take:      10,
			JobSetIds: []string{jobSet2},
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, job3.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, job4.job, jobInfos[1].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
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

func TestGetJobs_FilterByMultipleJobSets(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
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

func TestGetJobs_FilterJobSetsGlobSearchOrExact(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		job1 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job5 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		job6 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobSetIds: []string{"job-set-*"},
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

func TestGetJobs_FilterByMultipleJobSetsGlobSearch(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobSet1 := "hello-1"
		jobSet2 := "world-2"
		jobSet3 := "world-3"
		jobSet4 := "other-job-set"

		job1 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1)

		job2 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		job3 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		job4 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job5 := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet4).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobSetIds: []string{"hello*", "world*"},
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

func TestGetJobs_FilterEmptyJobSetsReturnsAll(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobSet1 := "job-set-1"
		jobSet2 := "job-set-2"
		jobSet3 := "job-set-3"

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet1).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet2).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, jobSet3).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:      10,
			JobSetIds: []string{""},
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, len(jobInfos))
	})
}

func TestGetJobs_FilterByJobId(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		job := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			JobId: job.job.Id,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, job.job, jobInfos[0].Job)
	})
}

func TestGetJobs_FilterByJobIdWithWrongQueue(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job := NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Queue: "queue-2",
			JobId: job.job.Id,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobInfos))
	})
}

func TestGetJobs_FilterByJobIdWithWrongJobSet(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job := NewJobSimulator(t, jobStore).
			CreateJobWithJobSet(queue, "job-set").
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			JobId:     job.job.Id,
			JobSetIds: []string{"other-job-set"},
			Take:      10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobInfos))
	})
}

func TestGetJobs_FilterByNoOwnerReturnsAll(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		NewJobSimulator(t, jobStore).
			CreateJobWithOwner(queue, "other-user").
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Owner: "",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, len(jobInfos))
	})
}

func TestGetJobs_FilterByOwner(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		job := NewJobSimulator(t, jobStore).
			CreateJobWithOwner(queue, "other-user").
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Owner: "other-user",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, job.job, jobInfos[0].Job)
	})
}

func TestGetJobs_FilterByOwnerStartsWith(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		NewJobSimulator(t, jobStore).
			CreateJob(queue)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId1)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId2).
			UnableToSchedule(cluster, k8sId2, node).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Pending(cluster, k8sId4).
			Running(cluster, k8sId4, node).
			Succeeded(cluster, k8sId4, node)

		first := NewJobSimulator(t, jobStore).
			CreateJobWithOwner(queue, "other-user-a").
			Failed(cluster, k8sId5, node, "Something bad")

		second := NewJobSimulator(t, jobStore).
			CreateJobWithOwner(queue, "other-user-b").
			Failed(cluster, k8sId5, node, "Something bad")

		NewJobSimulator(t, jobStore).
			CreateJob(queue).
			Cancelled()

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Owner: "other-user",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobInfos))

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Owner: "other-user-a",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Owner: "other-user-b",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, second.job, jobInfos[0].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Owner: "other-user*",
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
	})
}

func TestGetJobs_FilterBySingleAnnotation(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, "prefix/")
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		job := NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"prefix/a": "a",
				"b":        "b",
			})

		NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"a":        "a",
				"prefix/b": "b",
			})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			UserAnnotations: map[string]string{
				"a": "a",
			},
			Take: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, job.job, jobInfos[0].Job)
	})
}

func TestGetJobs_FilterByMultipleAnnotations(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, "prefix/")
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		first := NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"prefix/a": "a",
				"prefix/b": "b",
				"c":        "c",
			})

		second := NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"prefix/a": "a",
				"prefix/b": "b",
			})

		NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"a":        "a",
				"prefix/b": "b",
				"prefix/c": "c",
			})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			UserAnnotations: map[string]string{
				"a": "a",
				"b": "b",
			},
			Take: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
	})
}

func TestGetJobs_FilterByAnnotationWithValueStartingWith(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, "prefix/")
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		first := NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"prefix/a": "aab",
				"c":        "c",
			})

		second := NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"prefix/a": "aac",
			})

		NewJobSimulator(t, jobStore).
			CreateJobWithAnnotations(queue, map[string]string{
				"prefix/a": "abc",
			})

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			UserAnnotations: map[string]string{
				"a": "aa",
			},
			Take: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(jobInfos))
		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
	})
}

func TestGetJobs_GetJobsOrderedFromOldestToNewest(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		// Should be sorted by ULID
		jobId1 := "a"
		jobId2 := "b"
		jobId3 := "c"

		third := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, jobId3).
			Pending(cluster, k8sId1).
			Running(cluster, k8sId1, node)

		second := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, jobId2).
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		first := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, jobId1).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:        10,
			NewestFirst: false,
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(jobInfos))

		AssertJobsAreEquivalent(t, first.job, jobInfos[0].Job)
		AssertJobsAreEquivalent(t, second.job, jobInfos[1].Job)
		AssertJobsAreEquivalent(t, third.job, jobInfos[2].Job)
	})
}

func TestGetJobs_GetJobsOrderedFromNewestToOldest(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		// Should be sorted by ULID
		jobId1 := "a"
		jobId2 := "b"
		jobId3 := "c"

		first := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, jobId1).
			Pending(cluster, k8sId1).
			Running(cluster, k8sId1, node)

		second := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, jobId2).
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		third := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, jobId3).
			Pending(cluster, k8sId3).
			Running(cluster, k8sId3, node)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
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

func TestGetJobs_TakeOldestJobsFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		nJobs := 100
		take := 10

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore).
				CreateJobWithAnnotations(queue, map[string]string{
					userAnnotationPrefix + "a": "a",
					userAnnotationPrefix + "b": "b",
					userAnnotationPrefix + "c": "c",
				}).
				Pending(cluster, k8sId).
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				Running(cluster, otherK8sId, node)
		}

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take: uint32(take),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			AssertJobsAreEquivalent(t, allJobs[i].job, jobInfos[i].Job)
		}
	})
}

func TestGetJobs_TakeNewestJobsFirst(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		nJobs := 100
		take := 10

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore).
				CreateJobWithAnnotations(queue, map[string]string{
					userAnnotationPrefix + "a": "a",
					userAnnotationPrefix + "b": "b",
					userAnnotationPrefix + "c": "c",
				}).
				Pending(cluster, k8sId).
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				Running(cluster, otherK8sId, node)
		}

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			NewestFirst: true,
			Take:        uint32(take),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			AssertJobsAreEquivalent(t, allJobs[nJobs-i-1].job, jobInfos[i].Job)
		}
	})
}

func TestGetJobs_SkipFirstOldestJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore).
				CreateJob(queue).
				Pending(cluster, k8sId).
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				Running(cluster, otherK8sId, node)
		}

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take: uint32(take),
			Skip: uint32(skip),
		})
		assert.NoError(t, err)
		assert.Equal(t, take, len(jobInfos))
		for i := 0; i < take; i++ {
			AssertJobsAreEquivalent(t, allJobs[skip+i].job, jobInfos[i].Job)
		}
	})
}

func TestGetJobs_SkipFirstNewestJobs(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		nJobs := 100
		take := 10
		skip := 37

		allJobs := make([]*JobSimulator, nJobs)

		for i := 0; i < nJobs; i++ {
			k8sId := util.NewULID()
			otherK8sId := util.NewULID()
			allJobs[i] = NewJobSimulator(t, jobStore).
				CreateJob(queue).
				UnableToSchedule(cluster, k8sId, node).
				Pending(cluster, otherK8sId).
				Running(cluster, otherK8sId, node)
		}

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			NewestFirst: true,
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

func TestGetJobs_RemovesDuplicateJobsByDefault(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		correctJob := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, "correct").
			Pending(cluster, k8sId2).
			Running(cluster, k8sId2, node)

		duplicate := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, "duplicate").
			Duplicate("correct")

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:        10,
			NewestFirst: false,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, correctJob.job, jobInfos[0].Job)

		jobInfos, err = jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take:        10,
			NewestFirst: false,
			JobStates:   []string{string(JobDuplicate)},
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, duplicate.job, jobInfos[0].Job)
	})
}

func TestGetJobs_DoesntErrorIfJobSpecIsNull(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobRepository(db, &util.DefaultClock{})

		jobId := util.NewULID()
		err := SaveJobWithNullObj(db, jobId, queue, "job-set-1")
		assert.NoError(t, err)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			JobId: jobId,
			Take:  10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		assert.Equal(t, jobId, jobInfos[0].Job.Id)
		assert.Equal(t, queue, jobInfos[0].Job.Queue)
		assert.Equal(t, "job-set-1", jobInfos[0].Job.JobSetId)
		assert.Equal(t, "", jobInfos[0].JobJson)
	})
}

func TestGetJobs_TimeInStateMultipleRuns(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		clock := &util.DummyClock{T: someTime.Add(9 * time.Second)}
		jobStore := NewSQLJobStore(db, userAnnotationPrefix)
		jobRepo := NewSQLJobRepository(db, clock)

		job := NewJobSimulator(t, jobStore).
			CreateJobWithId(queue, "correct").
			Pending(cluster, k8sId1).
			Pending(cluster, k8sId2).
			RunningAtTime(cluster, k8sId2, node, someTime)

		jobInfos, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{
			Take: 10,
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobInfos))
		AssertJobsAreEquivalent(t, job.job, jobInfos[0].Job)
		assert.Equal(t, "9s", jobInfos[0].JobStateDuration)
	})
}
