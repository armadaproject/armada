package repository

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

// This test used to assert that submitting a job twice with the same clientId resulted
// in  the same job id being returned.  We now perform the client id check earlier in the process
// so now we assert that different ids are returned.
func TestJobDoubleSubmit(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		job2 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		assert.NotEqual(t, job1.Id, job2.Id)
	})
}

func TestJobAddDifferentQueuesCanHaveSameClientId(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		job2 := addTestJobWithClientId(t, r, "queue2", "my-job-1")
		assert.NotEqual(t, job1.Id, job2.Id)
	})
}

func TestJobCanBeLeasedOnlyOnce(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		leasedAgain, e := r.TryLeaseJobs("cluster2", map[string][]string{"queue1": {job.Id}})
		require.NoError(t, e)
		assert.Equal(t, 0, len(leasedAgain))
	})
}

func TestJobLeaseCanBeRenewed(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		renewed, e := r.RenewLease("cluster1", []string{job.Id})
		require.NoError(t, e)
		assert.Equal(t, 1, len(renewed))
		assert.Equal(t, job.Id, renewed[0])
	})
}

func TestJobLeaseExpiry(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		deadline := time.Now()
		addLeasedJob(t, r, "queue1", "cluster1")

		_, e := r.ExpireLeases("queue1", deadline)
		require.NoError(t, e)

		queued, e := r.PeekQueue("queue1", 10)
		require.NoError(t, e)
		assert.Equal(t, 1, len(queued), "Queue should have one job which expired")
		assert.Equal(t, job.Id, queued[0].Id)
	})
}

func TestEvenExpiredLeaseCanBeRenewed(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		deadline := time.Now()

		_, e := r.ExpireLeases("queue1", deadline)
		require.NoError(t, e)

		renewed, e := r.RenewLease("cluster1", []string{job.Id})
		require.NoError(t, e)
		assert.Equal(t, 1, len(renewed))
		assert.Equal(t, job.Id, renewed[0])
	})
}

func TestRenewingLeaseFailsForJobAssignedToDifferentCluster(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		renewed, e := r.RenewLease("cluster2", []string{job.Id})
		require.NoError(t, e)
		assert.Equal(t, 0, len(renewed))
	})
}

func TestRenewingNonExistentLease(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		renewed, e := r.RenewLease("cluster2", []string{"missingJobId"})
		require.NoError(t, e)
		assert.Equal(t, 0, len(renewed))
	})
}

func TestDeletingExpiredJobShouldDeleteJobFromQueue(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		deadline := time.Now()

		_, e := r.ExpireLeases("queue1", deadline)
		require.NoError(t, e)

		deletionResult, err := r.DeleteJobs([]*api.Job{job})
		require.NoError(t, err, "deleting jobs failed with error")

		err, deleted := deletionResult[job]

		assert.Equal(t, 1, len(deletionResult))
		assert.True(t, deleted)
		require.NoError(t, err)

		queue, e := r.PeekQueue("queue1", 100)
		require.NoError(t, e)
		assert.Equal(t, 0, len(queue))
	})
}

func TestReturnLeaseShouldReturnJobToQueue(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		returned, e := r.ReturnLease("cluster1", job.Id)
		require.NoError(t, e)
		assert.NotNil(t, returned)

		queue, e := r.PeekQueue("queue1", 100)
		require.NoError(t, e)
		assert.Equal(t, 1, len(queue))
		assert.Equal(t, job.Id, returned.Id)
	})
}

func TestReturnLeaseFromDifferentClusterIsNoop(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		returned, e := r.ReturnLease("cluster2", job.Id)
		require.NoError(t, e)
		assert.Nil(t, returned)

		queue, e := r.PeekQueue("queue1", 100)
		require.NoError(t, e)
		assert.Equal(t, 0, len(queue))
	})
}

func TestReturnLeaseForJobInQueueIsNoop(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addTestJob(t, r, "queue1")

		returned, e := r.ReturnLease("cluster2", job.Id)
		require.NoError(t, e)
		assert.Nil(t, returned)
	})
}

func TestDeleteRunningJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		result, deleteErr := r.DeleteJobs([]*api.Job{job})
		require.NoError(t, deleteErr, "delete jobs failed")
		err, deletionOccurred := result[job]
		assert.True(t, deletionOccurred)
		require.NoError(t, err)
	})
}

func TestDeleteQueuedJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addTestJob(t, r, "queue1")

		result, err := r.DeleteJobs([]*api.Job{job})
		require.NoError(t, err, "deleting jobs failed with error")
		err, deletionOccurred := result[job]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestDeleteWithSomeMissingJobs(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		missingJob := &api.Job{Id: "jobId"}
		runningJob := addLeasedJob(t, r, "queue1", "cluster1")
		result, err := r.DeleteJobs([]*api.Job{missingJob, runningJob})
		require.NoError(t, err, "delete failed")

		err, deletionOccurred := result[missingJob]
		assert.Nil(t, err)
		assert.False(t, deletionOccurred)

		err, deletionOccurred = result[runningJob]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestGetActiveJobIds(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		addTestJob(t, r, "queue1")
		addLeasedJob(t, r, "queue1", "cluster1")
		addTestJob(t, r, "queue2")

		ids, e := r.GetActiveJobIds("queue1", "set1")
		require.NoError(t, e)
		assert.Equal(t, 2, len(ids))
	})
}

func TestGetJobSetJobIds(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		queuedJob := addTestJob(t, r, "queue1")
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")

		// Gives all on when no filter provided
		ids, e := r.GetJobSetJobIds("queue1", "set1", nil)
		require.NoError(t, e)
		assert.Equal(t, 2, len(ids))

		// Gives all on when filter includes all options
		ids, e = r.GetJobSetJobIds("queue1", "set1", &JobSetFilter{
			IncludeQueued: true,
			IncludeLeased: true,
		})
		require.NoError(t, e)
		assert.Equal(t, 2, len(ids))

		// Gives only queued when queued filter provided
		ids, e = r.GetJobSetJobIds("queue1", "set1", &JobSetFilter{
			IncludeQueued: true,
			IncludeLeased: false,
		})
		require.NoError(t, e)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, ids[0], queuedJob.Id)

		// Gives only leased when leased filter provided
		ids, e = r.GetJobSetJobIds("queue1", "set1", &JobSetFilter{
			IncludeQueued: false,
			IncludeLeased: true,
		})
		require.NoError(t, e)
		assert.Equal(t, 1, len(ids))
		assert.Equal(t, ids[0], leasedJob.Id)
	})
}

func TestReturnLeaseForDeletedJobShouldKeepJobDeleted(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "cancel-test-queue", "cluster")

		result, err := r.DeleteJobs([]*api.Job{job})
		require.NoError(t, err, "delete failed")
		assert.Nil(t, result[job])

		returned, err := r.ReturnLease("cluster", job.Id)
		assert.Nil(t, returned)
		assert.Nil(t, err)

		q, err := r.PeekQueue("cancel-test-queue", 100)
		assert.Nil(t, err)
		assert.Empty(t, q)
	})
}

func TestGetLeasedJobIds(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		addTestJob(t, r, "queue1")
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")
		leasedJob2 := addLeasedJob(t, r, "queue1", "cluster2")
		addTestJob(t, r, "queue2")
		addLeasedJob(t, r, "queue2", "cluster1")

		ids, e := r.GetLeasedJobIds("queue1")
		require.NoError(t, e)
		assert.Equal(t, 2, len(ids))
		idsSet := util.StringListToSet(ids)
		assert.True(t, idsSet[leasedJob1.Id])
		assert.True(t, idsSet[leasedJob2.Id])
	})
}

func TestUpdateStartTime(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob.Id,
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)
	})
}

func TestUpdateStartTime_UsesEarlierTime(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		startTimePlusOneHour := time.Now().Add(4 * time.Hour)

		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{
			{
				JobId:     leasedJob.Id,
				ClusterId: "cluster1",
				StartTime: startTime,
			},
			{
				JobId:     leasedJob.Id,
				ClusterId: "cluster1",
				StartTime: startTimePlusOneHour,
			},
		})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob.Id})
		require.NoError(t, err)
		assert.Equal(t, 1, len(runInfos))
		assert.Equal(t, startTime.UTC(), runInfos[leasedJob.Id].StartTime.UTC())
		assert.NotEqual(t, startTimePlusOneHour.UTC(), runInfos[leasedJob.Id].StartTime.UTC())
	})
}

func TestUpdateStartTime_NonExistentJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		startTime := time.Now()
		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     "NonExistent",
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		require.NoError(t, err)
		assert.Len(t, jobErrors, 1)
		assert.Error(t, jobErrors[0])
		var e *ErrJobNotFound
		assert.True(t, errors.As(jobErrors[0], &e))
	})
}

func TestUpdateStartTime_FinishedJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		startTime := time.Now()
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")
		errs, err := r.DeleteJobs([]*api.Job{leasedJob})
		require.NoError(t, err, "delete failed")
		for _, err := range errs {
			require.NoError(t, err)
		}

		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob.Id,
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		require.NoError(t, err)
		assert.Len(t, jobErrors, 1)
		assert.Error(t, jobErrors[0])
		var e *ErrJobNotFound
		assert.True(t, errors.As(jobErrors[0], &e))
	})
}

// Saving/reading the start time shouldn't adjust the actual time it happened
// i.e If the start time happened "now" but in a different time zone, the difference between the start time and now should be ~0 seconds
func TestSaveAndRetrieveStartTime_HandlesDifferentTimeZones(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		loc, err := time.LoadLocation("Asia/Shanghai")
		require.NoError(t, err)
		now := time.Now().UTC()
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := now.In(loc)
		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob.Id,
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob.Id})
		assert.NoError(t, err)
		diff := runInfos[leasedJob.Id].StartTime.Sub(now).Seconds()
		diff = math.Abs(diff)
		assert.Len(t, runInfos, 1)
		assert.True(t, diff < float64(1))
	})
}

func TestGetJobRunInfos(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")
		leasedJob2 := addLeasedJob(t, r, "queue1", "cluster2")

		startTime := time.Now()
		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob1.Id,
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob1.Id, leasedJob2.Id})
		assert.NoError(t, err)
		assert.Len(t, runInfos, 1)
		assert.Equal(t, startTime.UTC(), runInfos[leasedJob1.Id].StartTime.UTC())
		assert.Equal(t, "cluster1", runInfos[leasedJob1.Id].CurrentClusterId)
	})
}

func TestGetJobRunInfos_HandlesJobWithoutClusterAssociation(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJob(t, r, "queue1")
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     job1.Id,
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)
		jobErrors, err = r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob1.Id,
			ClusterId: "cluster1",
			StartTime: startTime,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)

		runInfos, err := r.GetJobRunInfos([]string{job1.Id, leasedJob1.Id})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(runInfos))
		assert.Equal(t, startTime.UTC(), runInfos[leasedJob1.Id].StartTime.UTC())
	})
}

func TestGetJobRunInfos_ReturnStartTimeForCurrentAssociatedCluster(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		plusOneHour := startTime.Add(time.Hour)
		jobErrors, err := r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob1.Id,
			ClusterId: "cluster2",
			StartTime: startTime,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)

		jobErrors, err = r.UpdateStartTime([]*JobStartInfo{{
			JobId:     leasedJob1.Id,
			ClusterId: "cluster1",
			StartTime: plusOneHour,
		}})
		AssertUpdateStartTimeNoErrors(t, jobErrors, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob1.Id})
		assert.NoError(t, err)
		assert.Len(t, runInfos, 1)
		assert.Equal(t, plusOneHour.UTC(), runInfos[leasedJob1.Id].StartTime.UTC())
	})
}

func TestGetQueueActiveJobSets(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		addTestJob(t, r, "queue1")
		addLeasedJob(t, r, "queue1", "cluster1")
		addTestJob(t, r, "queue2")

		infos, e := r.GetQueueActiveJobSets("queue1")
		require.NoError(t, e)
		assert.Equal(t, []*api.JobSetInfo{{
			Name:       "set1",
			QueuedJobs: 1,
			LeasedJobs: 1,
		}}, infos)
	})
}

func TestNumberOfRetryAttemptsIsZeroForNonExistentJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		retries, err := r.GetNumberOfRetryAttempts("nonexistent-job-id")

		require.NoError(t, err)
		assert.Zero(t, retries)
	})
}

func TestNumberOfRetryAttemptsIsZeroForNewJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		testJob := addLeasedJob(t, r, "some-queue", "cluster-1")

		retries, err := r.GetNumberOfRetryAttempts(testJob.Id)

		assert.Nil(t, err)
		assert.Zero(t, retries)
	})
}

func TestAddRetryAttemptCreatesKeyIfJobDoesNotExist(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		err := r.AddRetryAttempt("nonexistent-job-id")

		require.NoError(t, err)

		retries, err := r.GetNumberOfRetryAttempts("nonexistent-job-id")

		require.NoError(t, err)
		assert.Equal(t, 1, retries)
	})
}

func TestJobRetriesAreIncrementedCorrectly(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		testJob := addLeasedJob(t, r, "some-queue", "cluster-1")

		expectedRetries := 7

		for i := 0; i < expectedRetries; i++ {
			err := r.AddRetryAttempt(testJob.Id)
			assert.Nil(t, err)
		}

		retries, err := r.GetNumberOfRetryAttempts(testJob.Id)

		require.NoError(t, err)
		assert.Equal(t, expectedRetries, retries)
	})
}

func TestRetriesOfDeletedJobShouldBeZero(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		testJob := addLeasedJob(t, r, "some-queue", "cluster-1")

		for i := 0; i < 11; i++ {
			err := r.AddRetryAttempt(testJob.Id)
			require.NoError(t, err)
		}

		_, deleteErr := r.DeleteJobs([]*api.Job{testJob})
		require.NoError(t, deleteErr)

		retries, err := r.GetNumberOfRetryAttempts(testJob.Id)

		require.NoError(t, err)
		assert.Zero(t, retries)
	})
}

func TestStoreAndGetPulsarSchedulerJobDetails(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		details := &schedulerobjects.PulsarSchedulerJobDetails{
			JobId:  util.NewULID(),
			Queue:  "testQueue",
			JobSet: "testJobset",
		}
		err := r.StorePulsarSchedulerJobDetails([]*schedulerobjects.PulsarSchedulerJobDetails{details})
		require.NoError(t, err)

		retrievedDetails, err := r.GetPulsarSchedulerJobDetails(details.JobId)
		require.NoError(t, err)
		assert.Equal(t, details, retrievedDetails)

		nonExistantDetails, err := r.GetPulsarSchedulerJobDetails("not a valid details key")
		require.NoError(t, err)
		assert.Nil(t, nonExistantDetails)
	})
}

func TestUpdateJobs_SingleJobThatExists_ChangesJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")

		newSchedName := "custom"

		results, err := r.UpdateJobs([]string{job1.Id}, func(jobs []*api.Job) {
			require.Equal(t, 1, len(jobs))
			jobs[0].PodSpec.SchedulerName = newSchedName
		})
		require.NoError(t, err)

		require.Equal(t, 1, len(results))
		assert.Nil(t, results[0].Error)
		assert.Equal(t, job1.Id, results[0].JobId)

		reloadedJobs, err := r.GetExistingJobsByIds([]string{job1.Id})
		require.NoError(t, err)
		require.Equal(t, 1, len(reloadedJobs))
		assert.Equal(t, newSchedName, reloadedJobs[0].PodSpec.SchedulerName)
		assert.Equal(t, results[0].Job, reloadedJobs[0])
	})
}

func TestUpdateJobs_WhenTransactionAlwaysFails_ReturnsError_JobNotChanged(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")

		newSchedName := "custom"

		results, err := r.UpdateJobs([]string{job1.Id}, func(jobs []*api.Job) {
			results2, err := r.UpdateJobs([]string{job1.Id}, func(jobs []*api.Job) {}) // 2nd update in middle of transaction
			require.NoError(t, err)
			if ok := assert.Equal(t, 1, len(results2)); !ok {
				t.FailNow()
			}
			require.NoError(t, results2[0].Error)
			require.Equal(t, 1, len(jobs))
			jobs[0].PodSpec.SchedulerName = newSchedName
		})
		require.NoError(t, err)

		require.Equal(t, 1, len(results))
		assert.Equal(t, job1.Id, results[0].JobId)
		assert.Nil(t, results[0].Job)
		assert.Equal(t, redis.TxFailedErr, results[0].Error)

		reloadedJobs, err := r.GetExistingJobsByIds([]string{job1.Id})
		require.NoError(t, err)
		require.Equal(t, 1, len(reloadedJobs))
		assert.Equal(t, "", reloadedJobs[0].PodSpec.SchedulerName)
	})
}

func TestUpdateJobs_WhenTransactionFailsOnce_Retries_JobChanged(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")

		newSchedName := "custom"

		first := true
		results := r.updateJobs([]string{job1.Id}, func(jobs []*api.Job) {
			if first {
				results2, err := r.UpdateJobs([]string{job1.Id}, func(jobs []*api.Job) {}) // 2nd update in middle of transaction
				require.NoError(t, err)
				assert.Equal(t, 1, len(results2))
				assert.Nil(t, results2[0].Error)
				first = false
			}
			assert.Equal(t, 1, len(jobs))
			jobs[0].PodSpec.SchedulerName = newSchedName
		}, 100, 3, time.Microsecond)

		assert.Equal(t, 1, len(results))
		assert.Nil(t, results[0].Error)
		assert.Equal(t, job1.Id, results[0].JobId)

		reloadedJobs, err := r.GetExistingJobsByIds([]string{job1.Id})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(reloadedJobs))
		assert.Equal(t, newSchedName, reloadedJobs[0].PodSpec.SchedulerName)
		assert.Equal(t, results[0].Job, reloadedJobs[0])
	})
}

func TestUpdateJobs_WhenTransactionAlwaysFailsForOneBatch_ReturnsErrorForThatBatch_OtherChangesWork(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		job2 := addTestJobWithClientId(t, r, "queue2", "my-job-1")
		job3 := addTestJobWithClientId(t, r, "queue3", "my-job-1")

		newSchedName := "custom"

		results := r.updateJobs([]string{job1.Id, job2.Id, job3.Id}, func(jobs []*api.Job) {
			assert.Equal(t, 1, len(jobs))
			job := jobs[0]
			if job.Id == job2.Id {
				results2, err := r.UpdateJobs([]string{job2.Id}, func(jobs []*api.Job) {}) // 2nd update in middle of transaction
				require.NoError(t, err)
				assert.Equal(t, 1, len(results2))
				assert.Nil(t, results2[0].Error)
			}
			job.PodSpec.SchedulerName = newSchedName
		}, 1, 3, time.Microsecond)

		assert.Equal(t, 3, len(results))

		assert.Equal(t, job1.Id, results[0].JobId)
		assert.Equal(t, job2.Id, results[1].JobId)
		assert.Equal(t, job3.Id, results[2].JobId)

		assert.Equal(t, job1.Id, results[0].Job.Id)
		assert.Nil(t, results[1].Job)
		assert.Equal(t, job3.Id, results[2].Job.Id)

		assert.Equal(t, newSchedName, results[0].Job.PodSpec.SchedulerName)
		assert.Equal(t, newSchedName, results[2].Job.PodSpec.SchedulerName)

		assert.Nil(t, results[0].Error)
		assert.Equal(t, redis.TxFailedErr, results[1].Error)
		assert.Nil(t, results[2].Error)

		reloadedJobs, err := r.GetExistingJobsByIds([]string{job1.Id, job2.Id, job3.Id})
		require.NoError(t, err)
		assert.Equal(t, 3, len(reloadedJobs))
		assert.Equal(t, newSchedName, reloadedJobs[0].PodSpec.SchedulerName)
		assert.Equal(t, "", reloadedJobs[1].PodSpec.SchedulerName)
		assert.Equal(t, newSchedName, reloadedJobs[2].PodSpec.SchedulerName)
	})
}

func TestUpdateJobs_AlreadyProcessed(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		queue := "test-queue"
		jobId := util.NewULID()
		submit1 := addTestJobInner(t, r, jobId, queue, "", 1, v1.ResourceRequirements{}, nil)
		assert.NoError(t, submit1.Error)
		assert.False(t, submit1.AlreadyProcessed)
		assert.Equal(t, jobId, submit1.JobId)

		submit2 := addTestJobInner(t, r, jobId, queue, "", 1, v1.ResourceRequirements{}, nil)
		require.NoError(t, submit2.Error)
		assert.True(t, submit2.AlreadyProcessed)
	})
}

func TestUpdateJobs_WhenOneOfThreeJobsIsMissing_SkipsMissingJob_OtherChangesSucceed_SameBatch(t *testing.T) {
	whenOneOfThreeJobsIsMissing_SkipsMissingJob_OtherChangesSucceed(t, 10)
}

func TestUpdateJobs_WhenOneOfThreeJobsIsMissing_SkipsMissingJob_OtherChangesSucceed_DifferentBatch(t *testing.T) {
	whenOneOfThreeJobsIsMissing_SkipsMissingJob_OtherChangesSucceed(t, 1)
}

func whenOneOfThreeJobsIsMissing_SkipsMissingJob_OtherChangesSucceed(t *testing.T, batchSize int) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		job3 := addTestJobWithClientId(t, r, "queue3", "my-job-1")

		newSchedName := "custom"

		results := r.updateJobs([]string{job1.Id, "wrong", job3.Id}, func(jobs []*api.Job) {
			for _, job := range jobs {
				job.PodSpec.SchedulerName = newSchedName
			}
		}, batchSize, 3, time.Microsecond)

		assert.Equal(t, 2, len(results))
		assert.Equal(t, job1.Id, results[0].JobId)
		assert.Equal(t, job3.Id, results[1].JobId)

		assert.Equal(t, job1.Id, results[0].Job.Id)
		assert.Equal(t, job3.Id, results[1].Job.Id)

		assert.Equal(t, newSchedName, results[0].Job.PodSpec.SchedulerName)
		assert.Equal(t, newSchedName, results[1].Job.PodSpec.SchedulerName)

		assert.Nil(t, results[0].Error)
		assert.Nil(t, results[1].Error)

		reloadedJobs, err := r.GetExistingJobsByIds([]string{job1.Id, job3.Id})
		assert.Nil(t, err)
		assert.Equal(t, 2, len(reloadedJobs))
		assert.Equal(t, newSchedName, reloadedJobs[0].PodSpec.SchedulerName)
		assert.Equal(t, newSchedName, reloadedJobs[1].PodSpec.SchedulerName)
	})
}

func addLeasedJob(t *testing.T, r *RedisJobRepository, queue string, cluster string) *api.Job {
	job := addTestJob(t, r, queue)
	leased, e := r.TryLeaseJobs(cluster, map[string][]string{queue: {job.Id}})
	assert.Nil(t, e)
	s, ok := leased[queue]
	require.True(t, ok)
	assert.Equal(t, []string{job.Id}, s)
	return job
}

func addTestJob(t *testing.T, r *RedisJobRepository, queue string) *api.Job {
	return addTestJobWithClientId(t, r, queue, "")
}

func addTestJobWithClientId(t *testing.T, r *RedisJobRepository, queue string, clientId string) *api.Job {
	cpu := resource.MustParse("1")
	memory := resource.MustParse("512Mi")

	result := addTestJobInner(t, r, "", queue, clientId, 1, v1.ResourceRequirements{
		Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
		Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
	}, []v1.Toleration{})
	result.SubmittedJob.Id = result.JobId // Update job ids for tests to be able to test the duplicate d
	return result.SubmittedJob
}

func addTestJobInner(
	t *testing.T,
	r *RedisJobRepository,
	id string,
	queue string,
	clientId string,
	priority float64,
	requirements v1.ResourceRequirements,
	tolerations []v1.Toleration,
) *SubmitJobResult {
	if id == "" {
		id = util.NewULID()
	}
	jobs := make([]*api.Job, 0, 1)
	j := &api.Job{
		Id:       id,
		ClientId: clientId,
		Queue:    queue,
		JobSetId: "set1",
		Priority: priority,
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: requirements,
				},
			},
			Tolerations: tolerations,
		},
		Created:                  time.Now(),
		Owner:                    "user",
		QueueOwnershipUserGroups: []string{},
	}
	jobs = append(jobs, j)

	results, e := r.AddJobs(jobs)
	assert.Nil(t, e)
	for _, result := range results {
		assert.Empty(t, result.Error)
	}
	return results[0]
}

func withRepository(action func(r *RedisJobRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()
	client.FlushDB()
	repo := NewRedisJobRepository(client)
	action(repo)
}

func AssertUpdateStartTimeNoErrors(t *testing.T, jobErrors []error, err error) {
	t.Helper()
	require.NoError(t, err)
	for _, err := range jobErrors {
		require.NoError(t, err)
	}
}
