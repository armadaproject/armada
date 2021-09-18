package repository

import (
	"math"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func TestJobDoubleSubmit(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		job2 := addTestJobWithClientId(t, r, "queue1", "my-job-1")
		assert.Equal(t, job1.Id, job2.Id)
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

		leasedAgain, e := r.TryLeaseJobs("cluster2", "queue1", []*api.Job{job})
		assert.Nil(t, e)
		assert.Equal(t, 0, len(leasedAgain))
	})
}

func TestJobLeaseCanBeRenewed(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		renewed, e := r.RenewLease("cluster1", []string{job.Id})
		assert.Nil(t, e)
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
		assert.Nil(t, e)

		queued, e := r.PeekQueue("queue1", 10)
		assert.Nil(t, e)
		assert.Equal(t, 1, len(queued), "Queue should have one job which expired")
		assert.Equal(t, job.Id, queued[0].Id)
	})
}

func TestEvenExpiredLeaseCanBeRenewed(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		deadline := time.Now()

		_, e := r.ExpireLeases("queue1", deadline)
		assert.Nil(t, e)

		renewed, e := r.RenewLease("cluster1", []string{job.Id})
		assert.Nil(t, e)
		assert.Equal(t, 1, len(renewed))
		assert.Equal(t, job.Id, renewed[0])
	})
}

func TestRenewingLeaseFailsForJobAssignedToDifferentCluster(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		renewed, e := r.RenewLease("cluster2", []string{job.Id})
		assert.Nil(t, e)
		assert.Equal(t, 0, len(renewed))
	})
}

func TestRenewingNonExistentLease(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		renewed, e := r.RenewLease("cluster2", []string{"missingJobId"})
		assert.Nil(t, e)
		assert.Equal(t, 0, len(renewed))
	})
}

func TestDeletingExpiredJobShouldDeleteJobFromQueue(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		deadline := time.Now()

		_, e := r.ExpireLeases("queue1", deadline)
		assert.Nil(t, e)

		deletionResult := r.DeleteJobs([]*api.Job{job})

		err, deleted := deletionResult[job]

		assert.Equal(t, 1, len(deletionResult))
		assert.True(t, deleted)
		assert.Nil(t, err)

		queue, e := r.PeekQueue("queue1", 100)
		assert.Nil(t, e)
		assert.Equal(t, 0, len(queue))
	})
}

func TestReturnLeaseShouldReturnJobToQueue(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		returned, e := r.ReturnLease("cluster1", job.Id)
		assert.Nil(t, e)
		assert.NotNil(t, returned)

		queue, e := r.PeekQueue("queue1", 100)
		assert.Nil(t, e)
		assert.Equal(t, 1, len(queue))
		assert.Equal(t, job.Id, returned.Id)
	})
}

func TestReturnLeaseFromDifferentClusterIsNoop(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		returned, e := r.ReturnLease("cluster2", job.Id)
		assert.Nil(t, e)
		assert.Nil(t, returned)

		queue, e := r.PeekQueue("queue1", 100)
		assert.Nil(t, e)
		assert.Equal(t, 0, len(queue))
	})
}

func TestReturnLeaseForJobInQueueIsNoop(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addTestJob(t, r, "queue1")

		returned, e := r.ReturnLease("cluster2", job.Id)
		assert.Nil(t, e)
		assert.Nil(t, returned)
	})
}

func TestDeleteRunningJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		result := r.DeleteJobs([]*api.Job{job})
		err, deletionOccurred := result[job]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestDeleteQueuedJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addTestJob(t, r, "queue1")

		result := r.DeleteJobs([]*api.Job{job})
		err, deletionOccurred := result[job]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestDeleteWithSomeMissingJobs(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		missingJob := &api.Job{Id: "jobId"}
		runningJob := addLeasedJob(t, r, "queue1", "cluster1")
		result := r.DeleteJobs([]*api.Job{missingJob, runningJob})

		err, deletionOccurred := result[missingJob]
		assert.Nil(t, err)
		assert.False(t, deletionOccurred)

		err, deletionOccurred = result[runningJob]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestReturnLeaseForDeletedJobShouldKeepJobDeleted(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {

		job := addLeasedJob(t, r, "cancel-test-queue", "cluster")

		result := r.DeleteJobs([]*api.Job{job})
		assert.Nil(t, result[job])

		returned, err := r.ReturnLease("cluster", job.Id)
		assert.Nil(t, returned)
		assert.Nil(t, err)

		q, err := r.PeekQueue("cancel-test-queue", 100)
		assert.Nil(t, err)
		assert.Empty(t, q)
	})
}

func TestGetActiveJobIds(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		addTestJob(t, r, "queue1")
		addLeasedJob(t, r, "queue1", "cluster1")
		addTestJob(t, r, "queue2")

		ids, e := r.GetActiveJobIds("queue1", "set1")
		assert.Nil(t, e)
		assert.Equal(t, 2, len(ids))
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
		assert.Nil(t, e)
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
		err := r.UpdateStartTime(leasedJob.Id, "cluster1", startTime)
		assert.Nil(t, err)
	})
}

func TestUpdateStartTime_UsesEarlierTime(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		startTimePlusOneHour := time.Now().Add(time.Hour)
		err := r.UpdateStartTime(leasedJob.Id, "cluster1", startTime)
		assert.Nil(t, err)
		err = r.UpdateStartTime(leasedJob.Id, "cluster1", startTimePlusOneHour)
		assert.Nil(t, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob.Id})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(runInfos))
		assert.Equal(t, runInfos[leasedJob.Id].StartTime.UTC(), startTime.UTC())
		assert.NotEqual(t, runInfos[leasedJob.Id].StartTime.UTC(), startTimePlusOneHour.UTC())
	})
}

func TestUpdateStartTime_NonExistentJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		startTime := time.Now()
		err := r.UpdateStartTime("NonExistent", "cluster1", startTime)
		assert.NotNil(t, err)
		assert.Equal(t, err.Error(), JobNotFound)
	})
}

// Saving/reading the start time shouldn't adjust the actual time it happened
// i.e If the start time happened "now" but in a different time zone, the difference between the start time and now should be ~0 seconds
func TestSaveAndRetrieveStartTime_HandlesDifferentTimeZones(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		loc, err := time.LoadLocation("Asia/Shanghai")
		assert.Nil(t, err)
		now := time.Now().UTC()
		leasedJob := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := now.In(loc)
		err = r.UpdateStartTime(leasedJob.Id, "cluster1", startTime)
		assert.Nil(t, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob.Id})
		assert.Nil(t, err)
		diff := runInfos[leasedJob.Id].StartTime.Sub(now).Seconds()
		diff = math.Abs(diff)
		assert.Equal(t, 1, len(runInfos))
		assert.True(t, diff < float64(1))
	})
}

func TestGetJobRunInfos(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")
		leasedJob2 := addLeasedJob(t, r, "queue1", "cluster2")

		startTime := time.Now()
		err := r.UpdateStartTime(leasedJob1.Id, "cluster1", startTime)
		assert.Nil(t, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob1.Id, leasedJob2.Id})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(runInfos))
		assert.Equal(t, runInfos[leasedJob1.Id].StartTime.UTC(), startTime.UTC())
		assert.Equal(t, runInfos[leasedJob1.Id].CurrentClusterId, "cluster1")
	})
}

func TestGetJobRunInfos_HandlesJobWithoutClusterAssociation(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job1 := addTestJob(t, r, "queue1")
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		err := r.UpdateStartTime(job1.Id, "cluster1", startTime)
		assert.Nil(t, err)
		err = r.UpdateStartTime(leasedJob1.Id, "cluster1", startTime)
		assert.Nil(t, err)

		runInfos, err := r.GetJobRunInfos([]string{job1.Id, leasedJob1.Id})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(runInfos))
		assert.Equal(t, runInfos[leasedJob1.Id].StartTime.UTC(), startTime.UTC())
	})
}

func TestGetJobRunInfos_ReturnStartTimeForCurrentAssociatedCluster(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		leasedJob1 := addLeasedJob(t, r, "queue1", "cluster1")

		startTime := time.Now()
		plusOneHour := startTime.Add(time.Hour)
		err := r.UpdateStartTime(leasedJob1.Id, "cluster2", startTime)
		assert.Nil(t, err)
		err = r.UpdateStartTime(leasedJob1.Id, "cluster1", plusOneHour)
		assert.Nil(t, err)

		runInfos, err := r.GetJobRunInfos([]string{leasedJob1.Id})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(runInfos))
		assert.Equal(t, runInfos[leasedJob1.Id].StartTime.UTC(), plusOneHour.UTC())
	})
}

func TestGetQueueActiveJobSets(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		addTestJob(t, r, "queue1")
		addLeasedJob(t, r, "queue1", "cluster1")
		addTestJob(t, r, "queue2")

		infos, e := r.GetQueueActiveJobSets("queue1")
		assert.Nil(t, e)
		assert.Equal(t, []*api.JobSetInfo{{
			Name:       "set1",
			QueuedJobs: 1,
			LeasedJobs: 1,
		}}, infos)
	})
}

func TestCreateJob_ApplyDefaultLimits(t *testing.T) {
	defaultLimits := common.ComputeResources{
		"cpu":               resource.MustParse("1"),
		"memory":            resource.MustParse("512Mi"),
		"ephemeral-storage": resource.MustParse("4Gi")}

	withRepositoryUsingJobDefaults(defaultLimits, []v1.Toleration{}, func(r *RedisJobRepository) {
		testCases := map[*v1.ResourceList]v1.ResourceList{
			nil: {
				"cpu":               resource.MustParse("1"),
				"memory":            resource.MustParse("512Mi"),
				"ephemeral-storage": resource.MustParse("4Gi"),
			},
			{
				"cpu": resource.MustParse("2"),
			}: {
				"cpu":               resource.MustParse("2"),
				"memory":            resource.MustParse("512Mi"),
				"ephemeral-storage": resource.MustParse("4Gi"),
			},
			{
				"nvidia/gpu": resource.MustParse("3"),
			}: {
				"cpu":               resource.MustParse("1"),
				"memory":            resource.MustParse("512Mi"),
				"ephemeral-storage": resource.MustParse("4Gi"),
				"nvidia/gpu":        resource.MustParse("3"),
			},
		}

		for requirements, expected := range testCases {
			resources := v1.ResourceRequirements{}
			if requirements != nil {
				resources.Requests = *requirements
				resources.Limits = *requirements
			}
			job := addTestJobWithRequirements(t, r, "test", resources)
			assert.Equal(t, expected, job.PodSpec.Containers[0].Resources.Limits)
			assert.Equal(t, expected, job.PodSpec.Containers[0].Resources.Requests)
		}
	})
}

func TestCreateJob_ApplyDefaultTolerations(t *testing.T) {
	defaultToleration := v1.Toleration{
		Key:      "default",
		Operator: v1.TolerationOpEqual,
		Value:    "true",
		Effect:   v1.TaintEffectNoSchedule,
	}

	alternateToleration := v1.Toleration{
		Key:      "alternate",
		Operator: v1.TolerationOpEqual,
		Value:    "true",
		Effect:   v1.TaintEffectNoSchedule,
	}

	withRepositoryUsingJobDefaults(nil, []v1.Toleration{defaultToleration}, func(r *RedisJobRepository) {
		job := addTestJobWithTolerations(t, r, "test", []v1.Toleration{})
		assert.Equal(t, []v1.Toleration{defaultToleration}, job.PodSpec.Tolerations)

		job = addTestJobWithTolerations(t, r, "test", []v1.Toleration{defaultToleration})
		assert.Equal(t, []v1.Toleration{defaultToleration}, job.PodSpec.Tolerations)

		job = addTestJobWithTolerations(t, r, "test", []v1.Toleration{alternateToleration})
		assert.Equal(t, []v1.Toleration{alternateToleration, defaultToleration}, job.PodSpec.Tolerations)
	})

	withRepositoryUsingJobDefaults(nil, []v1.Toleration{}, func(r *RedisJobRepository) {
		job := addTestJobWithTolerations(t, r, "test", []v1.Toleration{})
		assert.Equal(t, []v1.Toleration{}, job.PodSpec.Tolerations)

		job = addTestJobWithTolerations(t, r, "test", []v1.Toleration{alternateToleration})
		assert.Equal(t, []v1.Toleration{alternateToleration}, job.PodSpec.Tolerations)
	})
}

func TestNumberOfRetryAttemptsIsZeroForNonExistentJob(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		retries, err := r.GetNumberOfRetryAttempts("nonexistent-job-id")

		assert.Nil(t, err)
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

		assert.Nil(t, err)

		retries, err := r.GetNumberOfRetryAttempts("nonexistent-job-id")

		assert.Nil(t, err)
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

		assert.Nil(t, err)
		assert.Equal(t, expectedRetries, retries)
	})
}

func TestRetriesOfDeletedJobShouldBeZero(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		testJob := addLeasedJob(t, r, "some-queue", "cluster-1")

		for i := 0; i < 11; i++ {
			err := r.AddRetryAttempt(testJob.Id)
			assert.Nil(t, err)
		}

		r.DeleteJobs([]*api.Job{testJob})

		retries, err := r.GetNumberOfRetryAttempts(testJob.Id)

		assert.Nil(t, err)
		assert.Zero(t, retries)
	})
}

func TestIterateQueueJobs(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		addedJobs := []*api.Job{}
		for i := 0; i < 10; i++ {
			addedJobs = append(addedJobs, addTestJob(t, r, "q1"))
		}

		iteratedJobs := []*api.Job{}
		err := r.IterateQueueJobs("q1", func(j *api.Job) {
			iteratedJobs = append(iteratedJobs, j)
		})

		assert.Nil(t, err)
		for i, j := range addedJobs {
			assert.Equal(t, j.Id, iteratedJobs[i].Id)
		}
	})
}

func addLeasedJob(t *testing.T, r *RedisJobRepository, queue string, cluster string) *api.Job {
	job := addTestJob(t, r, queue)
	leased, e := r.TryLeaseJobs(cluster, queue, []*api.Job{job})
	assert.Nil(t, e)
	assert.Equal(t, 1, len(leased))
	assert.Equal(t, job.Id, leased[0].Id)
	return job
}

func addTestJob(t *testing.T, r *RedisJobRepository, queue string) *api.Job {
	return addTestJobWithClientId(t, r, queue, "")
}

func addTestJobWithClientId(t *testing.T, r *RedisJobRepository, queue string, clientId string) *api.Job {
	cpu := resource.MustParse("1")
	memory := resource.MustParse("512Mi")

	return addTestJobInner(t, r, queue, clientId, 1, v1.ResourceRequirements{
		Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
		Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
	}, []v1.Toleration{})
}

func addTestJobWithTolerations(t *testing.T, r *RedisJobRepository, queue string, tolerations []v1.Toleration) *api.Job {
	cpu := resource.MustParse("1")
	memory := resource.MustParse("512Mi")

	return addTestJobInner(t, r, queue, "", 1, v1.ResourceRequirements{
		Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
		Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
	}, tolerations)
}

func addTestJobWithRequirements(t *testing.T, r *RedisJobRepository, queue string, requirements v1.ResourceRequirements) *api.Job {
	return addTestJobInner(t, r, queue, "", 1, requirements, []v1.Toleration{})
}

func addTestJobInner(t *testing.T, r *RedisJobRepository, queue string, clientId string, priority float64, requirements v1.ResourceRequirements, tolerations []v1.Toleration) *api.Job {

	jobs, e := r.CreateJobs(&api.JobSubmitRequest{
		Queue:    queue,
		JobSetId: "set1",
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Priority: priority,
				ClientId: clientId,
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: requirements,
						},
					},
					Tolerations: tolerations,
				},
			},
		},
	}, "user", []string{})
	assert.NoError(t, e)

	results, e := r.AddJobs(jobs)
	assert.Nil(t, e)
	for i, result := range results {
		assert.Empty(t, result.Error)
		jobs[i].Id = result.JobId // Update job ids for tests to be able to test the duplicate detection
	}
	return jobs[0]
}

func withRepository(action func(r *RedisJobRepository)) {
	withRepositoryUsingJobDefaults(nil, []v1.Toleration{}, action)
}

func withRepositoryUsingJobDefaults(jobDefaultLimit common.ComputeResources, jobDefaultTolerations []v1.Toleration, action func(r *RedisJobRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewRedisJobRepository(client, jobDefaultLimit, jobDefaultTolerations)
	action(repo)
}
