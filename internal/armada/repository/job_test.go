package repository

import (
	"database/sql"
	"testing"
	"time"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/authorization"
	redis2 "github.com/G-Research/armada/internal/armada/repository/redis"
	sqlRepository "github.com/G-Research/armada/internal/armada/repository/sql"
	"github.com/G-Research/armada/internal/common/validation"
	"github.com/G-Research/armada/pkg/api"
)

func TestJobCanBeLeasedOnlyOnce(t *testing.T) {
	withRepository(func(r JobRepository) {

		job := addLeasedJob(t, r, "queue1", "cluster1")

		leasedAgain, e := r.TryLeaseJobs("cluster2", "queue1", []*api.Job{job})
		assert.Nil(t, e)
		assert.Equal(t, 0, len(leasedAgain))
	})
}

func TestJobLeaseCanBeRenewed(t *testing.T) {
	withRepository(func(r JobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		renewed, e := r.RenewLease("cluster1", []string{job.Id})
		assert.Nil(t, e)
		assert.Equal(t, 1, len(renewed))
		assert.Equal(t, job.Id, renewed[0])
	})
}

func TestJobLeaseExpiry(t *testing.T) {
	withRepository(func(r JobRepository) {
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
	withRepository(func(r JobRepository) {
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
	withRepository(func(r JobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		renewed, e := r.RenewLease("cluster2", []string{job.Id})
		assert.Nil(t, e)
		assert.Equal(t, 0, len(renewed))
	})
}

func TestRenewingNonExistentLease(t *testing.T) {
	withRepository(func(r JobRepository) {
		renewed, e := r.RenewLease("cluster2", []string{"missingJobId"})
		assert.Nil(t, e)
		assert.Equal(t, 0, len(renewed))
	})
}

func TestDeletingExpiredJobShouldDeleteJobFromQueue(t *testing.T) {
	withRepository(func(r JobRepository) {
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
	withRepository(func(r JobRepository) {
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
	withRepository(func(r JobRepository) {
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
	withRepository(func(r JobRepository) {
		job := addTestJob(t, r, "queue1")

		returned, e := r.ReturnLease("cluster2", job.Id)
		assert.Nil(t, e)
		assert.Nil(t, returned)
	})
}

func TestDeleteRunningJob(t *testing.T) {
	withRepository(func(r JobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")

		result := r.DeleteJobs([]*api.Job{job})
		err, deletionOccurred := result[job]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestDeleteQueuedJob(t *testing.T) {
	withRepository(func(r JobRepository) {
		job := addTestJob(t, r, "queue1")

		result := r.DeleteJobs([]*api.Job{job})
		err, deletionOccurred := result[job]
		assert.Nil(t, err)
		assert.True(t, deletionOccurred)
	})
}

func TestDeleteWithSomeMissingJobs(t *testing.T) {
	withRepository(func(r JobRepository) {
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
	withRepository(func(r JobRepository) {

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
	withRepository(func(r JobRepository) {
		addTestJob(t, r, "queue1")
		addLeasedJob(t, r, "queue1", "cluster1")
		addTestJob(t, r, "queue2")

		ids, e := r.GetActiveJobIds("queue1", "set1")
		assert.Nil(t, e)
		assert.Equal(t, 2, len(ids))
	})
}

func TestGetQueueActiveJobSets(t *testing.T) {
	withRepository(func(r JobRepository) {
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

func addLeasedJob(t *testing.T, r JobRepository, queue string, cluster string) *api.Job {
	job := addTestJob(t, r, queue)
	leased, e := r.TryLeaseJobs(cluster, queue, []*api.Job{job})
	assert.Nil(t, e)
	assert.Equal(t, 1, len(leased))
	assert.Equal(t, job.Id, leased[0].Id)
	return job
}

func addTestJob(t *testing.T, r JobRepository, queue string) *api.Job {
	cpu := resource.MustParse("1")
	memory := resource.MustParse("512Mi")

	jobs, e := validation.CreateJobs(&api.JobSubmitRequest{
		Queue:    queue,
		JobSetId: "set1",
		JobRequestItems: []*api.JobSubmitRequestItem{
			{
				Priority: 1,
				PodSpec: &v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
								Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
							},
						},
					},
				},
			},
		},
	}, authorization.NewStaticPrincipal("user", []string{}))
	assert.NoError(t, e)

	results, e := r.AddJobs(jobs)
	assert.Nil(t, e)
	for _, result := range results {
		assert.Empty(t, result.Error)
	}
	return jobs[0]
}

func withRepository(action func(r JobRepository)) {
	//withSqlRepository(action)
	withRedisRepository(action)
}

func withRedisRepository(action func(r JobRepository)) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := redis2.NewRedisJobRepository(client)
	action(repo)
}

func withSqlRepository(action func(r JobRepository)) {
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=psw dbname=postgres sslmode=disable")

	if err != nil {
		panic(err)
	}

	db.Exec("TRUNCATE TABLE job_queue")
	defer db.Exec("TRUNCATE TABLE job_queue")
	defer db.Close()

	repo := sqlRepository.NewJobRepository(db)
	action(repo)
}
