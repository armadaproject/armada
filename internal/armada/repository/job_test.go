package repository

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"testing"
	"time"
)

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

		e := r.ExpireLeases("queue1", deadline)
		assert.Nil(t, e)

		queued, e := r.PeekQueue("queue1", 10)
		assert.Equal(t, 1, len(queued), "Queue should have one job which expired")
		assert.Equal(t, job.Id, queued[0].Id)
	})
}

func TestEvenExpiredLeaseCanBeRenewed(t *testing.T) {
	withRepository(func(r *RedisJobRepository) {
		job := addLeasedJob(t, r, "queue1", "cluster1")
		deadline := time.Now()

		e := r.ExpireLeases("queue1", deadline)
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

func addLeasedJob(t *testing.T, r *RedisJobRepository, queue string, cluster string) *api.Job {
	job := addTestJob(t, r, queue)
	leased, e := r.TryLeaseJobs(cluster, queue, []*api.Job{job})
	assert.Nil(t, e)
	assert.Equal(t, 1, len(leased))
	assert.Equal(t, job.Id, leased[0].Id)
	return job
}

func addTestJob(t *testing.T, r *RedisJobRepository, queue string) *api.Job {
	job := r.CreateJob(&api.JobRequest{
		Queue:    queue,
		JobSetId: "set1",
		Priority: 1,
		PodSpec:  &v1.PodSpec{},
	})
	e := r.AddJob(job)
	assert.Nil(t, e)
	return job
}

func withRepository(action func(r *RedisJobRepository)) {

	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 10})
	defer client.FlushDB()
	defer client.Close()

	client.FlushDB()

	repo := NewRedisJobRepository(client)
	action(repo)
}
