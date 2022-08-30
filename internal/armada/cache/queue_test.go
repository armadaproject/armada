package cache

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
)

func TestCalculateRunningJobStats(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		now := time.Now()

		clusterInfo := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		addRunningJob(t, queueCache.jobRepository, queue1.Name, clusterInfo.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, queue1.Name, clusterInfo.ClusterId, now)

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.NotNil(t, runTimeMetrics)
		assert.NotNil(t, runTimeMetrics[clusterInfo.Pool])
		assert.Equal(t, runTimeMetrics[clusterInfo.Pool].GetCount(), uint64(2))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.NotNil(t, resourceMetrics)
		assert.NotNil(t, resourceMetrics[clusterInfo.Pool])
		assert.NotNil(t, resourceMetrics[clusterInfo.Pool]["cpu"])
		assert.Equal(t, resourceMetrics[clusterInfo.Pool]["cpu"].GetCount(), uint64(2))
	})
}

func TestCalculateRunningJobStats_WhenMultiCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		cluster2 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "cpu")
		now := time.Now()

		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster2.ClusterId, now)

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.Equal(t, runTimeMetrics[cluster1.Pool].GetCount(), uint64(3))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.Equal(t, resourceMetrics[cluster1.Pool]["cpu"].GetCount(), uint64(3))
	})
}

func TestCalculateRunningJobStats_WhenMultiPool(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		cluster2 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "gpu")
		now := time.Now()

		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster2.ClusterId, now)

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 2)
		assert.Equal(t, runTimeMetrics[cluster1.Pool].GetCount(), uint64(2))
		assert.Equal(t, runTimeMetrics[cluster2.Pool].GetCount(), uint64(1))
		assert.Equal(t, len(resourceMetrics), 2)
		assert.Equal(t, resourceMetrics[cluster1.Pool]["cpu"].GetCount(), uint64(2))
		assert.Equal(t, resourceMetrics[cluster2.Pool]["cpu"].GetCount(), uint64(1))
	})
}

func TestCalculateRunningJobStats_SkipsWhenJobOnInactiveCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		now := time.Now()

		cluster1 := addInactiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addRunningJob(t, queueCache.jobRepository, queue1.Name, cluster1.ClusterId, now)

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 0)
		assert.Equal(t, len(resourceMetrics), 0)
	})
}

func TestGetQueuedJobMetrics(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		clusterInfo := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		addTestJob(t, queueCache.jobRepository, queue1.Name)
		addTestJob(t, queueCache.jobRepository, queue1.Name)

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.NotNil(t, runTimeMetrics)
		assert.NotNil(t, runTimeMetrics[clusterInfo.Pool])
		assert.Equal(t, runTimeMetrics[clusterInfo.Pool].GetCount(), uint64(2))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.NotNil(t, resourceMetrics)
		assert.NotNil(t, resourceMetrics[clusterInfo.Pool])
		assert.NotNil(t, resourceMetrics[clusterInfo.Pool]["cpu"])
		assert.Equal(t, resourceMetrics[clusterInfo.Pool]["cpu"].GetCount(), uint64(2))
	})
}

func TestGetQueuedJobMetrics_CountedOnce_WhenMultiCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "cpu")

		addTestJob(t, queueCache.jobRepository, queue1.Name)

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.Equal(t, runTimeMetrics[cluster1.Pool].GetCount(), uint64(1))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.Equal(t, resourceMetrics[cluster1.Pool]["cpu"].GetCount(), uint64(1))
	})
}

func TestGetQueuedJobMetrics_CountedForEachMatchingPool_WhenMultiPool(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		cluster2 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "cpu2")
		addTestJob(t, queueCache.jobRepository, queue1.Name)

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 2)
		assert.Equal(t, runTimeMetrics[cluster1.Pool].GetCount(), uint64(1))
		assert.Equal(t, runTimeMetrics[cluster2.Pool].GetCount(), uint64(1))
		assert.Equal(t, len(resourceMetrics), 2)
		assert.Equal(t, resourceMetrics[cluster1.Pool]["cpu"].GetCount(), uint64(1))
		assert.Equal(t, resourceMetrics[cluster2.Pool]["cpu"].GetCount(), uint64(1))
	})
}

func TestGetQueuedJobMetrics_NotCounted_WhenJobCannotScheduleOntoCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addTestJob(t, queueCache.jobRepository, queue1.Name)

		// Make minimum job size masssive, so no jobs are schedulable
		cluster1.MinimumJobSize = map[string]resource.Quantity{
			"cpu":    resource.MustParse("1000"),
			"memory": resource.MustParse("1000Gi"),
		}
		err := queueCache.schedulingInfoRepository.UpdateClusterSchedulingInfo(cluster1)
		assert.NoError(t, err)

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 0)
		assert.Equal(t, len(resourceMetrics), 0)
	})
}

func TestGetQueuedJobMetrics_SkipsWhenJobOnInactiveCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		queueCache := createQueueCache(r)
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")

		addInactiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addTestJob(t, queueCache.jobRepository, queue1.Name)

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		runTimeMetrics := result.Durations
		resourceMetrics := result.Resources

		assert.Equal(t, len(runTimeMetrics), 0)
		assert.Equal(t, len(resourceMetrics), 0)
	})
}

func createQueueCache(redisClient redis.UniversalClient) *QueueCache {
	jobRepo := repository.NewRedisJobRepository(redisClient, configuration.DatabaseRetentionPolicy{JobRetentionDuration: time.Hour})
	queueRepo := repository.NewRedisQueueRepository(redisClient)
	schedulingInfoRepo := repository.NewRedisSchedulingInfoRepository(redisClient)

	return NewQueueCache(queueRepo, jobRepo, schedulingInfoRepo)
}

func addInactiveCluster(t *testing.T, r repository.SchedulingInfoRepository, clusterId string, pool string) *api.ClusterSchedulingInfoReport {
	return addCluster(t, r, clusterId, pool, time.Now().Add(-time.Hour))
}

func addActiveCluster(t *testing.T, r repository.SchedulingInfoRepository, clusterId string, pool string) *api.ClusterSchedulingInfoReport {
	return addCluster(t, r, clusterId, pool, time.Now())
}

func addCluster(t *testing.T, r repository.SchedulingInfoRepository, clusterId string, pool string, reportTime time.Time) *api.ClusterSchedulingInfoReport {
	clusterInfo := &api.ClusterSchedulingInfoReport{
		ClusterId:  clusterId,
		Pool:       pool,
		ReportTime: reportTime,
		NodeTypes: []*api.NodeType{
			{
				AllocatableResources: map[string]resource.Quantity{
					"cpu":    resource.MustParse("1000"),
					"memory": resource.MustParse("512Gi"),
				},
			},
		},
		MinimumJobSize: map[string]resource.Quantity{
			"cpu":    resource.MustParse("0.1"),
			"memory": resource.MustParse("1Mi"),
		},
	}

	err := r.UpdateClusterSchedulingInfo(clusterInfo)
	assert.NoError(t, err)
	return clusterInfo
}

func addQueue(t *testing.T, r repository.QueueRepository, queueName string) *queue.Queue {
	q := queue.Queue{Name: queueName, PriorityFactor: 1.0}

	err := r.CreateQueue(q)
	assert.NoError(t, err)
	return &q
}

func addRunningJob(t *testing.T, r repository.JobRepository, queue string, cluster string, startTime time.Time) *api.Job {
	job := addTestJob(t, r, queue)
	leased, e := r.TryLeaseJobs(cluster, queue, []*api.Job{job})
	assert.NoError(t, e)
	assert.Equal(t, 1, len(leased))
	assert.Equal(t, job.Id, leased[0].Id)
	jobErrors, e := r.UpdateStartTime([]*repository.JobStartInfo{{
		JobId:     job.Id,
		ClusterId: cluster,
		StartTime: startTime,
	}})
	assert.NoError(t, e)
	assert.Len(t, jobErrors, 1)
	assert.NoError(t, jobErrors[0])
	return job
}

func addTestJob(t *testing.T, r repository.JobRepository, queue string) *api.Job {
	cpu := resource.MustParse("1")
	memory := resource.MustParse("512Mi")

	return addTestJobWithRequirements(t, r, queue, "", v1.ResourceRequirements{
		Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
		Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
	})
}

func addTestJobWithRequirements(t *testing.T, r repository.JobRepository, queue string, clientId string, requirements v1.ResourceRequirements) *api.Job {
	jobs := []*api.Job{
		{
			Id:       util.NewULID(),
			ClientId: clientId,
			Queue:    queue,
			JobSetId: "set1",
			PodSpec: &v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: requirements,
					},
				},
			},
			Created:                  time.Now(),
			Owner:                    "user",
			QueueOwnershipUserGroups: []string{},
		},
	}

	results, e := r.AddJobs(jobs)
	assert.Nil(t, e)
	for _, result := range results {
		assert.Empty(t, result.Error)
	}
	return jobs[0]
}

func withRepository(action func(r *redis.Client)) {
	db, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: db.Addr()})
	action(redisClient)
}
