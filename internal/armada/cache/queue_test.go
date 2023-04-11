package cache

import (
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client/queue"
)

func TestCalculateRunningJobStats(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})

		clusterInfo := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		addRunningJob(t, queueCache.jobRepository, createJobWithResource(queue1.Name, "1", "1000"), clusterInfo.ClusterId, now.Add(-time.Minute*30))
		addRunningJob(t, queueCache.jobRepository, createJobWithResource(queue1.Name, "2", "2000"), clusterInfo.ClusterId, now.Add(-time.Minute*20))
		addRunningJob(t, queueCache.jobRepository, createJobWithResource(queue1.Name, "3", "3000"), clusterInfo.ClusterId, now.Add(-time.Minute*10))

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)

		assert.Equal(t, len(result), 1)
		runTimeMetrics := result[0].Durations
		assert.Equal(t, runTimeMetrics.GetCount(), uint64(9999))
		assert.Equal(t, runTimeMetrics.GetMin(), float64(60*10))
		assert.Equal(t, runTimeMetrics.GetMedian(), float64(60*20))
		assert.Equal(t, runTimeMetrics.GetMax(), float64(60*30))
		assert.Equal(t, runTimeMetrics.GetSum(), float64(60*60))

		resourceMetrics := result[0].Resources
		assert.NotNil(t, resourceMetrics["cpu"])
		assert.Equal(t, resourceMetrics["cpu"].GetCount(), uint64(3))
		assert.Equal(t, resourceMetrics["cpu"].GetMin(), float64(1))
		assert.Equal(t, resourceMetrics["cpu"].GetMedian(), float64(2))
		assert.Equal(t, resourceMetrics["cpu"].GetMax(), float64(3))
		assert.Equal(t, resourceMetrics["cpu"].GetSum(), float64(6))

		assert.NotNil(t, resourceMetrics["memory"])
		assert.Equal(t, resourceMetrics["memory"].GetCount(), uint64(3))
		assert.Equal(t, resourceMetrics["memory"].GetMin(), float64(1000))
		assert.Equal(t, resourceMetrics["memory"].GetMedian(), float64(2000))
		assert.Equal(t, resourceMetrics["memory"].GetMax(), float64(3000))
		assert.Equal(t, resourceMetrics["memory"].GetSum(), float64(6000))
	})
}

func TestCalculateRunningJobStats_WhenMultiCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		cluster2 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "cpu")

		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster2.ClusterId, now)

		queueCache.Refresh()

		result := queueCache.GetRunningJobMetrics(queue1.Name)
		assert.Equal(t, len(result), 1)

		runTimeMetrics := result[0].Durations
		resourceMetrics := result[0].Resources
		assert.Equal(t, runTimeMetrics.GetCount(), uint64(3))
		assert.Equal(t, resourceMetrics["cpu"].GetCount(), uint64(3))
	})
}

func TestCalculateRunningJobStats_WhenMultiPool(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		cluster2 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "gpu")

		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster1.ClusterId, now)
		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster2.ClusterId, now)

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)
		assert.Equal(t, len(result), 2)

		sort.Slice(result, func(i, j int) bool {
			return result[i].Pool < result[j].Pool
		})

		assert.Equal(t, result[0].Durations.GetCount(), uint64(2))
		assert.Equal(t, result[1].Durations.GetCount(), uint64(1))
		assert.Equal(t, result[0].Resources["cpu"].GetCount(), uint64(2))
		assert.Equal(t, result[1].Resources["cpu"].GetCount(), uint64(1))
	})
}

func TestCalculateRunningJobStats_SkipsWhenJobOnInactiveCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")

		cluster1 := addInactiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addRunningJob(t, queueCache.jobRepository, createJob(queue1.Name), cluster1.ClusterId, now)

		queueCache.Refresh()
		result := queueCache.GetRunningJobMetrics(queue1.Name)
		assert.Equal(t, len(result), 0)
	})
}

func TestGetQueuedJobMetrics(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		clusterInfo := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		addJob(t, queueCache.jobRepository, createJobWithResourceAndStartTime(queue1.Name, "1", "1000000000", now.Add(-time.Minute*10)))
		addJob(t, queueCache.jobRepository, createJobWithResourceAndStartTime(queue1.Name, "2", "2000000000", now.Add(-time.Minute*20)))
		addJob(t, queueCache.jobRepository, createJobWithResourceAndStartTime(queue1.Name, "3", "3000000000", now.Add(-time.Minute*30)))

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		assert.Equal(t, len(result), 1)

		runTimeMetrics := result[0].Durations
		resourceMetrics := result[0].Resources

		assert.Equal(t, runTimeMetrics.GetCount(), uint64(3))
		assert.Equal(t, runTimeMetrics.GetMin(), float64(60*10))
		assert.Equal(t, runTimeMetrics.GetMedian(), float64(60*20))
		assert.Equal(t, runTimeMetrics.GetMax(), float64(60*30))
		assert.Equal(t, runTimeMetrics.GetSum(), float64(60*60))

		assert.NotNil(t, resourceMetrics[clusterInfo.Pool])
		assert.NotNil(t, resourceMetrics["cpu"])
		assert.Equal(t, resourceMetrics["cpu"].GetCount(), uint64(3))
		assert.Equal(t, resourceMetrics["cpu"].GetMin(), float64(1))
		assert.Equal(t, resourceMetrics["cpu"].GetMedian(), float64(2))
		assert.Equal(t, resourceMetrics["cpu"].GetMax(), float64(3))
		assert.Equal(t, resourceMetrics["cpu"].GetSum(), float64(6))

		assert.NotNil(t, resourceMetrics["memory"])
		assert.Equal(t, resourceMetrics["memory"].GetCount(), uint64(3))
		assert.Equal(t, resourceMetrics["memory"].GetMin(), float64(1000000000))
		assert.Equal(t, resourceMetrics["memory"].GetMedian(), float64(2000000000))
		assert.Equal(t, resourceMetrics["memory"].GetMax(), float64(3000000000))
		assert.Equal(t, resourceMetrics["memory"].GetSum(), float64(6000000000))
	})
}

func TestGetQueuedJobMetrics_CountedOnce_WhenMultiCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "cpu")

		addJob(t, queueCache.jobRepository, createJob(queue1.Name))

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		assert.Equal(t, len(result), 1)
		assert.Equal(t, cluster1.Pool, result[0].Pool)
		assert.Equal(t, result[0].Durations.GetCount(), uint64(1))
		assert.Equal(t, result[0].Resources["cpu"].GetCount(), uint64(1))
	})
}

func TestGetQueuedJobMetrics_CountedForEachMatchingPool_WhenMultiPool(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		cluster2 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster2", "cpu2")
		addJob(t, queueCache.jobRepository, createJob(queue1.Name))

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)
		assert.Equal(t, len(result), 2)

		sort.Slice(result, func(i, j int) bool {
			return result[i].Pool < result[j].Pool
		})

		assert.Equal(t, cluster1.Pool, result[0].Pool)
		assert.Equal(t, cluster2.Pool, result[1].Pool)

		assert.Equal(t, result[0].Durations.GetCount(), uint64(1))
		assert.Equal(t, result[1].Durations.GetCount(), uint64(1))
		assert.Equal(t, result[0].Resources["cpu"].GetCount(), uint64(1))
		assert.Equal(t, result[1].Resources["cpu"].GetCount(), uint64(1))
	})
}

func TestGetQueuedJobMetrics_NotCounted_WhenJobCannotScheduleOntoCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")
		cluster1 := addActiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addJob(t, queueCache.jobRepository, createJob(queue1.Name))

		// Make minimum job size masssive, so no jobs are schedulable
		cluster1.MinimumJobSize = map[string]resource.Quantity{
			"cpu":    resource.MustParse("1000"),
			"memory": resource.MustParse("1000Gi"),
		}
		err := queueCache.schedulingInfoRepository.UpdateClusterSchedulingInfo(cluster1)
		assert.NoError(t, err)

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)

		assert.Equal(t, len(result), 0)
	})
}

func TestGetQueuedJobMetrics_SkipsWhenJobOnInactiveCluster(t *testing.T) {
	withRepository(func(r *redis.Client) {
		now := time.Now()
		queueCache := createQueueCache(r, &util.DummyClock{T: now})
		queue1 := addQueue(t, queueCache.queueRepository, "queue1")

		addInactiveCluster(t, queueCache.schedulingInfoRepository, "cluster1", "cpu")
		addJob(t, queueCache.jobRepository, createJob(queue1.Name))

		queueCache.Refresh()
		result := queueCache.GetQueuedJobMetrics(queue1.Name)

		assert.Equal(t, len(result), 0)
	})
}

func createQueueCache(redisClient redis.UniversalClient, clock util.Clock) *QueueCache {
	jobRepo := repository.NewRedisJobRepository(redisClient)
	queueRepo := repository.NewRedisQueueRepository(redisClient)
	schedulingInfoRepo := repository.NewRedisSchedulingInfoRepository(redisClient)

	return NewQueueCache(clock, queueRepo, jobRepo, schedulingInfoRepo)
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

func addRunningJob(t *testing.T, r repository.JobRepository, job *api.Job, cluster string, startTime time.Time) *api.Job {
	job = addJob(t, r, job)
	leased, e := r.TryLeaseJobs(cluster, map[string][]string{job.Queue: {job.Id}})
	assert.NoError(t, e)
	assert.Equal(t, 1, len(leased))
	assert.Equal(t, job.Id, leased[job.Queue][0])
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

func addJob(t *testing.T, r repository.JobRepository, job *api.Job) *api.Job {
	results, e := r.AddJobs([]*api.Job{job})
	assert.Nil(t, e)
	for _, result := range results {
		assert.Empty(t, result.Error)
	}
	return job
}

func createJob(queue string) *api.Job {
	return createJobWithResourceAndStartTime(queue, "1", "1Gi", time.Now())
}

func createJobWithResource(queue string, cpu string, memory string) *api.Job {
	return createJobWithResourceAndStartTime(queue, cpu, memory, time.Now())
}

func createJobWithResourceAndStartTime(queue string, cpu string, memory string, createdTime time.Time) *api.Job {
	return &api.Job{
		Id:       util.NewULID(),
		Queue:    queue,
		JobSetId: "set1",
		PodSpec: &v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Limits:   v1.ResourceList{"cpu": resource.MustParse(cpu), "memory": resource.MustParse(memory)},
						Requests: v1.ResourceList{"cpu": resource.MustParse(cpu), "memory": resource.MustParse(memory)},
					},
				},
			},
		},
		Created:                  createdTime,
		Owner:                    "user",
		QueueOwnershipUserGroups: []string{},
	}
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
