package metrics

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
)

func TestCalculateRunningJobStats(t *testing.T) {
	withRepository(func(r *repository.RedisJobRepository) {
		sut := QueueInfoCollector{
			jobRepository: r,
		}
		queue1 := &api.Queue{Name: "queue1"}
		cluster1 := createClusterInfo("cluster1", "cpu")
		now := time.Now()

		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)

		clusterInfos := map[string]*api.ClusterSchedulingInfoReport{
			cluster1.ClusterId: cluster1,
		}

		runTimeMetrics, resourceMetrics := sut.calculateRunningJobStats([]*api.Queue{queue1}, clusterInfos)

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.NotNil(t, runTimeMetrics[queue1.Name])
		assert.NotNil(t, runTimeMetrics[queue1.Name][cluster1.Pool])
		assert.Equal(t, runTimeMetrics[queue1.Name][cluster1.Pool].GetCount(), uint64(2))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.NotNil(t, resourceMetrics[queue1.Name])
		assert.NotNil(t, resourceMetrics[queue1.Name][cluster1.Pool])
		assert.NotNil(t, resourceMetrics[queue1.Name][cluster1.Pool]["cpu"])
		assert.Equal(t, resourceMetrics[queue1.Name][cluster1.Pool]["cpu"].GetCount(), uint64(2))
	})
}

func TestCalculateRunningJobStats_WhenMultiCluster(t *testing.T) {
	withRepository(func(r *repository.RedisJobRepository) {
		sut := QueueInfoCollector{
			jobRepository: r,
		}
		queue1 := &api.Queue{Name: "queue1"}
		cluster1 := createClusterInfo("cluster1", "cpu")
		cluster2 := createClusterInfo("cluster2", "cpu")
		now := time.Now()

		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, r, queue1.Name, cluster2.ClusterId, now)

		clusterInfos := map[string]*api.ClusterSchedulingInfoReport{
			cluster1.ClusterId: cluster1,
			cluster2.ClusterId: cluster2,
		}

		runTimeMetrics, resourceMetrics := sut.calculateRunningJobStats([]*api.Queue{queue1}, clusterInfos)

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.Equal(t, runTimeMetrics[queue1.Name][cluster1.Pool].GetCount(), uint64(3))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.Equal(t, resourceMetrics[queue1.Name][cluster1.Pool]["cpu"].GetCount(), uint64(3))
	})
}

func TestCalculateRunningJobStats_WhenMultiPool(t *testing.T) {
	withRepository(func(r *repository.RedisJobRepository) {
		sut := QueueInfoCollector{
			jobRepository: r,
		}
		queue1 := &api.Queue{Name: "queue1"}
		cluster1 := createClusterInfo("cluster1", "cpu")
		cluster2 := createClusterInfo("cluster2", "gpu")
		now := time.Now()

		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)
		addRunningJob(t, r, queue1.Name, cluster2.ClusterId, now)

		clusterInfos := map[string]*api.ClusterSchedulingInfoReport{
			cluster1.ClusterId: cluster1,
			cluster2.ClusterId: cluster2,
		}

		runTimeMetrics, resourceMetrics := sut.calculateRunningJobStats([]*api.Queue{queue1}, clusterInfos)

		assert.Equal(t, len(runTimeMetrics), 1)
		assert.Equal(t, runTimeMetrics[queue1.Name][cluster1.Pool].GetCount(), uint64(2))
		assert.Equal(t, runTimeMetrics[queue1.Name][cluster2.Pool].GetCount(), uint64(1))
		assert.Equal(t, len(resourceMetrics), 1)
		assert.Equal(t, resourceMetrics[queue1.Name][cluster1.Pool]["cpu"].GetCount(), uint64(2))
		assert.Equal(t, resourceMetrics[queue1.Name][cluster2.Pool]["cpu"].GetCount(), uint64(1))
	})
}

func TestCalculateRunningJobStats_SkipsWhenJobOnInactivecluster(t *testing.T) {
	withRepository(func(r *repository.RedisJobRepository) {
		sut := QueueInfoCollector{
			jobRepository: r,
		}
		queue1 := &api.Queue{Name: "queue1"}
		cluster1 := createClusterInfo("cluster1", "cpu")
		now := time.Now()

		addRunningJob(t, r, queue1.Name, cluster1.ClusterId, now)

		clusterInfos := map[string]*api.ClusterSchedulingInfoReport{}

		runTimeMetrics, resourceMetrics := sut.calculateRunningJobStats([]*api.Queue{queue1}, clusterInfos)

		assert.Equal(t, len(runTimeMetrics), 0)
		assert.Equal(t, len(resourceMetrics), 0)
	})
}

func createClusterInfo(clusterId string, pool string) *api.ClusterSchedulingInfoReport {
	return &api.ClusterSchedulingInfoReport{
		ClusterId: clusterId,
		Pool:      pool,
	}
}

func addRunningJob(t *testing.T, r *repository.RedisJobRepository, queue string, cluster string, startTime time.Time) *api.Job {
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

func addTestJob(t *testing.T, r *repository.RedisJobRepository, queue string) *api.Job {
	cpu := resource.MustParse("1")
	memory := resource.MustParse("512Mi")

	return addTestJobWithRequirements(t, r, queue, "", v1.ResourceRequirements{
		Limits:   v1.ResourceList{"cpu": cpu, "memory": memory},
		Requests: v1.ResourceList{"cpu": cpu, "memory": memory},
	})
}

func addTestJobWithRequirements(t *testing.T, r *repository.RedisJobRepository, queue string, clientId string, requirements v1.ResourceRequirements) *api.Job {

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

func withRepository(action func(r *repository.RedisJobRepository)) {
	db, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: db.Addr()})
	repo := repository.NewRedisJobRepository(redisClient, configuration.DatabaseRetentionPolicy{JobRetentionDuration: time.Hour})
	action(repo)
}
