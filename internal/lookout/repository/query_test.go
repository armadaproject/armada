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

func Test_QueueStats(t *testing.T) {
	withDatabase(t, func(db *sql.DB) {
		jobStore := NewSQLJobStore(db)

		job1 := createJob("queue")
		job2 := createJob("queue")
		job3 := createJob("queue")

		k8sId1 := util.NewULID()
		k8sId2 := util.NewULID()

		cluster := "cluster"
		node := "node"

		assert.NoError(t, jobStore.RecordJob(job1))
		assert.NoError(t, jobStore.RecordJob(job2))
		assert.NoError(t, jobStore.RecordJob(job3))

		assert.NoError(t, jobStore.RecordJobPending(createPendingEvent(cluster, k8sId1, job1)))
		assert.NoError(t, jobStore.RecordJobRunning(createRunningEvent(cluster, k8sId2, node, job1)))

		jobRepo := NewSQLJobRepository(db)

		stats, err := jobRepo.GetQueueStats()
		assert.NoError(t, err)
		assert.Equal(t, []*lookout.QueueInfo{{
			Queue:       "queue",
			JobsQueued:  1,
			JobsPending: 1,
			JobsRunning: 1,
		}}, stats)
	})
}

func createRunningEvent(cluster string, k8sId string, node string, job *api.Job) *api.JobRunningEvent {
	return &api.JobRunningEvent{
		JobId:        job.Id,
		JobSetId:     job.JobSetId,
		Queue:        job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
		NodeName:     node,
	}
}

func createPendingEvent(cluster string, k8sId string, job *api.Job) *api.JobPendingEvent {
	return &api.JobPendingEvent{
		JobId:        job.Id,
		JobSetId:     job.JobSetId,
		Queue:        job.Queue,
		Created:      time.Now(),
		ClusterId:    cluster,
		KubernetesId: k8sId,
	}
}

func createJob(queue string) *api.Job {
	return &api.Job{
		Id:          util.NewULID(),
		JobSetId:    "job-set",
		Queue:       queue,
		Namespace:   "nameSpace",
		Labels:      nil,
		Annotations: nil,
		Owner:       "user",
		Priority:    0,
		PodSpec:     &v1.PodSpec{},
		Created:     time.Now(),
	}
}
