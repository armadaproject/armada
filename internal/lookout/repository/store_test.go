package repository

import (
	"database/sql"
	"github.com/G-Research/armada/pkg/api/lookout"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/G-Research/armada/pkg/api"
)

func Test_RecordEvents(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobStore(db)

		job := &api.Job{
			Id:          util.NewULID(),
			JobSetId:    "job-set",
			Queue:       "queue",
			Namespace:   "nameSpace",
			Labels:      nil,
			Annotations: nil,
			Owner:       "user",
			Priority:    0,
			PodSpec:     &v1.PodSpec{},
			Created:     time.Now(),
		}

		k8sId := util.NewULID()
		cluster := "cluster"
		node := "node"

		err := jobRepo.RecordJob(job)
		assert.NoError(t, err)

		err = jobRepo.RecordJobPending(&api.JobPendingEvent{
			JobId:        job.Id,
			JobSetId:     job.JobSetId,
			Queue:        job.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			KubernetesId: k8sId,
		})
		assert.NoError(t, err)

		err = jobRepo.RecordJobRunning(&api.JobRunningEvent{
			JobId:        job.Id,
			JobSetId:     job.JobSetId,
			Queue:        job.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			KubernetesId: k8sId,
			NodeName:     node,
		})
		assert.NoError(t, err)

		err = jobRepo.RecordJobFailed(&api.JobFailedEvent{
			JobId:        job.Id,
			JobSetId:     job.JobSetId,
			Queue:        job.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "42",
			ExitCodes:    map[string]int32{"job": -1},
			KubernetesId: k8sId,
			NodeName:     node,
		})
		assert.NoError(t, err)

		err = jobRepo.MarkCancelled(&api.JobCancelledEvent{
			JobId:    job.Id,
			JobSetId: job.JobSetId,
			Queue:    job.Queue,
			Created:  time.Now(),
		})
		assert.NoError(t, err)

		assert.Equal(t, 1, count(t, db,
			"SELECT count(*) FROM job"))
		assert.Equal(t, 1, count(t, db,
			"SELECT count(*) FROM job_run WHERE created IS NOT NULL AND started IS NOT NULL AND finished IS NOT NULL"))
		assert.Equal(t, 1, count(t, db,
			"SELECT count(*) FROM job_run_container"))

	})
}

func Test_RecordLongError(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobStore(db)

		job := &api.Job{
			Id:          util.NewULID(),
			JobSetId:    "job-set",
			Queue:       "queue",
			Namespace:   "nameSpace",
			Labels:      nil,
			Annotations: nil,
			Owner:       "user",
			Priority:    0,
			PodSpec:     &v1.PodSpec{},
			Created:     time.Now(),
		}

		err := jobRepo.RecordJob(job)
		assert.NoError(t, err)

		err = jobRepo.RecordJobFailed(&api.JobFailedEvent{
			JobId:        job.Id,
			JobSetId:     job.JobSetId,
			Queue:        job.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       strings.Repeat("long error test ", 1000),
			ExitCodes:    nil,
			KubernetesId: util.NewULID(),
			NodeName:     "node",
		})

		assert.NoError(t, err)
	})
}

func Test_EmptyRunId(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobRepo := NewSQLJobRepository(db, &DefaultClock{})

		job_1 := &api.Job{
			Id:          util.NewULID(),
			JobSetId:    "job-set",
			Queue:       "queue",
			Namespace:   "nameSpace",
			Labels:      nil,
			Annotations: nil,
			Owner:       "user",
			Priority:    0,
			PodSpec:     &v1.PodSpec{},
			Created:     time.Now(),
		}
		err := jobStore.RecordJob(job_1)
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        job_1.Id,
			JobSetId:     job_1.JobSetId,
			Queue:        job_1.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "error",
			ExitCodes:    nil,
			KubernetesId: "",
			NodeName:     "node",
		})
		assert.NoError(t, err)

		job_2 := &api.Job{
			Id:          util.NewULID(),
			JobSetId:    "job-set-2",
			Queue:       "queue-2",
			Namespace:   "nameSpace-2",
			Labels:      nil,
			Annotations: nil,
			Owner:       "user",
			Priority:    0,
			PodSpec:     &v1.PodSpec{},
			Created:     time.Now(),
		}
		err = jobStore.RecordJob(job_2)
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        job_2.Id,
			JobSetId:     job_2.JobSetId,
			Queue:        job_2.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "other error",
			ExitCodes:    nil,
			KubernetesId: "",
			NodeName:     "node-2",
		})
		assert.NoError(t, err)

		received1, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{ JobId: job_1.Id})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(received1))
		assert.Equal(t, JobStates.Failed, received1[0].JobState)

		received2, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{ JobId: job_1.Id})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(received2))
		assert.Equal(t, JobStates.Failed, received2[0].JobState)
	})
}

func count(t *testing.T, db *goqu.Database, query string) int {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var count int
	r.Scan(&count)
	return count
}

func withDatabase(t *testing.T, action func(*goqu.Database)) {
	dbName := "test_" + util.NewULID()
	connectionString := "host=localhost port=5432 user=postgres password=psw sslmode=disable"
	db, err := sql.Open("postgres", connectionString)
	defer db.Close()

	assert.Nil(t, err)

	_, err = db.Exec("CREATE DATABASE " + dbName)
	assert.Nil(t, err)

	testDb, err := sql.Open("postgres", connectionString+" dbname="+dbName)
	assert.Nil(t, err)

	defer func() {
		err = testDb.Close()
		assert.Nil(t, err)
		// disconnect all db user before cleanup
		_, err = db.Exec(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			 FROM pg_stat_activity WHERE pg_stat_activity.datname = '` + dbName + `';`)
		assert.Nil(t, err)
		_, err = db.Exec("DROP DATABASE " + dbName)
		assert.Nil(t, err)
	}()

	err = schema.UpdateDatabase(testDb)
	assert.Nil(t, err)

	action(goqu.New("postgres", testDb))
}
