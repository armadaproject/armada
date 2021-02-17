package repository

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
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

		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job"))
		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE created IS NOT NULL AND started IS NOT NULL AND finished IS NOT NULL"))
		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run_container"))
		assert.Equal(t, JobStateToIntMap[JobCancelled], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func Test_RecordFinishedWithoutExitCodes(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		err := jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "42",
			ExitCodes:    nil,
			KubernetesId: "a1",
			NodeName:     node,
		})
		assert.NoError(t, err)

		assert.Equal(t, 0, selectInt(t, db,
			"SELECT COUNT(*) FROM job_run_container"))
	})
}

func Test_RecordMultipleContainers(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		err := jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "42",
			ExitCodes:    map[string]int32{"container-1": -1},
			KubernetesId: "a1",
			NodeName:     node,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "42",
			ExitCodes:    map[string]int32{"container-1": 2, "container-2": 3},
			KubernetesId: "a1",
			NodeName:     node,
		})
		assert.NoError(t, err)

		assert.Equal(t, 3, selectInt(t, db,
			"SELECT exit_code FROM job_run_container WHERE run_id = 'a1' AND container_name = 'container-2'"))
		assert.Equal(t, 2, selectInt(t, db,
			"SELECT exit_code FROM job_run_container WHERE run_id = 'a1' AND container_name = 'container-1'"))
	})
}

func Test_RecordNullNodeIfEmptyString(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		err := jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			KubernetesId: "a1",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "42",
			ExitCodes:    map[string]int32{"job": -1},
			KubernetesId: "a2",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			KubernetesId: "a3",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
			JobId:        "job-1",
			JobSetId:     "job-set",
			Queue:        queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			KubernetesId: "a4",
		})
		assert.NoError(t, err)

		assert.Equal(t, false, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a1'").Valid)
		assert.Equal(t, false, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a2'").Valid)
		assert.Equal(t, false, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a3'").Valid)
		assert.Equal(t, false, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a4'").Valid)
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

		received1, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{JobId: job_1.Id})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(received1))
		assert.Equal(t, string(JobFailed), received1[0].JobState)

		received2, err := jobRepo.GetJobs(ctx, &lookout.GetJobsRequest{JobId: job_1.Id})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(received2))
		assert.Equal(t, string(JobFailed), received2[0].JobState)
	})
}

func Test_UnableToSchedule(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

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
		err := jobStore.RecordJob(job)
		assert.NoError(t, err)

		err = jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
			JobId:        job.Id,
			JobSetId:     job.JobSetId,
			Queue:        job.Queue,
			Created:      time.Now(),
			ClusterId:    cluster,
			Reason:       "other error",
			KubernetesId: util.NewULID(),
			NodeName:     "node-2",
		})
		assert.NoError(t, err)

		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE job_run.unable_to_schedule = TRUE"))
		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE job_run.finished IS NOT NULL"))
	})
}

func Test_MostRecentJobStateIsRecorded(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		jobId := util.NewULID()

		err := jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime.Add(5 * time.Minute),
			ClusterId:    "cluster",
			KubernetesId: k8sId1,
			NodeName:     "node",
			PodNumber:    0,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobPending(&api.JobPendingEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime.Add(3),
			ClusterId:    "cluster",
			KubernetesId: k8sId1,
			PodNumber:    0,
		})
		assert.NoError(t, err)

		job := &api.Job{
			Id:          jobId,
			JobSetId:    "job-set",
			Queue:       "queue",
			Namespace:   "nameSpace",
			Labels:      nil,
			Annotations: nil,
			Owner:       "user",
			Priority:    0,
			PodSpec:     &v1.PodSpec{},
			Created:     someTime,
		}
		err = jobStore.RecordJob(job)
		assert.NoError(t, err)
		assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func Test_MultiNodeJobWithOneFailure(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		jobId := util.NewULID()

		err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime.Add(5 * time.Minute),
			ClusterId:    "cluster",
			KubernetesId: k8sId1,
			NodeName:     "node",
			PodNumber:    0,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime.Add(3 * time.Minute),
			ClusterId:    "cluster",
			KubernetesId: k8sId2,
			NodeName:     "node",
			PodNumber:    1,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        jobId,
			JobSetId:     "job-set",
			Queue:        "queue",
			Created:      someTime,
			ClusterId:    "cluster",
			KubernetesId: k8sId3,
			NodeName:     "node",
			PodNumber:    2,
		})
		assert.NoError(t, err)

		assert.NoError(t, err)
		assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func selectInt(t *testing.T, db *goqu.Database, query string) int {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var value int
	err = r.Scan(&value)
	assert.NoError(t, err)
	return value
}

func selectNullString(t *testing.T, db *goqu.Database, query string) sql.NullString {
	r, err := db.Query(query)
	assert.NoError(t, err)
	r.Next()
	var str sql.NullString
	err = r.Scan(&str)
	assert.NoError(t, err)
	return str
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
