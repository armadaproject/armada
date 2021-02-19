package repository

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/lookout/repository/schema"
	"github.com/G-Research/armada/pkg/api"
)

func Test_RecordRunEvents(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		jobId := util.NewULID()

		err := jobStore.RecordJobPending(&api.JobPendingEvent{
			JobId:        jobId,
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: k8sId1,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        jobId,
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: k8sId1,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        jobId,
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: k8sId1,
		})
		assert.NoError(t, err)

		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE created IS NOT NULL AND started IS NOT NULL AND finished IS NOT NULL"))
	})
}

func Test_RunContainers(t *testing.T) {
	t.Run("no exit codes", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)

			err := jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        "job-1",
				Queue:        queue,
				Created:      time.Now(),
				KubernetesId: "a1",
			})
			assert.NoError(t, err)

			assert.Equal(t, 0, selectInt(t, db,
				"SELECT COUNT(*) FROM job_run_container"))
		})
	})

	t.Run("multiple containers", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)

			err := jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        "job-1",
				Queue:        queue,
				Created:      time.Now(),
				ExitCodes:    map[string]int32{"container-1": -1},
				KubernetesId: "a1",
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobFailed(&api.JobFailedEvent{
				JobId:        "job-1",
				Queue:        queue,
				Created:      time.Now(),
				ExitCodes:    map[string]int32{"container-1": 2, "container-2": 3},
				KubernetesId: "a1",
			})
			assert.NoError(t, err)

			assert.Equal(t, 3, selectInt(t, db,
				"SELECT exit_code FROM job_run_container WHERE run_id = 'a1' AND container_name = 'container-2'"))
			assert.Equal(t, 2, selectInt(t, db,
				"SELECT exit_code FROM job_run_container WHERE run_id = 'a1' AND container_name = 'container-1'"))
		})
	})
}

func Test_RecordNullNodeIfEmptyString(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		err := jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a1",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a2",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a3",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "a4",
		})
		assert.NoError(t, err)

		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a1'").Valid)
		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a2'").Valid)
		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a3'").Valid)
		assert.False(t, selectNullString(t, db,
			"SELECT node FROM job_run WHERE run_id = 'a4'").Valid)
	})
}

func Test_RecordLongError(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobRepo := NewSQLJobStore(db)

		err := jobRepo.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			Reason:       strings.Repeat("long error test ", 1000),
			KubernetesId: util.NewULID(),
		})
		assert.NoError(t, err)
	})
}

func Test_EmptyRunId(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		err := jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-1",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "",
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        "job-2",
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: "",
		})
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
			"SELECT state FROM job WHERE job.job_id = 'job-1'"))
		assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
			"SELECT state FROM job WHERE job.job_id = 'job-2'"))
	})
}

func Test_UnableToSchedule(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)

		err := jobStore.RecordJobUnableToSchedule(&api.JobUnableToScheduleEvent{
			JobId:        util.NewULID(),
			Queue:        queue,
			Created:      time.Now(),
			KubernetesId: util.NewULID(),
		})
		assert.NoError(t, err)

		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE job_run.unable_to_schedule = TRUE"))
		assert.Equal(t, 1, selectInt(t, db,
			"SELECT count(*) FROM job_run WHERE job_run.finished IS NOT NULL"))
	})
}

func Test_Queued(t *testing.T) {
	t.Run("queued", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)

			err := jobStore.RecordJob(&api.Job{
				Id:      util.NewULID(),
				Queue:   queue,
				Created: someTime,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobQueued], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("queued after pending", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(1 * time.Minute),
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJob(&api.Job{
				Id:      jobId,
				Queue:   "queue",
				Created: someTime,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Pending(t *testing.T) {
	t.Run("single node job, pending", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, pending after running, same pod", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(5 * time.Minute),
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, pending after running, different pod", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(5 * time.Minute),
				KubernetesId: k8sId2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("multi node job, pending after running", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Running(t *testing.T) {
	t.Run("single node job, running", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)

			err := jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        util.NewULID(),
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, running after pending in separate pod", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId2,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("single node job, running after success", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobSucceeded], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("multi node job, running after success", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime.Add(3 * time.Minute),
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobRunning(&api.JobRunningEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobRunning], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Succeeded(t *testing.T) {
	t.Run("multi node job, not all successes", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobPending(&api.JobPendingEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId3,
				PodNumber:    2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobPending], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})

	t.Run("multi node job, all successes", func(t *testing.T) {
		withDatabase(t, func(db *goqu.Database) {
			jobStore := NewSQLJobStore(db)
			jobId := util.NewULID()

			err := jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId1,
				PodNumber:    0,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId2,
				PodNumber:    1,
			})
			assert.NoError(t, err)

			err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
				JobId:        jobId,
				Queue:        queue,
				Created:      someTime,
				KubernetesId: k8sId3,
				PodNumber:    2,
			})
			assert.NoError(t, err)

			assert.Equal(t, JobStateToIntMap[JobSucceeded], selectInt(t, db,
				"SELECT state FROM job"))
		})
	})
}

func Test_Failed(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobId := util.NewULID()

		err := jobStore.RecordJobFailed(&api.JobFailedEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId1,
			PodNumber:    0,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId2,
			PodNumber:    1,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobSucceeded(&api.JobSucceededEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId3,
			PodNumber:    2,
		})
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobFailed], selectInt(t, db,
			"SELECT state FROM job"))
	})
}

func Test_Cancelled(t *testing.T) {
	withDatabase(t, func(db *goqu.Database) {
		jobStore := NewSQLJobStore(db)
		jobId := util.NewULID()

		err := jobStore.RecordJobPending(&api.JobPendingEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId1,
			PodNumber:    0,
		})
		assert.NoError(t, err)

		err = jobStore.RecordJobRunning(&api.JobRunningEvent{
			JobId:        jobId,
			Queue:        "queue",
			Created:      someTime,
			KubernetesId: k8sId2,
			PodNumber:    1,
		})
		assert.NoError(t, err)

		err = jobStore.MarkCancelled(&api.JobCancelledEvent{
			JobId:   jobId,
			Queue:   "queue",
			Created: someTime,
		})
		assert.NoError(t, err)

		assert.Equal(t, JobStateToIntMap[JobCancelled], selectInt(t, db,
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
